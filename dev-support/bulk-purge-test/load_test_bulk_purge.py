#!/usr/bin/env python3
"""
load_test_bulk_purge.py — 1M-asset load test for Bulk Purge

Supports two purge modes:
  - connection   — POST /bulk-purge/connection?connectionQualifiedName=...
  - qualifiedName — POST /bulk-purge/qualifiedName?prefix=...

Both modes create the same assets (Connection + Tables + optional Processes).
The purge mode controls which API endpoint is exercised.

Streaming/fire-and-forget design: builds each batch on the fly, submits it,
counts success/fail, discards. Memory usage is O(batch_size * workers), not
O(total_assets).

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA clusters)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Prerequisites:
  - Atlas running with Connection/Table/Process types loaded
  - pip install requests pyyaml

Usage:
  # Local dev — 1K assets, connection mode (default)
  python3 load_test_bulk_purge.py --count 1000 --workers 10

  # Staging — 1K, qualifiedName mode
  python3 load_test_bulk_purge.py --tenant staging --count 1000 --purge-mode qualifiedName

  # Staging — full 1M load test
  python3 load_test_bulk_purge.py --tenant staging --count 1000000 --workers 50

  # Re-run purge only (creation already done)
  python3 load_test_bulk_purge.py --tenant staging --skip-create \
      --conn-qn "default/bulk-purge-test/1740000000"

  # qualifiedName mode with custom prefix (purges a subtree)
  python3 load_test_bulk_purge.py --tenant staging --count 1000 \
      --purge-mode qualifiedName --qn-prefix "default/bulk-purge-test/1740000000/table-"
"""

import argparse
import json
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------
ES_SYNC_TIMEOUT = 3600       # 1 hour — 1M entities take time to index
PURGE_TIMEOUT = 7200          # 2 hours — large-scale purge
PURGE_POLL_INTERVAL = 10      # seconds between purge status polls
API_REQUEST_TIMEOUT = 60      # per-request timeout
FIXED_ES_WAIT = 120           # let ES catch up before polling
LINEAGE_REPAIR_TIMEOUT = 60   # seconds to wait for lineage repair


# ---------------------------------------------------------------------------
# TokenManager — OAuth2 client-credentials with auto-refresh
# ---------------------------------------------------------------------------

class TokenManager:
    """OAuth2 client-credentials with auto-refresh."""

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0
        self._lock = threading.Lock()

    def get_token(self):
        """Return a valid token, refreshing if necessary (thread-safe)."""
        if not self.access_token or time.time() >= self.token_expiry:
            with self._lock:
                # Double-check after acquiring lock
                if not self.access_token or time.time() >= self.token_expiry:
                    self._refresh_token()
        return self.access_token

    def force_refresh(self):
        """Force a token refresh (e.g., after a 401)."""
        with self._lock:
            self._refresh_token()

    def _refresh_token(self):
        """Fetch a new token using client credentials."""
        payload = {
            "client_id": self.client_id,
            "grant_type": "client_credentials",
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(self.token_url, headers=headers, data=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        expires_in = data.get("expires_in", 60)
        self.token_expiry = int(time.time()) + expires_in - 30
        log(f"Token refreshed, valid for {expires_in}s")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class PhaseError(Exception):
    """Raised when a test phase fails."""
    pass


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {msg}", flush=True)


def should_log_progress(current, total, interval_pct=10):
    """Return True if we should log at this point (every interval_pct %)."""
    if total <= 0:
        return False
    if current >= total:
        return True
    step = max(1, total * interval_pct // 100)
    return current % step == 0


def _build_headers(token_manager, basic_auth):
    """Build request headers with appropriate auth."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token_manager:
        headers["Authorization"] = f"Bearer {token_manager.get_token()}"
    elif basic_auth:
        # requests.auth.HTTPBasicAuth is used at call site instead
        pass
    return headers


def _do_request(method, url, token_manager, basic_auth, json_data=None,
                params=None, timeout=API_REQUEST_TIMEOUT, expected_status=None):
    """Execute an HTTP request with optional 401 retry for OAuth."""
    headers = _build_headers(token_manager, basic_auth)
    auth = basic_auth if basic_auth else None

    resp = requests.request(
        method, url, headers=headers, auth=auth, json=json_data,
        params=params, timeout=timeout
    )

    # 401 retry: force-refresh token and try once more
    if resp.status_code == 401 and token_manager:
        token_manager.force_refresh()
        headers = _build_headers(token_manager, basic_auth)
        resp = requests.request(
            method, url, headers=headers, auth=auth, json=json_data,
            params=params, timeout=timeout
        )

    if expected_status and resp.status_code != expected_status:
        raise PhaseError(f"{method} {url} returned {resp.status_code}: {resp.text[:500]}")
    return resp


def api_get(api_base, path, token_manager, basic_auth, params=None, expected_status=200):
    url = f"{api_base}{path}"
    return _do_request("GET", url, token_manager, basic_auth,
                       params=params, expected_status=expected_status)


def api_post(api_base, path, token_manager, basic_auth, data=None,
             params=None, expected_status=200):
    url = f"{api_base}{path}"
    return _do_request("POST", url, token_manager, basic_auth,
                       json_data=data, params=params, expected_status=expected_status)


def api_delete(api_base, path, token_manager, basic_auth, expected_status=200):
    url = f"{api_base}{path}"
    return _do_request("DELETE", url, token_manager, basic_auth,
                       expected_status=expected_status)


def search_by_connection_qn(api_base, conn_qn, token_manager, basic_auth, size=1):
    """Count ACTIVE entities belonging to a connection via ES."""
    # Strategy 1: connectionQualifiedName term (production)
    payload = {
        "dsl": {
            "from": 0, "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"connectionQualifiedName": conn_qn}},
                        {"term": {"__state": "ACTIVE"}}
                    ]
                }
            }
        }
    }
    resp = api_post(api_base, "/search/indexsearch", token_manager, basic_auth,
                    data=payload, expected_status=None)
    if resp.status_code == 200:
        count = resp.json().get("approximateCount", 0)
        if count > 0:
            return count

    # Strategy 2: __qualifiedNameHierarchy prefix (local dev)
    payload = {
        "dsl": {
            "from": 0, "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"prefix": {"__qualifiedNameHierarchy": conn_qn}},
                        {"term": {"__state": "ACTIVE"}}
                    ]
                }
            }
        }
    }
    resp = api_post(api_base, "/search/indexsearch", token_manager, basic_auth,
                    data=payload, expected_status=None)
    if resp.status_code == 200:
        return resp.json().get("approximateCount", 0)
    return -1


def search_by_qn_prefix(api_base, qn_prefix, token_manager, basic_auth, size=1):
    """Count ACTIVE entities matching a qualifiedName prefix via ES."""
    payload = {
        "dsl": {
            "from": 0, "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"prefix": {"__qualifiedNameHierarchy": qn_prefix}},
                        {"term": {"__state": "ACTIVE"}}
                    ]
                }
            }
        }
    }
    resp = api_post(api_base, "/search/indexsearch", token_manager, basic_auth,
                    data=payload, expected_status=None)
    if resp.status_code == 200:
        return resp.json().get("approximateCount", 0)
    return -1


def wait_for_es_count(api_base, conn_qn, token_manager, basic_auth,
                      expected_min, timeout=ES_SYNC_TIMEOUT, label=""):
    """Poll ES until entity count >= expected_min or timeout."""
    log(f"Waiting for ES count >= {expected_min:,} for {conn_qn} {label}...")
    start = time.time()
    last_count = 0
    last_log_time = 0
    while time.time() - start < timeout:
        count = search_by_connection_qn(api_base, conn_qn, token_manager, basic_auth)
        if count >= expected_min:
            log(f"  ES sync: {count:,}/{expected_min:,} (100%) — done in {time.time()-start:.1f}s")
            return count
        elapsed = time.time() - start
        pct = max(0, count) / expected_min * 100 if expected_min > 0 else 0
        # Log on count change or every 30s
        if count != last_count or (elapsed - last_log_time) >= 30:
            log(f"  ES sync: {max(0, count):,}/{expected_min:,} ({pct:.0f}%) — {elapsed:.0f}s elapsed")
            last_log_time = elapsed
        last_count = count
        time.sleep(5)
    raise PhaseError(
        f"ES sync timeout after {timeout}s: count={last_count}, expected>={expected_min} {label}"
    )


# ---------------------------------------------------------------------------
# Phase 0: Preflight
# ---------------------------------------------------------------------------

def phase_0_preflight(cfg):
    log("=== Phase 0: Preflight ===")
    try:
        resp = _do_request(
            "GET", f"{cfg['atlas_url']}/api/atlas/admin/status",
            cfg['token_manager'], cfg['basic_auth'], timeout=10
        )
        if resp.status_code != 200:
            raise PhaseError(f"Atlas status endpoint returned {resp.status_code}")
        status = resp.json().get("Status", "UNKNOWN")
        log(f"Atlas status: {status}")
        if status != "ACTIVE":
            raise PhaseError(f"Atlas is not ACTIVE (status={status})")
    except requests.ConnectionError:
        raise PhaseError(f"Cannot connect to Atlas at {cfg['atlas_url']}")

    log(f"Configuration:")
    log(f"  URL:           {cfg['atlas_url']}")
    log(f"  Auth mode:     {'OAuth2' if cfg['token_manager'] else 'Basic'}")
    log(f"  Asset count:   {cfg['count']:,}")
    log(f"  Workers:       {cfg['workers']}")
    log(f"  Batch size:    {cfg['batch_size']}")
    log(f"  Purge mode:    {cfg['purge_mode']}")
    if cfg['purge_mode'] == 'qualifiedName':
        log(f"  QN prefix:     {cfg['qn_prefix']}")
    log(f"  Connection QN: {cfg['conn_qn']}")
    log(f"  Skip create:   {cfg['skip_create']}")
    log(f"  Skip lineage:  {cfg['skip_lineage']}")


# ---------------------------------------------------------------------------
# Phase 1: Check Types
# ---------------------------------------------------------------------------

def phase_1_check_types(cfg):
    log("=== Phase 1: Ensure Types Exist ===")
    resp = api_get(cfg['api_base'], "/types/typedefs/headers",
                   cfg['token_manager'], cfg['basic_auth'])
    type_names = {h.get("name") for h in resp.json()}
    required = ["Connection", "Table", "Process"]
    missing = [t for t in required if t not in type_names]
    if missing:
        raise PhaseError(f"Required types not found: {missing}. Load minimal.json first.")
    log(f"All required types present: {required}")


# ---------------------------------------------------------------------------
# Phase 2: Create Connection
# ---------------------------------------------------------------------------

def phase_2_create_connection(cfg):
    log("=== Phase 2: Create Test Connection ===")
    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": cfg['conn_qn'],
                "name": "bulk-purge-load-test",
                "connectorName": "test",
                "category": "warehouse"
            }
        }
    }
    resp = api_post(cfg['api_base'], "/entity", cfg['token_manager'], cfg['basic_auth'],
                    data=payload)
    body = resp.json()
    mutated = body.get("mutatedEntities", {})
    creates = mutated.get("CREATE", [])
    if not creates:
        updates = mutated.get("UPDATE", [])
        if updates:
            guid = updates[0]["guid"]
            log(f"Connection already existed, GUID: {guid}")
            return guid
        raise PhaseError(f"No entity created/updated: {json.dumps(body)[:500]}")
    guid = creates[0]["guid"]
    log(f"Connection created: GUID={guid}, QN={cfg['conn_qn']}")
    return guid


# ---------------------------------------------------------------------------
# Phase 3: Create Assets — streaming, no GUID storage
# ---------------------------------------------------------------------------

def batch_generator(conn_qn, total, batch_size):
    """Yield batches one at a time — O(batch_size) memory per batch."""
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        yield [
            {
                "typeName": "Table",
                "attributes": {
                    "qualifiedName": f"{conn_qn}/table-{i}",
                    "name": f"test-table-{i}",
                    "connectionQualifiedName": conn_qn,
                }
            }
            for i in range(start, end)
        ]


def phase_3_create_assets(cfg):
    log(f"=== Phase 3: Create {cfg['count']:,} Table Assets (Streaming) ===")

    total = cfg['count']
    batch_size = cfg['batch_size']
    workers = cfg['workers']
    total_batches = (total + batch_size - 1) // batch_size

    log(f"Submitting {total_batches:,} batches ({batch_size} entities each) "
        f"with {workers} workers")

    # Semaphore caps in-flight batches to workers*2 for backpressure
    semaphore = threading.Semaphore(workers * 2)
    entities_created = 0
    batches_failed = 0
    lock = threading.Lock()
    start_time = time.time()

    max_retries = 3

    def create_batch(entities, batch_idx):
        nonlocal entities_created, batches_failed
        semaphore.acquire()
        try:
            payload = {"entities": entities}
            batch_count = len(entities)
            for attempt in range(max_retries):
                try:
                    resp = api_post(cfg['api_base'], "/entity/bulk",
                                    cfg['token_manager'], cfg['basic_auth'],
                                    data=payload, expected_status=None)
                    if resp.status_code == 200:
                        with lock:
                            entities_created += batch_count
                        return
                    # Retry on 5xx (server overload) and 429 (rate limit)
                    if resp.status_code >= 500 or resp.status_code == 429:
                        if attempt < max_retries - 1:
                            wait = 2 ** attempt * 5  # 5s, 10s, 20s
                            with lock:
                                log(f"  Batch {batch_idx} got HTTP {resp.status_code}, "
                                    f"retry {attempt+1}/{max_retries} in {wait}s", "WARN")
                            time.sleep(wait)
                            continue
                    # Non-retryable error
                    with lock:
                        batches_failed += 1
                        if batches_failed <= 5:
                            log(f"  Batch {batch_idx} failed (HTTP {resp.status_code}): "
                                f"{resp.text[:200]}", "WARN")
                    return
                except (requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError) as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt * 5
                        with lock:
                            log(f"  Batch {batch_idx} {type(e).__name__}, "
                                f"retry {attempt+1}/{max_retries} in {wait}s", "WARN")
                        time.sleep(wait)
                        continue
                    with lock:
                        batches_failed += 1
                        if batches_failed <= 5:
                            log(f"  Batch {batch_idx} failed after {max_retries} retries: {e}", "ERROR")
                    return
            # All retries exhausted
            with lock:
                batches_failed += 1
                if batches_failed <= 5:
                    log(f"  Batch {batch_idx} failed after {max_retries} retries", "ERROR")
        finally:
            semaphore.release()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for batch_idx, batch in enumerate(batch_generator(cfg['conn_qn'], total, batch_size)):
            f = executor.submit(create_batch, batch, batch_idx)
            futures[f] = batch_idx

        batches_done = 0
        for future in as_completed(futures):
            future.result()  # propagate any unhandled exceptions
            batches_done += 1
            if should_log_progress(batches_done, total_batches, interval_pct=5):
                elapsed = time.time() - start_time
                with lock:
                    created = entities_created
                    failed = batches_failed
                rate = created / elapsed if elapsed > 0 else 0
                pct = created / total * 100 if total > 0 else 0
                log(f"  Creating: {created:,}/{total:,} ({pct:.0f}%) | "
                    f"batches: {batches_done:,}/{total_batches:,} | "
                    f"failed={failed} | {rate:,.0f} ent/s")

    elapsed = time.time() - start_time
    rate = entities_created / elapsed if elapsed > 0 else 0
    log(f"Asset creation complete: {entities_created:,} entities in {elapsed:.1f}s "
        f"({rate:,.0f} ent/s, {batches_failed} failed batches)")

    if batches_failed > total_batches * 0.1:
        raise PhaseError(f"Too many batch failures: {batches_failed}/{total_batches}")

    return {
        "entities_created": entities_created,
        "batches_failed": batches_failed,
        "duration": elapsed,
        "rate": rate,
    }


# ---------------------------------------------------------------------------
# Phase 4: Create Cross-Connection Lineage
# ---------------------------------------------------------------------------

def phase_4_create_lineage(cfg):
    log("=== Phase 4: Create Cross-Connection Lineage ===")

    ext_conn_qn = cfg['ext_conn_qn']

    # 4a: Create external connection
    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": ext_conn_qn,
                "name": "bulk-purge-ext-connection",
                "connectorName": "test-external",
                "category": "warehouse"
            }
        }
    }
    resp = api_post(cfg['api_base'], "/entity", cfg['token_manager'], cfg['basic_auth'],
                    data=payload)
    body = resp.json()
    mutated = body.get("mutatedEntities", {})
    creates = mutated.get("CREATE", mutated.get("UPDATE", []))
    if not creates:
        raise PhaseError("Failed to create external connection")
    ext_conn_guid = creates[0]["guid"]
    log(f"External connection created: GUID={ext_conn_guid}")

    # 4b: Create external tables
    lineage_count = cfg['lineage_count']
    ext_table_count = min(lineage_count, 100)  # cap external tables at 100
    ext_tables = []
    for i in range(ext_table_count):
        ext_tables.append({
            "typeName": "Table",
            "attributes": {
                "qualifiedName": f"{ext_conn_qn}/ext-table-{i}",
                "name": f"ext-table-{i}",
                "connectionQualifiedName": ext_conn_qn
            }
        })
    resp = api_post(cfg['api_base'], "/entity/bulk", cfg['token_manager'], cfg['basic_auth'],
                    data={"entities": ext_tables})
    body = resp.json()
    ext_asset_guids = []
    for action in ("CREATE", "UPDATE"):
        for e in body.get("mutatedEntities", {}).get(action, []):
            ext_asset_guids.append(e["guid"])
    log(f"External tables created: {len(ext_asset_guids)} entities")

    # Wait for external tables to appear in ES
    wait_for_es_count(cfg['api_base'], ext_conn_qn, cfg['token_manager'], cfg['basic_auth'],
                      expected_min=ext_table_count, timeout=120, label="(external tables)")

    # 4c: Create Process entities linking test tables -> external tables
    log(f"Creating {lineage_count:,} lineage Process entities...")
    process_count = 0
    process_failed = 0
    process_batch_size = 20  # submit processes in small batches for speed

    for batch_start in range(0, lineage_count, process_batch_size):
        batch_end = min(batch_start + process_batch_size, lineage_count)
        process_entities = []
        for i in range(batch_start, batch_end):
            ext_idx = i % ext_table_count
            process_entities.append({
                "typeName": "Process",
                "attributes": {
                    "qualifiedName": f"{cfg['conn_qn']}/lineage-process-{i}",
                    "name": f"lineage-process-{i}",
                    "connectionQualifiedName": cfg['conn_qn'],
                    "inputs": [{"typeName": "Table", "uniqueAttributes": {
                        "qualifiedName": f"{cfg['conn_qn']}/table-{i}"
                    }}],
                    "outputs": [{"typeName": "Table", "uniqueAttributes": {
                        "qualifiedName": f"{ext_conn_qn}/ext-table-{ext_idx}"
                    }}]
                }
            })

        resp = api_post(cfg['api_base'], "/entity/bulk", cfg['token_manager'], cfg['basic_auth'],
                        data={"entities": process_entities}, expected_status=None)
        if resp.status_code == 200:
            body = resp.json()
            for action in ("CREATE", "UPDATE"):
                process_count += len(body.get("mutatedEntities", {}).get(action, []))
        else:
            process_failed += 1
            if process_failed <= 3:
                log(f"  Process batch {batch_start}-{batch_end} failed "
                    f"(HTTP {resp.status_code}): {resp.text[:200]}", "WARN")

        if should_log_progress(batch_end, lineage_count, interval_pct=25):
            log(f"  Processes: {batch_end}/{lineage_count:,} submitted")

    log(f"Lineage creation complete: {process_count} processes created, "
        f"{process_failed} batches failed")

    return {
        "ext_conn_guid": ext_conn_guid,
        "ext_asset_guids": ext_asset_guids,
        "process_count": process_count,
    }


# ---------------------------------------------------------------------------
# Phase 5: Wait for ES Sync
# ---------------------------------------------------------------------------

def phase_5_wait_es_sync(cfg, creation_result):
    log("=== Phase 5: Wait for ES Sync ===")
    # Use actual created count (may be less than requested if some batches failed)
    expected = creation_result['entities_created'] if creation_result else cfg['count']
    log(f"Expecting {expected:,} entities (requested {cfg['count']:,})")
    log(f"Waiting {FIXED_ES_WAIT}s fixed delay for ES to catch up...")
    time.sleep(FIXED_ES_WAIT)
    wait_for_es_count(cfg['api_base'], cfg['conn_qn'], cfg['token_manager'], cfg['basic_auth'],
                      expected_min=expected, timeout=ES_SYNC_TIMEOUT,
                      label="(test connection assets)")


# ---------------------------------------------------------------------------
# Phase 6: Verify Creation
# ---------------------------------------------------------------------------

def phase_6_verify_creation(cfg, creation_result, lineage_result):
    log("=== Phase 6: Verify Creation ===")

    expected = creation_result['entities_created'] if creation_result else cfg['count']
    count = search_by_connection_qn(cfg['api_base'], cfg['conn_qn'],
                                    cfg['token_manager'], cfg['basic_auth'])
    log(f"Test connection ES count: {count:,} (expected: {expected:,})")
    if count < expected:
        raise PhaseError(f"Expected >= {expected:,} entities, got {count:,}")

    if lineage_result:
        ext_count = search_by_connection_qn(cfg['api_base'], cfg['ext_conn_qn'],
                                            cfg['token_manager'], cfg['basic_auth'])
        log(f"External connection ES count: {ext_count:,}")
        expected_ext = len(lineage_result.get('ext_asset_guids', []))
        if ext_count < expected_ext:
            raise PhaseError(f"Expected >= {expected_ext} external entities, got {ext_count}")

    # Verify connection entity itself exists
    resp = api_get(cfg['api_base'], f"/entity/guid/{cfg['conn_guid']}",
                   cfg['token_manager'], cfg['basic_auth'])
    conn_entity = resp.json().get("entity", {})
    log(f"Test connection entity: typeName={conn_entity.get('typeName')}, "
        f"status={conn_entity.get('status')}")

    log("Creation verification passed")


# ---------------------------------------------------------------------------
# Phase 7: Trigger Bulk Purge
# ---------------------------------------------------------------------------

def phase_7_trigger_bulk_purge(cfg):
    log("=== Phase 7: Trigger Bulk Purge ===")

    purge_mode = cfg['purge_mode']
    api_start = time.time()

    if purge_mode == 'qualifiedName':
        qn_prefix = cfg['qn_prefix']
        purge_params = {"prefix": qn_prefix}
        if cfg.get('server_workers', 0) > 0:
            purge_params["workerCount"] = cfg['server_workers']
            log(f"  Server-side worker override: {cfg['server_workers']}")
        log(f"  Using qualifiedName mode with prefix: {qn_prefix}")
        resp = api_post(cfg['api_base'], "/bulk-purge/qualifiedName",
                        cfg['token_manager'], cfg['basic_auth'],
                        params=purge_params)
    else:
        purge_params = {"connectionQualifiedName": cfg['conn_qn']}
        if cfg.get('server_workers', 0) > 0:
            purge_params["workerCount"] = cfg['server_workers']
            log(f"  Server-side worker override: {cfg['server_workers']}")
        resp = api_post(cfg['api_base'], "/bulk-purge/connection",
                        cfg['token_manager'], cfg['basic_auth'],
                        params=purge_params)

    api_latency_ms = (time.time() - api_start) * 1000
    body = resp.json()
    request_id = body.get("requestId")
    purge_key = body.get("purgeKey")
    resp_purge_mode = body.get("purgeMode")
    message = body.get("message", "")

    log(f"Purge submitted: requestId={request_id}, purgeKey={purge_key}, mode={resp_purge_mode}")
    log(f"  API latency: {api_latency_ms:.0f}ms")
    if message:
        log(f"  Message: {message}")

    if not request_id:
        raise PhaseError(f"No requestId in purge response: {json.dumps(body)}")

    # Poll for completion
    start_time = time.time()
    last_status = None
    peak_rate = 0
    poll_count = 0
    poll_latencies_ms = []

    while time.time() - start_time < PURGE_TIMEOUT:
        time.sleep(PURGE_POLL_INTERVAL)
        poll_count += 1
        elapsed = time.time() - start_time

        poll_start = time.time()
        resp = api_get(cfg['api_base'], "/bulk-purge/status",
                       cfg['token_manager'], cfg['basic_auth'],
                       params={"requestId": request_id}, expected_status=None)
        poll_latency = (time.time() - poll_start) * 1000
        poll_latencies_ms.append(poll_latency)

        if resp.status_code == 404:
            log(f"  [Poll #{poll_count}] 404 — retrying... ({elapsed:.0f}s)")
            continue
        if resp.status_code != 200:
            log(f"  [Poll #{poll_count}] HTTP {resp.status_code} — {resp.text[:200]}", "WARN")
            continue

        status = resp.json()
        current_status = status.get("status", "UNKNOWN")
        deleted_count = status.get("deletedCount", 0)
        total_discovered = status.get("totalDiscovered", 0)
        failed_count = status.get("failedCount", 0)
        completed_batches = status.get("completedBatches", 0)
        worker_count = status.get("workerCount", "?")
        batch_size = status.get("batchSize", "?")

        rate = deleted_count / elapsed if elapsed > 0 and deleted_count > 0 else 0
        peak_rate = max(peak_rate, rate)
        pct = (deleted_count / total_discovered * 100) if total_discovered > 0 else 0
        current_phase = status.get("currentPhase", "")

        phase_str = f" | phase={current_phase}" if current_phase else ""
        log(f"  [Poll #{poll_count}] status={current_status} | "
            f"deleted={deleted_count:,}/{total_discovered:,} ({pct:.1f}%) | "
            f"failed={failed_count} | batches={completed_batches} | "
            f"workers={worker_count} | batchSize={batch_size} | "
            f"rate={rate:,.0f} ent/s | poll={poll_latency:.0f}ms{phase_str}")

        last_status = current_status

        if current_status == "COMPLETED":
            purge_duration = time.time() - start_time
            log(f"Purge COMPLETED in {purge_duration:.1f}s ({poll_count} status polls)")
            return {
                "requestId": request_id,
                "duration": purge_duration,
                "deletedCount": deleted_count,
                "failedCount": failed_count,
                "peakRate": peak_rate,
                "avgRate": deleted_count / purge_duration if purge_duration > 0 else 0,
                "pollCount": poll_count,
                "apiLatencyMs": api_latency_ms,
                "pollLatencies": poll_latencies_ms,
            }

        if current_status == "FAILED":
            error_msg = status.get("errorMessage", "unknown error")
            raise PhaseError(f"Purge FAILED: {error_msg}")

        if current_status == "CANCELLED":
            raise PhaseError(f"Purge was CANCELLED after {poll_count} polls")

    raise PhaseError(f"Purge timeout after {PURGE_TIMEOUT}s (last status: {last_status})")


# ---------------------------------------------------------------------------
# Phase 8: Post-Purge Verification
# ---------------------------------------------------------------------------

def phase_8_verify_purge(cfg, purge_result, lineage_result):
    log("=== Phase 8: Post-Purge Verification ===")
    all_passed = True

    # 8a: Assets deleted
    log("[8a] Checking assets are deleted...")
    time.sleep(10)

    if cfg['purge_mode'] == 'qualifiedName' and cfg.get('qn_prefix_is_custom'):
        # Custom prefix: only assets matching the prefix should be gone
        prefix_count = search_by_qn_prefix(cfg['api_base'], cfg['qn_prefix'],
                                           cfg['token_manager'], cfg['basic_auth'])
        if prefix_count == 0:
            log(f"  PASS: No assets matching prefix '{cfg['qn_prefix']}' (count={prefix_count})")
        else:
            log(f"  FAIL: Expected 0 assets matching prefix, found {prefix_count:,}", "ERROR")
            all_passed = False
        # Connection-level count may still have non-prefix entities
        conn_count = search_by_connection_qn(cfg['api_base'], cfg['conn_qn'],
                                             cfg['token_manager'], cfg['basic_auth'])
        log(f"  Info: Total connection entity count: {conn_count:,} (some may remain with custom prefix)")
    else:
        # Default prefix or connection mode: all children should be purged
        count = search_by_connection_qn(cfg['api_base'], cfg['conn_qn'],
                                        cfg['token_manager'], cfg['basic_auth'])
        if count <= 1:
            log(f"  PASS: No child assets found (count={count})")
        else:
            log(f"  FAIL: Expected at most 1 (connection only), found {count:,}", "ERROR")
            all_passed = False

    # 8b: Connection entity preserved
    log("[8b] Checking test connection is preserved...")
    resp = api_get(cfg['api_base'], f"/entity/guid/{cfg['conn_guid']}",
                   cfg['token_manager'], cfg['basic_auth'], expected_status=None)
    if resp.status_code == 200:
        entity = resp.json().get("entity", {})
        log(f"  PASS: Test connection exists (status={entity.get('status')})")
    else:
        log(f"  FAIL: Test connection not found (HTTP {resp.status_code})", "ERROR")
        all_passed = False

    # 8c: External connection + entities preserved
    if lineage_result:
        log("[8c] Checking external connection is preserved...")
        ext_conn_guid = lineage_result.get("ext_conn_guid")
        if ext_conn_guid:
            resp = api_get(cfg['api_base'], f"/entity/guid/{ext_conn_guid}",
                           cfg['token_manager'], cfg['basic_auth'], expected_status=None)
            if resp.status_code == 200:
                log("  PASS: External connection exists")
            else:
                log(f"  FAIL: External connection not found (HTTP {resp.status_code})", "ERROR")
                all_passed = False

        log("[8d] Checking external entities are preserved...")
        ext_count = search_by_connection_qn(cfg['api_base'], cfg['ext_conn_qn'],
                                            cfg['token_manager'], cfg['basic_auth'])
        expected_ext = len(lineage_result.get('ext_asset_guids', []))
        if ext_count >= expected_ext:
            log(f"  PASS: External entities preserved (count={ext_count})")
        else:
            log(f"  FAIL: Expected >= {expected_ext} external entities, found {ext_count}", "ERROR")
            all_passed = False

        # 8e: Lineage repair
        log("[8e] Checking lineage repair on external tables...")
        ext_asset_guids = lineage_result.get("ext_asset_guids", [])
        if ext_asset_guids:
            repaired_count = 0
            checked_count = min(len(ext_asset_guids), 10)  # spot-check 10
            start = time.time()
            while time.time() - start < LINEAGE_REPAIR_TIMEOUT:
                repaired_count = 0
                for guid in ext_asset_guids[:checked_count]:
                    resp = api_get(cfg['api_base'], f"/lineage/{guid}",
                                   cfg['token_manager'], cfg['basic_auth'],
                                   params={"depth": 1, "direction": "BOTH"},
                                   expected_status=None)
                    if resp.status_code == 200:
                        relations = resp.json().get("relations", [])
                        if len(relations) == 0:
                            repaired_count += 1
                    elif resp.status_code == 404:
                        repaired_count += 1
                if repaired_count == checked_count:
                    break
                time.sleep(5)

            if repaired_count == checked_count:
                log(f"  PASS: Lineage repaired ({repaired_count}/{checked_count} "
                    f"external tables have no lineage relations)")
            else:
                log(f"  WARN: Lineage repair partial — {repaired_count}/{checked_count} "
                    f"cleaned (may be async via tasks)", "WARN")
    else:
        log("[8c-8e] SKIP: Lineage was not created")

    return all_passed


# ---------------------------------------------------------------------------
# Phase 9: Cleanup — delete external connection, ext tables, test connection
# ---------------------------------------------------------------------------

def phase_9_cleanup(cfg, lineage_result):
    log("=== Phase 9: Cleanup ===")

    deleted = 0
    failed = 0

    # 9a: Delete external tables and external connection via bulk purge
    if lineage_result and lineage_result.get("ext_conn_guid"):
        ext_conn_qn = cfg['ext_conn_qn']
        log(f"[9a] Purging external connection: {ext_conn_qn}")

        resp = api_post(cfg['api_base'], "/bulk-purge/connection",
                        cfg['token_manager'], cfg['basic_auth'],
                        params={"connectionQualifiedName": ext_conn_qn},
                        expected_status=None)

        if resp.status_code == 200:
            body = resp.json()
            request_id = body.get("requestId")
            log(f"  External purge submitted: requestId={request_id}")

            # Poll until complete (short timeout — only ~10-100 entities)
            start = time.time()
            while time.time() - start < 300:
                time.sleep(5)
                resp = api_get(cfg['api_base'], "/bulk-purge/status",
                               cfg['token_manager'], cfg['basic_auth'],
                               params={"requestId": request_id},
                               expected_status=None)
                if resp.status_code == 200:
                    status = resp.json().get("status", "")
                    ext_deleted = resp.json().get("deletedCount", 0)
                    if status == "COMPLETED":
                        log(f"  External purge completed: {ext_deleted} entities deleted")
                        deleted += ext_deleted
                        break
                    elif status == "FAILED":
                        log(f"  External purge FAILED", "WARN")
                        failed += 1
                        break
            else:
                log(f"  External purge timed out after 300s", "WARN")
                failed += 1
        else:
            log(f"  External purge submit failed (HTTP {resp.status_code}): "
                f"{resp.text[:200]}", "WARN")
            failed += 1

        # Delete the external connection entity itself
        ext_conn_guid = lineage_result["ext_conn_guid"]
        log(f"[9b] Deleting external connection entity: {ext_conn_guid}")
        resp = api_delete(cfg['api_base'], f"/entity/guid/{ext_conn_guid}",
                          cfg['token_manager'], cfg['basic_auth'],
                          expected_status=None)
        if resp.status_code == 200:
            log(f"  External connection deleted")
            deleted += 1
        else:
            log(f"  Failed to delete external connection (HTTP {resp.status_code})", "WARN")
            failed += 1
    else:
        log("[9a-9b] SKIP: No external connection to clean up")

    # 9c: Delete test connection entity
    if cfg.get('conn_guid'):
        log(f"[9c] Deleting test connection entity: {cfg['conn_guid']}")
        resp = api_delete(cfg['api_base'], f"/entity/guid/{cfg['conn_guid']}",
                          cfg['token_manager'], cfg['basic_auth'],
                          expected_status=None)
        if resp.status_code == 200:
            log(f"  Test connection deleted")
            deleted += 1
        else:
            log(f"  Failed to delete test connection (HTTP {resp.status_code})", "WARN")
            failed += 1
    else:
        log("[9c] SKIP: No test connection GUID to clean up")

    log(f"Cleanup complete: {deleted} entities deleted, {failed} failures")
    return {"deleted": deleted, "failed": failed}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(cfg, creation_result, lineage_result, purge_result,
                  phase_timings, all_passed):
    total_count = cfg['count']
    url = cfg['atlas_url']

    log("")
    log("=" * 55)
    log(f"BULK PURGE LOAD TEST SUMMARY ({total_count:,} assets)")
    log("=" * 55)
    log(f"  Tenant:              {url}")
    log(f"  Purge mode:          {cfg['purge_mode']}")
    if cfg['purge_mode'] == 'qualifiedName':
        log(f"  QN prefix:           {cfg['qn_prefix']}")
    log(f"  Connection QN:       {cfg['conn_qn']}")

    if creation_result:
        cr = creation_result
        log(f"  Assets created:      {cr['entities_created']:,}")
        log(f"  Creation duration:   {cr['duration']:.1f}s ({cr['rate']:,.0f} ent/s)")
        log(f"  Creation failures:   {cr['batches_failed']} batches")
    else:
        log(f"  Assets created:      (skipped)")

    if lineage_result:
        log(f"  Lineage processes:   {lineage_result['process_count']:,}")
    else:
        log(f"  Lineage processes:   (skipped)")

    log("")
    log("  --- Phase Timings ---")
    for phase_name, duration in phase_timings.items():
        log(f"  {phase_name:35s} {duration:>10.1f}s")
    log("")

    log("  --- Bulk Purge ---")
    if purge_result:
        log(f"  Entities purged:     {purge_result['deletedCount']:,}")
        log(f"  Failed deletions:    {purge_result['failedCount']}")
        log(f"  Duration:            {purge_result['duration']:.1f}s")
        log(f"  Avg rate:            {purge_result['avgRate']:,.0f} entities/s")
        log(f"  Peak rate:           {purge_result['peakRate']:,.0f} entities/s")
    else:
        log(f"  (not reached)")
    log("")

    log("  --- API Latency ---")
    if purge_result:
        log(f"  Purge submit (POST): {purge_result['apiLatencyMs']:.0f}ms")
        polls = purge_result.get("pollLatencies", [])
        if polls:
            sorted_polls = sorted(polls)
            log(f"  Status polls (GET):  avg={sum(polls)/len(polls):.0f}ms, "
                f"min={min(polls):.0f}ms, max={max(polls):.0f}ms, "
                f"p50={sorted_polls[len(sorted_polls)//2]:.0f}ms")
    else:
        log(f"  (not reached)")
    log("")

    log(f"  Overall result:      {'PASS' if all_passed else 'FAIL'}")
    log("=" * 55)


# ---------------------------------------------------------------------------
# CLI & Main
# ---------------------------------------------------------------------------

def load_creds(creds_file, tenant):
    """Load client_secret from creds.yaml for a given tenant."""
    try:
        import yaml
    except ImportError:
        raise PhaseError("pyyaml is required for --tenant mode. Install: pip install pyyaml")

    if not os.path.exists(creds_file):
        raise PhaseError(f"Creds file not found: {creds_file}")

    with open(creds_file, "r") as f:
        creds = yaml.safe_load(f)

    # Format: creds: [{tenant: "staging.atlan.com", client_secret: "..."}, ...]
    # Match "staging" against "staging.atlan.com" (prefix match)
    creds_list = creds.get("creds", []) if isinstance(creds, dict) else []
    if isinstance(creds_list, list):
        for entry in creds_list:
            entry_tenant = entry.get("tenant", "")
            # Match exact, or match short name (e.g., "staging" matches "staging.atlan.com")
            if entry_tenant == tenant or entry_tenant == f"{tenant}.atlan.com":
                secret = entry.get("client_secret") or entry.get("secret")
                if secret:
                    return secret

    # Fallback: top-level dict keyed by tenant name
    if isinstance(creds, dict) and tenant in creds:
        entry = creds[tenant]
        if isinstance(entry, dict):
            return entry.get("client_secret") or entry.get("secret")

    raise PhaseError(
        f"Could not find credentials for tenant '{tenant}' in {creds_file}. "
        f"Available tenants: {[e.get('tenant') for e in creds_list]}"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Bulk Purge Load Test — 1M asset scale",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local dev (1K assets, connection mode)
  python3 load_test_bulk_purge.py --count 1000 --workers 10

  # Staging — qualifiedName mode
  python3 load_test_bulk_purge.py --tenant staging --count 1000 --purge-mode qualifiedName

  # qualifiedName mode with custom prefix (purge only tables, not processes)
  python3 load_test_bulk_purge.py --tenant staging --count 1000 \\
      --purge-mode qualifiedName --qn-prefix "default/bulk-purge-test/1740000000/table-"

  # Full 1M load test
  python3 load_test_bulk_purge.py --tenant staging --count 1000000 --workers 50

  # Re-run purge only
  python3 load_test_bulk_purge.py --tenant staging --skip-create \\
      --conn-qn "default/bulk-purge-test/1740000000"
"""
    )
    parser.add_argument("--url", default=None,
                        help="Atlas base URL (default: http://localhost:21000, "
                             "or auto-constructed from --tenant)")
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth (e.g., 'staging'). "
                             "Constructs URL as https://{tenant}.atlan.com")
    parser.add_argument("--client-secret", default=None,
                        help="OAuth client_secret (optional if --creds-file has it)")
    parser.add_argument("--creds-file",
                        default=os.path.expanduser(
                            "~/Desktop/Projects/tokens/creds.yaml"),
                        help="Path to creds.yaml (default: ~/Desktop/Projects/tokens/creds.yaml)")
    parser.add_argument("--user", default=None,
                        help="Basic auth username (for local dev)")
    parser.add_argument("--password", default=None,
                        help="Basic auth password (for local dev)")
    parser.add_argument("--count", type=int, default=1_000_000,
                        help="Number of assets to create (default: 1000000)")
    parser.add_argument("--workers", type=int, default=50,
                        help="Parallel workers (default: 50)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Entities per batch (default: 500)")
    parser.add_argument("--conn-qn", default=None,
                        help="Connection QN (auto-generated if not provided)")
    parser.add_argument("--purge-mode", default="connection",
                        choices=["connection", "qualifiedName"],
                        help="Purge mode: 'connection' (default) or 'qualifiedName'")
    parser.add_argument("--qn-prefix", default=None,
                        help="QN prefix for qualifiedName mode "
                             "(default: {conn_qn}/ — matches all children)")
    parser.add_argument("--skip-create", action="store_true",
                        help="Skip creation, go straight to purge (requires --conn-qn)")
    parser.add_argument("--skip-lineage", action="store_true",
                        help="Skip lineage creation")
    parser.add_argument("--lineage-count", type=int, default=10,
                        help="Number of Process lineage entities to create (default: 10)")
    parser.add_argument("--server-workers", type=int, default=0,
                        help="Server-side worker count override (0 = auto-scale)")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt before purge")
    return parser.parse_args()


def main():
    args = parse_args()

    # --- Auth mode selection ---
    token_manager = None
    basic_auth = None

    if args.tenant:
        # OAuth2 mode
        atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        token_url = f"https://{args.tenant}.atlan.com/auth/realms/default/protocol/openid-connect/token"
        client_id = "atlan-argo"
        client_secret = args.client_secret
        if not client_secret:
            client_secret = load_creds(args.creds_file, args.tenant)
        token_manager = TokenManager(token_url, client_id, client_secret)
    elif args.user:
        # Explicit basic auth
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth(args.user, args.password or "admin")
    else:
        # Default: local dev with admin/admin
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth("admin", "admin")

    # --- Connection QN ---
    timestamp = int(time.time())
    conn_qn = args.conn_qn or f"default/bulk-purge-test/{timestamp}"
    ext_conn_qn = f"default/bulk-purge-ext/{timestamp}"

    if args.skip_create and not args.conn_qn:
        log("--skip-create requires --conn-qn", "ERROR")
        sys.exit(1)

    # --- API base path ---
    # Remote tenants use /api/meta, local dev uses /api/atlas/v2
    if args.tenant:
        api_base = f"{atlas_url}/api/meta"
    else:
        api_base = f"{atlas_url}/api/atlas/v2"

    # --- Purge mode ---
    purge_mode = args.purge_mode
    # Default QN prefix: conn_qn + "/" to match all children but not the connection itself
    qn_prefix_is_custom = args.qn_prefix is not None
    qn_prefix = args.qn_prefix or (conn_qn + "/")

    # --- Build config dict ---
    cfg = {
        "atlas_url": atlas_url,
        "api_base": api_base,
        "token_manager": token_manager,
        "basic_auth": basic_auth,
        "count": args.count,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "conn_qn": conn_qn,
        "ext_conn_qn": ext_conn_qn,
        "conn_guid": None,
        "skip_create": args.skip_create,
        "skip_lineage": args.skip_lineage,
        "lineage_count": args.lineage_count,
        "server_workers": args.server_workers,
        "purge_mode": purge_mode,
        "qn_prefix": qn_prefix,
        "qn_prefix_is_custom": qn_prefix_is_custom,
    }

    log("=" * 60)
    log("BULK PURGE LOAD TEST — Starting")
    log("=" * 60)
    overall_start = time.time()
    phase_timings = {}
    creation_result = None
    lineage_result = None
    purge_result = None

    def run_phase(name, fn, *fn_args):
        phase_start = time.time()
        result = fn(*fn_args)
        phase_timings[name] = time.time() - phase_start
        log(f">> {name}: OK ({phase_timings[name]:.1f}s)\n")
        return result

    try:
        # Phase 0: Preflight
        run_phase("Phase 0: Preflight", phase_0_preflight, cfg)

        # Phase 1: Check types
        run_phase("Phase 1: Check Types", phase_1_check_types, cfg)

        if not args.skip_create:
            # Phase 2: Create connection
            cfg['conn_guid'] = run_phase("Phase 2: Create Connection",
                                         phase_2_create_connection, cfg)

            # Phase 3: Create assets (streaming)
            creation_result = run_phase("Phase 3: Create Assets",
                                        phase_3_create_assets, cfg)

            # Phase 4: Create lineage
            if not args.skip_lineage:
                lineage_result = run_phase("Phase 4: Create Lineage",
                                           phase_4_create_lineage, cfg)

            # Phase 5: Wait for ES sync
            run_phase("Phase 5: Wait for ES Sync", phase_5_wait_es_sync,
                      cfg, creation_result)

            # Phase 6: Verify creation
            run_phase("Phase 6: Verify Creation", phase_6_verify_creation,
                      cfg, creation_result, lineage_result)
        else:
            log("=== Skipping creation phases (--skip-create) ===")
            # Need to resolve conn_guid for the existing connection
            resp = api_post(cfg['api_base'], "/search/indexsearch",
                            cfg['token_manager'], cfg['basic_auth'],
                            data={
                                "dsl": {
                                    "from": 0, "size": 1,
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"term": {"__qualifiedName": conn_qn}},
                                                {"term": {"__typeName.keyword": "Connection"}}
                                            ]
                                        }
                                    }
                                }
                            }, expected_status=None)
            if resp.status_code == 200:
                entities = resp.json().get("entities", [])
                if entities:
                    cfg['conn_guid'] = entities[0].get("guid")
                    log(f"Resolved connection GUID: {cfg['conn_guid']}")

            if not cfg['conn_guid']:
                # Fallback: try uniqueAttribute lookup
                resp = api_get(cfg['api_base'],
                               "/entity/uniqueAttribute/type/Connection",
                               cfg['token_manager'], cfg['basic_auth'],
                               params={"attr:qualifiedName": conn_qn},
                               expected_status=None)
                if resp.status_code == 200:
                    cfg['conn_guid'] = resp.json().get("entity", {}).get("guid")
                    log(f"Resolved connection GUID (uniqueAttr): {cfg['conn_guid']}")

            if not cfg['conn_guid']:
                log("WARNING: Could not resolve connection GUID. "
                    "Post-purge verification may be incomplete.", "WARN")

        # --- Confirmation prompt before purge ---
        if not args.yes:
            log("")
            log("=" * 55)
            log("PRE-PURGE SUMMARY")
            log("=" * 55)
            log(f"  Purge mode:      {cfg['purge_mode']}")
            if cfg['purge_mode'] == 'qualifiedName':
                log(f"  QN prefix:       {cfg['qn_prefix']}")
            log(f"  Connection QN:   {cfg['conn_qn']}")
            log(f"  Connection GUID: {cfg['conn_guid'] or '(unknown)'}")

            es_count = search_by_connection_qn(
                cfg['api_base'], cfg['conn_qn'],
                cfg['token_manager'], cfg['basic_auth'])
            log(f"  ES entity count: {es_count:,}")

            if creation_result:
                log(f"  Created:         {creation_result['entities_created']:,} entities "
                    f"({creation_result['batches_failed']} failed batches)")
            if lineage_result:
                log(f"  Processes:       {lineage_result['process_count']:,}")
                log(f"  Ext tables:      {len(lineage_result.get('ext_asset_guids', []))}")
            log("=" * 55)
            log("")

            try:
                answer = input("Proceed with bulk purge? [y/N] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
                print()

            if answer not in ("y", "yes"):
                log("Aborted by user.")
                sys.exit(0)

            log("")

        # Phase 7: Trigger bulk purge
        purge_result = run_phase("Phase 7: Bulk Purge", phase_7_trigger_bulk_purge, cfg)

        # Phase 8: Post-purge verification
        phase_start = time.time()
        all_passed = phase_8_verify_purge(cfg, purge_result, lineage_result)
        phase_timings["Phase 8: Verify Purge"] = time.time() - phase_start
        log(f">> Phase 8: Verify Purge: OK ({phase_timings['Phase 8: Verify Purge']:.1f}s)\n")

        # Phase 9: Cleanup — delete external connection + test connection
        run_phase("Phase 9: Cleanup", phase_9_cleanup, cfg, lineage_result)

    except PhaseError as e:
        log(f"PHASE FAILED — {e}", "ERROR")
        total_time = time.time() - overall_start
        log(f"\nTotal test time: {total_time:.1f}s")
        print_summary(cfg, creation_result, lineage_result, purge_result,
                      phase_timings, False)
        sys.exit(1)
    except Exception as e:
        log(f"UNEXPECTED ERROR — {e}", "ERROR")
        traceback.print_exc()
        total_time = time.time() - overall_start
        log(f"\nTotal test time: {total_time:.1f}s")
        print_summary(cfg, creation_result, lineage_result, purge_result,
                      phase_timings, False)
        sys.exit(1)

    total_time = time.time() - overall_start
    log(f"\nTotal test time: {total_time:.1f}s")
    print_summary(cfg, creation_result, lineage_result, purge_result,
                  phase_timings, all_passed)

    if all_passed:
        log("ALL PHASES PASSED")
        sys.exit(0)
    else:
        log("SOME CHECKS FAILED", "ERROR")
        sys.exit(1)


if __name__ == "__main__":
    main()
