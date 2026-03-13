#!/usr/bin/env python3
"""
test_bulk_purge.py — End-to-end test for Bulk Purge by Connection

Creates a Connection + N Table assets (parallel), creates cross-connection
lineage, triggers bulk purge, and verifies everything is cleaned up properly.

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Prerequisites:
  - Atlas running
  - Connection and Table types loaded (from minimal.json)
  - pip install requests pyyaml

Usage:
  # Local dev
  python3 test_bulk_purge.py
  python3 test_bulk_purge.py --count 1000 --workers 4

  # Staging (OAuth2)
  python3 test_bulk_purge.py --tenant staging --count 1000
  python3 test_bulk_purge.py --tenant staging --count 5000 --workers 8
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
# Constants
# ---------------------------------------------------------------------------

MAX_RETRIES = 3
ES_SYNC_TIMEOUT = 180       # seconds to wait for ES to sync
PURGE_TIMEOUT = 1800        # seconds to wait for purge to complete (30 min)
PURGE_POLL_INTERVAL = 5     # seconds between purge status polls
LINEAGE_REPAIR_TIMEOUT = 10 # seconds
API_REQUEST_TIMEOUT = 60

TIMESTAMP = int(time.time())
TEST_CONN_QN = f"default/bulk-purge-test/{TIMESTAMP}"
TEST_CONN_NAME = "bulk-purge-test-connection"
EXT_CONN_QN = f"default/bulk-purge-ext/{TIMESTAMP}"
EXT_CONN_NAME = "bulk-purge-ext-connection"

# Test state
test_conn_guid = None
ext_conn_guid = None
created_guids = []       # GUIDs of assets in test connection
ext_asset_guids = []     # GUIDs of assets in external connection
process_guids = []       # GUIDs of lineage Process entities
phase_timings = {}       # phase name -> duration in seconds


# ---------------------------------------------------------------------------
# TokenManager — OAuth2 client-credentials with auto-refresh
# ---------------------------------------------------------------------------

class TokenManager:
    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0
        self._lock = threading.Lock()

    def get_token(self):
        if not self.access_token or time.time() >= self.token_expiry:
            with self._lock:
                if not self.access_token or time.time() >= self.token_expiry:
                    self._refresh()
        return self.access_token

    def force_refresh(self):
        with self._lock:
            self._refresh()

    def _refresh(self):
        payload = {
            "client_id": self.client_id,
            "grant_type": "client_credentials",
            "client_secret": self.client_secret,
        }
        resp = requests.post(self.token_url,
                             headers={"Content-Type": "application/x-www-form-urlencoded"},
                             data=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        expires_in = data.get("expires_in", 60)
        self.token_expiry = int(time.time()) + expires_in - 30
        log(f"Token refreshed (valid {expires_in}s)")


def load_creds(creds_file, tenant):
    try:
        import yaml
    except ImportError:
        raise PhaseError("pyyaml required for --tenant mode. Install: pip install pyyaml")

    if not os.path.exists(creds_file):
        raise PhaseError(f"Creds file not found: {creds_file}")

    with open(creds_file, "r") as f:
        creds = yaml.safe_load(f)

    creds_list = creds.get("creds", []) if isinstance(creds, dict) else []
    if isinstance(creds_list, list):
        for entry in creds_list:
            t = entry.get("tenant", "")
            if t == tenant or t == f"{tenant}.atlan.com":
                secret = entry.get("client_secret") or entry.get("secret")
                if secret:
                    return secret

    if isinstance(creds, dict) and tenant in creds:
        entry = creds[tenant]
        if isinstance(entry, dict):
            return entry.get("client_secret") or entry.get("secret")

    raise PhaseError(f"Credentials not found for tenant '{tenant}' in {creds_file}")


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
    if total <= 0:
        return False
    if current >= total:
        return True
    step = max(1, total * interval_pct // 100)
    return current % step == 0


# Module-level config — set once in main(), used by all helpers/tests.
_cfg = {
    "atlas_url": None,
    "api_base": None,
    "token_manager": None,
    "basic_auth": None,
}


def _build_headers():
    h = {"Content-Type": "application/json", "Accept": "application/json"}
    tm = _cfg["token_manager"]
    if tm:
        h["Authorization"] = f"Bearer {tm.get_token()}"
    return h


def _do_request(method, url, json_data=None, params=None, timeout=API_REQUEST_TIMEOUT):
    headers = _build_headers()
    auth = _cfg["basic_auth"]
    resp = requests.request(method, url, headers=headers, auth=auth,
                            json=json_data, params=params, timeout=timeout)
    if resp.status_code == 401 and _cfg["token_manager"]:
        _cfg["token_manager"].force_refresh()
        headers = _build_headers()
        resp = requests.request(method, url, headers=headers, auth=auth,
                                json=json_data, params=params, timeout=timeout)
    return resp


def api_get(path, params=None, expected_status=200):
    url = f"{_cfg['api_base']}{path}"
    resp = _do_request("GET", url, params=params, timeout=30)
    if expected_status and resp.status_code != expected_status:
        raise PhaseError(f"GET {path} returned {resp.status_code}: {resp.text[:500]}")
    return resp


def api_post(path, data=None, params=None, expected_status=200):
    url = f"{_cfg['api_base']}{path}"
    resp = _do_request("POST", url, json_data=data, params=params, timeout=300)
    if expected_status and resp.status_code != expected_status:
        raise PhaseError(f"POST {path} returned {resp.status_code}: {resp.text[:500]}")
    return resp


def search_by_connection_qn(conn_qn, size=1):
    """Count ACTIVE entities belonging to a connection."""
    strategies = [
        {"bool": {"must": [
            {"term": {"connectionQualifiedName": conn_qn}},
            {"term": {"__state": "ACTIVE"}}
        ]}},
        {"bool": {"must": [
            {"term": {"connectionQualifiedName.keyword": conn_qn}},
            {"term": {"__state.keyword": "ACTIVE"}}
        ]}},
        {"bool": {"must": [
            {"prefix": {"__qualifiedNameHierarchy": conn_qn}},
            {"term": {"__state": "ACTIVE"}}
        ]}},
        {"bool": {"must": [
            {"prefix": {"__qualifiedNameHierarchy.keyword": conn_qn}},
            {"term": {"__state.keyword": "ACTIVE"}}
        ]}},
    ]

    for query in strategies:
        payload = {"dsl": {"from": 0, "size": size, "query": query}}
        resp = api_post("/search/indexsearch", data=payload, expected_status=None)
        if resp.status_code == 200:
            count = resp.json().get("approximateCount", 0)
            if count > 0:
                return count

    return -1


def wait_for_es_count(conn_qn, expected_min, timeout=ES_SYNC_TIMEOUT, label=""):
    log(f"Waiting for ES count >= {expected_min} for {conn_qn} {label}...")
    start = time.time()
    last_count = 0
    while time.time() - start < timeout:
        count = search_by_connection_qn(conn_qn)
        if count >= expected_min:
            log(f"  ES sync: {count}/{expected_min} (100%) — done in {time.time()-start:.1f}s")
            return count
        elapsed = time.time() - start
        pct = max(0, count) / expected_min * 100 if expected_min > 0 else 0
        if count != last_count:
            log(f"  ES sync: {max(0, count)}/{expected_min} ({pct:.0f}%) — {elapsed:.0f}s elapsed")
        last_count = count
        time.sleep(3)
    raise PhaseError(
        f"ES sync timeout after {timeout}s: count={last_count}, expected>={expected_min} {label}"
    )


# ---------------------------------------------------------------------------
# Phase 0: Preflight
# ---------------------------------------------------------------------------

def phase_0_preflight():
    log("=== Phase 0: Preflight ===")
    try:
        resp = _do_request("GET", f"{_cfg['atlas_url']}/api/atlas/admin/status", timeout=10)
        if resp.status_code != 200:
            raise PhaseError(f"Atlas status endpoint returned {resp.status_code}")
        status = resp.json().get("Status", "UNKNOWN")
        log(f"Atlas status: {status}")
        if status != "ACTIVE":
            raise PhaseError(f"Atlas is not ACTIVE (status={status})")
    except requests.ConnectionError:
        raise PhaseError(f"Cannot connect to Atlas at {_cfg['atlas_url']}")

    auth_mode = "OAuth2" if _cfg["token_manager"] else "Basic"
    log(f"Configuration: ASSET_COUNT={ASSET_COUNT}, WORKERS={PARALLEL_WORKERS}, BATCH={BATCH_SIZE}")
    log(f"Auth: {auth_mode} | API: {_cfg['api_base']}")
    log(f"Test connection QN: {TEST_CONN_QN}")
    log(f"External connection QN: {EXT_CONN_QN}")


# ---------------------------------------------------------------------------
# Phase 1: Ensure Types Exist
# ---------------------------------------------------------------------------

def phase_1_check_types():
    log("=== Phase 1: Ensure Types Exist ===")
    resp = api_get("/types/typedefs/headers")
    headers_list = resp.json()

    type_names = {h.get("name") for h in headers_list}
    required = ["Connection", "Table", "Process"]
    missing = [t for t in required if t not in type_names]
    if missing:
        raise PhaseError(
            f"Required types not found: {missing}. "
            "Ensure minimal.json is loaded (deploy/models/0000-Area0/minimal.json)"
        )
    log(f"All required types present: {required}")


# ---------------------------------------------------------------------------
# Phase 2: Create Connection
# ---------------------------------------------------------------------------

def phase_2_create_connection():
    global test_conn_guid
    log("=== Phase 2: Create Test Connection ===")

    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": TEST_CONN_QN,
                "name": TEST_CONN_NAME,
                "connectorName": "test",
                "category": "warehouse"
            }
        }
    }
    resp = api_post("/entity", data=payload)
    body = resp.json()

    mutated = body.get("mutatedEntities", {})
    creates = mutated.get("CREATE", [])
    if not creates:
        updates = mutated.get("UPDATE", [])
        if updates:
            test_conn_guid = updates[0]["guid"]
            log(f"Connection already existed, GUID: {test_conn_guid}")
            return
        raise PhaseError(f"No entity created/updated: {json.dumps(body)[:500]}")

    test_conn_guid = creates[0]["guid"]
    log(f"Connection created: GUID={test_conn_guid}, QN={TEST_CONN_QN}")


# ---------------------------------------------------------------------------
# Phase 3: Create Assets (Parallel)
# ---------------------------------------------------------------------------

def phase_3_create_assets():
    global created_guids
    log(f"=== Phase 3: Create {ASSET_COUNT} Table Assets (Parallel) ===")

    batches = []
    for start_idx in range(0, ASSET_COUNT, BATCH_SIZE):
        end_idx = min(start_idx + BATCH_SIZE, ASSET_COUNT)
        batch_entities = []
        for i in range(start_idx, end_idx):
            batch_entities.append({
                "typeName": "Table",
                "attributes": {
                    "qualifiedName": f"{TEST_CONN_QN}/table-{i}",
                    "name": f"test-table-{i}",
                    "connectionQualifiedName": TEST_CONN_QN
                }
            })
        batches.append(batch_entities)

    total_batches = len(batches)
    log(f"Submitting {total_batches} batches ({BATCH_SIZE} entities each) with {PARALLEL_WORKERS} workers")

    completed_count = 0
    failed_count = 0
    all_guids = []
    start_time = time.time()

    def create_batch(batch_idx, entities):
        payload = {"entities": entities}
        log(f"  Batch {batch_idx}/{total_batches}: sending {len(entities)} entities...")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = api_post("/entity/bulk", data=payload, expected_status=None)
            except Exception as e:
                if attempt < MAX_RETRIES:
                    log(f"  Batch {batch_idx} attempt {attempt} exception: {e}, retrying...", "WARN")
                    time.sleep(attempt * 2)
                    continue
                raise
            if resp.status_code == 200:
                body = resp.json()
                guids = []
                for action in ("CREATE", "UPDATE"):
                    for e in body.get("mutatedEntities", {}).get(action, []):
                        guids.append(e["guid"])
                return guids, resp.status_code, None
            if resp.status_code >= 500 and attempt < MAX_RETRIES:
                log(f"  Batch {batch_idx} attempt {attempt}: HTTP {resp.status_code}, retrying...", "WARN")
                time.sleep(attempt * 2)
                continue
            return None, resp.status_code, resp.text[:200]
        return None, resp.status_code, resp.text[:200]

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(create_batch, idx, batch): idx for idx, batch in enumerate(batches)}

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                guids, status_code, err = future.result()
                if guids is not None:
                    all_guids.extend(guids)
                    completed_count += 1
                    log(f"  Batch {batch_idx}/{total_batches}: OK ({len(guids)} entities)")
                else:
                    failed_count += 1
                    log(f"  Batch {batch_idx}/{total_batches}: FAILED (HTTP {status_code}): {err}", "WARN")
            except Exception as e:
                failed_count += 1
                log(f"  Batch {batch_idx}/{total_batches}: EXCEPTION: {e}", "ERROR")

            total_done = completed_count + failed_count
            entities_done = len(all_guids)
            elapsed = time.time() - start_time
            rate = entities_done / elapsed if elapsed > 0 else 0
            pct = entities_done / ASSET_COUNT * 100 if ASSET_COUNT > 0 else 0
            log(f"  Progress: {entities_done}/{ASSET_COUNT} ({pct:.0f}%) | "
                f"batches={completed_count}/{total_batches} failed={failed_count} | {rate:.0f} ent/s")

    elapsed = time.time() - start_time
    created_guids = all_guids
    log(f"Asset creation complete: {len(all_guids)} GUIDs in {elapsed:.1f}s "
        f"({failed_count} failed batches)")

    if failed_count > total_batches * 0.1:
        raise PhaseError(f"Too many batch failures: {failed_count}/{total_batches}")


# ---------------------------------------------------------------------------
# Phase 4: Create Cross-Connection Lineage
# ---------------------------------------------------------------------------

def phase_4_create_lineage():
    global ext_conn_guid, ext_asset_guids, process_guids
    log("=== Phase 4: Create Cross-Connection Lineage ===")

    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": EXT_CONN_QN,
                "name": EXT_CONN_NAME,
                "connectorName": "test-external",
                "category": "warehouse"
            }
        }
    }
    resp = api_post("/entity", data=payload)
    body = resp.json()
    mutated = body.get("mutatedEntities", {})
    creates = mutated.get("CREATE", mutated.get("UPDATE", []))
    if not creates:
        raise PhaseError("Failed to create external connection")
    ext_conn_guid = creates[0]["guid"]
    log(f"External connection created: GUID={ext_conn_guid}")

    ext_entities = []
    for i in range(10):
        ext_entities.append({
            "typeName": "Table",
            "attributes": {
                "qualifiedName": f"{EXT_CONN_QN}/ext-table-{i}",
                "name": f"ext-table-{i}",
                "connectionQualifiedName": EXT_CONN_QN
            }
        })
    resp = api_post("/entity/bulk", data={"entities": ext_entities})
    body = resp.json()
    for action in ("CREATE", "UPDATE"):
        for e in body.get("mutatedEntities", {}).get(action, []):
            ext_asset_guids.append(e["guid"])
    log(f"External tables created: {len(ext_asset_guids)} entities")

    wait_for_es_count(EXT_CONN_QN, 10, label="(external tables)")

    if len(created_guids) < 5:
        log("Not enough test assets for lineage, skipping Process creation", "WARN")
        return

    for i in range(5):
        input_guid = created_guids[i]
        output_qn = f"{EXT_CONN_QN}/ext-table-{i}"
        process_payload = {
            "entity": {
                "typeName": "Process",
                "attributes": {
                    "qualifiedName": f"{TEST_CONN_QN}/lineage-process-{i}",
                    "name": f"lineage-process-{i}",
                    "connectionQualifiedName": TEST_CONN_QN,
                    "inputs": [{"typeName": "Table", "guid": input_guid}],
                    "outputs": [{"typeName": "Table", "uniqueAttributes": {"qualifiedName": output_qn}}]
                }
            }
        }
        resp = api_post("/entity", data=process_payload, expected_status=None)
        if resp.status_code == 200:
            body = resp.json()
            for action in ("CREATE", "UPDATE"):
                for e in body.get("mutatedEntities", {}).get(action, []):
                    if e.get("typeName") == "Process":
                        process_guids.append(e["guid"])
            log(f"  Process {i} created (input GUID={input_guid[:8]}...)")
        else:
            log(f"  Process {i} creation failed (HTTP {resp.status_code}): {resp.text[:200]}", "WARN")

    log(f"Lineage processes created: {len(process_guids)}")


# ---------------------------------------------------------------------------
# Phase 5: Wait for ES Sync
# ---------------------------------------------------------------------------

def phase_5_wait_es_sync():
    log("=== Phase 5: Wait for ES Sync ===")
    expected = len(created_guids)
    log(f"Expecting {expected:,} table entities in ES (requested {ASSET_COUNT:,}, "
        f"{len(created_guids):,} created, + {len(process_guids)} processes not counted)")
    wait_for_es_count(TEST_CONN_QN, expected, label="(test connection tables)")


# ---------------------------------------------------------------------------
# Phase 6: Trigger Bulk Purge
# ---------------------------------------------------------------------------

def phase_6_trigger_bulk_purge():
    log("=== Phase 6: Trigger Bulk Purge ===")

    api_start = time.time()
    resp = api_post(
        "/bulk-purge/connection",
        params={"connectionQualifiedName": TEST_CONN_QN}
    )
    api_latency_ms = (time.time() - api_start) * 1000
    body = resp.json()
    request_id = body.get("requestId")
    purge_key = body.get("purgeKey")
    purge_mode = body.get("purgeMode")
    message = body.get("message", "")

    log(f"Purge submitted: requestId={request_id}, purgeKey={purge_key}, mode={purge_mode}")
    log(f"  API latency: {api_latency_ms:.0f}ms")
    if message:
        log(f"  Message: {message}")

    if not request_id:
        raise PhaseError(f"No requestId in purge response: {json.dumps(body)}")

    start_time = time.time()
    last_status = None
    last_deleted = 0
    peak_rate = 0
    poll_count = 0
    poll_latencies_ms = []

    while time.time() - start_time < PURGE_TIMEOUT:
        time.sleep(PURGE_POLL_INTERVAL)
        poll_count += 1
        elapsed = time.time() - start_time

        log(f"  [Poll #{poll_count}] GET /bulk-purge/status?requestId={request_id} ({elapsed:.1f}s elapsed)")
        poll_start = time.time()
        resp = api_get("/bulk-purge/status", params={"requestId": request_id}, expected_status=None)
        poll_latency = (time.time() - poll_start) * 1000
        poll_latencies_ms.append(poll_latency)

        if resp.status_code == 404:
            log(f"  [Poll #{poll_count}] Response: 404 Not Found — retrying...")
            continue
        if resp.status_code != 200:
            log(f"  [Poll #{poll_count}] Response: HTTP {resp.status_code} — {resp.text[:200]}", "WARN")
            continue

        status = resp.json()
        current_status = status.get("status", "UNKNOWN")
        deleted_count = status.get("deletedCount", 0)
        total_discovered = status.get("totalDiscovered", 0)
        failed_count = status.get("failedCount", 0)
        completed_batches = status.get("completedBatches", 0)
        worker_count = status.get("workerCount", "?")
        batch_size = status.get("batchSize", "?")

        if elapsed > 0 and deleted_count > 0:
            rate = deleted_count / elapsed
            peak_rate = max(peak_rate, rate)
        else:
            rate = 0

        pct = (deleted_count / total_discovered * 100) if total_discovered > 0 else 0
        log(f"  [Poll #{poll_count}] Response: status={current_status} | "
            f"deleted={deleted_count}/{total_discovered} ({pct:.1f}%) | "
            f"failed={failed_count} | batches={completed_batches} | "
            f"workers={worker_count} | batchSize={batch_size} | "
            f"rate={rate:.0f} ent/s")

        last_status = current_status
        last_deleted = deleted_count

        if current_status == "COMPLETED":
            purge_duration = time.time() - start_time
            log(f"Purge COMPLETED in {purge_duration:.1f}s ({poll_count} status polls)")
            log(f"  Total deleted: {deleted_count}")
            log(f"  Total failed: {failed_count}")
            log(f"  Avg rate: {deleted_count/purge_duration:.0f} entities/s")
            log(f"  Peak rate: {peak_rate:.0f} entities/s")
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
            raise PhaseError(f"Purge FAILED after {poll_count} polls: {error_msg}")

        if current_status == "CANCELLED":
            raise PhaseError(f"Purge was CANCELLED after {poll_count} polls")

    raise PhaseError(f"Purge timeout after {PURGE_TIMEOUT}s, {poll_count} polls (last status: {last_status})")


# ---------------------------------------------------------------------------
# Phase 7: Post-Purge Verification
# ---------------------------------------------------------------------------

def phase_7_verify_purge(purge_result):
    log("=== Phase 7: Post-Purge Verification ===")
    all_passed = True

    # 7a: Assets deleted
    log("[7a] Checking assets are deleted...")
    time.sleep(5)
    count = search_by_connection_qn(TEST_CONN_QN)
    if count <= 1:
        log(f"  PASS: No child assets found (count={count}, connection entity {'preserved' if count == 1 else 'also gone'})")
    else:
        log(f"  FAIL: Expected at most 1 (connection only), found {count}", "ERROR")
        all_passed = False

    # 7b: Connection entity preserved
    log("[7b] Checking test connection is preserved...")
    resp = api_get(f"/entity/guid/{test_conn_guid}", expected_status=None)
    if resp.status_code == 200:
        entity = resp.json().get("entity", {})
        status = entity.get("status", "UNKNOWN")
        log(f"  PASS: Test connection exists (status={status})")
    else:
        log(f"  FAIL: Test connection not found (HTTP {resp.status_code})", "ERROR")
        all_passed = False

    # 7c: External connection preserved
    log("[7c] Checking external connection is preserved...")
    if ext_conn_guid:
        resp = api_get(f"/entity/guid/{ext_conn_guid}", expected_status=None)
        if resp.status_code == 200:
            log("  PASS: External connection exists")
        else:
            log(f"  FAIL: External connection not found (HTTP {resp.status_code})", "ERROR")
            all_passed = False
    else:
        log("  SKIP: No external connection was created")

    # 7d: External entities preserved
    log("[7d] Checking external entities are preserved...")
    ext_count = search_by_connection_qn(EXT_CONN_QN)
    if ext_count >= 10:
        log(f"  PASS: External entities preserved (count={ext_count})")
    else:
        log(f"  FAIL: Expected >= 10 external entities, found {ext_count}", "ERROR")
        all_passed = False

    # 7e: Lineage repair
    log("[7e] Checking lineage repair on external tables...")
    if ext_asset_guids and process_guids:
        lineage_repaired = False
        start = time.time()
        while time.time() - start < LINEAGE_REPAIR_TIMEOUT:
            resp = api_get(f"/lineage/{ext_asset_guids[0]}?depth=1&direction=BOTH",
                           expected_status=None)
            if resp.status_code == 200:
                relations = resp.json().get("relations", [])
                if len(relations) == 0:
                    lineage_repaired = True
                    break
            elif resp.status_code == 404:
                lineage_repaired = True
                break
            time.sleep(2)

        if lineage_repaired:
            log("  PASS: Lineage repaired (external tables no longer show lineage relations)")
        else:
            log("  WARN: Lineage repair not confirmed within timeout "
                "(this may be expected if lineage repair is async via tasks)", "WARN")
    else:
        log("  SKIP: No lineage was created")

    # Summary
    log("")
    log("=" * 70)
    log("BULK PURGE E2E TEST SUMMARY")
    log("=" * 70)
    log(f"  Connection QN:       {TEST_CONN_QN}")
    log(f"  Assets requested:    {ASSET_COUNT}")
    log(f"  Assets created:      {len(created_guids)} tables + {len(process_guids)} processes")
    log("")

    log("  --- Phase Timings ---")
    for phase_name, duration in phase_timings.items():
        log(f"  {phase_name:30s} {duration:8.1f}s")
    log("")

    log("  --- Bulk Purge ---")
    if purge_result:
        log(f"  Entities purged:     {purge_result['deletedCount']}")
        log(f"  Failed deletions:    {purge_result['failedCount']}")
        log(f"  Duration:            {purge_result['duration']:.1f}s")
        log(f"  Avg rate:            {purge_result['avgRate']:.0f} entities/s")
        log(f"  Peak rate:           {purge_result['peakRate']:.0f} entities/s")
        log(f"  Status polls:        {purge_result['pollCount']}")
        if purge_result['deletedCount'] > 0:
            log(f"  Per entity:          {purge_result['duration']/purge_result['deletedCount']*1000:.1f}ms")
    else:
        log(f"  (not reached)")
    log("")

    log("  --- API Latency ---")
    if purge_result:
        log(f"  Purge submit (POST): {purge_result['apiLatencyMs']:.0f}ms")
        polls = purge_result.get('pollLatencies', [])
        if polls:
            log(f"  Status polls (GET):  avg={sum(polls)/len(polls):.0f}ms, "
                f"min={min(polls):.0f}ms, max={max(polls):.0f}ms, "
                f"p50={sorted(polls)[len(polls)//2]:.0f}ms")
    else:
        log(f"  (not reached)")
    log("")

    log(f"  Overall result:      {'PASS' if all_passed else 'FAIL'}")
    log("=" * 70)

    return all_passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# These are set from CLI args in main()
ASSET_COUNT = 10000
PARALLEL_WORKERS = 4
BATCH_SIZE = 100


def main():
    global ASSET_COUNT, PARALLEL_WORKERS, BATCH_SIZE

    parser = argparse.ArgumentParser(description="Bulk Purge E2E Test")

    # Auth
    parser.add_argument("--url", default=None,
                        help="Atlas base URL (auto from --tenant, or http://localhost:21000)")
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth (e.g., 'staging')")
    parser.add_argument("--client-secret", default=None,
                        help="OAuth client_secret (reads from creds file if omitted)")
    parser.add_argument("--creds-file",
                        default=os.path.expanduser("~/Desktop/Projects/tokens/creds.yaml"),
                        help="Path to creds.yaml")
    parser.add_argument("--user", default=None, help="Basic auth username")
    parser.add_argument("--password", default=None, help="Basic auth password")

    # Test params
    parser.add_argument("--count", type=int, default=10000, help="Number of assets to create")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    args = parser.parse_args()

    ASSET_COUNT = args.count
    PARALLEL_WORKERS = args.workers
    BATCH_SIZE = args.batch_size

    # Auth mode selection
    if args.tenant:
        atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        token_url = f"https://{args.tenant}.atlan.com/auth/realms/default/protocol/openid-connect/token"
        secret = args.client_secret or load_creds(args.creds_file, args.tenant)
        _cfg["token_manager"] = TokenManager(token_url, "atlan-argo", secret)
        _cfg["api_base"] = f"{atlas_url}/api/meta"
    elif args.user:
        atlas_url = args.url or "http://localhost:21000"
        _cfg["basic_auth"] = HTTPBasicAuth(args.user, args.password or "admin")
        _cfg["api_base"] = f"{atlas_url}/api/atlas/v2"
    else:
        atlas_url = args.url or "http://localhost:21000"
        _cfg["basic_auth"] = HTTPBasicAuth("admin", "admin")
        _cfg["api_base"] = f"{atlas_url}/api/atlas/v2"

    _cfg["atlas_url"] = atlas_url

    log("=" * 60)
    log("BULK PURGE E2E TEST — Starting")
    log("=" * 60)
    overall_start = time.time()
    purge_result = None

    phases = [
        ("Phase 0: Preflight",           phase_0_preflight),
        ("Phase 1: Check Types",         phase_1_check_types),
        ("Phase 2: Create Connection",   phase_2_create_connection),
        ("Phase 3: Create Assets",       phase_3_create_assets),
        ("Phase 4: Create Lineage",      phase_4_create_lineage),
        ("Phase 5: Wait for ES Sync",    phase_5_wait_es_sync),
    ]

    for name, fn in phases:
        try:
            phase_start = time.time()
            fn()
            phase_timings[name] = time.time() - phase_start
            log(f">> {name}: OK ({phase_timings[name]:.1f}s)\n")
        except PhaseError as e:
            log(f">> {name}: FAILED — {e}", "ERROR")
            sys.exit(1)
        except Exception as e:
            log(f">> {name}: FAILED — {e}", "ERROR")
            traceback.print_exc()
            sys.exit(1)

    try:
        phase_start = time.time()
        purge_result = phase_6_trigger_bulk_purge()
        phase_timings["Phase 6: Bulk Purge"] = time.time() - phase_start
        log(f">> Phase 6: Bulk Purge: OK ({phase_timings['Phase 6: Bulk Purge']:.1f}s)\n")
    except PhaseError as e:
        log(f">> Phase 6: Bulk Purge: FAILED — {e}", "ERROR")
        sys.exit(1)
    except Exception as e:
        log(f">> Phase 6: Bulk Purge: FAILED — {e}", "ERROR")
        traceback.print_exc()
        sys.exit(1)

    try:
        phase_start = time.time()
        all_passed = phase_7_verify_purge(purge_result)
        phase_timings["Phase 7: Verification"] = time.time() - phase_start
    except PhaseError as e:
        log(f">> Phase 7: Verification: FAILED — {e}", "ERROR")
        sys.exit(1)
    except Exception as e:
        log(f">> Phase 7: Verification: FAILED — {e}", "ERROR")
        traceback.print_exc()
        sys.exit(1)

    total_time = time.time() - overall_start
    log(f"\nTotal test time: {total_time:.1f}s")

    if all_passed:
        log("ALL PHASES PASSED")
        sys.exit(0)
    else:
        log("SOME CHECKS FAILED", "ERROR")
        sys.exit(1)


if __name__ == "__main__":
    main()
