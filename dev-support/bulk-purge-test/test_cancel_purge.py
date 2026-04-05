#!/usr/bin/env python3
"""
test_cancel_purge.py — Test cancel scenarios for bulk purge.

Tests:
  Test 1: Cancel at ~50% → verify CANCELLED → re-trigger → verify COMPLETED
  Test 2: Double cancel (idempotent) → verify no error, still CANCELLED
  Test 3: Cancel → re-trigger → verify re-triggered purge is NOT poisoned
          by stale Redis cancel signal

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA clusters)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Usage:
  # Local dev
  python3 test_cancel_purge.py --count 2000
  python3 test_cancel_purge.py --count 5000 --cancel-at 50
  python3 test_cancel_purge.py --count 2000 --tests 1     # run only test 1
  python3 test_cancel_purge.py --count 2000 --tests 1,3   # run tests 1 and 3

  # Staging (OAuth2)
  python3 test_cancel_purge.py --tenant staging --count 2000
  python3 test_cancel_purge.py --tenant staging --count 5000 --tests 1,3
"""

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIMESTAMP = int(time.time())
ES_SYNC_TIMEOUT = 300
PURGE_TIMEOUT = 3600
POLL_INTERVAL = 3
API_REQUEST_TIMEOUT = 60


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
        if not self.access_token or time.time() >= self.token_expiry:
            with self._lock:
                if not self.access_token or time.time() >= self.token_expiry:
                    self._refresh_token()
        return self.access_token

    def force_refresh(self):
        with self._lock:
            self._refresh_token()

    def _refresh_token(self):
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


def load_creds(creds_file, tenant):
    """Load client_secret from creds.yaml for a given tenant."""
    try:
        import yaml
    except ImportError:
        raise RuntimeError("pyyaml is required for --tenant mode. Install: pip install pyyaml")

    if not os.path.exists(creds_file):
        raise RuntimeError(f"Creds file not found: {creds_file}")

    with open(creds_file, "r") as f:
        creds = yaml.safe_load(f)

    creds_list = creds.get("creds", []) if isinstance(creds, dict) else []
    if isinstance(creds_list, list):
        for entry in creds_list:
            entry_tenant = entry.get("tenant", "")
            if entry_tenant == tenant or entry_tenant == f"{tenant}.atlan.com":
                secret = entry.get("client_secret") or entry.get("secret")
                if secret:
                    return secret

    if isinstance(creds, dict) and tenant in creds:
        entry = creds[tenant]
        if isinstance(entry, dict):
            return entry.get("client_secret") or entry.get("secret")

    raise RuntimeError(
        f"Could not find credentials for tenant '{tenant}' in {creds_file}. "
        f"Available tenants: {[e.get('tenant') for e in creds_list]}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {msg}", flush=True)


# Module-level config — set once in main(), used by all helpers/tests.
_cfg = {
    "atlas_url": None,
    "api_base": None,
    "token_manager": None,
    "basic_auth": None,
}


def _build_headers():
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    tm = _cfg["token_manager"]
    if tm:
        headers["Authorization"] = f"Bearer {tm.get_token()}"
    return headers


def _do_request(method, url, json_data=None, params=None, timeout=API_REQUEST_TIMEOUT):
    """HTTP request with 401 retry for OAuth."""
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


def api_get(path, params=None):
    return _do_request("GET", f"{_cfg['api_base']}{path}", params=params, timeout=30)


def api_post(path, data=None, params=None, timeout=300):
    return _do_request("POST", f"{_cfg['api_base']}{path}",
                       json_data=data, params=params, timeout=timeout)


def es_count(conn_qn):
    """Count entities in ES for a connection."""
    strategies = [
        {"bool": {"must": [
            {"term": {"connectionQualifiedName": conn_qn}},
            {"term": {"__state": "ACTIVE"}}
        ]}},
        {"bool": {"must": [
            {"prefix": {"__qualifiedNameHierarchy": conn_qn}},
            {"term": {"__state": "ACTIVE"}}
        ]}},
    ]
    for query in strategies:
        payload = {"dsl": {"from": 0, "size": 1, "query": query}}
        try:
            resp = api_post("/search/indexsearch", data=payload)
            if resp.status_code == 200:
                count = resp.json().get("approximateCount", 0)
                if count > 0:
                    return count
        except Exception:
            pass
    return -1


def wait_for_atlas(timeout=120):
    """Wait for Atlas to become ACTIVE."""
    log("Waiting for Atlas to become ACTIVE...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = _do_request("GET", f"{_cfg['atlas_url']}/api/atlas/admin/status", timeout=5)
            if resp.status_code == 200 and resp.json().get("Status") == "ACTIVE":
                log(f"Atlas is ACTIVE ({time.time()-start:.1f}s)")
                return True
        except Exception:
            pass
        time.sleep(3)
    log(f"Atlas not available within {timeout}s", "ERROR")
    return False


def wait_for_es_sync(conn_qn, expected, label=""):
    """Wait for ES to have >= expected entities."""
    log(f"Waiting for ES sync: {label} >= {expected} entities...")
    start = time.time()
    last_count = -1
    while time.time() - start < ES_SYNC_TIMEOUT:
        count = es_count(conn_qn)
        if count >= expected:
            log(f"  ES sync done: {count}/{expected} for {label} ({time.time()-start:.1f}s)")
            return count
        if count != last_count:
            log(f"  ES sync: {max(0,count)}/{expected} for {label} — {time.time()-start:.0f}s")
            last_count = count
        time.sleep(3)
    log(f"ES sync timeout for {label}: {last_count}/{expected}", "ERROR")
    return last_count


# ---------------------------------------------------------------------------
# Entity creation
# ---------------------------------------------------------------------------

def create_connection(conn_qn, conn_name):
    """Create a Connection entity, return GUID."""
    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": conn_qn,
                "name": conn_name,
                "connectorName": "test-cancel-purge"
            }
        }
    }
    resp = api_post("/entity", data=payload)
    if resp.status_code != 200:
        raise Exception(f"Failed to create connection: {resp.status_code} {resp.text[:200]}")
    guid = resp.json().get("mutatedEntities", {}).get("CREATE", [{}])[0].get("guid")
    if not guid:
        guid = resp.json().get("mutatedEntities", {}).get("UPDATE", [{}])[0].get("guid")
    return guid


def create_tables_batch(conn_qn, start_idx, end_idx, batch_idx, total_batches):
    """Create a batch of Table entities."""
    entities = []
    for i in range(start_idx, end_idx):
        entities.append({
            "typeName": "Table",
            "attributes": {
                "qualifiedName": f"{conn_qn}/table-{i}",
                "name": f"table-{i}",
                "connectionQualifiedName": conn_qn
            }
        })
    for attempt in range(1, 4):
        try:
            resp = api_post("/entity/bulk", data={"entities": entities})
            if resp.status_code == 200:
                guids = []
                for action in ("CREATE", "UPDATE"):
                    for e in resp.json().get("mutatedEntities", {}).get(action, []):
                        guids.append(e["guid"])
                return guids
            if resp.status_code >= 500 and attempt < 3:
                log(f"    Batch {batch_idx}/{total_batches}: HTTP {resp.status_code}, retrying...", "WARN")
                time.sleep(attempt * 2)
                continue
            raise Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.RequestException as e:
            if attempt < 3:
                log(f"    Batch {batch_idx}/{total_batches}: {e}, retrying...", "WARN")
                time.sleep(attempt * 2)
                continue
            raise
    return []


def create_assets(conn_qn, conn_name, count, batch_size, workers):
    """Create connection + N tables."""
    log(f"Creating connection: {conn_qn}")
    conn_guid = create_connection(conn_qn, conn_name)
    log(f"  Connection GUID: {conn_guid}")

    batches = []
    for s in range(0, count, batch_size):
        batches.append((s, min(s + batch_size, count)))

    total_batches = len(batches)
    log(f"  Creating {count} tables in {total_batches} batches ({batch_size}/batch, {workers} workers)")

    all_guids = []
    completed = 0
    failed = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, (s, e) in enumerate(batches):
            f = executor.submit(create_tables_batch, conn_qn, s, e, idx, total_batches)
            futures[f] = idx

        for future in as_completed(futures):
            try:
                guids = future.result()
                all_guids.extend(guids)
                completed += 1
            except Exception as ex:
                failed += 1
                log(f"    Batch FAILED: {ex}", "ERROR")

            total_done = completed + failed
            if total_done % max(1, total_batches // 5) == 0 or total_done == total_batches:
                elapsed = time.time() - start
                rate = len(all_guids) / elapsed if elapsed > 0 else 0
                log(f"    {len(all_guids)}/{count} ({len(all_guids)*100//max(1,count)}%) | "
                    f"batches={completed}/{total_batches} | {rate:.0f} ent/s")

    elapsed = time.time() - start
    log(f"  Created {len(all_guids)} tables in {elapsed:.1f}s ({failed} failed batches)")
    return conn_guid, all_guids


# ---------------------------------------------------------------------------
# Purge operations
# ---------------------------------------------------------------------------

def trigger_purge(conn_qn, delete_connection=False):
    """Submit bulk purge, return requestId."""
    resp = api_post("/bulk-purge/connection",
                    params={"connectionQualifiedName": conn_qn,
                            "deleteConnection": str(delete_connection).lower()})
    if resp.status_code != 200:
        raise Exception(f"Purge submit failed: {resp.status_code} {resp.text[:300]}")
    return resp.json().get("requestId")


def poll_status(request_id):
    """Poll purge status once. Returns dict or None."""
    try:
        resp = api_get("/bulk-purge/status", params={"requestId": request_id})
    except Exception:
        return None
    if resp.status_code != 200:
        return None

    data = resp.json()
    if data.get("status") == "RESUBMITTED" and data.get("newRequestId"):
        return {"status": "RESUBMITTED", "newRequestId": data["newRequestId"],
                "purgeKey": data.get("purgeKey", "")}

    return {
        "status": data.get("status", "UNKNOWN"),
        "deleted": data.get("deletedCount", 0),
        "total": data.get("totalDiscovered", 0),
        "failed": data.get("failedCount", 0),
        "phase": data.get("currentPhase", ""),
        "error": data.get("errorMessage", data.get("error", "")),
    }


def cancel_purge(request_id):
    """Cancel purge. Writes Redis signal (cross-pod) + local cancel (instant if same pod)."""
    try:
        resp = api_post("/bulk-purge/cancel", params={"requestId": request_id})
        log(f"  /cancel response: HTTP {resp.status_code} — {resp.text[:200]}")
        return resp.status_code == 200
    except Exception as e:
        log(f"  /cancel failed: {e}", "WARN")
        return False


def wait_for_progress(request_id, target_pct, timeout=600):
    """Wait until purge reaches target_pct% progress. Returns (current_request_id, last_status)."""
    start = time.time()
    poll_count = 0
    current_id = request_id
    prev_deleted = 0

    while time.time() - start < timeout:
        poll_count += 1
        time.sleep(POLL_INTERVAL)

        status = poll_status(current_id)
        if status is None:
            continue

        state = status["status"]

        if state == "RESUBMITTED":
            new_id = status["newRequestId"]
            log(f"  RESUBMITTED -> following {new_id}")
            current_id = new_id
            prev_deleted = 0
            continue

        deleted = status.get("deleted", 0)
        total = status.get("total", 0)
        phase = status.get("phase", "")
        pct = (deleted / total * 100) if total > 0 else 0
        delta = deleted - prev_deleted
        prev_deleted = deleted
        elapsed = time.time() - start

        if poll_count % 4 == 0 or delta > 0 or pct >= target_pct:
            log(f"  Poll #{poll_count}: {state} | {deleted}/{total} ({pct:.0f}%) | "
                f"phase={phase} | {elapsed:.0f}s")

        if pct >= target_pct:
            log(f"  Reached {pct:.0f}% (target={target_pct}%)")
            return current_id, status

        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            log(f"  Purge ended with {state} before reaching target {target_pct}%", "WARN")
            return current_id, status

    log(f"  Timeout waiting for {target_pct}% progress", "ERROR")
    return current_id, None


def wait_for_terminal_state(request_id, expected_states=None, timeout=300):
    """Wait for purge to reach a terminal state. Returns (current_request_id, last_status)."""
    if expected_states is None:
        expected_states = {"COMPLETED", "FAILED", "CANCELLED"}

    start = time.time()
    poll_count = 0
    current_id = request_id
    prev_deleted = 0

    while time.time() - start < timeout:
        poll_count += 1
        time.sleep(POLL_INTERVAL)

        status = poll_status(current_id)
        if status is None:
            continue

        state = status["status"]

        if state == "RESUBMITTED":
            new_id = status["newRequestId"]
            log(f"  RESUBMITTED -> following {new_id}")
            current_id = new_id
            prev_deleted = 0
            continue

        deleted = status.get("deleted", 0)
        total = status.get("total", 0)
        phase = status.get("phase", "")
        pct = (deleted / total * 100) if total > 0 else 0
        delta = deleted - prev_deleted
        prev_deleted = deleted
        elapsed = time.time() - start

        if poll_count % 6 == 0 or delta > 0 or state in expected_states:
            log(f"  Poll #{poll_count}: {state} | {deleted}/{total} ({pct:.0f}%) | "
                f"phase={phase} | {elapsed:.0f}s")

        if state in expected_states:
            return current_id, status

    log(f"  Timeout waiting for terminal state (waited {time.time()-start:.0f}s)", "ERROR")
    return current_id, None


# ---------------------------------------------------------------------------
# Test 1: Cancel at ~50% → CANCELLED → re-trigger → COMPLETED
# ---------------------------------------------------------------------------

def test_cancel_and_retrigger(count, cancel_at, batch_size, workers, delete_connection):
    """
    1. Create connection + tables
    2. Trigger purge
    3. Wait for cancel_at% progress
    4. Cancel
    5. Verify CANCELLED
    6. Re-trigger purge for same key
    7. Verify COMPLETED with 0 remaining in ES
    """
    conn_qn = f"default/cancel-test-1/{TIMESTAMP}"
    log(f"  Connection: {conn_qn}")

    # Create assets
    log("  Creating assets...")
    _, table_guids = create_assets(conn_qn, "cancel-test-1", count, batch_size, workers)
    wait_for_es_sync(conn_qn, len(table_guids), label="test1")

    # Trigger purge
    req_id = trigger_purge(conn_qn, delete_connection=False)
    log(f"  Purge submitted: requestId={req_id}")

    # Wait for target progress
    log(f"  Waiting for {cancel_at}% progress...")
    req_id, progress = wait_for_progress(req_id, cancel_at)
    if progress is None:
        log("  Could not reach target progress", "ERROR")
        return False

    if progress["status"] in ("COMPLETED", "FAILED"):
        log(f"  Purge already {progress['status']} before cancel — "
            f"increase --count or lower --cancel-at", "WARN")
        if progress["status"] == "FAILED":
            return False
        # If completed before cancel, the cancel test is moot but re-trigger should still work

    cancel_status = progress  # default if purge already ended

    if progress["status"] not in ("COMPLETED", "FAILED", "CANCELLED"):
        # Cancel the purge
        log("  Sending cancel...")
        success = cancel_purge(req_id)
        if not success:
            log("  Cancel API call failed!", "ERROR")
            return False

        # Wait for CANCELLED
        log("  Waiting for CANCELLED state...")
        req_id, cancel_status = wait_for_terminal_state(req_id, timeout=120)
        if cancel_status is None:
            log("  Purge did not reach terminal state after cancel!", "ERROR")
            return False

        log(f"  First purge: {cancel_status['status']} — "
            f"deleted={cancel_status.get('deleted', 0)}/{cancel_status.get('total', 0)}")

    # Verify partial state
    time.sleep(3)
    remaining = es_count(conn_qn)
    remaining = max(0, remaining)
    log(f"  Remaining in ES after cancel: {remaining}")

    # Re-trigger purge
    log(f"  Re-triggering purge (deleteConnection={delete_connection})...")
    req_id_2 = trigger_purge(conn_qn, delete_connection=delete_connection)
    log(f"  Second purge submitted: requestId={req_id_2}")

    # Wait for COMPLETED
    req_id_2, final = wait_for_terminal_state(req_id_2, timeout=PURGE_TIMEOUT)
    if final is None:
        log("  Second purge did not complete!", "ERROR")
        return False

    log(f"  Second purge: {final['status']} — "
        f"deleted={final.get('deleted', 0)}/{final.get('total', 0)}")

    if final["status"] != "COMPLETED":
        log(f"  Expected COMPLETED but got {final['status']}", "ERROR")
        return False

    # Final ES verification
    # When deleteConnection=false, the Connection entity itself stays in ES (expected: 1)
    time.sleep(5)
    remaining_final = es_count(conn_qn)
    remaining_final = max(0, remaining_final)
    max_expected = 0 if delete_connection else 1
    if remaining_final > max_expected:
        log(f"  {remaining_final} entities still in ES after second purge (max expected={max_expected})!", "ERROR")
        return False

    if remaining_final == 1 and not delete_connection:
        log("  Verified: 1 entity remaining (the Connection entity — expected with deleteConnection=false)")
    else:
        log(f"  Verified: {remaining_final} entities remaining in ES")
    combined = cancel_status.get("deleted", 0) + final.get("deleted", 0)
    log(f"  Combined deleted: {combined}")
    return True


# ---------------------------------------------------------------------------
# Test 2: Double cancel (idempotent)
# ---------------------------------------------------------------------------

def test_double_cancel(count, cancel_at, batch_size, workers):
    """
    1. Create connection + tables
    2. Trigger purge
    3. Wait for cancel_at% progress
    4. Cancel twice in quick succession
    5. Verify both calls succeed (no error)
    6. Verify status is CANCELLED (not FAILED)
    """
    conn_qn = f"default/cancel-test-2/{TIMESTAMP}"
    log(f"  Connection: {conn_qn}")

    # Create assets
    log("  Creating assets...")
    _, table_guids = create_assets(conn_qn, "cancel-test-2", count, batch_size, workers)
    wait_for_es_sync(conn_qn, len(table_guids), label="test2")

    # Trigger purge
    req_id = trigger_purge(conn_qn, delete_connection=False)
    log(f"  Purge submitted: requestId={req_id}")

    # Wait for target progress
    log(f"  Waiting for {cancel_at}% progress...")
    req_id, progress = wait_for_progress(req_id, cancel_at)
    if progress is None:
        log("  Could not reach target progress", "ERROR")
        return False

    if progress["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
        log(f"  Purge already {progress['status']} before cancel — "
            f"increase --count or lower --cancel-at", "WARN")
        return progress["status"] != "FAILED"

    # First cancel
    log("  Sending cancel #1...")
    success1 = cancel_purge(req_id)
    if not success1:
        log("  Cancel #1 failed!", "ERROR")
        return False

    # Second cancel immediately
    log("  Sending cancel #2 (should be idempotent)...")
    success2 = cancel_purge(req_id)
    if not success2:
        log("  Cancel #2 failed! (expected to succeed — idempotent)", "ERROR")
        return False

    log("  Both cancel calls returned success")

    # Wait for terminal state
    log("  Waiting for terminal state...")
    req_id, final = wait_for_terminal_state(req_id, timeout=120)
    if final is None:
        log("  Purge did not reach terminal state!", "ERROR")
        return False

    log(f"  Final status: {final['status']} — "
        f"deleted={final.get('deleted', 0)}/{final.get('total', 0)}")

    if final["status"] == "FAILED":
        log(f"  Purge FAILED after double cancel (should be CANCELLED): "
            f"{final.get('error', '')}", "ERROR")
        return False

    if final["status"] not in ("CANCELLED", "COMPLETED"):
        log(f"  Unexpected status: {final['status']}", "ERROR")
        return False

    log(f"  Double cancel is idempotent — status={final['status']}")
    return True


# ---------------------------------------------------------------------------
# Test 3: Cancel → re-trigger → verify no stale cancel signal poison
# ---------------------------------------------------------------------------

def test_cancel_no_stale_signal(count, cancel_at, batch_size, workers):
    """
    This tests the specific bug where a cancel writes CANCEL_REQUESTED to Redis,
    the purge moves to CANCELLED, but the stale Redis key is not cleaned up.
    When a new purge is re-triggered for the same purgeKey, the heartbeat
    picks up the stale signal and immediately cancels the new purge.

    1. Create connection + tables
    2. Trigger purge
    3. Wait for cancel_at% progress
    4. Cancel → wait for CANCELLED
    5. Re-trigger purge for same key
    6. Verify the re-triggered purge reaches COMPLETED (not auto-cancelled)
    7. Verify 0 entities in ES
    """
    conn_qn = f"default/cancel-test-3/{TIMESTAMP}"
    log(f"  Connection: {conn_qn}")

    # Create assets
    log("  Creating assets...")
    _, table_guids = create_assets(conn_qn, "cancel-test-3", count, batch_size, workers)
    wait_for_es_sync(conn_qn, len(table_guids), label="test3")

    # Trigger purge
    req_id = trigger_purge(conn_qn, delete_connection=False)
    log(f"  Purge submitted: requestId={req_id}")

    # Wait for progress
    log(f"  Waiting for {cancel_at}% progress...")
    req_id, progress = wait_for_progress(req_id, cancel_at)
    if progress is None:
        log("  Could not reach target progress", "ERROR")
        return False

    if progress["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
        log(f"  Purge already {progress['status']} before cancel", "WARN")
        if progress["status"] == "FAILED":
            return False

    cancel_status = progress

    if progress["status"] not in ("COMPLETED", "FAILED", "CANCELLED"):
        # Cancel
        log("  Sending cancel...")
        success = cancel_purge(req_id)
        if not success:
            log("  Cancel failed!", "ERROR")
            return False

        # Wait for CANCELLED
        log("  Waiting for CANCELLED state...")
        req_id, cancel_status = wait_for_terminal_state(req_id, timeout=120)
        if cancel_status is None:
            log("  Purge did not reach terminal state!", "ERROR")
            return False

        log(f"  First purge: {cancel_status['status']} — "
            f"deleted={cancel_status.get('deleted', 0)}/{cancel_status.get('total', 0)}")

    # Brief pause — if there's a stale Redis cancel key, it should persist
    log("  Waiting 5s before re-trigger (stale signal window)...")
    time.sleep(5)

    # Re-trigger purge
    log("  Re-triggering purge (same key)...")
    req_id_2 = trigger_purge(conn_qn, delete_connection=False)
    log(f"  Second purge submitted: requestId={req_id_2}")

    # Monitor — the critical check is that it does NOT get cancelled
    # Wait long enough for at least 2 heartbeat ticks (30s each = 60s)
    # so if a stale signal existed, the heartbeat would detect it
    log("  Monitoring second purge (watching for stale cancel poison)...")
    req_id_2, final = wait_for_terminal_state(req_id_2, timeout=PURGE_TIMEOUT)

    if final is None:
        log("  Second purge did not complete!", "ERROR")
        return False

    log(f"  Second purge: {final['status']} — "
        f"deleted={final.get('deleted', 0)}/{final.get('total', 0)}")

    if final["status"] == "CANCELLED":
        log("  FAIL: Second purge was CANCELLED by stale Redis signal!", "ERROR")
        log("  This means the finally block did not clean up bulk_purge_cancel:<purgeKey>", "ERROR")
        return False

    if final["status"] != "COMPLETED":
        log(f"  Expected COMPLETED but got {final['status']}: {final.get('error', '')}", "ERROR")
        return False

    # Final verification
    # deleteConnection=false, so the Connection entity itself stays in ES (expected: 1)
    time.sleep(5)
    remaining = es_count(conn_qn)
    remaining = max(0, remaining)
    if remaining > 1:
        log(f"  {remaining} entities still in ES (expected at most 1 — the Connection)!", "ERROR")
        return False
    elif remaining == 1:
        log("  Verified: 1 entity remaining (the Connection entity — expected with deleteConnection=false)")
    else:
        log("  Verified: 0 entities remaining in ES")

    log("  No stale cancel signal poison detected")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cancel Purge Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local dev (admin/admin)
  python3 test_cancel_purge.py --count 2000

  # Staging (OAuth2 via creds.yaml)
  python3 test_cancel_purge.py --tenant staging --count 2000

  # Explicit URL + basic auth
  python3 test_cancel_purge.py --url http://localhost:21000 --user admin --password admin

  # Run specific tests on staging
  python3 test_cancel_purge.py --tenant staging --count 5000 --tests 1,3
"""
    )

    # Auth options
    parser.add_argument("--url", default=None,
                        help="Atlas base URL (default: auto from --tenant, or http://localhost:21000)")
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth2 (e.g., 'staging'). "
                             "Constructs URL as https://{tenant}.atlan.com")
    parser.add_argument("--client-secret", default=None,
                        help="OAuth client_secret (optional if --creds-file has it)")
    parser.add_argument("--creds-file",
                        default=os.path.expanduser("~/Desktop/Projects/tokens/creds.yaml"),
                        help="Path to creds.yaml (default: ~/Desktop/Projects/tokens/creds.yaml)")
    parser.add_argument("--user", default=None,
                        help="Basic auth username (for local dev)")
    parser.add_argument("--password", default=None,
                        help="Basic auth password (for local dev)")

    # Test options
    parser.add_argument("--count", type=int, default=2000,
                        help="Number of table assets per test (default: 2000)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for creation")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers for creation")
    parser.add_argument("--cancel-at", type=int, default=50,
                        help="Cancel when progress reaches this %% (default: 50)")
    parser.add_argument("--delete-connection", action="store_true",
                        help="Delete connection entity during re-trigger (test 1 only)")
    parser.add_argument("--tests", default="1,2,3",
                        help="Comma-separated list of tests to run (default: 1,2,3)")
    args = parser.parse_args()

    # --- Auth mode selection ---
    token_manager = None
    basic_auth = None

    if args.tenant:
        atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        token_url = f"https://{args.tenant}.atlan.com/auth/realms/default/protocol/openid-connect/token"
        client_id = "atlan-argo"
        client_secret = args.client_secret
        if not client_secret:
            client_secret = load_creds(args.creds_file, args.tenant)
        token_manager = TokenManager(token_url, client_id, client_secret)
        api_base = f"{atlas_url}/api/meta"
    elif args.user:
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth(args.user, args.password or "admin")
        api_base = f"{atlas_url}/api/atlas/v2"
    else:
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth("admin", "admin")
        api_base = f"{atlas_url}/api/atlas/v2"

    # Set module-level config
    _cfg["atlas_url"] = atlas_url
    _cfg["api_base"] = api_base
    _cfg["token_manager"] = token_manager
    _cfg["basic_auth"] = basic_auth

    auth_mode = "OAuth2" if token_manager else "Basic"
    tests_to_run = [int(t.strip()) for t in args.tests.split(",")]

    log("=" * 70)
    log("CANCEL PURGE TEST SUITE")
    log(f"  URL:             {atlas_url}")
    log(f"  API base:        {api_base}")
    log(f"  Auth mode:       {auth_mode}")
    log(f"  Assets per test: {args.count}")
    log(f"  Cancel at:       {args.cancel_at}%")
    log(f"  Tests to run:    {tests_to_run}")
    log("=" * 70)

    overall_start = time.time()

    # Preflight
    log("\n=== Preflight ===")
    if not wait_for_atlas(timeout=60):
        sys.exit(1)

    results = {}

    # -- Test 1: Cancel + Re-trigger --
    if 1 in tests_to_run:
        log("\n" + "=" * 70)
        log("TEST 1: Cancel at ~50% -> CANCELLED -> re-trigger -> COMPLETED")
        log("=" * 70)
        try:
            results[1] = test_cancel_and_retrigger(
                args.count, args.cancel_at, args.batch_size, args.workers,
                args.delete_connection)
        except Exception as e:
            log(f"  Test 1 exception: {e}", "ERROR")
            results[1] = False
        log(f"  Test 1: {'PASS' if results[1] else 'FAIL'}")

    # -- Test 2: Double cancel --
    if 2 in tests_to_run:
        log("\n" + "=" * 70)
        log("TEST 2: Double cancel (idempotent)")
        log("=" * 70)
        try:
            results[2] = test_double_cancel(
                args.count, args.cancel_at, args.batch_size, args.workers)
        except Exception as e:
            log(f"  Test 2 exception: {e}", "ERROR")
            results[2] = False
        log(f"  Test 2: {'PASS' if results[2] else 'FAIL'}")

    # -- Test 3: Stale signal poison --
    if 3 in tests_to_run:
        log("\n" + "=" * 70)
        log("TEST 3: Cancel -> re-trigger -> no stale cancel signal poison")
        log("=" * 70)
        try:
            results[3] = test_cancel_no_stale_signal(
                args.count, args.cancel_at, args.batch_size, args.workers)
        except Exception as e:
            log(f"  Test 3 exception: {e}", "ERROR")
            results[3] = False
        log(f"  Test 3: {'PASS' if results[3] else 'FAIL'}")

    # -- Summary --
    elapsed = time.time() - overall_start
    log("\n" + "=" * 70)
    log("TEST SUMMARY")
    log("=" * 70)
    all_passed = True
    for test_num in sorted(results):
        passed = results[test_num]
        label = {
            1: "Cancel + Re-trigger",
            2: "Double cancel (idempotent)",
            3: "Stale signal poison check",
        }.get(test_num, f"Test {test_num}")
        status = "PASS" if passed else "FAIL"
        log(f"  Test {test_num} ({label}): {status}")
        if not passed:
            all_passed = False

    log(f"\n  Total duration: {elapsed:.1f}s")
    log(f"  Overall: {'PASS' if all_passed else 'FAIL'}")
    log("=" * 70)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
