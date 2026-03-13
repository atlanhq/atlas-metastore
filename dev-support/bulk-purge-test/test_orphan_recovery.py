#!/usr/bin/env python3
"""
test_orphan_recovery.py — Test orphan recovery for bulk purge.

Validates that when Atlas crashes mid-purge, the orphan checker detects the
stalled purge, resubmits it, and eventually completes. Tests double-crash
recovery (crash -> resubmit -> crash again -> resubmit -> COMPLETED).

This tests Bugs 1, 3, 4 from the fix plan:
  - Bug 1: addToActivePurgeKeys missing in resubmitOrphanedPurge
  - Bug 3: Premature RESUBMITTED status on original requestId
  - Bug 4: Force-release vs Redisson watchdog race

How it works:
  1. Creates a Connection + N Table assets
  2. Triggers bulk purge
  3. Waits for some progress (~20%)
  4. Prompts you to kill Atlas (or kills it automatically with --auto-kill)
  5. Waits for Atlas to restart
  6. Monitors status — expects RESUBMITTED -> new requestId -> RUNNING
  7. Waits for some more progress (~20% of remaining)
  8. Prompts to kill Atlas again (second crash)
  9. Waits for Atlas to restart
  10. Monitors status — expects second RESUBMITTED -> COMPLETED

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Usage:
  # Local dev (manual kill — script will prompt you)
  python3 test_orphan_recovery.py --count 5000

  # Local dev (auto-kill — kills java Atlas process)
  python3 test_orphan_recovery.py --count 5000 --auto-kill

  # Staging (manual kill — you kill the pod)
  python3 test_orphan_recovery.py --tenant staging --count 5000

  # Single crash only
  python3 test_orphan_recovery.py --count 5000 --crashes 1
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIMESTAMP = int(time.time())
ES_SYNC_TIMEOUT = 300
PURGE_TIMEOUT = 3600
POLL_INTERVAL = 5
API_REQUEST_TIMEOUT = 60
ATLAS_RESTART_TIMEOUT = 300  # max time to wait for Atlas to come back
ORPHAN_CHECK_TIMEOUT = 600   # max time to wait for orphan checker to resubmit


# ---------------------------------------------------------------------------
# TokenManager
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


def is_atlas_up():
    try:
        resp = _do_request("GET", f"{_cfg['atlas_url']}/api/atlas/admin/status", timeout=5)
        return resp.status_code == 200 and resp.json().get("Status") == "ACTIVE"
    except Exception:
        return False


def wait_for_atlas(timeout=ATLAS_RESTART_TIMEOUT):
    log(f"Waiting for Atlas to become ACTIVE (timeout={timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        if is_atlas_up():
            elapsed = time.time() - start
            log(f"Atlas is ACTIVE ({elapsed:.1f}s)")
            return True
        time.sleep(3)
    log(f"Atlas not available within {timeout}s", "ERROR")
    return False


def wait_for_atlas_down(timeout=30):
    """Wait for Atlas to go down (after kill)."""
    log("Waiting for Atlas to go down...")
    start = time.time()
    while time.time() - start < timeout:
        if not is_atlas_up():
            log(f"Atlas is down ({time.time()-start:.1f}s)")
            return True
        time.sleep(1)
    log("Atlas did not go down within timeout", "WARN")
    return False


def wait_for_es_sync(conn_qn, expected, label=""):
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
    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": conn_qn,
                "name": conn_name,
                "connectorName": "test-orphan-recovery"
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
    from concurrent.futures import ThreadPoolExecutor, as_completed

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
    resp = api_post("/bulk-purge/connection",
                    params={"connectionQualifiedName": conn_qn,
                            "deleteConnection": str(delete_connection).lower()})
    if resp.status_code != 200:
        raise Exception(f"Purge submit failed: {resp.status_code} {resp.text[:300]}")
    return resp.json().get("requestId")


def poll_status(request_id):
    try:
        resp = api_get("/bulk-purge/status", params={"requestId": request_id})
    except Exception:
        return None
    if resp.status_code != 200:
        return None

    data = resp.json()
    if data.get("status") == "RESUBMITTED" and data.get("newRequestId"):
        return {"status": "RESUBMITTED", "newRequestId": data["newRequestId"],
                "purgeKey": data.get("purgeKey", ""),
                "resubmitCount": data.get("resubmitCount", 0)}

    return {
        "status": data.get("status", "UNKNOWN"),
        "deleted": data.get("deletedCount", 0),
        "total": data.get("totalDiscovered", 0),
        "failed": data.get("failedCount", 0),
        "phase": data.get("currentPhase", ""),
        "error": data.get("errorMessage", data.get("error", "")),
        "resubmitCount": data.get("resubmitCount", 0),
    }


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


def wait_for_resubmit_and_running(request_id, timeout=ORPHAN_CHECK_TIMEOUT):
    """Wait for orphan checker to resubmit a crashed purge and the new purge to start RUNNING.
    Returns (new_request_id, last_status) once the resubmitted purge is RUNNING.
    Also returns early on terminal states (COMPLETED/FAILED/CANCELLED)."""
    start = time.time()
    poll_count = 0
    current_id = request_id
    saw_resubmit = False

    while time.time() - start < timeout:
        poll_count += 1
        time.sleep(POLL_INTERVAL)

        status = poll_status(current_id)
        if status is None:
            if poll_count % 6 == 0:
                log(f"  Poll #{poll_count}: No response (Atlas down?) — {time.time()-start:.0f}s")
            continue

        state = status["status"]

        if state == "RESUBMITTED":
            new_id = status["newRequestId"]
            resubmit_count = status.get("resubmitCount", "?")
            log(f"  RESUBMITTED (count={resubmit_count}) -> following {new_id}")
            current_id = new_id
            saw_resubmit = True
            continue

        deleted = status.get("deleted", 0)
        total = status.get("total", 0)
        phase = status.get("phase", "")
        pct = (deleted / total * 100) if total > 0 else 0
        elapsed = time.time() - start

        if poll_count % 4 == 0 or state in ("RUNNING", "COMPLETED", "FAILED", "CANCELLED"):
            log(f"  Poll #{poll_count}: {state} | {deleted}/{total} ({pct:.0f}%) | "
                f"phase={phase} | resubmits={status.get('resubmitCount', 0)} | {elapsed:.0f}s")

        # Return once resubmitted purge is RUNNING (so caller can kill again)
        if saw_resubmit and state == "RUNNING":
            log(f"  Resubmitted purge is RUNNING (new requestId={current_id})")
            return current_id, status

        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            return current_id, status

    log(f"  Timeout waiting for resubmit ({time.time()-start:.0f}s)", "ERROR")
    return current_id, None


def wait_for_terminal(request_id, timeout=PURGE_TIMEOUT):
    """Wait for purge to reach a terminal state. Follows RESUBMITTED chain."""
    start = time.time()
    poll_count = 0
    current_id = request_id
    prev_deleted = 0

    while time.time() - start < timeout:
        poll_count += 1
        time.sleep(POLL_INTERVAL)

        status = poll_status(current_id)
        if status is None:
            if poll_count % 6 == 0:
                log(f"  Poll #{poll_count}: No response (Atlas down?) — {time.time()-start:.0f}s")
            continue

        state = status["status"]

        if state == "RESUBMITTED":
            new_id = status["newRequestId"]
            resubmit_count = status.get("resubmitCount", "?")
            log(f"  RESUBMITTED (count={resubmit_count}) -> following {new_id}")
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

        if poll_count % 4 == 0 or delta > 0 or state in ("COMPLETED", "FAILED", "CANCELLED"):
            log(f"  Poll #{poll_count}: {state} | {deleted}/{total} ({pct:.0f}%) | "
                f"phase={phase} | resubmits={status.get('resubmitCount', 0)} | {elapsed:.0f}s")

        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            return current_id, status

    log(f"  Timeout waiting for terminal state ({time.time()-start:.0f}s)", "ERROR")
    return current_id, None


# ---------------------------------------------------------------------------
# Kill Atlas
# ---------------------------------------------------------------------------

def kill_atlas_local():
    """Kill local Atlas process (java org.apache.atlas.Atlas)."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "org.apache.atlas.Atlas"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")
        pids = [p.strip() for p in pids if p.strip()]
        if not pids:
            log("No Atlas process found to kill", "WARN")
            return False

        for pid in pids:
            log(f"Killing Atlas PID {pid} (SIGKILL)...")
            os.kill(int(pid), signal.SIGKILL)

        return True
    except Exception as e:
        log(f"Failed to kill Atlas: {e}", "ERROR")
        return False


def prompt_kill_atlas(crash_num):
    """Prompt user to manually kill Atlas."""
    log("=" * 70)
    log(f">>> ACTION REQUIRED: Kill Atlas now! (crash #{crash_num})")
    log(">>>")
    log(">>> For local dev:  kill -9 $(pgrep -f 'org.apache.atlas.Atlas')")
    log(">>> For staging:    kubectl delete pod <atlas-pod-name>")
    log(">>>")
    log(">>> Press ENTER after killing Atlas...")
    log("=" * 70)
    try:
        input()
    except EOFError:
        pass


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

def run_test(args):
    conn_qn = f"default/orphan-test/{TIMESTAMP}"
    conn_name = "orphan-recovery-test"
    num_crashes = args.crashes
    kill_at_pct = args.kill_at

    log("=" * 70)
    log("ORPHAN RECOVERY TEST")
    log(f"  URL:          {_cfg['atlas_url']}")
    log(f"  API base:     {_cfg['api_base']}")
    log(f"  Connection:   {conn_qn}")
    log(f"  Asset count:  {args.count}")
    log(f"  Crashes:      {num_crashes}")
    log(f"  Kill at:      {kill_at_pct}% progress")
    log(f"  Auto-kill:    {args.auto_kill}")
    log("=" * 70)

    # Step 1: Preflight
    log("\n--- Step 1: Preflight ---")
    if not wait_for_atlas(timeout=60):
        log("Atlas is not available", "ERROR")
        return False

    # Step 2: Create assets
    log(f"\n--- Step 2: Create {args.count} assets ---")
    conn_guid, table_guids = create_assets(
        conn_qn, conn_name, args.count, args.batch_size, args.workers)
    actual_count = len(table_guids)
    log(f"Created {actual_count} tables")

    wait_for_es_sync(conn_qn, actual_count, label="test assets")

    # Step 3: Trigger purge
    log("\n--- Step 3: Trigger bulk purge ---")
    request_id = trigger_purge(conn_qn, delete_connection=False)
    log(f"Purge submitted: requestId={request_id}")
    original_request_id = request_id
    current_request_id = request_id

    for crash_num in range(1, num_crashes + 1):
        log(f"\n{'='*70}")
        log(f"--- Crash #{crash_num}/{num_crashes} ---")
        log(f"{'='*70}")

        # Wait for some progress before killing
        log(f"\nWaiting for {kill_at_pct}% progress before crash #{crash_num}...")
        current_request_id, progress = wait_for_progress(
            current_request_id, kill_at_pct, timeout=600)

        if progress is None:
            log(f"Could not reach {kill_at_pct}% progress before crash #{crash_num}", "ERROR")
            return False

        if progress["status"] in ("COMPLETED",):
            log(f"Purge already COMPLETED before crash #{crash_num} — test passed (nothing to crash)")
            break

        if progress["status"] in ("FAILED", "CANCELLED"):
            log(f"Purge ended with {progress['status']} — cannot continue", "ERROR")
            return False

        deleted_before_crash = progress.get("deleted", 0)
        total = progress.get("total", 0)
        log(f"Progress before crash #{crash_num}: {deleted_before_crash}/{total}")

        # Kill Atlas
        if args.auto_kill:
            log(f"\nAuto-killing Atlas (crash #{crash_num})...")
            if not kill_atlas_local():
                log("Auto-kill failed, falling back to manual prompt", "WARN")
                prompt_kill_atlas(crash_num)
            else:
                wait_for_atlas_down(timeout=10)
        else:
            prompt_kill_atlas(crash_num)

        # Verify Atlas is down
        if is_atlas_up():
            log("Atlas is still up — waiting for it to go down...", "WARN")
            time.sleep(5)
            if is_atlas_up():
                log("Atlas is still up. Did you kill it?", "ERROR")
                return False

        log(f"Atlas is down. Waiting for restart...")

        # Wait for Atlas to come back
        if not wait_for_atlas(timeout=ATLAS_RESTART_TIMEOUT):
            log("Atlas did not restart within timeout", "ERROR")
            return False

        # Wait for orphan checker to detect stale heartbeat and resubmit
        log(f"\nWaiting for orphan checker to detect and resubmit (up to {ORPHAN_CHECK_TIMEOUT}s)...")
        log("(Orphan checker runs every ~5min, detects stale heartbeat after ~5min)")

        is_last_crash = (crash_num == num_crashes)

        if is_last_crash:
            # Last crash: wait all the way to COMPLETED
            current_request_id, status = wait_for_terminal(
                current_request_id, timeout=ORPHAN_CHECK_TIMEOUT)
        else:
            # Not last crash: wait only until resubmitted purge is RUNNING, then loop back to kill again
            current_request_id, status = wait_for_resubmit_and_running(
                current_request_id, timeout=ORPHAN_CHECK_TIMEOUT)

        if status is None:
            log(f"Purge did not recover after crash #{crash_num}", "ERROR")
            return False

        if status["status"] == "COMPLETED":
            log(f"Purge COMPLETED after crash #{crash_num} recovery")
            break

        if status["status"] == "FAILED":
            log(f"Purge FAILED after crash #{crash_num}: {status.get('error', '')}", "ERROR")
            return False

        if status["status"] == "CANCELLED":
            log(f"Purge CANCELLED after crash #{crash_num} (stale cancel signal?)", "ERROR")
            return False

        # Resubmitted and RUNNING — continue to next crash
        deleted_after = status.get("deleted", 0)
        log(f"After crash #{crash_num} recovery: {status['status']} | "
            f"deleted={deleted_after}/{status.get('total', 0)} | "
            f"resubmits={status.get('resubmitCount', 0)}")
        log(f"Proceeding to crash #{crash_num + 1}...")

    # Step 4: Verify
    log("\n--- Final Verification ---")
    time.sleep(5)
    remaining = es_count(conn_qn)
    remaining = max(0, remaining)

    # deleteConnection=false, so Connection entity stays (expected: 1)
    if remaining > 1:
        log(f"FAIL: {remaining} entities still in ES (expected at most 1 — the Connection)", "ERROR")
        return False
    elif remaining == 1:
        log("Verified: 1 entity remaining (the Connection entity — expected)")
    else:
        log("Verified: 0 entities remaining in ES")

    # Check original requestId chain
    log("\n--- Request chain verification ---")
    chain_id = original_request_id
    chain = []
    for _ in range(10):  # max chain depth
        status = poll_status(chain_id)
        if status is None:
            log(f"  {chain_id}: (not found)")
            break
        chain.append((chain_id, status["status"]))
        log(f"  {chain_id}: {status['status']}")
        if status["status"] == "RESUBMITTED" and "newRequestId" in status:
            chain_id = status["newRequestId"]
        else:
            break

    log(f"\nRequest chain: {' -> '.join(f'{s}({st})' for s, st in chain)}")

    # Verify chain integrity
    terminal_states = [s for _, s in chain if s in ("COMPLETED", "FAILED", "CANCELLED")]
    if not terminal_states:
        log("FAIL: No terminal state found in request chain", "ERROR")
        return False
    if terminal_states[-1] != "COMPLETED":
        log(f"FAIL: Final state is {terminal_states[-1]}, expected COMPLETED", "ERROR")
        return False

    resubmit_count = sum(1 for _, s in chain if s == "RESUBMITTED")
    log(f"Resubmit chain length: {resubmit_count}")

    log("\n" + "=" * 70)
    log("ORPHAN RECOVERY TEST: PASS")
    log("=" * 70)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Orphan Recovery Test for Bulk Purge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local dev — manual kill (script prompts you)
  python3 test_orphan_recovery.py --count 5000

  # Local dev — auto kill (sends SIGKILL to Atlas process)
  python3 test_orphan_recovery.py --count 5000 --auto-kill

  # Staging — manual kill (you delete the pod)
  python3 test_orphan_recovery.py --tenant staging --count 5000

  # Single crash test
  python3 test_orphan_recovery.py --count 5000 --crashes 1

  # Custom kill point
  python3 test_orphan_recovery.py --count 10000 --kill-at 30
"""
    )

    # Auth
    parser.add_argument("--url", default=None,
                        help="Atlas base URL (auto from --tenant, or http://localhost:21000)")
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth2 (e.g., 'staging')")
    parser.add_argument("--client-secret", default=None,
                        help="OAuth client_secret (optional if --creds-file has it)")
    parser.add_argument("--creds-file",
                        default=os.path.expanduser("~/Desktop/Projects/tokens/creds.yaml"),
                        help="Path to creds.yaml")
    parser.add_argument("--user", default=None, help="Basic auth username")
    parser.add_argument("--password", default=None, help="Basic auth password")

    # Test options
    parser.add_argument("--count", type=int, default=5000,
                        help="Number of table assets (default: 5000)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for creation")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers for creation")
    parser.add_argument("--crashes", type=int, default=2,
                        help="Number of times to crash Atlas (default: 2)")
    parser.add_argument("--kill-at", type=int, default=20,
                        help="Kill Atlas when purge reaches this %% progress (default: 20)")
    parser.add_argument("--auto-kill", action="store_true",
                        help="Automatically kill local Atlas process (SIGKILL)")

    args = parser.parse_args()

    # Auth mode selection
    if args.tenant:
        atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        token_url = f"https://{args.tenant}.atlan.com/auth/realms/default/protocol/openid-connect/token"
        client_id = "atlan-argo"
        client_secret = args.client_secret or load_creds(args.creds_file, args.tenant)
        _cfg["token_manager"] = TokenManager(token_url, client_id, client_secret)
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

    try:
        passed = run_test(args)
        sys.exit(0 if passed else 1)
    except KeyboardInterrupt:
        log("\nInterrupted by user.")
        sys.exit(130)
    except Exception as e:
        log(f"Unhandled exception: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
