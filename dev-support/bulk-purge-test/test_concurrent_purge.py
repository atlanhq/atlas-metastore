#!/usr/bin/env python3
"""
test_concurrent_purge.py — Test concurrent bulk purge of 2 connections,
with optional Atlas kill mid-purge to validate orphan recovery.

Modes:
  normal   — Create 2 connections, purge both concurrently, verify
  kill     — Same, but kill Atlas midway and restart it to test recovery

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Usage:
  # Local dev
  python3 test_concurrent_purge.py --count 1000
  python3 test_concurrent_purge.py --count 1000 --mode kill

  # Staging (OAuth2)
  python3 test_concurrent_purge.py --tenant staging --count 1000
  python3 test_concurrent_purge.py --tenant staging --count 5000 --mode kill --kill-after 10
"""

import argparse
import os
import signal
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIMESTAMP = int(time.time())
CONN_A_QN = f"default/concurrent-test-a/{TIMESTAMP}"
CONN_B_QN = f"default/concurrent-test-b/{TIMESTAMP}"

ES_SYNC_TIMEOUT = 300
PURGE_TIMEOUT = 3600   # 1 hour max wait per purge
POLL_INTERVAL = 5
API_REQUEST_TIMEOUT = 60


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
        log("pyyaml required for --tenant mode. Install: pip install pyyaml", "ERROR")
        sys.exit(1)

    if not os.path.exists(creds_file):
        log(f"Creds file not found: {creds_file}", "ERROR")
        sys.exit(1)

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

    log(f"Credentials not found for tenant '{tenant}' in {creds_file}", "ERROR")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {msg}", flush=True)


# Module-level config
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


def api_get(path, params=None, timeout=30):
    return _do_request("GET", f"{_cfg['api_base']}{path}", params=params, timeout=timeout)


def api_post(path, data=None, params=None, timeout=300):
    return _do_request("POST", f"{_cfg['api_base']}{path}", json_data=data,
                       params=params, timeout=timeout)


def es_count(conn_qn):
    """Count entities in ES for a connection. Returns count or -1."""
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


def wait_for_atlas(timeout=300):
    """Wait for Atlas to become ACTIVE."""
    log("Waiting for Atlas to become ACTIVE...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = _do_request("GET", f"{_cfg['atlas_url']}/api/atlas/admin/status", timeout=5)
            if resp.status_code == 200 and resp.json().get("Status") == "ACTIVE":
                log(f"Atlas is ACTIVE (waited {time.time()-start:.1f}s)")
                return True
        except Exception:
            pass
        time.sleep(3)
    log(f"Atlas did not become ACTIVE within {timeout}s", "ERROR")
    return False


def wait_for_es_sync(conn_qn, expected, label=""):
    """Wait for ES to have >= expected entities for a connection."""
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
# Phase 1: Create connections + assets
# ---------------------------------------------------------------------------

def create_connection(conn_qn, conn_name):
    """Create a Connection entity, return GUID."""
    payload = {
        "entity": {
            "typeName": "Connection",
            "attributes": {
                "qualifiedName": conn_qn,
                "name": conn_name,
                "connectorName": "test-bulk-purge"
            }
        }
    }
    resp = api_post("/entity", data=payload)
    if resp.status_code != 200:
        raise Exception(f"Failed to create connection {conn_qn}: {resp.status_code} {resp.text[:200]}")
    guid = resp.json().get("mutatedEntities", {}).get("CREATE", [{}])[0].get("guid")
    if not guid:
        guid = resp.json().get("mutatedEntities", {}).get("UPDATE", [{}])[0].get("guid")
    return guid


def create_tables_batch(conn_qn, start_idx, end_idx, batch_idx, total_batches):
    """Create a batch of Table entities. Returns list of GUIDs."""
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


def create_assets_for_connection(conn_qn, conn_name, asset_count, batch_size, workers):
    """Create connection + N tables. Returns (conn_guid, table_guids)."""
    log(f"Creating connection: {conn_qn}")
    conn_guid = create_connection(conn_qn, conn_name)
    log(f"  Connection created: {conn_guid}")

    batches = []
    for start_idx in range(0, asset_count, batch_size):
        end_idx = min(start_idx + batch_size, asset_count)
        batches.append((start_idx, end_idx))

    total_batches = len(batches)
    log(f"  Creating {asset_count} tables in {total_batches} batches ({batch_size}/batch, {workers} workers)")

    all_guids = []
    completed = 0
    failed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, (s, e) in enumerate(batches):
            f = executor.submit(create_tables_batch, conn_qn, s, e, idx, total_batches)
            futures[f] = idx

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                guids = future.result()
                all_guids.extend(guids)
                completed += 1
            except Exception as ex:
                failed += 1
                log(f"    Batch {batch_idx}/{total_batches} FAILED: {ex}", "ERROR")

            total_done = completed + failed
            elapsed = time.time() - start_time
            rate = len(all_guids) / elapsed if elapsed > 0 else 0
            if total_done % max(1, total_batches // 10) == 0 or total_done == total_batches:
                log(f"    {conn_name}: {len(all_guids)}/{asset_count} "
                    f"({len(all_guids)*100//max(1,asset_count)}%) | "
                    f"batches={completed}/{total_batches} failed={failed} | {rate:.0f} ent/s")

    elapsed = time.time() - start_time
    log(f"  {conn_name}: {len(all_guids)} tables created in {elapsed:.1f}s ({failed} failed batches)")
    return conn_guid, all_guids


# ---------------------------------------------------------------------------
# Phase 2: Trigger purges
# ---------------------------------------------------------------------------

def trigger_purge(conn_qn, delete_connection=True):
    """Submit bulk purge, return requestId."""
    resp = api_post("/bulk-purge/connection",
                    params={"connectionQualifiedName": conn_qn,
                            "deleteConnection": str(delete_connection).lower()})
    if resp.status_code != 200:
        raise Exception(f"Purge submit failed for {conn_qn}: {resp.status_code} {resp.text[:200]}")
    body = resp.json()
    return body.get("requestId")


# ---------------------------------------------------------------------------
# Phase 3: Monitor purges
# ---------------------------------------------------------------------------

def poll_purge_status(request_id):
    """Poll status once. Returns dict with status info, or None on error."""
    try:
        resp = api_get("/bulk-purge/status", params={"requestId": request_id}, timeout=10)
    except Exception:
        return None

    if resp.status_code != 200:
        return None

    data = resp.json()

    # Follow resubmit chain
    if data.get("status") == "RESUBMITTED" and data.get("newRequestId"):
        return {"status": "RESUBMITTED", "newRequestId": data["newRequestId"],
                "purgeKey": data.get("purgeKey", "")}

    return {
        "status": data.get("status", "UNKNOWN"),
        "deleted": data.get("deletedCount", 0),
        "total": data.get("totalDiscovered", 0),
        "failed": data.get("failedCount", 0),
        "phase": data.get("currentPhase", ""),
        "purgeKey": data.get("purgeKey", ""),
        "error": data.get("errorMessage", data.get("error", "")),
        "resubmitCount": data.get("resubmitCount", 0),
    }


def monitor_purge(label, request_id, result_dict):
    """Monitor a purge until completion. Stores result in result_dict[label]."""
    start = time.time()
    poll_count = 0
    prev_deleted = 0
    current_request_id = request_id

    while time.time() - start < PURGE_TIMEOUT:
        poll_count += 1
        time.sleep(POLL_INTERVAL)

        status = poll_purge_status(current_request_id)
        elapsed = time.time() - start

        if status is None:
            if poll_count % 12 == 0:
                log(f"  [{label}] Poll #{poll_count}: Atlas unreachable — {elapsed:.0f}s", "WARN")
            continue

        state = status["status"]

        if state == "RESUBMITTED":
            new_id = status["newRequestId"]
            log(f"  [{label}] RESUBMITTED -> following {new_id}")
            current_request_id = new_id
            prev_deleted = 0
            continue

        deleted = status.get("deleted", 0)
        total = status.get("total", 0)
        failed = status.get("failed", 0)
        phase = status.get("phase", "")
        pct = (deleted / total * 100) if total > 0 else 0
        delta = deleted - prev_deleted
        prev_deleted = deleted

        parts = [f"[{label}]", f"Poll #{poll_count}", f"{state}",
                 f"{deleted}/{total} ({pct:.0f}%)"]
        if delta > 0:
            parts.append(f"+{delta}")
        if phase:
            parts.append(phase)
        parts.append(f"{elapsed:.0f}s")

        if poll_count % 6 == 0 or delta > 0 or state in ("COMPLETED", "FAILED", "CANCELLED"):
            log("  " + " | ".join(parts))

        if state == "COMPLETED":
            result_dict[label] = {
                "status": "COMPLETED", "deleted": deleted, "failed": failed,
                "total": total, "duration": elapsed, "resubmits": status.get("resubmitCount", 0)
            }
            return

        if state in ("FAILED", "CANCELLED"):
            result_dict[label] = {
                "status": state, "error": status.get("error", ""),
                "deleted": deleted, "total": total, "duration": elapsed,
                "resubmits": status.get("resubmitCount", 0)
            }
            return

    result_dict[label] = {"status": "TIMEOUT", "duration": time.time() - start}


# ---------------------------------------------------------------------------
# Phase 4: Verify cleanup
# ---------------------------------------------------------------------------

def verify_cleanup(conn_qn, label):
    """Check ES for remaining entities. Returns count."""
    count = es_count(conn_qn)
    count = max(0, count)
    if count == 0:
        log(f"  [{label}] Verified: 0 entities remaining in ES")
    else:
        log(f"  [{label}] WARNING: {count} entities still in ES", "WARN")
    return count


# ---------------------------------------------------------------------------
# Atlas kill/restart (for --mode kill)
# ---------------------------------------------------------------------------

def find_atlas_pid():
    """Find Atlas JVM process ID."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "org.apache.atlas.Atlas"],
            capture_output=True, text=True, timeout=5
        )
        pids = result.stdout.strip().split('\n')
        pids = [p.strip() for p in pids if p.strip()]
        return int(pids[0]) if pids else None
    except Exception:
        return None


def kill_atlas():
    """Kill Atlas process with SIGKILL (simulates pod kill)."""
    pid = find_atlas_pid()
    if pid:
        log(f"Killing Atlas process (PID={pid}) with SIGKILL...")
        os.kill(pid, signal.SIGKILL)
        time.sleep(2)
        log("Atlas killed.")
        return True
    else:
        log("Could not find Atlas PID", "ERROR")
        return False


# ---------------------------------------------------------------------------
# Main test flow
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Concurrent Bulk Purge Test")

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
    parser.add_argument("--count", type=int, default=1000,
                        help="Number of table assets per connection (default: 1000)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for creation")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers for creation")
    parser.add_argument("--mode", choices=["normal", "kill"], default="normal",
                        help="Test mode: normal or kill (kills Atlas mid-purge)")
    parser.add_argument("--kill-after", type=int, default=15,
                        help="Seconds after purge start to kill Atlas (only in kill mode)")
    args = parser.parse_args()

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
    auth_mode = "OAuth2" if _cfg["token_manager"] else "Basic"

    log("=" * 70)
    log(f"CONCURRENT BULK PURGE TEST — mode={args.mode}")
    log(f"  URL:  {atlas_url} | Auth: {auth_mode}")
    log(f"  Connections: 2 x {args.count} tables each")
    log(f"  Connection A: {CONN_A_QN}")
    log(f"  Connection B: {CONN_B_QN}")
    if args.mode == "kill":
        log(f"  Kill Atlas after: {args.kill_after}s")
    log("=" * 70)

    overall_start = time.time()

    # -- Step 1: Preflight --
    log("\n=== Step 1: Preflight ===")
    if not wait_for_atlas(timeout=60):
        log("Atlas not available. Exiting.", "ERROR")
        sys.exit(1)

    # -- Step 2: Create 2 connections with assets --
    log("\n=== Step 2: Create Connection A ===")
    guid_a, guids_a = create_assets_for_connection(
        CONN_A_QN, "concurrent-test-a", args.count, args.batch_size, args.workers)

    log("\n=== Step 3: Create Connection B ===")
    guid_b, guids_b = create_assets_for_connection(
        CONN_B_QN, "concurrent-test-b", args.count, args.batch_size, args.workers)

    # -- Step 3: Wait for ES sync --
    log("\n=== Step 4: Wait for ES sync ===")
    wait_for_es_sync(CONN_A_QN, len(guids_a), label="Connection A")
    wait_for_es_sync(CONN_B_QN, len(guids_b), label="Connection B")

    # -- Step 4: Trigger both purges concurrently --
    log("\n=== Step 5: Trigger concurrent purges ===")
    req_id_a = trigger_purge(CONN_A_QN)
    log(f"  Connection A purge submitted: requestId={req_id_a}")
    req_id_b = trigger_purge(CONN_B_QN)
    log(f"  Connection B purge submitted: requestId={req_id_b}")

    # -- Step 5 (kill mode): Kill Atlas after delay --
    if args.mode == "kill":
        log(f"\n=== Step 5b: Killing Atlas in {args.kill_after}s ===")
        time.sleep(args.kill_after)
        killed = kill_atlas()
        if not killed:
            log("Failed to kill Atlas. Continuing in normal mode.", "WARN")
        else:
            log("Waiting 10s before restarting Atlas...")
            time.sleep(10)
            log("Please restart Atlas now (or it should auto-restart if managed).")
            log("Waiting for Atlas to come back...")
            if not wait_for_atlas(timeout=600):
                log("Atlas did not come back. Cannot verify recovery.", "ERROR")
                sys.exit(1)
            log("Atlas is back. Orphan checker should recover purges in ~5-10 min.")

    # -- Step 6: Monitor both purges concurrently --
    log("\n=== Step 6: Monitor purges ===")
    results = {}
    monitor_threads = [
        threading.Thread(target=monitor_purge, args=("A", req_id_a, results), daemon=True),
        threading.Thread(target=monitor_purge, args=("B", req_id_b, results), daemon=True),
    ]
    for t in monitor_threads:
        t.start()
    for t in monitor_threads:
        t.join(timeout=PURGE_TIMEOUT)

    # -- Step 7: Verify cleanup --
    log("\n=== Step 7: Verify cleanup ===")
    remaining_a = verify_cleanup(CONN_A_QN, "A")
    remaining_b = verify_cleanup(CONN_B_QN, "B")

    # -- Summary --
    elapsed = time.time() - overall_start
    log("\n" + "=" * 70)
    log("TEST SUMMARY")
    log("=" * 70)
    log(f"  Mode: {args.mode}")
    log(f"  Assets per connection: {args.count}")
    log(f"  Total test duration: {elapsed:.1f}s")

    for label, req_id in [("A", req_id_a), ("B", req_id_b)]:
        r = results.get(label, {"status": "UNKNOWN"})
        status = r.get("status", "UNKNOWN")
        deleted = r.get("deleted", "?")
        total = r.get("total", "?")
        dur = r.get("duration", 0)
        resubmits = r.get("resubmits", 0)
        err = r.get("error", "")

        parts = [f"Connection {label}: {status}",
                 f"deleted={deleted}/{total}",
                 f"{dur:.1f}s"]
        if resubmits > 0:
            parts.append(f"resubmits={resubmits}")
        if err:
            parts.append(f"error={err}")
        log(f"  {' | '.join(parts)}")

    log(f"  Remaining in ES — A: {remaining_a}, B: {remaining_b}")

    all_passed = (
        results.get("A", {}).get("status") == "COMPLETED" and
        results.get("B", {}).get("status") == "COMPLETED" and
        remaining_a == 0 and remaining_b == 0
    )
    log(f"\n  {'PASS' if all_passed else 'FAIL'}")
    log("=" * 70)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
