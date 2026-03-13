#!/usr/bin/env python3
"""
bulk_purge.py — Trigger and monitor a Bulk Purge operation.

Simple operational script: submit a purge, poll status until done, log results.
No asset creation — just the purge workflow.

Supports two purge modes:
  - connection      — purge all assets under a connection
  - qualifiedName   — purge all assets matching a QN prefix

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Prerequisites:
  pip install requests pyyaml

Usage:
  # Purge by connection (local dev)
  python3 bulk_purge.py --conn-qn "default/snowflake/1234567890"

  # Purge by connection (staging)
  python3 bulk_purge.py --tenant staging --conn-qn "default/snowflake/1234567890"

  # Purge by connection + delete the connection entity itself
  python3 bulk_purge.py --tenant staging --conn-qn "default/snowflake/1234567890" --delete-connection

  # Purge by qualifiedName prefix
  python3 bulk_purge.py --tenant staging --mode qualifiedName --qn-prefix "default/snowflake/1234567890/"

  # Monitor an already-submitted purge
  python3 bulk_purge.py --tenant staging --monitor "request-uuid-here"

  # Cancel a running purge
  python3 bulk_purge.py --tenant staging --cancel "request-uuid-here"

  # Dry run — see what would be purged without submitting
  python3 bulk_purge.py --tenant staging --conn-qn "default/snowflake/1234567890" --dry-run
"""

import argparse
import os
import sys
import threading
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PURGE_TIMEOUT = 7200        # 2 hours max wait
POLL_INTERVAL = 10          # seconds between polls
API_TIMEOUT = 60            # per-request timeout


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {msg}", flush=True)


def _headers(token_manager):
    h = {"Content-Type": "application/json", "Accept": "application/json"}
    if token_manager:
        h["Authorization"] = f"Bearer {token_manager.get_token()}"
    return h


def _request(method, url, token_manager, basic_auth, **kwargs):
    headers = _headers(token_manager)
    auth = basic_auth or None
    timeout = kwargs.pop("timeout", API_TIMEOUT)

    resp = requests.request(method, url, headers=headers, auth=auth,
                            timeout=timeout, **kwargs)

    if resp.status_code == 401 and token_manager:
        token_manager.force_refresh()
        headers = _headers(token_manager)
        resp = requests.request(method, url, headers=headers, auth=auth,
                                timeout=timeout, **kwargs)
    return resp


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

def submit_purge(api_base, token_manager, basic_auth, args):
    """Submit a bulk purge and return the requestId."""
    mode = args.mode

    if mode == "connection":
        if not args.conn_qn:
            log("--conn-qn is required for connection mode", "ERROR")
            sys.exit(1)
        params = {"connectionQualifiedName": args.conn_qn}
        if args.delete_connection:
            params["deleteConnection"] = "true"
        endpoint = "/bulk-purge/connection"
        log(f"Submitting purge: mode=connection, connectionQN={args.conn_qn}, "
            f"deleteConnection={args.delete_connection}")
    else:
        if not args.qn_prefix:
            log("--qn-prefix is required for qualifiedName mode", "ERROR")
            sys.exit(1)
        params = {"prefix": args.qn_prefix}
        endpoint = "/bulk-purge/qualifiedName"
        log(f"Submitting purge: mode=qualifiedName, prefix={args.qn_prefix}")

    if args.worker_count > 0:
        params["workerCount"] = str(args.worker_count)

    url = f"{api_base}{endpoint}"
    start = time.time()
    resp = _request("POST", url, token_manager, basic_auth, params=params)
    latency_ms = (time.time() - start) * 1000

    if resp.status_code != 200:
        log(f"Purge submit failed (HTTP {resp.status_code}): {resp.text[:500]}", "ERROR")
        sys.exit(1)

    body = resp.json()
    request_id = body.get("requestId")
    log(f"Purge submitted successfully")
    log(f"  requestId:  {request_id}")
    log(f"  purgeKey:   {body.get('purgeKey')}")
    log(f"  purgeMode:  {body.get('purgeMode')}")
    log(f"  latency:    {latency_ms:.0f}ms")
    if body.get("workerCountOverride"):
        log(f"  workers:    {body.get('workerCountOverride')}")

    return request_id


def monitor_purge(api_base, token_manager, basic_auth, request_id, poll_interval):
    """Poll purge status until terminal state. Returns the final status dict."""
    log(f"Monitoring purge: requestId={request_id}")
    log(f"  Poll interval: {poll_interval}s")
    log("")

    start_time = time.time()
    poll_count = 0
    peak_rate = 0
    last_deleted = 0

    while time.time() - start_time < PURGE_TIMEOUT:
        time.sleep(poll_interval)
        poll_count += 1
        elapsed = time.time() - start_time

        url = f"{api_base}/bulk-purge/status"
        resp = _request("GET", url, token_manager, basic_auth,
                        params={"requestId": request_id})

        if resp.status_code == 404:
            log(f"  [Poll #{poll_count}] 404 — not found yet ({elapsed:.0f}s)")
            continue
        if resp.status_code != 200:
            log(f"  [Poll #{poll_count}] HTTP {resp.status_code}: {resp.text[:200]}", "WARN")
            continue

        status = resp.json()
        current = status.get("status", "UNKNOWN")
        deleted = status.get("deletedCount", 0)
        total = status.get("totalDiscovered", 0)
        failed = status.get("failedCount", 0)
        batches = status.get("completedBatches", 0)
        workers = status.get("workerCount", "?")
        batch_size = status.get("batchSize", "?")
        phase = status.get("currentPhase", "")
        remaining = status.get("remainingAfterCleanup", None)

        rate = deleted / elapsed if elapsed > 0 and deleted > 0 else 0
        peak_rate = max(peak_rate, rate)
        pct = (deleted / total * 100) if total > 0 else 0

        # Incremental rate (entities deleted since last poll)
        delta = deleted - last_deleted
        delta_rate = delta / poll_interval if delta > 0 else 0
        last_deleted = deleted

        phase_str = f" | phase={phase}" if phase else ""
        log(f"  [Poll #{poll_count}] status={current} | "
            f"deleted={deleted:,}/{total:,} ({pct:.1f}%) | "
            f"failed={failed} | batches={batches} | "
            f"workers={workers} | batchSize={batch_size} | "
            f"rate={rate:,.0f} avg, {delta_rate:,.0f} cur ent/s{phase_str}")

        if current == "COMPLETED":
            duration = time.time() - start_time
            avg_rate = deleted / duration if duration > 0 else 0
            log("")
            log("=" * 60)
            log("PURGE COMPLETED")
            log("=" * 60)
            log(f"  requestId:       {request_id}")
            log(f"  purgeKey:        {status.get('purgeKey')}")
            log(f"  purgeMode:       {status.get('purgeMode')}")
            log(f"  totalDiscovered: {total:,}")
            log(f"  deletedCount:    {deleted:,}")
            log(f"  failedCount:     {failed}")
            if remaining is not None:
                log(f"  remaining:       {remaining}")
            if status.get("connectionDeleted"):
                log(f"  connectionDel:   true")
            log(f"  duration:        {duration:.1f}s")
            log(f"  avg rate:        {avg_rate:,.0f} entities/s")
            log(f"  peak rate:       {peak_rate:,.0f} entities/s")
            log(f"  polls:           {poll_count}")
            if status.get("error"):
                log(f"  warning:         {status['error']}")
            log("=" * 60)
            return status

        if current == "FAILED":
            log("")
            log(f"PURGE FAILED: {status.get('error', 'unknown')}", "ERROR")
            log(f"  deleted={deleted:,}, failed={failed}, total={total:,}")
            return status

        if current == "CANCELLED":
            log("")
            log(f"PURGE CANCELLED after {elapsed:.0f}s", "WARN")
            log(f"  deleted={deleted:,}, failed={failed}, total={total:,}")
            return status

    log(f"TIMEOUT: purge did not complete within {PURGE_TIMEOUT}s", "ERROR")
    return None


def dry_run(api_base, token_manager, basic_auth, args):
    """Query ES to show what would be purged, without actually submitting."""
    sample_size = 10

    if args.mode == "connection":
        target = args.conn_qn
        log(f"DRY RUN: mode=connection, connectionQN={target}")

        # Strategy 1: connectionQualifiedName term
        payload = {
            "dsl": {
                "from": 0, "size": sample_size,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"connectionQualifiedName": target}},
                            {"term": {"__state": "ACTIVE"}}
                        ]
                    }
                }
            }
        }
        resp = _request("POST", f"{api_base}/search/indexsearch",
                         token_manager, basic_auth, json=payload)
        count = -1
        entities = []
        if resp.status_code == 200:
            body = resp.json()
            count = body.get("approximateCount", 0)
            entities = body.get("entities", [])

        # Strategy 2 fallback: prefix on __qualifiedNameHierarchy
        if count <= 0:
            payload = {
                "dsl": {
                    "from": 0, "size": sample_size,
                    "query": {
                        "bool": {
                            "must": [
                                {"prefix": {"__qualifiedNameHierarchy": target + "/"}},
                                {"term": {"__state": "ACTIVE"}}
                            ]
                        }
                    }
                }
            }
            resp = _request("POST", f"{api_base}/search/indexsearch",
                             token_manager, basic_auth, json=payload)
            if resp.status_code == 200:
                body = resp.json()
                count = body.get("approximateCount", 0)
                entities = body.get("entities", [])

    else:  # qualifiedName mode
        target = args.qn_prefix
        log(f"DRY RUN: mode=qualifiedName, prefix={target}")

        payload = {
            "dsl": {
                "from": 0, "size": sample_size,
                "query": {
                    "bool": {
                        "must": [
                            {"prefix": {"__qualifiedNameHierarchy": target}},
                            {"term": {"__state": "ACTIVE"}}
                        ]
                    }
                }
            }
        }
        resp = _request("POST", f"{api_base}/search/indexsearch",
                         token_manager, basic_auth, json=payload)
        count = -1
        entities = []
        if resp.status_code == 200:
            body = resp.json()
            count = body.get("approximateCount", 0)
            entities = body.get("entities", [])

    if count < 0:
        log(f"Failed to query ES (HTTP {resp.status_code}): {resp.text[:300]}", "ERROR")
        sys.exit(1)

    log("")
    log("=" * 60)
    log("DRY RUN RESULTS")
    log("=" * 60)
    log(f"  Target:             {target}")
    log(f"  Mode:               {args.mode}")
    log(f"  Entities to purge:  {count:,}")
    if args.delete_connection and args.mode == "connection":
        log(f"  Delete connection:  yes (connection entity will also be removed)")

    if entities:
        log("")
        log(f"  Sample entities (up to {sample_size}):")
        for ent in entities:
            name = ent.get("attributes", {}).get("name") or ent.get("displayText", "?")
            etype = ent.get("typeName", "?")
            qn = ent.get("attributes", {}).get("qualifiedName", "")
            # Truncate long QNs
            if len(qn) > 80:
                qn = qn[:77] + "..."
            log(f"    - [{etype}] {name}  ({qn})")
    elif count == 0:
        log("")
        log("  No entities match the criteria. Nothing would be purged.")

    log("=" * 60)
    log("")
    log("This was a dry run. No purge was submitted.")
    log("Remove --dry-run to execute the purge.")


def cancel_purge(api_base, token_manager, basic_auth, request_id):
    """Cancel a running purge."""
    log(f"Cancelling purge: requestId={request_id}")

    url = f"{api_base}/bulk-purge/cancel"
    resp = _request("POST", url, token_manager, basic_auth,
                    params={"requestId": request_id})

    if resp.status_code == 200:
        body = resp.json()
        log(f"  {body.get('message', 'Cancel requested')}")
    elif resp.status_code == 404:
        body = resp.json()
        log(f"  {body.get('message', 'Not found')}", "WARN")
    else:
        log(f"  Cancel failed (HTTP {resp.status_code}): {resp.text[:300]}", "ERROR")


def check_status(api_base, token_manager, basic_auth, request_id):
    """One-shot status check (no polling)."""
    url = f"{api_base}/bulk-purge/status"
    resp = _request("GET", url, token_manager, basic_auth,
                    params={"requestId": request_id})

    if resp.status_code == 404:
        log(f"Purge not found: {request_id}", "WARN")
        return

    if resp.status_code != 200:
        log(f"Status check failed (HTTP {resp.status_code}): {resp.text[:300]}", "ERROR")
        return

    import json
    status = resp.json()
    log(f"Status for requestId={request_id}:")
    print(json.dumps(status, indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Trigger and monitor a Bulk Purge operation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Purge by connection (local dev)
  python3 bulk_purge.py --conn-qn "default/snowflake/1234567890"

  # Purge by connection (staging) + delete connection entity
  python3 bulk_purge.py --tenant staging --conn-qn "default/snowflake/123" --delete-connection

  # Purge by qualifiedName prefix
  python3 bulk_purge.py --tenant staging --mode qualifiedName --qn-prefix "default/snowflake/123/"

  # Monitor an existing purge
  python3 bulk_purge.py --tenant staging --monitor "request-uuid"

  # One-shot status check
  python3 bulk_purge.py --tenant staging --status "request-uuid"

  # Cancel a running purge
  python3 bulk_purge.py --tenant staging --cancel "request-uuid"

  # Dry run — see what would be purged
  python3 bulk_purge.py --tenant staging --conn-qn "default/snowflake/123" --dry-run
"""
    )

    # Auth
    parser.add_argument("--url", default=None,
                        help="Atlas base URL (auto-constructed from --tenant if not set)")
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth (e.g., 'staging')")
    parser.add_argument("--client-secret", default=None,
                        help="OAuth client_secret (reads from creds file if omitted)")
    parser.add_argument("--creds-file",
                        default=os.path.expanduser("~/Desktop/Projects/tokens/creds.yaml"),
                        help="Path to creds.yaml")
    parser.add_argument("--user", default=None, help="Basic auth username")
    parser.add_argument("--password", default=None, help="Basic auth password")

    # Purge params
    parser.add_argument("--mode", default="connection",
                        choices=["connection", "qualifiedName"],
                        help="Purge mode (default: connection)")
    parser.add_argument("--conn-qn", default=None,
                        help="Connection qualifiedName (for connection mode)")
    parser.add_argument("--qn-prefix", default=None,
                        help="QualifiedName prefix (for qualifiedName mode)")
    parser.add_argument("--delete-connection", action="store_true",
                        help="Also delete the Connection entity after children are purged")
    parser.add_argument("--worker-count", type=int, default=0,
                        help="Server-side worker count override (0 = auto-scale)")
    parser.add_argument("--poll-interval", type=int, default=POLL_INTERVAL,
                        help=f"Seconds between status polls (default: {POLL_INTERVAL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Query ES to show what would be purged, without submitting")

    # Actions (mutually exclusive with submit)
    parser.add_argument("--monitor", metavar="REQUEST_ID", default=None,
                        help="Monitor an already-submitted purge (skip submit)")
    parser.add_argument("--cancel", metavar="REQUEST_ID", default=None,
                        help="Cancel a running purge")
    parser.add_argument("--status", metavar="REQUEST_ID", default=None,
                        help="One-shot status check (no polling)")

    return parser.parse_args()


def main():
    args = parse_args()

    # --- Auth ---
    token_manager = None
    basic_auth = None

    if args.tenant:
        atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        token_url = f"https://{args.tenant}.atlan.com/auth/realms/default/protocol/openid-connect/token"
        secret = args.client_secret or load_creds(args.creds_file, args.tenant)
        token_manager = TokenManager(token_url, "atlan-argo", secret)
    elif args.user:
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth(args.user, args.password or "admin")
    else:
        atlas_url = args.url or "http://localhost:21000"
        basic_auth = HTTPBasicAuth("admin", "admin")

    if args.tenant:
        api_base = f"{atlas_url}/api/meta"
    else:
        api_base = f"{atlas_url}/api/atlas/v2"

    # --- Action: cancel ---
    if args.cancel:
        cancel_purge(api_base, token_manager, basic_auth, args.cancel)
        return

    # --- Action: one-shot status ---
    if args.status:
        check_status(api_base, token_manager, basic_auth, args.status)
        return

    # --- Action: monitor only ---
    if args.monitor:
        result = monitor_purge(api_base, token_manager, basic_auth,
                               args.monitor, args.poll_interval)
        sys.exit(0 if result and result.get("status") == "COMPLETED" else 1)

    # --- Validate purge params ---
    if not args.conn_qn and args.mode == "connection":
        log("--conn-qn is required for connection mode", "ERROR")
        sys.exit(1)
    if not args.qn_prefix and args.mode == "qualifiedName":
        log("--qn-prefix is required for qualifiedName mode", "ERROR")
        sys.exit(1)

    # --- Action: dry run ---
    if args.dry_run:
        dry_run(api_base, token_manager, basic_auth, args)
        return

    # --- Action: submit + monitor ---
    request_id = submit_purge(api_base, token_manager, basic_auth, args)
    log("")
    result = monitor_purge(api_base, token_manager, basic_auth,
                           request_id, args.poll_interval)

    if result and result.get("status") == "COMPLETED":
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
