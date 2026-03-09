#!/usr/bin/env python3
"""
monitor_purge.py — Monitor a bulk purge request by polling its status.

Supports two auth modes:
  - OAuth2 client-credentials (--tenant flag, for staging/QA)
  - Basic auth (--user/--password, for local dev; defaults to admin/admin)

Usage:
  # Local dev
  python3 monitor_purge.py <requestId>
  python3 monitor_purge.py <requestId> --url http://localhost:21000

  # Staging (OAuth2)
  python3 monitor_purge.py <requestId> --tenant staging
  python3 monitor_purge.py <requestId> --tenant staging --poll 3
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


# Module-level config — set once in main(), used by all helpers.
_cfg = {
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


def _do_request(method, url, params=None, timeout=10):
    headers = _build_headers()
    auth = _cfg["basic_auth"]
    resp = requests.request(method, url, headers=headers, auth=auth,
                            params=params, timeout=timeout)
    if resp.status_code == 401 and _cfg["token_manager"]:
        _cfg["token_manager"].force_refresh()
        headers = _build_headers()
        resp = requests.request(method, url, headers=headers, auth=auth,
                                params=params, timeout=timeout)
    return resp


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def poll_status(request_id, poll_interval):
    api_base = _cfg["api_base"]
    log(f"Monitoring purge requestId={request_id}")
    log(f"API: {api_base}/bulk-purge/status?requestId={request_id}")
    log(f"Poll interval: {poll_interval}s")
    log("=" * 70)

    start = time.time()
    poll_count = 0
    prev_deleted = 0

    while True:
        poll_count += 1
        elapsed = time.time() - start

        try:
            resp = _do_request("GET", f"{api_base}/bulk-purge/status",
                               params={"requestId": request_id})
        except requests.exceptions.ConnectionError:
            log(f"Poll #{poll_count}: Connection refused (Atlas down?) — {elapsed:.0f}s", "WARN")
            time.sleep(poll_interval)
            continue
        except Exception as e:
            log(f"Poll #{poll_count}: Request failed: {e} — {elapsed:.0f}s", "WARN")
            time.sleep(poll_interval)
            continue

        if resp.status_code == 404:
            log(f"Poll #{poll_count}: 404 — request not found (Atlas may have restarted and lost state)", "WARN")
            time.sleep(poll_interval)
            continue

        if resp.status_code != 200:
            log(f"Poll #{poll_count}: HTTP {resp.status_code}: {resp.text[:200]}", "WARN")
            time.sleep(poll_interval)
            continue

        status = resp.json()
        state = status.get("status", "UNKNOWN")

        # Follow resubmit chain: if this request was resubmitted, switch to new requestId
        if state == "RESUBMITTED" and status.get("newRequestId"):
            new_id = status["newRequestId"]
            log(f"Poll #{poll_count}: RESUBMITTED -> following newRequestId={new_id}")
            request_id = new_id
            prev_deleted = 0
            time.sleep(poll_interval)
            continue

        deleted = status.get("deletedCount", 0)
        total = status.get("totalDiscovered", 0)
        failed = status.get("failedCount", 0)
        phase = status.get("currentPhase", "")
        purge_key = status.get("purgeKey", "")
        error_msg = status.get("errorMessage", "")
        pct = (deleted / total * 100) if total > 0 else 0
        delta = deleted - prev_deleted
        rate = delta / poll_interval if poll_interval > 0 and delta > 0 else 0
        prev_deleted = deleted

        parts = [
            f"Poll #{poll_count}",
            f"status={state}",
            f"deleted={deleted}/{total} ({pct:.0f}%)",
            f"failed={failed}",
        ]
        if delta > 0:
            parts.append(f"+{delta} ({rate:.0f}/s)")
        if phase:
            parts.append(f"phase={phase}")
        if purge_key:
            parts.append(f"key={purge_key}")
        parts.append(f"{elapsed:.0f}s")

        log(" | ".join(parts))

        if state == "COMPLETED":
            log("=" * 70)
            log(f"COMPLETED — deleted={deleted}, failed={failed}, total={total}, duration={elapsed:.1f}s")
            import json
            log(f"Full response:\n{json.dumps(status, indent=2)}")
            return 0

        if state == "FAILED":
            log("=" * 70)
            log(f"FAILED — error: {error_msg}", "ERROR")
            import json
            log(f"Full response:\n{json.dumps(status, indent=2)}")
            return 1

        if state == "CANCELLED":
            log("=" * 70)
            log(f"CANCELLED after {elapsed:.1f}s", "WARN")
            return 1

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Monitor a bulk purge request")
    parser.add_argument("request_id", help="The purge requestId to monitor")

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

    parser.add_argument("--poll", type=int, default=5, help="Poll interval in seconds")
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

    auth_mode = "OAuth2" if _cfg["token_manager"] else "Basic"
    log(f"URL: {atlas_url} | Auth: {auth_mode} | API: {_cfg['api_base']}")

    try:
        sys.exit(poll_status(args.request_id, args.poll))
    except KeyboardInterrupt:
        log("\nInterrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
