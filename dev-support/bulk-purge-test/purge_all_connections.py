#!/usr/bin/env python3
"""
purge_all_connections.py — Discover all Connection entities and bulk-purge them sequentially.

Finds all Connection entities via ES, submits bulk-purge for each (with deleteConnection=true),
and waits for completion before moving to the next.

WARNING: This script purges ALL connections. It is intentionally limited to local dev
environments only (basic auth, no OAuth/tenant support) to prevent accidental use
against staging or production.

Usage:
  python3 dev-support/bulk-purge-test/purge_all_connections.py
  python3 dev-support/bulk-purge-test/purge_all_connections.py --url http://localhost:21000
  python3 dev-support/bulk-purge-test/purge_all_connections.py --dry-run   # list connections without purging
"""

import argparse
import sys
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

POLL_INTERVAL = 5   # seconds between status polls
TIMEOUT = 1800      # max seconds to wait per purge (30 min)


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {msg}", flush=True)


def discover_connections(api_base, auth):
    """Find all Connection GUIDs via ES, then fetch their qualifiedNames."""
    # Step 1: Get GUIDs from ES
    payload = {
        "dsl": {
            "from": 0,
            "size": 200,
            "query": {"match": {"__typeName": "Connection"}}
        }
    }
    resp = requests.post(f"{api_base}/search/indexsearch", auth=auth,
                         headers={"Content-Type": "application/json"},
                         json=payload, timeout=30)

    guids = []
    if resp.status_code == 200:
        for entity in resp.json().get("entities", []):
            guids.append(entity.get("guid"))

    # Fallback: query ES directly if indexsearch returned nothing
    if not guids:
        try:
            es_resp = requests.get("http://localhost:9200/janusgraph_vertex_index/_search",
                                   headers={"Content-Type": "application/json"},
                                   json={"size": 200, "query": {"match": {"__typeName": "Connection"}}},
                                   timeout=10)
            if es_resp.status_code == 200:
                for hit in es_resp.json().get("hits", {}).get("hits", []):
                    guid = hit.get("_source", {}).get("__guid")
                    if guid:
                        guids.append(guid)
        except Exception:
            pass

    if not guids:
        return []

    # Step 2: Fetch qualifiedName for each GUID
    connections = []
    for guid in guids:
        try:
            resp = requests.get(f"{api_base}/entity/guid/{guid}?minExtInfo=true",
                                auth=auth,
                                headers={"Content-Type": "application/json"},
                                timeout=10)
            if resp.status_code == 200:
                entity = resp.json().get("entity", {})
                qn = entity.get("attributes", {}).get("qualifiedName", "")
                name = entity.get("attributes", {}).get("name", "")
                status = entity.get("status", "UNKNOWN")
                if qn:
                    connections.append({"guid": guid, "qualifiedName": qn, "name": name, "status": status})
        except Exception as e:
            log(f"  Failed to fetch GUID {guid}: {e}", "WARN")

    return connections


def purge_connection(api_base, auth, conn_qn):
    """Submit bulk purge and return requestId."""
    resp = requests.post(
        f"{api_base}/bulk-purge/connection",
        params={"connectionQualifiedName": conn_qn, "deleteConnection": "true"},
        auth=auth,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}: {resp.text[:200]}"
    body = resp.json()
    return body.get("requestId"), None


def wait_for_purge(api_base, auth, request_id):
    """Poll status until COMPLETED/FAILED/CANCELLED or timeout."""
    start = time.time()
    poll_count = 0
    while time.time() - start < TIMEOUT:
        time.sleep(POLL_INTERVAL)
        poll_count += 1
        elapsed = time.time() - start

        try:
            resp = requests.get(
                f"{api_base}/bulk-purge/status",
                params={"requestId": request_id},
                auth=auth,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
        except Exception as e:
            log(f"    Poll #{poll_count} failed: {e}", "WARN")
            continue

        if resp.status_code == 404:
            log(f"    Poll #{poll_count}: 404 (not found yet, retrying...)")
            continue
        if resp.status_code != 200:
            log(f"    Poll #{poll_count}: HTTP {resp.status_code}", "WARN")
            continue

        status = resp.json()
        state = status.get("status", "UNKNOWN")
        deleted = status.get("deletedCount", 0)
        total = status.get("totalDiscovered", 0)
        failed = status.get("failedCount", 0)
        pct = (deleted / total * 100) if total > 0 else 0

        log(f"    Poll #{poll_count}: {state} | deleted={deleted}/{total} ({pct:.0f}%) | "
            f"failed={failed} | {elapsed:.0f}s")

        if state == "COMPLETED":
            return {"status": "COMPLETED", "deleted": deleted, "failed": failed, "duration": elapsed}
        if state == "FAILED":
            return {"status": "FAILED", "error": status.get("errorMessage", "unknown"), "duration": elapsed}
        if state == "CANCELLED":
            return {"status": "CANCELLED", "duration": elapsed}

    return {"status": "TIMEOUT", "duration": time.time() - start}


def main():
    parser = argparse.ArgumentParser(description="Purge all connections (LOCAL DEV ONLY)")
    parser.add_argument("--url", default="http://localhost:21000", help="Atlas base URL")
    parser.add_argument("--user", default="admin", help="Username")
    parser.add_argument("--password", default="admin", help="Password")
    parser.add_argument("--dry-run", action="store_true", help="List connections without purging")
    args = parser.parse_args()

    api_base = f"{args.url}/api/atlas/v2"
    auth = HTTPBasicAuth(args.user, args.password)

    log("WARNING: This script purges ALL connections. Local dev only.")
    log(f"URL: {args.url} | Auth: Basic ({args.user})")

    log("Discovering connections...")
    connections = discover_connections(api_base, auth)

    if not connections:
        log("No connections found.")
        sys.exit(0)

    log(f"Found {len(connections)} connections:")
    for i, c in enumerate(connections):
        log(f"  {i+1}. {c['qualifiedName']}  (name={c['name']}, status={c['status']}, guid={c['guid']})")

    if args.dry_run:
        log("Dry run — not purging.")
        sys.exit(0)

    log("")
    total_deleted = 0
    total_failed = 0
    overall_start = time.time()

    for i, c in enumerate(connections):
        qn = c["qualifiedName"]
        log(f"[{i+1}/{len(connections)}] Purging: {qn}")

        request_id, err = purge_connection(api_base, auth, qn)
        if err:
            log(f"  Submit failed: {err}", "ERROR")
            continue

        log(f"  Submitted: requestId={request_id}")
        result = wait_for_purge(api_base, auth, request_id)

        if result["status"] == "COMPLETED":
            log(f"  Done: {result['deleted']} deleted, {result['failed']} failed in {result['duration']:.1f}s")
            total_deleted += result["deleted"]
            total_failed += result["failed"]
        else:
            log(f"  Ended with status: {result['status']} after {result['duration']:.1f}s", "WARN")

        log("")

    elapsed = time.time() - overall_start
    log("=" * 60)
    log(f"All done. {len(connections)} connections processed in {elapsed:.1f}s")
    log(f"  Total deleted: {total_deleted}")
    log(f"  Total failed:  {total_failed}")
    log("=" * 60)


if __name__ == "__main__":
    main()
