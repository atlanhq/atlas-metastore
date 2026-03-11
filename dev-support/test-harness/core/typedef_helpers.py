"""Typedef creation helper with verify-after-500 for server-side timeouts.

On preprod/staging, POST /types/typedefs sometimes returns 500 because the
internal processing exceeds the gateway timeout (~15s), but the typedef IS
actually created server-side.  This module provides a helper that detects
this by polling GET after a 500 response.
"""

import time

# Maps payload category key -> GET path segment for verification
_CATEGORY_TO_PATH = {
    "classificationDefs": "classificationdef",
    "businessMetadataDefs": "businessmetadatadef",
    "enumDefs": "enumdef",
    "structDefs": "structdef",
    "entityDefs": "entitydef",
    "relationshipDefs": "relationshipdef",
}


def create_typedef_verified(client, payload, max_wait=60, interval=15):
    """POST /types/typedefs with verify-after-timeout.

    1. POST the typedef payload (client internally retries 3x on 500/502/503).
    2. If 200/409 -> poll GET until queryable (type cache propagation).
    3. If still 500/502/503 after client retries -> poll GET to check if the
       type was actually created despite the error response.

    Args:
        client:    AtlasClient instance
        payload:   dict with typedef category keys, e.g.
                   {"classificationDefs": [{"name": "MyTag", ...}]}
        max_wait:  max seconds to poll GET for type cache / verify (default 60)
        interval:  seconds between polls (default 15)

    Returns:
        (success: bool, resp: ApiResponse)
        success=True means all types in the payload are queryable via GET.
    """
    resp = client.post("/types/typedefs", json_data=payload)

    # Build verification targets from the payload
    verify_targets = _extract_verify_targets(payload)

    if resp.status_code in (200, 409):
        if not verify_targets:
            return True, resp
        # Type created/exists — wait for type cache propagation
        ok = _poll_types_queryable(client, verify_targets, max_wait, interval,
                                   label="type-cache")
        return ok, resp

    if resp.status_code in (500, 502, 503):
        if not verify_targets:
            return False, resp
        # Server-side timeout — verify if types were actually created
        print(f"  [typedef] POST returned {resp.status_code}, verifying "
              f"{len(verify_targets)} type(s) via GET...")
        ok = _poll_types_queryable(client, verify_targets, max_wait, interval,
                                   label="verify-after-500")
        if ok:
            print(f"  [typedef] Types confirmed created despite POST "
                  f"{resp.status_code}")
        else:
            print(f"  [typedef] Types NOT found after {max_wait}s — "
                  f"creation truly failed")
        return ok, resp

    # Other status codes (400, 404, etc.) — genuine failure
    return False, resp


def _extract_verify_targets(payload):
    """Extract (get_path_segment, name) pairs from a typedef payload."""
    targets = []
    for category_key, defs_list in payload.items():
        path_segment = _CATEGORY_TO_PATH.get(category_key)
        if not path_segment or not isinstance(defs_list, list):
            continue
        for typedef in defs_list:
            name = typedef.get("name") if isinstance(typedef, dict) else None
            if name:
                targets.append((path_segment, name))
    return targets


def _poll_types_queryable(client, verify_targets, max_wait, interval, label=""):
    """Poll GET until all types in verify_targets are queryable.

    Returns True if all types found within max_wait seconds.
    """
    prefix = f"  [{label}] " if label else "  "
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        all_found = True
        for path_segment, name in verify_targets:
            check = client.get(f"/types/{path_segment}/name/{name}")
            if check.status_code != 200:
                all_found = False
                break
        if all_found:
            print(f"{prefix}All {len(verify_targets)} type(s) queryable "
                  f"after {elapsed}s")
            return True
        names = [n for _, n in verify_targets]
        print(f"{prefix}Waiting for type cache ({elapsed}s/{max_wait}s): "
              f"{names}")
    return False
