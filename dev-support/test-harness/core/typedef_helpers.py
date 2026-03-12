"""Typedef creation helper with verify-after-500 for server-side timeouts.

On preprod/staging, POST /types/typedefs often returns 500 because the
gateway timeout (~15s) is shorter than the server-side processing time.

Additionally, the type cache on staging/preprod NEVER refreshes for certain
typedef categories (classification, businessMetadata).  POST returns 200
with the full typedef body, subsequent POSTs return 409 ("already exists"),
but GET /types/{category}/name/{name} keeps returning 404 indefinitely.

Strategy:
  1. POST the typedef with retries=0 (no client-level retries).
  2. If 200 with non-empty body -> type is created. Return success.
  3. If 409 -> type already exists. Return success.
  4. If 500 -> re-POST up to 3 times with increasing waits (10s, 20s, 30s)
     looking for 409 (already exists) confirmation.  Then fall back to GET
     polling for max_wait seconds.
  5. Otherwise -> failure.

GET polling is ONLY used to verify creation after 500 errors.
For 200-with-body and 409, we trust the POST response and skip GET entirely
(the type cache may never refresh on staging for classification/BM defs).
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
    """POST /types/typedefs with verification on server-side timeouts.

    Args:
        client:    AtlasClient instance
        payload:   dict with typedef category keys
        max_wait:  max seconds to poll GET after 500 (default 60)
        interval:  seconds between GET polls (default 15)

    Returns:
        (success: bool, resp: ApiResponse)
        success=True means the type was created (POST 200 with body, or 409).
    """
    verify_targets = _extract_verify_targets(payload)
    names = [n for _, n in verify_targets] if verify_targets else []

    # Single POST, no client-level retries (retries=0 = 1 attempt)
    resp = client.post("/types/typedefs", json_data=payload, timeout=30, retries=0)

    # 409 = type already exists — definitive success
    if resp.status_code == 409:
        print(f"  [typedef] 409 (already exists) — type confirmed: {names}")
        return True, resp

    # 200 with non-empty body = type created — trust it
    if resp.status_code == 200:
        body = resp.body if hasattr(resp, 'body') else None
        has_body = body and body != {} and body != ""
        if has_body:
            print(f"  [typedef] 200 with body — type confirmed: {names}")
            return True, resp
        # 200 with empty body — ambiguous, re-POST once to get 409
        print(f"  [typedef] 200 with empty body — re-POSTing once to confirm...")
        resp2 = client.post("/types/typedefs", json_data=payload, timeout=30, retries=0)
        if resp2.status_code in (200, 409):
            print(f"  [typedef] Confirmed via re-POST ({resp2.status_code}): {names}")
            return True, resp2
        print(f"  [typedef] Re-POST returned {resp2.status_code} — treating as success "
              f"(original POST was 200)")
        return True, resp

    # 500/502/503 — server error (usually gateway timeout), check if type
    # was silently created.  Re-POST multiple times with increasing waits;
    # if the server finished processing, the re-POST returns 409.
    if resp.status_code in (500, 502, 503):
        print(f"  [typedef] POST returned {resp.status_code}, verifying...")
        for attempt in range(3):
            wait = 10 * (attempt + 1)  # 10s, 20s, 30s
            time.sleep(wait)
            resp2 = client.post("/types/typedefs", json_data=payload, timeout=30, retries=0)
            if resp2.status_code == 409:
                print(f"  [typedef] Re-POST returned 409 after {wait}s — "
                      f"type was created: {names}")
                return True, resp2
            if resp2.status_code == 200:
                body = resp2.body if hasattr(resp2, 'body') else None
                if body and body != {} and body != "":
                    print(f"  [typedef] Re-POST returned 200 with body — "
                          f"confirmed: {names}")
                    return True, resp2
            print(f"  [typedef] Re-POST attempt {attempt + 1}/3 returned "
                  f"{resp2.status_code} (waited {wait}s)")

        # Fall back to GET polling if re-POSTs didn't confirm
        if verify_targets:
            ok = _poll_types_queryable(client, verify_targets, max_wait,
                                       interval, label="verify-after-500")
            if ok:
                print(f"  [typedef] Types confirmed via GET despite POST {resp.status_code}")
                return True, resp

        print(f"  [typedef] POST {resp.status_code} — creation could not be confirmed")
        return False, resp

    # Other status codes (400, 404, etc.) — genuine failure
    print(f"  [typedef] POST returned {resp.status_code} — not retryable")
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


def ensure_classification_types(client, names):
    """Create classification types, fall back to existing types if not usable.

    On staging/preprod, newly created classification types may not propagate
    across pods.  POST /types/typedefs returns 200/409, but GET returns 404
    and POST /entity/.../classifications returns 404 ("invalid classification").

    When this happens, discovers existing classification types that ARE
    queryable and returns those instead.

    Args:
        client:  AtlasClient instance
        names:   list of classification type names to create

    Returns:
        (usable_names: list[str], created_new: bool, ok: bool)
        - usable_names: type names usable for entity operations
        - created_new: True if new types were created (caller should cleanup)
        - ok: True if enough usable types available
    """
    from core.data_factory import build_classification_def

    count = len(names)
    defs = [build_classification_def(name=n) for n in names]

    # Step 1: Try to create new types (single-shot, no retry loop)
    created, _resp = create_typedef_verified(
        client, {"classificationDefs": defs},
    )

    if created:
        # Step 2: Check if first type is queryable (usable across pods)
        check = client.get(f"/types/classificationdef/name/{names[0]}")
        if check.status_code == 200:
            return list(names), True, True

        # Created but not queryable — cross-pod type cache issue
        print(f"  [setup] Classification types created (POST 200/409) but "
              f"not queryable — staging cross-pod type cache issue")
    else:
        print(f"  [setup] Classification typedef creation failed "
              f"(POST returned errors)")

    # Step 3: Fall back to existing classification types
    print(f"  [setup] Discovering existing classification types...")
    existing = _find_existing_classification_types(client, count=count)
    if len(existing) >= count:
        print(f"  [setup] Using {count} existing types: {existing[:count]}")
        return existing[:count], False, True

    print(f"  [setup] Only found {len(existing)} existing classification "
          f"types, need {count}")
    return list(names), created, False


def _find_existing_classification_types(client, count=3):
    """Find existing classification type names that can be applied to DataSet.

    Uses /types/typedefs/headers to get candidates, then GET the full
    classificationdef to check:
      1. Type is queryable (GET returns 200)
      2. Type has no entityTypes restriction, or includes DataSet/Asset

    Types with entityTypes restrictions (e.g. only Table, Column) will
    silently fail when applied to DataSet — POST returns 200 but the
    classification never appears on the entity.
    """
    resp = client.get("/types/typedefs/headers")
    if resp.status_code != 200:
        print(f"  [setup] GET /types/typedefs/headers returned {resp.status_code}")
        return []
    body = resp.json()
    candidates = []
    if isinstance(body, list):
        for td in body:
            cat = td.get("category", "")
            if cat.upper() == "CLASSIFICATION":
                name = td.get("name", "")
                if name and not name.startswith("__"):
                    candidates.append(name)
    elif isinstance(body, dict):
        # Some Atlas versions nest under category keys
        for td in body.get("classificationDefs", []):
            name = td.get("name", "")
            if name and not name.startswith("__"):
                candidates.append(name)

    print(f"  [setup] Found {len(candidates)} classification type candidates "
          f"in headers: {candidates[:10]}")

    # Verify candidates are queryable AND applicable to DataSet
    results = []
    checked = 0
    for name in candidates:
        check = client.get(f"/types/classificationdef/name/{name}")
        if check.status_code != 200:
            continue
        checked += 1
        typedef_body = check.json()
        entity_types = typedef_body.get("entityTypes", []) or []
        # Accept if no restriction (empty list) or if DataSet/Asset is allowed
        if entity_types:
            allowed = {t.lower() for t in entity_types}
            if not (allowed & {"dataset", "asset", "referenceable"}):
                if checked <= 5:
                    print(f"  [setup] Skipping '{name}': restricted to "
                          f"entityTypes={entity_types}")
                continue
        results.append(name)
        if len(results) >= count:
            break
        # Don't spend too long checking candidates
        if checked > 30:
            break
    if results:
        print(f"  [setup] Found {len(results)} unrestricted classification "
              f"types: {results}")
    return results


def _poll_types_queryable(client, verify_targets, max_wait, interval, label=""):
    """Poll GET until all types in verify_targets are queryable.

    Only used after 500 errors to check if types were silently created.
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
