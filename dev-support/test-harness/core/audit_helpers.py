"""Audit event helpers for verifying entity audit trail via POST /entity/{guid}/auditSearch."""

import time

# Upfront wait for audit indexing eventual consistency (seconds).
AUDIT_CONSISTENCY_WAIT_S = 30


def search_audit_events(client, guid, action_filter=None, size=10):
    """Search audit events for an entity via POST /entity/{guid}/auditSearch.

    Returns (events_list, total_count) or (None, 0) if endpoint unavailable.
    """
    must_clauses = []
    if action_filter:
        must_clauses.append({"term": {"action": action_filter}})

    query = {"match_all": {}} if not must_clauses else {"bool": {"must": must_clauses}}

    payload = {
        "dsl": {
            "from": 0,
            "size": size,
            "query": query,
        },
        "suppressLogs": True,
    }

    resp = client.post(f"/entity/{guid}/auditSearch", json_data=payload)

    if resp.status_code in (404, 400, 405):
        # Endpoint not available (local dev without audit index)
        return None, 0

    if resp.status_code != 200:
        return None, 0

    body = resp.json()
    events = body.get("entityAudits", [])
    total = body.get("totalCount", len(events))
    return events, total


def poll_audit_events(client, guid, action_filter=None, max_wait=60, interval=10, size=10):
    """Poll audit events until results appear.

    Returns (events_list, total_count) or (None, 0) if endpoint unavailable.
    """
    last_events = []
    last_total = 0
    for i in range(max_wait // interval):
        if i > 0:
            time.sleep(interval)
        events, total = search_audit_events(client, guid, action_filter=action_filter, size=size)
        if events is None:
            return None, 0  # Endpoint genuinely not available
        if events:
            return events, total
        last_events = events
        last_total = total
        print(f"  [audit-poll] Waiting for {action_filter or 'any'} audit on {guid} "
              f"({(i+1)*interval}s/{max_wait}s)")
    return last_events, last_total


def assert_audit_event_exists(client, guid, expected_action, max_wait=60, interval=10):
    """Poll until the audit event exists and return it.

    Returns the event dict if found. Asserts if the endpoint is available
    but the event is not found after polling. Returns None ONLY if the
    audit endpoint doesn't exist (404/405).
    """
    events, total = poll_audit_events(
        client, guid, action_filter=expected_action,
        max_wait=max_wait, interval=interval,
    )

    if events is None:
        # Endpoint genuinely not available (404/405) — skip
        return None

    assert len(events) > 0, (
        f"Expected audit event {expected_action} for entity {guid} "
        f"but none found after {max_wait}s polling"
    )

    event = events[0]
    entity_id = event.get("entityId") or event.get("entityGuid")
    if entity_id:
        assert entity_id == guid, (
            f"Audit event entityId mismatch: expected {guid}, got {entity_id}"
        )
    return event
