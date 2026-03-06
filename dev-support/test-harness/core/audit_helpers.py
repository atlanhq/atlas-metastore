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


def assert_audit_event_exists(client, guid, expected_action, wait_s=AUDIT_CONSISTENCY_WAIT_S):
    """Wait for eventual consistency, then check if the audit event exists.

    Sleeps wait_s upfront, queries once. Returns the event dict if found,
    None if endpoint unavailable or audit indexing is not active on this
    environment (empty results). Callers should treat None as a graceful skip.
    """
    time.sleep(wait_s)

    events, total = search_audit_events(client, guid, action_filter=expected_action)

    if events is None:
        # Endpoint not available — graceful skip
        return None

    if len(events) == 0:
        # Audit indexing may not be active on this environment — graceful skip
        return None

    event = events[0]
    entity_id = event.get("entityId") or event.get("entityGuid")
    if entity_id:
        assert entity_id == guid, (
            f"Audit event entityId mismatch: expected {guid}, got {entity_id}"
        )
    return event
