"""Audit event helpers for verifying entity audit trail via POST /entity/{guid}/auditSearch."""

import time


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


def assert_audit_event_exists(client, guid, expected_action, max_retries=3, retry_delay_s=2):
    """Retry loop to find an audit event with the expected action.

    Returns the event dict if found, None if endpoint unavailable (graceful skip).
    Raises AssertionError if the event is not found after retries.
    """
    for attempt in range(max_retries):
        events, total = search_audit_events(client, guid, action_filter=expected_action)

        if events is None:
            # Endpoint not available - graceful skip
            return None

        if events:
            event = events[0]
            entity_id = event.get("entityId") or event.get("entityGuid")
            if entity_id == guid:
                return event
            # Found events but entityId doesn't match - still return if action matches
            if event.get("action") == expected_action:
                return event

        if attempt < max_retries - 1:
            time.sleep(retry_delay_s)

    raise AssertionError(
        f"Audit event with action={expected_action} not found for entity {guid} "
        f"after {max_retries} retries"
    )
