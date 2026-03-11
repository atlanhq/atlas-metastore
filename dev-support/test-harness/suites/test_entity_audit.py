"""Entity audit history tests via POST /entity/{guid}/auditSearch."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present
from core.audit_helpers import (
    search_audit_events, poll_audit_events, assert_audit_event_exists,
)
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("entity_audit", depends_on_suites=["entity_crud"],
       description="Entity audit search endpoints")
class EntityAuditSuite:

    def setup(self, client, ctx):
        # Verify audit endpoint is reachable (first check)
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered; depends on entity_crud suite")

        # Poll until at least one audit event is available (up to 60s)
        print(f"  [audit-setup] Polling for audit events on ds1 ({guid})...")
        events, total = poll_audit_events(client, guid, max_wait=60, interval=10)
        if events is None:
            # Endpoint genuinely doesn't exist (404/405) — flag for tests
            self.audit_available = False
            print("  [audit-setup] Audit endpoint not available (404/405)")
            return

        self.audit_available = True
        assert total > 0, (
            f"Expected at least 1 audit event for entity {guid} after 60s polling, "
            f"got totalCount={total}. Audit indexing may not be configured."
        )

    @test("audit_search_basic", tags=["audit"], order=1)
    def test_audit_search_basic(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        events, total = search_audit_events(client, guid, size=10)
        assert events is not None, "Audit endpoint returned None unexpectedly"
        assert isinstance(events, list), f"Expected list of audit events, got {type(events)}"
        assert total > 0, f"Expected totalCount > 0 for entity {guid}, got {total}"

    @test("audit_search_entity_create", tags=["audit"], order=2)
    def test_audit_search_entity_create(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_CREATE",
                                          max_wait=30, interval=10)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_CREATE audit event for {guid}, got 0 events after polling"
        )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Expected entityId={guid}, got {entity_id}"
        assert event.get("action") == "ENTITY_CREATE", (
            f"Expected action=ENTITY_CREATE, got {event.get('action')}"
        )
        assert "user" in event, "Expected 'user' field in audit event"
        assert "timestamp" in event, "Expected 'timestamp' field in audit event"

    @test("audit_search_entity_update", tags=["audit"], order=3)
    def test_audit_search_entity_update(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_UPDATE",
                                          max_wait=30, interval=10)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_UPDATE audit event for {guid}, got 0 events after polling"
        )
        event = events[0]
        assert event.get("action") == "ENTITY_UPDATE", (
            f"Expected ENTITY_UPDATE, got {event.get('action')}"
        )

    @test("audit_search_action_filter", tags=["audit"], order=4)
    def test_audit_search_action_filter(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        events, total = search_audit_events(client, guid, action_filter="ENTITY_CREATE")
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, "Expected at least 1 ENTITY_CREATE event"
        # All returned events should match the requested action filter
        for event in events:
            assert event.get("action") == "ENTITY_CREATE", (
                f"Expected all events to have action=ENTITY_CREATE, got {event.get('action')}"
            )

    @test("audit_search_pagination", tags=["audit"], order=5)
    def test_audit_search_pagination(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        # Fetch page 1 (size=1)
        events_p1, total = search_audit_events(client, guid, size=1)
        assert events_p1 is not None, "Audit endpoint returned None"
        assert len(events_p1) <= 1, f"Expected at most 1 event with size=1, got {len(events_p1)}"
        # Fetch larger page to verify pagination returns more
        if total > 1:
            events_p2, total2 = search_audit_events(client, guid, size=5)
            assert events_p2 is not None, "Audit endpoint returned None"
            assert len(events_p2) > len(events_p1), (
                f"Expected more events with size=5 ({len(events_p2)}) than size=1 ({len(events_p1)})"
            )

    @test("audit_search_event_fields", tags=["audit"], order=6)
    def test_audit_search_event_fields(self, client, ctx):
        assert self.audit_available, "Audit endpoint not available on this environment"
        guid = ctx.get_entity_guid("ds1")
        events, total = search_audit_events(client, guid, size=1)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, f"Expected at least 1 audit event for {guid}"
        event = events[0]
        required_fields = ["entityId", "action", "user"]
        for field_name in required_fields:
            assert field_name in event, f"Audit event missing required field '{field_name}'"

    @test("audit_search_entity_delete", tags=["audit"], order=7)
    def test_audit_search_entity_delete(self, client, ctx):
        """AUD-03: Verify ENTITY_DELETE audit event after entity deletion."""
        assert self.audit_available, "Audit endpoint not available on this environment"

        # Prefer GUID from entity_crud's delete_entity_by_guid test
        guid = ctx.get("deleted_entity_guid")

        if not guid:
            # Fallback: create and delete our own entity
            qn = unique_qn("audit-del")
            entity = build_dataset_entity(qn=qn, name=unique_name("audit-del"))
            resp = client.post("/entity", json_data={"entity": entity})
            assert_status(resp, 200)
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            assert entities, "Entity creation returned no entities in mutatedEntities"
            guid = entities[0]["guid"]
            resp = client.delete(f"/entity/guid/{guid}")
            assert_status_in(resp, [200, 204])

        # Poll for ENTITY_DELETE audit event
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_DELETE",
                                          max_wait=60, interval=10)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_DELETE audit event for {guid} after 60s polling, got 0"
        )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Expected entityId={guid}, got {entity_id}"
        assert event.get("action") == "ENTITY_DELETE", (
            f"Expected action=ENTITY_DELETE, got {event.get('action')}"
        )
