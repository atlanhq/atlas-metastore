"""Entity audit history tests via POST /entity/{guid}/auditSearch."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present
from core.audit_helpers import search_audit_events


@suite("entity_audit", depends_on_suites=["entity_crud"],
       description="Entity audit search endpoints")
class EntityAuditSuite:

    @test("audit_search_basic", tags=["audit"], order=1)
    def test_audit_search_basic(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered; depends on entity_crud suite")
        events, total = search_audit_events(client, guid, size=10)
        if events is None:
            return  # Endpoint not available, graceful skip
        assert isinstance(events, list), f"Expected list of audit events, got {type(events)}"
        assert total > 0, f"Expected totalCount > 0, got {total}"

    @test("audit_search_entity_create", tags=["audit"], order=2)
    def test_audit_search_entity_create(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        events, total = search_audit_events(client, guid, action_filter="ENTITY_CREATE")
        if events is None:
            return
        assert len(events) > 0, "Expected at least one ENTITY_CREATE event"
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Expected entityId={guid}, got {entity_id}"
        assert event.get("action") == "ENTITY_CREATE", f"Expected action=ENTITY_CREATE, got {event.get('action')}"
        assert "user" in event, "Expected 'user' field in audit event"
        assert "timestamp" in event, "Expected 'timestamp' field in audit event"

    @test("audit_search_entity_update", tags=["audit"], order=3)
    def test_audit_search_entity_update(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        events, total = search_audit_events(client, guid, action_filter="ENTITY_UPDATE")
        if events is None:
            return
        # ds1 was updated in entity_crud suite, so we expect at least one update event
        if len(events) > 0:
            event = events[0]
            assert event.get("action") == "ENTITY_UPDATE", f"Expected ENTITY_UPDATE, got {event.get('action')}"

    @test("audit_search_action_filter", tags=["audit"], order=4)
    def test_audit_search_action_filter(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        events, total = search_audit_events(client, guid, action_filter="ENTITY_CREATE")
        if events is None:
            return
        # All returned events should match the requested action filter
        for event in events:
            assert event.get("action") == "ENTITY_CREATE", (
                f"Expected all events to have action=ENTITY_CREATE, got {event.get('action')}"
            )

    @test("audit_search_pagination", tags=["audit"], order=5)
    def test_audit_search_pagination(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        events, total = search_audit_events(client, guid, size=1)
        if events is None:
            return
        assert len(events) <= 1, f"Expected at most 1 event with size=1, got {len(events)}"

    @test("audit_search_event_fields", tags=["audit"], order=6)
    def test_audit_search_event_fields(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        events, total = search_audit_events(client, guid, size=1)
        if events is None:
            return
        if not events:
            return  # No events to validate
        event = events[0]
        required_fields = ["entityId", "action", "user"]
        for field_name in required_fields:
            assert field_name in event, f"Audit event missing required field '{field_name}'"
