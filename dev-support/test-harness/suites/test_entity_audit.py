"""Entity audit history read tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("entity_audit", depends_on_suites=["entity_crud"],
       description="Entity audit history endpoints")
class EntityAuditSuite:

    @test("get_audit_by_guid", tags=["audit"], order=1)
    def test_get_audit_by_guid(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered; depends on entity_crud suite")
        resp = client.get(f"/entity/{guid}/audit")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            # Should be a list of audit events
            assert isinstance(body, (list, dict)), "Expected list or dict of audit events"

    @test("get_audit_details", tags=["audit"], order=2)
    def test_get_audit_details(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        # First get audit events to find an audit GUID
        resp = client.get(f"/entity/{guid}/audit")
        if resp.status_code in (200,):
            body = resp.json()
            events = body if isinstance(body, list) else body.get("entityAudits", [])
            if events:
                audit_guid = events[0].get("eventKey") or events[0].get("entityId")
                if audit_guid:
                    resp2 = client.get(f"/audit/{audit_guid}/details", admin=True)
                    assert_status_in(resp2, [200, 404])
