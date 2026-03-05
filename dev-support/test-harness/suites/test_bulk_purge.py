"""Bulk purge lifecycle tests (slow, optional)."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present


@suite("bulk_purge", depends_on_suites=["entity_crud"],
       description="Bulk purge lifecycle (slow)")
class BulkPurgeSuite:

    @test("status_nonexistent", tags=["bulk_purge"], order=1)
    def test_status_nonexistent(self, client, ctx):
        resp = client.get("/bulk-purge/status", params={
            "requestId": "00000000-0000-0000-0000-000000000000",
        })
        assert_status_in(resp, [200, 404])

    @test("purge_invalid_prefix", tags=["bulk_purge"], order=2)
    def test_purge_invalid_prefix(self, client, ctx):
        # Prefix too short (< 10 chars) should fail validation
        resp = client.post("/bulk-purge/qualifiedName", params={
            "prefix": "short",
        })
        assert_status_in(resp, [400, 422, 500])

    @test("system_status", tags=["bulk_purge"], order=3)
    def test_system_status(self, client, ctx):
        resp = client.get("/bulk-purge/system-status")
        assert_status_in(resp, [200, 404])

    @test("cancel_nonexistent", tags=["bulk_purge"], order=4)
    def test_cancel_nonexistent(self, client, ctx):
        resp = client.delete("/bulk-purge/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [200, 404, 500])
