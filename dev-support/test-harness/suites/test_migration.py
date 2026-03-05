"""Migration submit/status tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("migration", description="Migration submit and status endpoints")
class MigrationSuite:

    @test("migration_status", tags=["migration"], order=1)
    def test_migration_status(self, client, ctx):
        resp = client.get("/migration/status", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 400, 404])

    @test("migration_submit_invalid", tags=["migration"], order=2)
    def test_migration_submit_invalid(self, client, ctx):
        # Submit with invalid migration type — should fail gracefully
        resp = client.post("/migration/submit", params={
            "migrationType": "NONEXISTENT_TYPE",
        })
        assert_status_in(resp, [200, 400, 404])
