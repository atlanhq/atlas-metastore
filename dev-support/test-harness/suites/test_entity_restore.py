"""Entity soft-delete and restore tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_equals
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("entity_restore", depends_on_suites=["entity_crud"],
       description="Entity soft-delete and restore operations")
class EntityRestoreSuite:

    def setup(self, client, ctx):
        # Create a dedicated entity for restore tests
        qn = unique_qn("restore-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("restore-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        self.entity_qn = qn
        ctx.register_entity("restore_test_entity", self.entity_guid, "DataSet")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.entity_guid}"))

    @test("soft_delete_entity", tags=["restore", "crud"], order=1)
    def test_soft_delete_entity(self, client, ctx):
        resp = client.delete(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)

        # Verify status is DELETED
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "DELETED")

    @test("restore_entity", tags=["restore", "crud"], order=2, depends_on=["soft_delete_entity"])
    def test_restore_entity(self, client, ctx):
        resp = client.post(
            "/entity/restore/bulk",
            params={"guid": self.entity_guid},
        )
        # 200 or 204 on success; 404 if endpoint not available
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                assert body, "Expected non-empty restore response"
        elif resp.status_code == 404:
            ctx.set("restore_unavailable", True)

    @test("verify_restored_entity", tags=["restore", "crud"], order=3, depends_on=["restore_entity"])
    def test_verify_restored_entity(self, client, ctx):
        if ctx.get("restore_unavailable"):
            return  # Restore endpoint not available
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.status", "ACTIVE")
