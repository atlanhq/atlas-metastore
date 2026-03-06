"""Entity label add/delete tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.audit_helpers import assert_audit_event_exists
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("entity_labels", depends_on_suites=["entity_crud"],
       description="Entity label operations")
class EntityLabelsSuite:

    def setup(self, client, ctx):
        qn = unique_qn("label-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("label-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        ctx.register_entity("label_test_entity", self.entity_guid, "DataSet")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.entity_guid}"))

    @test("add_labels", tags=["labels"], order=1)
    def test_add_labels(self, client, ctx):
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["test-label-1", "test-label-2"],
        )
        assert_status_in(resp, [200, 204])

    @test("get_entity_with_labels", tags=["labels"], order=2, depends_on=["add_labels"])
    def test_get_entity_with_labels(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        labels = entity.get("labels", [])
        assert "test-label-1" in labels, f"Expected 'test-label-1' in labels, got {labels}"

    @test("delete_labels", tags=["labels"], order=3, depends_on=["add_labels"])
    def test_delete_labels(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["test-label-1"],
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify "test-label-1" is gone
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        labels = entity.get("labels", [])
        assert "test-label-1" not in labels, f"Expected 'test-label-1' removed, got {labels}"

    @test("add_labels_audit", tags=["labels", "audit"], order=4, depends_on=["add_labels"])
    def test_add_labels_audit(self, client, ctx):
        event = assert_audit_event_exists(client, self.entity_guid, "LABEL_ADD")
        if event is None:
            return  # Audit endpoint not available, graceful skip
