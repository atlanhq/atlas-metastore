"""Classification add/update/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_list_min_length,
)
from core.data_factory import (
    build_classification_def, build_dataset_entity, unique_name, unique_qn,
    unique_type_name,
)


@suite("entity_classifications", depends_on_suites=["entity_crud"],
       description="Entity classification operations")
class EntityClassificationsSuite:

    def setup(self, client, ctx):
        # Create a dedicated classification for this suite
        self.tag_name = unique_type_name("HarnessTag")
        payload = {"classificationDefs": [build_classification_def(name=self.tag_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        # Wait for type cache propagation on staging
        time.sleep(5)
        ctx.set("harness_tag_name", self.tag_name)
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
        )

        # Create a dedicated entity
        qn = unique_qn("tag-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("tag-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        ctx.register_entity("tag_test_entity", self.entity_guid, "DataSet")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.entity_guid}"))

    @test("add_classification", tags=["classification"], order=1)
    def test_add_classification(self, client, ctx):
        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

    @test("get_classifications", tags=["classification"], order=2, depends_on=["add_classification"])
    def test_get_classifications(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        # Body is either a list or has "list" field
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, f"Classification {self.tag_name} not found on entity"

    @test("get_single_classification", tags=["classification"], order=3, depends_on=["add_classification"])
    def test_get_single_classification(self, client, ctx):
        resp = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status(resp, 200)
        assert_field_equals(resp, "typeName", self.tag_name)

    @test("update_classification", tags=["classification"], order=4, depends_on=["add_classification"])
    def test_update_classification(self, client, ctx):
        payload = [{"typeName": self.tag_name}]
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

    @test("delete_classification", tags=["classification"], order=10, depends_on=["add_classification"])
    def test_delete_classification(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status_in(resp, [200, 204])

        # Verify removed
        resp = client.get(f"/entity/guid/{self.entity_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert not found, f"Classification {self.tag_name} should have been removed"

    @test("add_classification_by_unique_attr", tags=["classification"], order=11)
    def test_add_classification_by_unique_attr(self, client, ctx):
        # Create another entity for this test
        qn = unique_qn("tag-ua-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("tag-ua"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

        # Add classification by unique attribute
        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            "/entity/uniqueAttribute/type/DataSet/classifications",
            json_data=payload,
            params={"attr:qualifiedName": qn},
        )
        assert_status_in(resp, [200, 204])

        # Clean up: delete classification
        resp = client.delete(
            f"/entity/uniqueAttribute/type/DataSet/classification/{self.tag_name}",
            params={"attr:qualifiedName": qn},
        )
        assert_status_in(resp, [200, 204])
