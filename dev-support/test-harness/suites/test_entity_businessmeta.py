"""Business metadata add/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.data_factory import (
    build_business_metadata_def, build_dataset_entity,
    unique_name, unique_qn, unique_type_name,
)


@suite("entity_businessmeta", depends_on_suites=["entity_crud"],
       description="Business metadata on entities")
class EntityBusinessMetaSuite:

    def setup(self, client, ctx):
        # Create a BM type
        self.bm_name = unique_type_name("HarnessBM")
        payload = {"businessMetadataDefs": [build_business_metadata_def(name=self.bm_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        # Wait for type cache propagation on staging
        time.sleep(5)
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.bm_name}")
        )

        # Create entity
        qn = unique_qn("bm-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("bm-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.entity_guid}"))

    @test("add_business_metadata", tags=["businessmeta"], order=1)
    def test_add_business_metadata(self, client, ctx):
        payload = {self.bm_name: {"bmField1": "test-value"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

    @test("verify_business_metadata", tags=["businessmeta"], order=2, depends_on=["add_business_metadata"])
    def test_verify_business_metadata(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "test-value", f"Expected bmField1='test-value', got {bm}"

    @test("delete_business_metadata", tags=["businessmeta"], order=3, depends_on=["add_business_metadata"])
    def test_delete_business_metadata(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/businessmetadata/{self.bm_name}",
        )
        assert_status_in(resp, [200, 204])
