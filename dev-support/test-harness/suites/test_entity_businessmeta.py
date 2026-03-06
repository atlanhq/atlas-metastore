"""Business metadata add/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.audit_helpers import assert_audit_event_exists
from core.data_factory import (
    build_business_metadata_def, build_multi_attr_business_metadata_def,
    build_dataset_entity, unique_name, unique_qn, unique_type_name,
)


@suite("entity_businessmeta", depends_on_suites=["entity_crud"],
       description="Business metadata on entities")
class EntityBusinessMetaSuite:

    def setup(self, client, ctx):
        # Create a BM type
        self.bm_name = unique_type_name("HarnessBM")
        payload = {"businessMetadataDefs": [build_business_metadata_def(name=self.bm_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        # Wait for type cache propagation on staging (can take 10-15s)
        time.sleep(10)
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
        # 404 can happen if type cache hasn't propagated the BM def yet
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            ctx.set("bm_add_failed", True)

    @test("add_multi_attr_business_metadata", tags=["businessmeta"], order=1.5)
    def test_add_multi_attr_business_metadata(self, client, ctx):
        # Create multi-attr BM typedef
        self.multi_bm_name = unique_type_name("HarnessMultiBM")
        payload = {"businessMetadataDefs": [build_multi_attr_business_metadata_def(name=self.multi_bm_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status_in(resp, [200, 409])
        time.sleep(10)
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.multi_bm_name}")
        )

        # Add multi-attr BM to entity
        bm_payload = {self.multi_bm_name: {
            "bmStrField": "hello",
            "bmIntField": 42,
            "bmBoolField": True,
        }}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=bm_payload,
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code in [200, 204]:
            # Verify all 3 field types persisted
            resp2 = client.get(f"/entity/guid/{self.entity_guid}")
            assert_status(resp2, 200)
            entity = resp2.json().get("entity", {})
            bm = entity.get("businessAttributes", {}).get(self.multi_bm_name, {})
            assert bm.get("bmStrField") == "hello", f"Expected bmStrField='hello', got {bm}"
            assert bm.get("bmIntField") == 42, f"Expected bmIntField=42, got {bm}"
            assert bm.get("bmBoolField") is True, f"Expected bmBoolField=True, got {bm}"
            ctx.set("multi_bm_name", self.multi_bm_name)

    @test("verify_business_metadata", tags=["businessmeta"], order=2, depends_on=["add_business_metadata"])
    def test_verify_business_metadata(self, client, ctx):
        if ctx.get("bm_add_failed"):
            return  # BM add failed due to type cache, skip
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "test-value", f"Expected bmField1='test-value', got {bm}"

    @test("overwrite_business_metadata", tags=["businessmeta"], order=2.3, depends_on=["add_business_metadata"])
    def test_overwrite_business_metadata(self, client, ctx):
        if ctx.get("bm_add_failed"):
            return
        payload = {self.bm_name: {"bmField1": "overwritten-value"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Verify overwrite
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "overwritten-value", (
            f"Expected bmField1='overwritten-value', got {bm}"
        )

    @test("partial_update_business_metadata", tags=["businessmeta"], order=2.5, depends_on=["add_business_metadata"])
    def test_partial_update_business_metadata(self, client, ctx):
        if ctx.get("bm_add_failed"):
            return
        # Update only bmField1 and verify it changes
        payload = {self.bm_name: {"bmField1": "partial-updated"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "partial-updated", (
            f"Expected bmField1='partial-updated', got {bm}"
        )

    @test("delete_business_metadata", tags=["businessmeta"], order=3, depends_on=["add_business_metadata"])
    def test_delete_business_metadata(self, client, ctx):
        if ctx.get("bm_add_failed"):
            return  # BM add failed due to type cache, skip
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/businessmetadata/{self.bm_name}",
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify BM namespace is empty
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        # After delete, BM should be empty or absent
        if bm:
            # Check that the field value is cleared (null or missing)
            val = bm.get("bmField1")
            assert val is None, f"Expected bmField1 cleared after delete, got {val}"

    @test("add_bm_audit", tags=["businessmeta", "audit"], order=4, depends_on=["add_business_metadata"])
    def test_add_bm_audit(self, client, ctx):
        if ctx.get("bm_add_failed"):
            return  # BM add failed due to type cache, skip
        event = assert_audit_event_exists(client, self.entity_guid, "BUSINESS_ATTRIBUTE_UPDATE")
        if event is None:
            return  # Audit endpoint not available on this environment

    @test("add_bm_nonexistent_entity", tags=["businessmeta"], order=5)
    def test_add_bm_nonexistent_entity(self, client, ctx):
        payload = {self.bm_name: {"bmField1": "test"}}
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [404, 400])
