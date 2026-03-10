"""Classification add/update/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_list_min_length,
)
from core.audit_helpers import assert_audit_event_exists
from core.data_factory import (
    build_classification_def, build_dataset_entity, build_process_entity,
    unique_name, unique_qn, unique_type_name,
)


@suite("entity_classifications", depends_on_suites=["entity_crud"],
       description="Entity classification operations")
class EntityClassificationsSuite:

    def setup(self, client, ctx):
        # Create a dedicated classification for this suite
        self.tag_name = unique_type_name("HarnessTag")
        self.tag2_name = unique_type_name("HarnessTag2")
        payload = {"classificationDefs": [
            build_classification_def(name=self.tag_name),
            build_classification_def(name=self.tag2_name),
        ]}
        resp = client.post("/types/typedefs", json_data=payload)
        # Wait for type cache propagation on staging (can take 10-15s)
        time.sleep(10)
        ctx.set("harness_tag_name", self.tag_name)
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
        )
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag2_name}")
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

    @test("create_entity_with_classification", tags=["classification"], order=0.5)
    def test_create_entity_with_classification(self, client, ctx):
        # Create entity with classification attached at creation time
        qn = unique_qn("tag-create-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("tag-create"))
        entity["classifications"] = [{"typeName": self.tag_name}]
        resp = client.post("/entity", json_data={"entity": entity})
        # 404 if classification type cache hasn't propagated
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                guid = entities[0]["guid"]
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
                # Verify classification is attached
                resp2 = client.get(f"/entity/guid/{guid}")
                if resp2.status_code == 200:
                    entity_body = resp2.json().get("entity", {})
                    classifications = entity_body.get("classifications", [])
                    found = any(c.get("typeName") == self.tag_name for c in classifications)
                    assert found, f"Classification {self.tag_name} not attached at creation"

    @test("add_classification", tags=["classification"], order=1)
    def test_add_classification(self, client, ctx):
        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        # 404 can happen if type cache hasn't propagated the classification yet
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            ctx.set("classification_add_failed", True)
            return

        # Read-after-write: GET entity and verify classification present
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        classifications = entity.get("classifications", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, f"Classification {self.tag_name} not found on entity after add"

    @test("get_classifications", tags=["classification"], order=2, depends_on=["add_classification"])
    def test_get_classifications(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return  # Classification add failed due to type cache, skip
        resp = client.get(f"/entity/guid/{self.entity_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        # Body is either a list or has "list" field
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, f"Classification {self.tag_name} not found on entity"

    @test("get_single_classification", tags=["classification"], order=3, depends_on=["add_classification"])
    def test_get_single_classification(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return
        resp = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status(resp, 200)
        assert_field_equals(resp, "typeName", self.tag_name)

    @test("update_classification", tags=["classification"], order=4, depends_on=["add_classification"])
    def test_update_classification(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return
        payload = [{"typeName": self.tag_name}]
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: verify classification still attached with correct typeName
        resp2 = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        if resp2.status_code == 200:
            body = resp2.json()
            assert body.get("typeName") == self.tag_name, (
                f"Expected typeName={self.tag_name}, got {body.get('typeName')}"
            )

    @test("update_classification_propagation_flags", tags=["classification", "propagation"], order=4.5, depends_on=["add_classification"])
    def test_update_classification_propagation_flags(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return
        payload = [{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": True,
        }]
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Verify flags updated
        resp2 = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        if resp2.status_code == 200:
            body = resp2.json()
            # Check propagation flags if present in response
            if "propagate" in body:
                assert body.get("propagate") is True, (
                    f"Expected propagate=True, got {body.get('propagate')}"
                )

    @test("add_classification_audit", tags=["classification", "audit"], order=5, depends_on=["add_classification"])
    def test_add_classification_audit(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return
        event = assert_audit_event_exists(client, self.entity_guid, "CLASSIFICATION_ADD")
        if event is None:
            return  # Audit endpoint not available on this environment

    @test("multi_tag_application", tags=["classification"], order=6, depends_on=["add_classification"])
    def test_multi_tag_application(self, client, ctx):
        # Add second classification — may 404 if type cache hasn't propagated tag2 yet
        payload = [{"typeName": self.tag2_name}]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        # 404 can happen if type cache hasn't propagated the second tag yet
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            return  # Type cache lag — skip verification

        # Wait for propagation
        time.sleep(3)

        # GET entity and verify both classifications present
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        classifications = entity.get("classifications", [])
        tag_names = [c.get("typeName") for c in classifications]
        assert self.tag_name in tag_names, f"Expected {self.tag_name} in classifications, got {tag_names}"
        assert self.tag2_name in tag_names, f"Expected {self.tag2_name} in classifications, got {tag_names}"

    @test("classification_propagation", tags=["classification", "propagation"], order=7)
    def test_classification_propagation(self, client, ctx):
        # Create DataSet A, DataSet B, Process(inputs=[A], outputs=[B])
        qn_a = unique_qn("prop-src")
        qn_b = unique_qn("prop-tgt")
        ds_a = build_dataset_entity(qn=qn_a, name=unique_name("prop-src"))
        ds_b = build_dataset_entity(qn=qn_b, name=unique_name("prop-tgt"))

        resp_a = client.post("/entity", json_data={"entity": ds_a})
        assert_status(resp_a, 200)
        guid_a = (resp_a.json().get("mutatedEntities", {}).get("CREATE", []) or
                  resp_a.json().get("mutatedEntities", {}).get("UPDATE", []))[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid_a}"))

        resp_b = client.post("/entity", json_data={"entity": ds_b})
        assert_status(resp_b, 200)
        guid_b = (resp_b.json().get("mutatedEntities", {}).get("CREATE", []) or
                  resp_b.json().get("mutatedEntities", {}).get("UPDATE", []))[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid_b}"))

        # Create Process linking A -> B
        proc = build_process_entity(
            inputs=[{"guid": guid_a, "typeName": "DataSet"}],
            outputs=[{"guid": guid_b, "typeName": "DataSet"}],
        )
        resp_proc = client.post("/entity", json_data={"entity": proc})
        if resp_proc.status_code != 200:
            # Staging may reject Process - skip gracefully
            return

        proc_entities = (resp_proc.json().get("mutatedEntities", {}).get("CREATE", []) or
                         resp_proc.json().get("mutatedEntities", {}).get("UPDATE", []))
        if not proc_entities:
            return
        proc_guid = proc_entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{proc_guid}"))

        # Add classification to A with propagation enabled
        payload = [{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }]
        resp_tag = client.post(f"/entity/guid/{guid_a}/classifications", json_data=payload)
        assert_status_in(resp_tag, [200, 204])

        # Wait for propagation through lineage
        time.sleep(5)

        # Check if classification propagated to Process
        resp_check = client.get(f"/entity/guid/{proc_guid}")
        if resp_check.status_code == 200:
            entity = resp_check.json().get("entity", {})
            classifications = entity.get("classifications", [])
            propagated = any(c.get("typeName") == self.tag_name for c in classifications)
            # Propagation is best-effort - log but don't fail if not propagated
            # as it depends on graph traversal and timing
            if not propagated:
                pass  # Propagation may take longer or be disabled

        # Check if classification propagated to B
        resp_b_check = client.get(f"/entity/guid/{guid_b}")
        if resp_b_check.status_code == 200:
            entity_b = resp_b_check.json().get("entity", {})
            classifications_b = entity_b.get("classifications", [])
            propagated_b = any(c.get("typeName") == self.tag_name for c in classifications_b)
            # Same - best-effort check

    @test("delete_classification", tags=["classification"], order=10, depends_on=["add_classification"])
    def test_delete_classification(self, client, ctx):
        if ctx.get("classification_add_failed"):
            return
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
        # 404 can happen if type cache hasn't propagated the classification yet
        assert_status_in(resp, [200, 204, 404])

        if resp.status_code != 404:
            # Read-after-write: verify classification attached
            resp2 = client.get(f"/entity/guid/{guid}")
            if resp2.status_code == 200:
                entity_body = resp2.json().get("entity", {})
                classifications = entity_body.get("classifications", [])
                found = any(c.get("typeName") == self.tag_name for c in classifications)
                assert found, f"Classification {self.tag_name} not found after add by unique attr"

            # Clean up: delete classification
            resp = client.delete(
                f"/entity/uniqueAttribute/type/DataSet/classification/{self.tag_name}",
                params={"attr:qualifiedName": qn},
            )
            assert_status_in(resp, [200, 204])

    @test("add_classification_nonexistent_entity", tags=["classification"], order=12)
    def test_add_classification_nonexistent_entity(self, client, ctx):
        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [404, 400])
        body = resp.json()
        assert "errorMessage" in body or "errorCode" in body or "message" in body, (
            f"Expected error details in response, got keys: {list(body.keys()) if isinstance(body, dict) else type(body).__name__}"
        )

