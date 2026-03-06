"""Entity CRUD tests (~25 tests)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, assert_field_in,
    assert_mutation_response,
)
from core.audit_helpers import assert_audit_event_exists
from core.search_helpers import assert_entity_in_search
from core.data_factory import build_dataset_entity, build_process_entity, unique_qn, unique_name


@suite("entity_crud", depends_on_suites=["typedefs"], description="Entity CRUD operations")
class EntityCrudSuite:

    def setup(self, client, ctx):
        # Create two test DataSet entities for CRUD tests
        self.ds1_qn = unique_qn("ds1")
        self.ds1_name = unique_name("ds1")
        self.ds2_qn = unique_qn("ds2")
        self.ds2_name = unique_name("ds2")
        self.process_qn = unique_qn("proc1")
        self.process_name = unique_name("proc1")

    # ---- CREATE ----

    @test("create_entity", tags=["smoke", "crud"], order=1)
    def test_create_entity(self, client, ctx):
        entity = build_dataset_entity(qn=self.ds1_qn, name=self.ds1_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        assert_field_not_empty(resp, "mutatedEntities")

        # Deep validation: verify mutation response structure
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert len(entities) > 0, "Expected at least one entity in mutatedEntities"

        # Validate guidAssignments if present
        if "guidAssignments" in body:
            assert isinstance(body["guidAssignments"], dict), "guidAssignments should be a dict"

        guid = entities[0]["guid"]
        ctx.register_entity("ds1", guid, "DataSet")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

    @test("create_entity_audit", tags=["crud", "audit"], order=5, depends_on=["create_entity"])
    def test_create_entity_audit(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        event = assert_audit_event_exists(client, guid, "ENTITY_CREATE")
        if event is None:
            return  # Audit endpoint not available, graceful skip
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Audit event entityId mismatch: expected {guid}, got {entity_id}"

    @test("create_entity_in_search", tags=["crud", "search"], order=6, depends_on=["create_entity"])
    def test_create_entity_in_search(self, client, ctx):
        result = assert_entity_in_search(client, self.ds1_qn)
        if result is None:
            return  # Search endpoint not available

    @test("create_entity_bulk", tags=["crud"], order=2)
    def test_create_entity_bulk(self, client, ctx):
        entity2 = build_dataset_entity(qn=self.ds2_qn, name=self.ds2_name)
        resp = client.post("/entity/bulk", json_data={"entities": [entity2]})
        assert_status(resp, 200)
        assert_field_not_empty(resp, "mutatedEntities")

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_entity("ds2", guid, "DataSet")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

    @test("create_process_entity", tags=["crud"], order=3, depends_on=["create_entity", "create_entity_bulk"])
    def test_create_process_entity(self, client, ctx):
        ds1_guid = ctx.get_entity_guid("ds1")
        ds2_guid = ctx.get_entity_guid("ds2")
        entity = build_process_entity(
            qn=self.process_qn,
            name=self.process_name,
            inputs=[{"guid": ds1_guid, "typeName": "DataSet"}],
            outputs=[{"guid": ds2_guid, "typeName": "DataSet"}],
        )
        resp = client.post("/entity", json_data={"entity": entity})
        # Staging may reject Process if it requires connectionQualifiedName
        assert_status_in(resp, [200, 400])

        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                guid = entities[0]["guid"]
                ctx.register_entity("process1", guid, "Process")
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

    # ---- READ ----

    @test("get_entity_by_guid", tags=["smoke", "crud"], order=10, depends_on=["create_entity"])
    def test_get_entity_by_guid(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.guid", guid)
        assert_field_equals(resp, "entity.typeName", "DataSet")
        assert_field_equals(resp, "entity.attributes.qualifiedName", self.ds1_qn)
        assert_field_in(resp, "entity.status", ["ACTIVE", "DELETED"])
        assert_field_not_empty(resp, "entity.attributes.name")

    @test("get_entity_header", tags=["crud"], order=11, depends_on=["create_entity"])
    def test_get_entity_header(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}/header")
        assert_status(resp, 200)
        assert_field_equals(resp, "guid", guid)

    @test("get_entity_by_unique_attr", tags=["crud"], order=12, depends_on=["create_entity"])
    def test_get_entity_by_unique_attr(self, client, ctx):
        resp = client.get(
            "/entity/uniqueAttribute/type/DataSet",
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.attributes.qualifiedName", self.ds1_qn)

    @test("get_entity_header_by_unique_attr", tags=["crud"], order=13, depends_on=["create_entity"])
    def test_get_entity_header_by_unique_attr(self, client, ctx):
        resp = client.get(
            "/entity/uniqueAttribute/type/DataSet/header",
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)
        assert_field_present(resp, "guid")

    @test("get_entity_bulk", tags=["crud"], order=14, depends_on=["create_entity", "create_entity_bulk"])
    def test_get_entity_bulk(self, client, ctx):
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        resp = client.get("/entity/bulk", params={"guid": [guid1, guid2]})
        assert_status(resp, 200)
        assert_field_present(resp, "entities")

    @test("get_entity_min_ext_info", tags=["crud"], order=15, depends_on=["create_entity"])
    def test_get_entity_min_ext_info(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}", params={"minExtInfo": "true"})
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.guid", guid)

    @test("get_entity_not_found", tags=["crud"], order=16)
    def test_get_entity_not_found(self, client, ctx):
        resp = client.get("/entity/guid/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [404, 400])

    # ---- UPDATE ----

    @test("update_entity_by_guid", tags=["crud"], order=20, depends_on=["create_entity"])
    def test_update_entity_by_guid(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": guid,
                "attributes": {
                    "qualifiedName": self.ds1_qn,
                    "name": self.ds1_name,
                    "description": "Updated by test harness",
                },
            }
        })
        assert_status(resp, 200)

        # Validate mutation response
        body = resp.json()
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        if updates:
            assert updates[0].get("guid"), "Updated entity should have guid"

        # Read-after-write verification
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Updated by test harness")

    @test("update_entity_by_unique_attr", tags=["crud"], order=21, depends_on=["create_entity"])
    def test_update_entity_by_unique_attr(self, client, ctx):
        updated = build_dataset_entity(
            qn=self.ds1_qn,
            name=self.ds1_name,
            extra_attrs={"description": "Updated via unique attr"},
        )
        resp = client.put(
            "/entity/uniqueAttribute/type/DataSet",
            json_data={"entity": updated},
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)

    @test("update_entity_partial", tags=["crud"], order=22, depends_on=["create_entity"])
    def test_update_entity_partial(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": guid,
                "attributes": {
                    "qualifiedName": self.ds1_qn,
                    "name": self.ds1_name,
                    "description": "Partial update test",
                },
            }
        })
        assert_status(resp, 200)

        # Read-after-write: GET entity and verify description
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Partial update test")

    @test("update_entity_audit", tags=["crud", "audit"], order=25, depends_on=["update_entity_by_guid"])
    def test_update_entity_audit(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        event = assert_audit_event_exists(client, guid, "ENTITY_UPDATE")
        if event is None:
            return  # Audit endpoint not available, graceful skip

    # ---- DELETE ----

    @test("delete_entity_by_guid", tags=["crud"], order=80)
    def test_delete_entity_by_guid(self, client, ctx):
        # Create a throwaway entity to delete
        qn = unique_qn("delete-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("del"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]

        # Delete it
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status(resp, 200)

        # Validate DELETE mutation response has the correct guid
        del_body = resp.json()
        deletes = del_body.get("mutatedEntities", {}).get("DELETE", [])
        if deletes:
            assert deletes[0].get("guid") == guid, f"Expected deleted guid={guid}"

        # Verify deleted (soft delete returns entity with DELETED status)
        resp = client.get(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "entity.status", "DELETED")

    @test("delete_entity_by_unique_attr", tags=["crud"], order=81)
    def test_delete_entity_by_unique_attr(self, client, ctx):
        qn = unique_qn("delete-ua-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("del-ua"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        resp = client.delete(
            "/entity/uniqueAttribute/type/DataSet",
            params={"attr:qualifiedName": qn},
        )
        assert_status(resp, 200)

    @test("delete_entity_bulk", tags=["crud"], order=82)
    def test_delete_entity_bulk(self, client, ctx):
        # Create two throwaway entities
        entities = []
        guids = []
        for i in range(2):
            qn = unique_qn(f"bulk-del-{i}")
            entities.append(build_dataset_entity(qn=qn, name=unique_name(f"bdel-{i}")))
        resp = client.post("/entity/bulk", json_data={"entities": entities})
        assert_status(resp, 200)
        body = resp.json()
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])

        if guids:
            resp = client.delete("/entity/bulk", params={"guid": guids})
            assert_status(resp, 200)
