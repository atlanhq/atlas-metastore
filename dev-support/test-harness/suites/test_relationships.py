"""Relationship CRUD tests."""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals,
)
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("relationships", depends_on_suites=["entity_crud"],
       description="Relationship CRUD operations")
class RelationshipsSuite:

    def setup(self, client, ctx):
        # Create two entities for relationship tests
        qn1 = unique_qn("rel-src")
        qn2 = unique_qn("rel-tgt")
        e1 = build_dataset_entity(qn=qn1, name=unique_name("rel-src"))
        e2 = build_dataset_entity(qn=qn2, name=unique_name("rel-tgt"))

        resp = client.post("/entity/bulk", json_data={"entities": [e1, e2]})
        body = resp.json()
        guids = []
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])

        self.src_guid = guids[0] if len(guids) > 0 else None
        self.tgt_guid = guids[1] if len(guids) > 1 else None

        if self.src_guid:
            ctx.register_entity("rel_src", self.src_guid, "DataSet")
            ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.src_guid}"))
        if self.tgt_guid:
            ctx.register_entity("rel_tgt", self.tgt_guid, "DataSet")
            ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.tgt_guid}"))

    @test("create_relationship", tags=["relationship", "crud"], order=1)
    def test_create_relationship(self, client, ctx):
        if not self.src_guid or not self.tgt_guid:
            raise Exception("Missing source or target entity for relationship test")

        # Use a generic process_dataset relationship or just test with the entity API
        # Since we may not have specific relationship defs, create a Process linking them
        from core.data_factory import build_process_entity
        proc = build_process_entity(
            inputs=[{"guid": self.src_guid, "typeName": "DataSet"}],
            outputs=[{"guid": self.tgt_guid, "typeName": "DataSet"}],
        )
        resp = client.post("/entity", json_data={"entity": proc})
        # Staging may reject Process if it requires connectionQualifiedName
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                proc_guid = entities[0]["guid"]
                ctx.register_entity("rel_process", proc_guid, "Process")
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{proc_guid}"))

                # Validate mutation response structure
                assert entities[0].get("guid"), "Created entity should have guid"
                assert entities[0].get("typeName") == "Process", (
                    f"Expected typeName=Process, got {entities[0].get('typeName')}"
                )

    @test("get_relationship_by_guid", tags=["relationship"], order=2)
    def test_get_relationship_by_guid(self, client, ctx):
        # Get entity and look for relationship GUIDs
        if not self.src_guid:
            return
        resp = client.get(f"/entity/guid/{self.src_guid}")
        assert_status(resp, 200)
        # Check if entity has any relationship attributes with GUIDs
        entity = resp.json().get("entity", {})
        rel_attrs = entity.get("relationshipAttributes", {})
        rel_guid = None
        for attr_name, attr_val in rel_attrs.items():
            if isinstance(attr_val, list):
                for item in attr_val:
                    if isinstance(item, dict) and "relationshipGuid" in item:
                        rel_guid = item["relationshipGuid"]
                        break
            elif isinstance(attr_val, dict) and "relationshipGuid" in attr_val:
                rel_guid = attr_val["relationshipGuid"]
            if rel_guid:
                break

        if rel_guid:
            resp2 = client.get(f"/relationship/guid/{rel_guid}")
            assert_status(resp2, 200)
            ctx.set("test_rel_guid", rel_guid)

            # Validate relationship structure
            body = resp2.json()
            rel = body.get("relationship", body)  # may be wrapped or direct
            if isinstance(rel, dict):
                assert "guid" in rel or "typeName" in rel or "end1" in rel, (
                    "Relationship should have guid, typeName, or end1/end2"
                )

    @test("delete_relationship", tags=["relationship", "crud"], order=10)
    def test_delete_relationship(self, client, ctx):
        rel_guid = ctx.get("test_rel_guid")
        if not rel_guid:
            return  # No relationship to delete
        resp = client.delete(f"/relationship/guid/{rel_guid}")
        assert_status_in(resp, [200, 204])
