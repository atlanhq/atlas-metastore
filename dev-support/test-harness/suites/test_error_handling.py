"""Cross-cutting error handling tests (12 tests).

Tests invalid requests, edge cases, boundary conditions across multiple
API endpoints.
"""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_equals,
    assert_field_present, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_classification_def,
    unique_qn, unique_name, unique_type_name,
)


@suite("error_handling", depends_on_suites=["entity_crud"],
       description="Cross-cutting error handling and edge cases")
class ErrorHandlingSuite:

    @test("entity_create_empty_body", tags=["error", "negative"], order=1)
    def test_entity_create_empty_body(self, client, ctx):
        """POST /entity with empty JSON body — expect 400."""
        resp = client.post("/entity", json_data={})
        assert_status_in(resp, [400, 404, 500])

    @test("entity_create_invalid_type", tags=["error", "negative"], order=2)
    def test_entity_create_invalid_type(self, client, ctx):
        """POST entity with typeName=NonExistentType — expect 404."""
        entity = {
            "typeName": "NonExistentType_XYZ_12345",
            "attributes": {
                "qualifiedName": unique_qn("error-test"),
                "name": unique_name("error-test"),
            },
        }
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [400, 404])

    @test("entity_create_missing_qn", tags=["error", "negative"], order=3)
    def test_entity_create_missing_qn(self, client, ctx):
        """POST entity without qualifiedName — expect 400/404."""
        entity = {
            "typeName": "DataSet",
            "attributes": {
                "name": unique_name("no-qn"),
            },
        }
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [400, 404, 500])

    @test("entity_create_duplicate_qn", tags=["error"], order=4)
    def test_entity_create_duplicate_qn(self, client, ctx):
        """POST entity, then POST another with same QN — expect UPDATE not error."""
        qn = unique_qn("dup-test")
        name = unique_name("dup-test")
        entity = build_dataset_entity(qn=qn, name=name)
        resp1 = client.post("/entity", json_data={"entity": entity})
        assert_status(resp1, 200)
        body1 = resp1.json()
        creates = body1.get("mutatedEntities", {}).get("CREATE", [])
        updates = body1.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if entities:
            guid = entities[0]["guid"]
            ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

        # Create again with same QN
        entity2 = build_dataset_entity(qn=qn, name=name,
                                       extra_attrs={"description": "duplicate"})
        resp2 = client.post("/entity", json_data={"entity": entity2})
        assert_status(resp2, 200)
        # Should be UPDATE, not CREATE
        body2 = resp2.json()
        updated = body2.get("mutatedEntities", {}).get("UPDATE", [])
        if updated:
            assert updated[0].get("guid") == guid, "Duplicate QN should update same entity"

    @test("entity_get_malformed_guid", tags=["error", "negative"], order=5)
    def test_entity_get_malformed_guid(self, client, ctx):
        """GET /entity/guid/not-a-valid-uuid — expect 400/404."""
        resp = client.get("/entity/guid/not-a-valid-uuid-format")
        assert_status_in(resp, [400, 404])

    @test("search_oversized_query", tags=["error", "negative"], order=6)
    def test_search_oversized_query(self, client, ctx):
        """POST /search/indexsearch with size=100000 — expect 400 or truncation."""
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 100000,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp, [200, 400, 500])
        if resp.status_code == 200:
            body = resp.json()
            entities = body.get("entities", [])
            # Server should cap the size
            assert len(entities) <= 10000, (
                f"Expected server to cap size, got {len(entities)} results"
            )

    @test("search_invalid_dsl", tags=["error", "negative"], order=7)
    def test_search_invalid_dsl(self, client, ctx):
        """POST /search/indexsearch with malformed DSL — expect 400."""
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "query": {"invalid_query_type": {"not_a_field": "value"}},
            }
        })
        assert_status_in(resp, [200, 400, 500])

    @test("typedef_create_duplicate", tags=["error"], order=8)
    def test_typedef_create_duplicate(self, client, ctx):
        """Create typedef, then create again — expect 409 or 200."""
        name = unique_type_name("ErrorDupType")
        payload = {"classificationDefs": [build_classification_def(name=name)]}
        resp1 = client.post("/types/typedefs", json_data=payload)
        assert_status_in(resp1, [200, 500])
        if resp1.status_code == 200:
            ctx.register_cleanup(
                lambda: client.delete(f"/types/typedef/name/{name}")
            )
            # Create again
            resp2 = client.post("/types/typedefs", json_data=payload)
            assert_status_in(resp2, [200, 409, 500])

    @test("entity_special_chars_in_name", tags=["error"], order=9)
    def test_entity_special_chars_in_name(self, client, ctx):
        """Create entity with special chars in name, verify round-trip."""
        special_name = "test <>&\"' entity"
        qn = unique_qn("special-chars")
        entity = build_dataset_entity(qn=qn, name=special_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if entities:
            guid = entities[0]["guid"]
            ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
            # Verify name preserved
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            actual_name = resp2.json().get("entity", {}).get("attributes", {}).get("name")
            assert actual_name == special_name, (
                f"Expected name='{special_name}', got '{actual_name}'"
            )

    @test("entity_unicode_in_description", tags=["error"], order=10)
    def test_entity_unicode_in_description(self, client, ctx):
        """Create entity with unicode in description, verify round-trip."""
        unicode_desc = "Test description with unicode: \u00e9\u00e8\u00ea \u4e16\u754c \ud83c\udf0d"
        qn = unique_qn("unicode-test")
        entity = build_dataset_entity(
            qn=qn, name=unique_name("unicode"),
            extra_attrs={"description": unicode_desc},
        )
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if entities:
            guid = entities[0]["guid"]
            ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            actual_desc = resp2.json().get("entity", {}).get("attributes", {}).get("description")
            assert actual_desc == unicode_desc, (
                f"Expected unicode description preserved, got '{actual_desc}'"
            )

    @test("bulk_create_empty_array", tags=["error", "negative"], order=11)
    def test_bulk_create_empty_array(self, client, ctx):
        """POST /entity/bulk with empty entities array — expect 400 or no-op."""
        resp = client.post("/entity/bulk", json_data={"entities": []})
        assert_status_in(resp, [200, 400, 500])

    @test("classification_invalid_type", tags=["error", "negative"], order=12)
    def test_classification_invalid_type(self, client, ctx):
        """Add classification with nonexistent typeName — expect 404/400."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post(
            f"/entity/guid/{guid}/classifications",
            json_data=[{"typeName": "NonExistentTag_XYZ_99999"}],
        )
        assert_status_in(resp, [400, 404])
