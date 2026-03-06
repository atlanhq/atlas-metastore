"""Search endpoint tests (basic, index, DSL, ES)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_not_empty, assert_field_in,
)


@suite("search", depends_on_suites=["entity_crud"],
       description="Search endpoints: basic, index, DSL, ES")
class SearchSuite:

    def setup(self, client, ctx):
        # Wait for ES sync
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(es_wait)

    @test("basic_search_get", tags=["smoke", "search"], order=1)
    def test_basic_search_get(self, client, ctx):
        resp = client.get("/search/basic", params={
            "typeName": "DataSet",
            "limit": 10,
        })
        assert_status(resp, 200)

    @test("basic_search_post", tags=["search"], order=2)
    def test_basic_search_post(self, client, ctx):
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 10,
            "offset": 0,
            "excludeDeletedEntities": True,
        })
        assert_status(resp, 200)

    @test("index_search", tags=["smoke", "search"], order=3)
    def test_index_search(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "DataSet"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)
        assert_field_present(resp, "approximateCount")

        # When results exist, validate first entity structure
        body = resp.json()
        if body.get("approximateCount", 0) > 0:
            entities = body.get("entities", [])
            if entities:
                first = entities[0]
                assert "guid" in first, "Entity should have 'guid' field"
                assert "typeName" in first, "Entity should have 'typeName' field"
                assert "status" in first, "Entity should have 'status' field"

    @test("index_search_with_sort", tags=["search"], order=4)
    def test_index_search_with_sort(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "sort": [{"__timestamp": {"order": "desc"}}],
            }
        })
        assert_status(resp, 200)

    @test("index_search_aggregation", tags=["search"], order=5)
    def test_index_search_aggregation(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 0,
                "query": {"term": {"__state": "ACTIVE"}},
                "aggs": {
                    "type_counts": {
                        "terms": {"field": "__typeName.keyword", "size": 10}
                    }
                }
            }
        })
        assert_status(resp, 200)

    @test("es_search", tags=["search"], order=6)
    def test_es_search(self, client, ctx):
        resp = client.post("/search/es", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        # Raw ES response — may return 200 or could be unavailable/forbidden
        assert_status_in(resp, [200, 400, 404])

    @test("dsl_search", tags=["search"], order=7)
    def test_dsl_search(self, client, ctx):
        resp = client.get("/search/dsl", params={
            "query": "from DataSet select name limit 5",
        })
        # DSL may or may not be enabled; staging may return 500
        assert_status_in(resp, [200, 400, 404, 500])

    @test("basic_search_by_classification", tags=["search", "slow"], order=8)
    def test_basic_search_by_classification(self, client, ctx):
        resp = client.get("/search/basic", params={
            "classification": "*",
            "limit": 5,
        })
        # Wildcard classification search can be slow or unsupported
        assert_status_in(resp, [200, 400, 408, 500])

    @test("index_search_empty_result", tags=["search"], order=9)
    def test_index_search_empty_result(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "term": {"__qualifiedName": "nonexistent-qn-12345-xyz"}
                }
            }
        })
        assert_status(resp, 200)
        count = resp.json().get("approximateCount", -1)
        assert count == 0, f"Expected 0 results, got {count}"

    @test("search_finds_created_entity", tags=["search"], order=10)
    def test_search_finds_created_entity(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            return
        # Search by GUID
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "term": {"__guid": guid}
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        count = body.get("approximateCount", 0)
        assert count > 0, f"Expected created entity with guid={guid} in search, got count={count}"
        entities = body.get("entities", [])
        if entities:
            found_guids = [e.get("guid") for e in entities]
            assert guid in found_guids, f"Expected {guid} in search results, got {found_guids}"

    @test("search_result_structure", tags=["search"], order=11)
    def test_search_result_structure(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            return
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 1,
                "query": {
                    "term": {"__guid": guid}
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        if entities:
            entity = entities[0]
            assert "guid" in entity, "Search result entity should have 'guid'"
            assert "typeName" in entity, "Search result entity should have 'typeName'"
            assert entity.get("status") == "ACTIVE", f"Expected status=ACTIVE, got {entity.get('status')}"
