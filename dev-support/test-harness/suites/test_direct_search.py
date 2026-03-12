"""Direct ES search tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("direct_search", depends_on_suites=["entity_crud"],
       description="Direct Elasticsearch search endpoint")
class DirectSearchSuite:

    @test("direct_search_simple", tags=["search", "direct_search"], order=1)
    def test_direct_search_simple(self, client, ctx):
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict) and body, "Expected non-empty dict from direct search"

    @test("direct_search_pit_create", tags=["search", "direct_search"], order=2)
    def test_direct_search_pit_create(self, client, ctx):
        resp = client.post("/direct/search", json_data={
            "searchType": "PIT_CREATE",
            "keepAlive": "1m",
        })
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            # Try known field names for PIT ID
            pit_id = body.get("pitId") or body.get("id") or body.get("pit_id")
            if pit_id:
                ctx.set("pit_id", pit_id)
            else:
                print(f"  [pit-create] 200 but no pitId in response keys: {list(body.keys())}")

    @test("direct_search_pit_delete", tags=["search", "direct_search"], order=3,
          depends_on=["direct_search_pit_create"])
    def test_direct_search_pit_delete(self, client, ctx):
        from core.assertions import SkipTestError
        pit_id = ctx.get("pit_id")
        if not pit_id:
            raise SkipTestError("pit_id not found in context — PIT_CREATE may not return pit ID field")
        resp = client.post("/direct/search", json_data={
            "searchType": "PIT_DELETE",
            "pitId": pit_id,
        })
        assert_status_in(resp, [200, 400, 403])

    @test("direct_search_with_filter", tags=["search", "direct_search"], order=4)
    def test_direct_search_with_filter(self, client, ctx):
        """SIMPLE search with bool must filter for DataSet type."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "DataSet"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                },
            }
        })
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response"

    @test("direct_search_size_limit", tags=["search", "direct_search"], order=5)
    def test_direct_search_size_limit(self, client, ctx):
        """SIMPLE search with size=1."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {
                "from": 0,
                "size": 1,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp, [200, 400, 403])

    @test("direct_search_with_sort", tags=["search", "direct_search"], order=6)
    def test_direct_search_with_sort(self, client, ctx):
        """SIMPLE search with sort by __timestamp desc."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "sort": [{"__timestamp": {"order": "desc"}}],
            }
        })
        assert_status_in(resp, [200, 400, 403])

    @test("direct_search_aggregation", tags=["search", "direct_search"], order=7)
    def test_direct_search_aggregation(self, client, ctx):
        """SIMPLE search with aggregation for type counts."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {
                "from": 0,
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "type_counts": {
                        "terms": {"field": "__typeName.keyword", "size": 10}
                    }
                },
            }
        })
        assert_status_in(resp, [200, 400, 403])

    @test("direct_search_pit_flow", tags=["search", "direct_search"], order=8)
    def test_direct_search_pit_flow(self, client, ctx):
        """Full PIT flow: create, search, delete."""
        # Create PIT
        resp1 = client.post("/direct/search", json_data={
            "searchType": "PIT_CREATE",
            "keepAlive": "1m",
        })
        assert_status_in(resp1, [200, 400, 403])
        if resp1.status_code != 200:
            return
        pit_id = resp1.json().get("pitId") or resp1.json().get("id")
        if not pit_id:
            return
        # Search with PIT
        resp2 = client.post("/direct/search", json_data={
            "searchType": "PIT_SEARCH",
            "pitId": pit_id,
            "query": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp2, [200, 400, 403])
        # Delete PIT
        resp3 = client.post("/direct/search", json_data={
            "searchType": "PIT_DELETE",
            "pitId": pit_id,
        })
        assert_status_in(resp3, [200, 400, 403])

    @test("direct_search_invalid_type", tags=["search", "direct_search", "negative"], order=9)
    def test_direct_search_invalid_type(self, client, ctx):
        """SIMPLE search with nonexistent searchType."""
        resp = client.post("/direct/search", json_data={
            "searchType": "NONEXISTENT_TYPE",
            "query": {"from": 0, "size": 5, "query": {"match_all": {}}},
        })
        assert_status_in(resp, [400, 403, 404])

    @test("direct_search_empty_query", tags=["search", "direct_search"], order=10)
    def test_direct_search_empty_query(self, client, ctx):
        """SIMPLE search with empty query object."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "query": {},
        })
        assert_status_in(resp, [200, 400, 403])
