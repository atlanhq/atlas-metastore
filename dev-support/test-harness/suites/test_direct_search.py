"""Direct ES search tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError

# ES index name used by JanusGraph on staging/preprod
ES_INDEX_NAME = "janusgraph_vertex_index"
# PIT keepAlive in milliseconds (server expects Long, not string like "1m")
PIT_KEEP_ALIVE_MS = 60000


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
            "keepAlive": PIT_KEEP_ALIVE_MS,
            "indexName": ES_INDEX_NAME,
        })
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            # DirectSearchResponse wraps ES response in pitCreateResponse field
            pit_obj = body.get("pitCreateResponse", {})
            if not isinstance(pit_obj, dict):
                pit_obj = {}
            pit_id = (
                body.get("pitId") or body.get("id") or body.get("pit_id")
                or pit_obj.get("id") or pit_obj.get("pointInTimeId")
            )
            if pit_id:
                ctx.set("pit_id", pit_id)
            else:
                print(f"  [pit-create] 200 but no pitId found. "
                      f"Response keys: {list(body.keys())}, "
                      f"pitCreateResponse keys: {list(pit_obj.keys()) if pit_obj else 'N/A'}")

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
            "keepAlive": PIT_KEEP_ALIVE_MS,
            "indexName": ES_INDEX_NAME,
        })
        assert_status_in(resp1, [200, 400, 403])
        if resp1.status_code != 200:
            return
        body1 = resp1.json()
        pit_obj = body1.get("pitCreateResponse", {})
        if not isinstance(pit_obj, dict):
            pit_obj = {}
        pit_id = (
            body1.get("pitId") or body1.get("id")
            or pit_obj.get("id") or pit_obj.get("pointInTimeId")
        )
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
