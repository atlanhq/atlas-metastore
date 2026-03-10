"""Direct ES search tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


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
            pit_id = body.get("pitId") or body.get("id")
            if pit_id:
                ctx.set("pit_id", pit_id)

    @test("direct_search_pit_delete", tags=["search", "direct_search"], order=3,
          depends_on=["direct_search_pit_create"])
    def test_direct_search_pit_delete(self, client, ctx):
        pit_id = ctx.get("pit_id")
        if not pit_id:
            return
        resp = client.post("/direct/search", json_data={
            "searchType": "PIT_DELETE",
            "pitId": pit_id,
        })
        assert_status_in(resp, [200, 400, 403])
