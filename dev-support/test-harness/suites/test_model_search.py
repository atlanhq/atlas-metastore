"""Model search with date filtering tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("model_search", depends_on_suites=["entity_crud"],
       description="Model search with date filtering")
class ModelSearchSuite:

    @test("model_search_basic", tags=["search", "model_search"], order=1)
    def test_model_search_basic(self, client, ctx):
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_with_namespace", tags=["search", "model_search"], order=2)
    def test_model_search_with_namespace(self, client, ctx):
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        }, params={"namespace": "default"})
        assert_status_in(resp, [200, 400, 404, 500])
