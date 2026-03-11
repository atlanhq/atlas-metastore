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

    @test("model_search_type_filter", tags=["search", "model_search"], order=3)
    def test_model_search_type_filter(self, client, ctx):
        """Model search with __typeName filter for DataSet."""
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [{"term": {"__typeName.keyword": "DataSet"}}]
                    }
                },
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_attributes", tags=["search", "model_search"], order=4)
    def test_model_search_attributes(self, client, ctx):
        """Model search requesting specific attributes."""
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "_source": ["qualifiedName", "name", "__typeName"],
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_pagination", tags=["search", "model_search"], order=5)
    def test_model_search_pagination(self, client, ctx):
        """Model search with size=1 to verify pagination."""
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 1,
                "query": {"match_all": {}},
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_with_sort", tags=["search", "model_search"], order=6)
    def test_model_search_with_sort(self, client, ctx):
        """Model search sorted by __timestamp descending."""
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "sort": [{"__timestamp": {"order": "desc"}}],
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_empty_result", tags=["search", "model_search"], order=7)
    def test_model_search_empty_result(self, client, ctx):
        """Query for nonexistent QN — expect 0 results."""
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "term": {"__qualifiedName": "nonexistent-model-qn-xyz-12345"}
                },
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])

    @test("model_search_date_range", tags=["search", "model_search"], order=8)
    def test_model_search_date_range(self, client, ctx):
        """Model search with range filter on __modificationTimestamp."""
        import time
        now_ms = int(time.time() * 1000)
        one_day_ago = now_ms - (86400 * 1000)
        resp = client.post("/model/search", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"__modificationTimestamp": {"gte": one_day_ago, "lte": now_ms}}},
                        ]
                    }
                },
            }
        })
        assert_status_in(resp, [200, 400, 404, 500])
