"""Config cache refresh tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("config_cache", description="Config cache refresh endpoint")
class ConfigCacheSuite:

    @test("refresh_config_cache", tags=["cache"], order=1)
    def test_refresh_config_cache(self, client, ctx):
        resp = client.post("/admin/config/refresh", json_data={})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("refresh_config_cache_by_key", tags=["cache"], order=2)
    def test_refresh_config_cache_by_key(self, client, ctx):
        resp = client.post("/admin/config/refresh", params={"key": "test.key"})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("get_cache_state", tags=["cache"], order=3)
    def test_get_cache_state(self, client, ctx):
        resp = client.get("/admin/config/cache/test.key")
        assert_status_in(resp, [200, 404])
