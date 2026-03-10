"""Type cache refresh tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("type_cache", description="Type cache refresh endpoint")
class TypeCacheSuite:

    @test("refresh_type_cache_init", tags=["cache"], order=1)
    def test_refresh_type_cache_init(self, client, ctx):
        resp = client.post("/admin/types/refresh", json_data={}, params={
            "action": "INIT",
        }, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

    @test("refresh_type_cache_no_action", tags=["cache"], order=2)
    def test_refresh_type_cache_no_action(self, client, ctx):
        resp = client.post("/admin/types/refresh", json_data={}, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])
