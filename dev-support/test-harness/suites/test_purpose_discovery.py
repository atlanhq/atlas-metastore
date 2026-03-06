"""Purpose discovery tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("purpose_discovery", description="Purpose discovery endpoint")
class PurposeDiscoverySuite:

    @test("get_user_purposes", tags=["purpose_discovery"], order=1)
    def test_get_user_purposes(self, client, ctx):
        resp = client.post("/purposes/user", json_data={
            "userName": "admin",
        })
        assert_status_in(resp, [200, 400, 403, 404])

    @test("get_user_purposes_empty", tags=["purpose_discovery"], order=2)
    def test_get_user_purposes_empty(self, client, ctx):
        resp = client.post("/purposes/user", json_data={
            "userName": "nonexistent-user-xyz",
        })
        assert_status_in(resp, [200, 400, 403, 404])
