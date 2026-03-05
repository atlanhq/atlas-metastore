"""Business policy link/unlink tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("business_policy", depends_on_suites=["entity_crud"],
       description="Business policy link/unlink")
class BusinessPolicySuite:

    @test("link_policy_invalid", tags=["business_policy"], order=1)
    def test_link_policy_invalid(self, client, ctx):
        resp = client.post("/business-policy/link-business-policy", json_data={
            "policyId": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("unlink_policy_invalid", tags=["business_policy"], order=2)
    def test_unlink_policy_invalid(self, client, ctx):
        resp = client.post(
            "/business-policy/00000000-0000-0000-0000-000000000000/unlink-business-policy",
            json_data={"assetGuids": []},
        )
        assert_status_in(resp, [200, 204, 400, 404, 500])

    @test("unlink_policy_v2_invalid", tags=["business_policy"], order=3)
    def test_unlink_policy_v2_invalid(self, client, ctx):
        resp = client.post("/business-policy/unlink-business-policy/v2", json_data={
            "policyId": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])
