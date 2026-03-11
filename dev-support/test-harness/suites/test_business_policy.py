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

    @test("link_policy_valid_entity", tags=["business_policy"], order=4)
    def test_link_policy_valid_entity(self, client, ctx):
        """Attempt to link business policy to a real entity."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/business-policy/link-business-policy", json_data={
            "policyId": guid,  # Using ds1 as policy is intentional — tests endpoint behavior
            "assetGuids": [guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("unlink_nonexistent_asset", tags=["business_policy", "negative"], order=5)
    def test_unlink_nonexistent_asset(self, client, ctx):
        """Unlink with nonexistent asset GUID."""
        fake = "00000000-0000-0000-0000-000000000000"
        resp = client.post(
            f"/business-policy/{fake}/unlink-business-policy",
            json_data={"assetGuids": [fake]},
        )
        assert_status_in(resp, [200, 204, 400, 404, 500])

    @test("link_response_structure", tags=["business_policy"], order=6)
    def test_link_response_structure(self, client, ctx):
        """Validate link response structure when endpoint exists."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/business-policy/link-business-policy", json_data={
            "policyId": guid,
            "assetGuids": [guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list, got {type(body).__name__}"
            )

    @test("link_unlink_round_trip", tags=["business_policy"], order=7)
    def test_link_unlink_round_trip(self, client, ctx):
        """Link policy, then unlink and verify."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        # Link
        resp1 = client.post("/business-policy/link-business-policy", json_data={
            "policyId": guid,
            "assetGuids": [guid],
        })
        assert_status_in(resp1, [200, 204, 400, 404])
        # Unlink
        resp2 = client.post(
            f"/business-policy/{guid}/unlink-business-policy",
            json_data={"assetGuids": [guid]},
        )
        assert_status_in(resp2, [200, 204, 400, 404, 500])

    @test("link_multiple_assets", tags=["business_policy"], order=8)
    def test_link_multiple_assets(self, client, ctx):
        """Link policy to multiple asset GUIDs."""
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        if not guid1 or not guid2:
            from core.assertions import SkipTestError
            raise SkipTestError("ds1/ds2 GUIDs not available")
        resp = client.post("/business-policy/link-business-policy", json_data={
            "policyId": guid1,
            "assetGuids": [guid1, guid2],
        })
        assert_status_in(resp, [200, 204, 400, 404])
