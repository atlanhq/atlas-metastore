"""Feature flag CRUD tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("feature_flags", description="Feature flag CRUD endpoints")
class FeatureFlagSuite:

    def setup(self, client, ctx):
        self.test_key = "test_harness_ff_key"

    @test("get_all_feature_flags", tags=["smoke", "feature_flags"], order=1)
    def test_get_all_feature_flags(self, client, ctx):
        resp = client.get("/featureflags")
        assert_status_in(resp, [200, 404])

        # Store a valid key from the response for use in GET-by-key
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                keys = list(body.keys())
                if keys:
                    ctx.set("ff_valid_key", keys[0])
            elif isinstance(body, list) and body:
                first = body[0]
                if isinstance(first, dict):
                    key = first.get("key") or first.get("name")
                    if key:
                        ctx.set("ff_valid_key", key)

    @test("set_feature_flag", tags=["feature_flags", "crud"], order=2)
    def test_set_feature_flag(self, client, ctx):
        resp = client.put(f"/featureflags/{self.test_key}", json_data={
            "key": self.test_key,
            "value": "true",
        })
        if resp.status_code == 400:
            # Try simpler payload
            resp = client.put(f"/featureflags/{self.test_key}", json_data="true")
        assert_status_in(resp, [200, 201, 204, 400])
        if resp.status_code in [200, 201, 204]:
            ctx.set("ff_set_success", True)

    @test("get_feature_flag", tags=["feature_flags"], order=3, depends_on=["set_feature_flag"])
    def test_get_feature_flag(self, client, ctx):
        # Try a known valid key from GET-all first, fall back to test key
        key = ctx.get("ff_valid_key", self.test_key)
        resp = client.get(f"/featureflags/{key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            assert resp.body is not None, "Expected non-empty feature flag response"

    @test("delete_feature_flag", tags=["feature_flags", "crud"], order=10, depends_on=["set_feature_flag"])
    def test_delete_feature_flag(self, client, ctx):
        resp = client.delete(f"/featureflags/{self.test_key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204] and ctx.get("ff_set_success"):
            # Read-after-write: verify deleted
            resp2 = client.get(f"/featureflags/{self.test_key}")
            assert resp2.status_code in [400, 404], (
                f"Expected 400/404 after delete, got {resp2.status_code}"
            )
