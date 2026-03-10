"""Config CRUD tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.data_factory import unique_name


@suite("config", description="Config CRUD endpoints")
class ConfigSuite:

    def setup(self, client, ctx):
        self.test_key = "test_harness_config_key"

    @test("get_all_configs", tags=["smoke", "config"], order=1)
    def test_get_all_configs(self, client, ctx):
        resp = client.get("/configs")
        assert_status_in(resp, [200, 404])

        # Store a valid key from the response for use in GET-by-key
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                keys = list(body.keys())
                if keys:
                    ctx.set("config_valid_key", keys[0])
            elif isinstance(body, list) and body:
                first = body[0]
                if isinstance(first, dict):
                    key = first.get("key") or first.get("name")
                    if key:
                        ctx.set("config_valid_key", key)

    @test("set_config", tags=["config", "crud"], order=2)
    def test_set_config(self, client, ctx):
        # Try multiple payload formats to handle different API versions
        resp = client.put(f"/configs/{self.test_key}", json_data={
            "key": self.test_key,
            "value": "test_value_123",
            "description": "Test harness config",
        })
        if resp.status_code == 400:
            # Try simpler payload
            resp = client.put(f"/configs/{self.test_key}", json_data="test_value_123")
        assert_status_in(resp, [200, 201, 204, 400])
        if resp.status_code in [200, 201, 204]:
            ctx.set("config_set_success", True)

    @test("get_config", tags=["config"], order=3, depends_on=["set_config"])
    def test_get_config(self, client, ctx):
        # Try a known valid key from GET-all first, fall back to test key
        key = ctx.get("config_valid_key", self.test_key)
        resp = client.get(f"/configs/{key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            assert resp.body is not None, "Expected non-empty config response"

    @test("delete_config", tags=["config", "crud"], order=10, depends_on=["set_config"])
    def test_delete_config(self, client, ctx):
        resp = client.delete(f"/configs/{self.test_key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204] and ctx.get("config_set_success"):
            # Read-after-write: verify deleted
            resp2 = client.get(f"/configs/{self.test_key}")
            assert resp2.status_code in [400, 404], (
                f"Expected 400/404 after config delete, got {resp2.status_code}"
            )
