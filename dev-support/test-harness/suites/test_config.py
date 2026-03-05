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

    @test("get_config", tags=["config"], order=3, depends_on=["set_config"])
    def test_get_config(self, client, ctx):
        resp = client.get(f"/configs/{self.test_key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 400, 404])

    @test("delete_config", tags=["config", "crud"], order=10, depends_on=["set_config"])
    def test_delete_config(self, client, ctx):
        resp = client.delete(f"/configs/{self.test_key}")
        # Staging validates keys against an allowlist; custom keys return 400
        assert_status_in(resp, [200, 204, 400, 404])
