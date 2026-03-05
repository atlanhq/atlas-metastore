"""Auth download endpoint tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("auth", description="Auth download endpoints")
class AuthSuite:

    @test("download_roles", tags=["auth"], order=1)
    def test_download_roles(self, client, ctx):
        resp = client.get("/auth/download/roles/atlas", params={
            "pluginId": "test-harness",
        })
        assert_status_in(resp, [200, 404, 403])

    @test("download_users", tags=["auth"], order=2)
    def test_download_users(self, client, ctx):
        resp = client.get("/auth/download/users/atlas", params={
            "pluginId": "test-harness",
        })
        assert_status_in(resp, [200, 404, 403])

    @test("download_policies", tags=["auth"], order=3)
    def test_download_policies(self, client, ctx):
        resp = client.get("/auth/download/policies/atlas", params={
            "pluginId": "test-harness",
        })
        assert_status_in(resp, [200, 404, 403])
