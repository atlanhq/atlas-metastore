"""DLQ replay status tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("dlq", description="DLQ replay status endpoint")
class DlqSuite:

    @test("get_dlq_status", tags=["smoke", "dlq"], order=1)
    def test_get_dlq_status(self, client, ctx):
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list from DLQ status, got {type(body).__name__}"
            )

    @test("dlq_replay_trigger", tags=["dlq"], order=2)
    def test_dlq_replay_trigger(self, client, ctx):
        """POST /dlq/replay — trigger replay."""
        resp = client.post("/dlq/replay", json_data={})
        assert_status_in(resp, [200, 202, 400, 404])

    @test("dlq_status_after_replay", tags=["dlq"], order=3, depends_on=["dlq_replay_trigger"])
    def test_dlq_status_after_replay(self, client, ctx):
        """After triggering replay, GET status."""
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])

    @test("dlq_status_structure", tags=["dlq"], order=4)
    def test_dlq_status_structure(self, client, ctx):
        """When DLQ status returns 200, validate response structure."""
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                has_fields = any(k in body for k in ("status", "count", "lastRunTime", "state"))
                assert has_fields or body == {}, (
                    f"DLQ status should have status/count/state fields, got keys: {list(body.keys())}"
                )

    @test("dlq_replay_with_params", tags=["dlq"], order=5)
    def test_dlq_replay_with_params(self, client, ctx):
        """POST /dlq/replay with optional params."""
        resp = client.post("/dlq/replay", json_data={
            "type": "ES",
            "limit": 10,
        })
        assert_status_in(resp, [200, 202, 400, 404])
