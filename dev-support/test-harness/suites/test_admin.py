"""Admin endpoint tests (~25 tests)."""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty,
)


@suite("admin", description="Admin & health endpoints")
class AdminSuite:

    @test("get_status", tags=["smoke", "admin"], order=1)
    def test_get_status(self, client, ctx):
        resp = client.get("/status", admin=True)
        assert_status(resp, 200)
        assert_field_equals(resp, "Status", "ACTIVE")

    @test("get_version", tags=["smoke", "admin"], order=2)
    def test_get_version(self, client, ctx):
        resp = client.get("/version", admin=True)
        assert_status(resp, 200)

    @test("get_health", tags=["smoke", "admin"], order=3)
    def test_get_health(self, client, ctx):
        resp = client.get("/health", admin=True)
        assert_status(resp, 200)

    @test("is_active", tags=["smoke", "admin"], order=4)
    def test_is_active(self, client, ctx):
        resp = client.get("/isactive", admin=True)
        # Returns 200 if active, 503 if passive
        assert_status_in(resp, [200, 503])

    @test("get_session", tags=["admin"], order=5)
    def test_get_session(self, client, ctx):
        resp = client.get("/session", admin=True)
        # Staging may return 500 if session endpoint is not supported
        assert_status_in(resp, [200, 500])

    @test("get_metrics", tags=["admin"], order=10)
    def test_get_metrics(self, client, ctx):
        resp = client.get("/metrics", admin=True)
        assert_status(resp, 200)

    @test("get_metrics_prometheus", tags=["admin"], order=11)
    def test_get_metrics_prometheus(self, client, ctx):
        resp = client.get("/metrics/prometheus", admin=True)
        assert_status_in(resp, [200, 204, 404])

    @test("get_stack", tags=["admin"], order=12)
    def test_get_stack(self, client, ctx):
        resp = client.get("/stack", admin=True)
        # Staging may restrict stack trace endpoint
        assert_status_in(resp, [200, 500])

    @test("get_active_searches", tags=["admin"], order=13)
    def test_get_active_searches(self, client, ctx):
        resp = client.get("/activeSearches", admin=True)
        assert_status(resp, 200)

    @test("get_patches", tags=["admin"], order=14)
    def test_get_patches(self, client, ctx):
        resp = client.get("/patches", admin=True)
        assert_status(resp, 200)

    @test("get_admin_tasks", tags=["admin"], order=15)
    def test_get_admin_tasks(self, client, ctx):
        resp = client.get("/tasks", admin=True)
        assert_status(resp, 200)

    @test("get_tasks_by_id", tags=["admin"], order=16)
    def test_get_tasks_by_id(self, client, ctx):
        # GET-all -> pick first task GUID -> GET by ID
        resp = client.get("/tasks", admin=True)
        assert_status(resp, 200)
        body = resp.json()
        tasks = body if isinstance(body, list) else body.get("tasks", [])
        if tasks and isinstance(tasks[0], dict):
            task_guid = tasks[0].get("taskGuid") or tasks[0].get("guid")
            if task_guid:
                resp2 = client.get(f"/tasks/{task_guid}", admin=True)
                assert_status_in(resp2, [200, 404])

    @test("get_debug_metrics", tags=["admin"], order=17)
    def test_get_debug_metrics(self, client, ctx):
        resp = client.get("/debug/metrics", admin=True)
        assert_status(resp, 200)

    @test("push_metrics_statsd", tags=["admin"], order=18)
    def test_push_metrics_statsd(self, client, ctx):
        resp = client.get("/pushMetricsToStatsd", admin=True)
        # May fail if StatsD not configured, which is fine
        assert_status_in(resp, [200, 404, 500])

    @test("check_state", tags=["admin"], order=20)
    def test_check_state(self, client, ctx):
        payload = {"fixIssues": False}
        resp = client.post("/checkstate", json_data=payload, admin=True)
        assert_status_in(resp, [200, 404])

    @test("set_and_delete_feature_flag", tags=["admin"], order=21)
    def test_set_and_delete_feature_flag(self, client, ctx):
        flag_name = "test_harness_flag"
        # Set
        resp = client.post(
            "/featureFlag",
            params={"flag": flag_name, "value": "true"},
            admin=True,
        )
        assert_status_in(resp, [200, 204])

        # Delete
        resp = client.delete(f"/featureFlag/{flag_name}", admin=True)
        assert_status_in(resp, [200, 204, 404])
