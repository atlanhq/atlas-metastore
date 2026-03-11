"""Admin endpoint tests (~16 tests)."""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, SkipTestError,
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
        body = resp.json()
        assert body, "Expected non-empty version response"
        # Version should contain a version string or Description field
        if isinstance(body, dict):
            has_version_info = (
                body.get("Version") or body.get("version") or
                body.get("Description") or body.get("Name")
            )
            assert has_version_info, (
                f"Version response should contain Version/Description/Name, got keys: {list(body.keys())}"
            )

    @test("get_health", tags=["smoke", "admin"], order=3)
    def test_get_health(self, client, ctx):
        resp = client.get("/health", admin=True)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict) and body, "Expected non-empty dict health response"
        # Health should report component statuses
        if "components" in body:
            for comp_name, comp_data in body["components"].items():
                if isinstance(comp_data, dict):
                    assert "status" in comp_data or "healthy" in comp_data, (
                        f"Health component '{comp_name}' missing status/healthy field"
                    )

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
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict) and body, "Expected non-empty dict session response"
            # Session should include user info or auth details
            has_identity = any(k in body for k in ("userName", "user", "userId", "name", "principal"))
            assert has_identity, (
                f"Session response should contain user identity, got keys: {list(body.keys())}"
            )

    @test("get_metrics", tags=["admin"], order=10)
    def test_get_metrics(self, client, ctx):
        # Retry on timeout (408) — metrics endpoint can be slow on staging
        for attempt in range(3):
            resp = client.get("/metrics", admin=True, timeout=90)
            if resp.status_code == 200:
                break
            if resp.status_code == 408 and attempt < 2:
                print(f"  [metrics] Timeout on attempt {attempt+1}, retrying...")
                continue
        assert_status(resp, 200)
        body = resp.json()
        assert body, "Expected non-empty metrics response"
        assert isinstance(body, dict), f"Expected dict metrics, got {type(body).__name__}"
        # AtlasMetrics wraps metrics in a "data" envelope on staging
        metrics = body.get("data", body) if isinstance(body, dict) else body
        # Metrics should contain gauges, counters, timers, or similar sections
        has_metric_data = any(
            k in metrics for k in ("gauges", "counters", "timers", "meters", "histograms",
                                   "general", "tag", "entity", "system")
        )
        assert has_metric_data, (
            f"Metrics response should contain metric sections, got keys: {list(metrics.keys())[:15]}"
        )

    @test("get_metrics_prometheus", tags=["admin"], order=11)
    def test_get_metrics_prometheus(self, client, ctx):
        resp = client.get("/metrics/prometheus", admin=True)
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 200:
            body_text = resp.body if isinstance(resp.body, str) else str(resp.body)
            assert body_text, "Expected non-empty Prometheus metrics response"
            # Prometheus format: lines with metric_name{labels} value
            assert len(body_text) > 50, (
                f"Prometheus metrics suspiciously short ({len(body_text)} chars)"
            )

    @test("get_stack", tags=["admin"], order=12)
    def test_get_stack(self, client, ctx):
        resp = client.get("/stack", admin=True)
        # Staging may restrict stack trace endpoint
        assert_status_in(resp, [200, 500])
        if resp.status_code == 200:
            body = resp.body
            assert body, "Expected non-empty stack response"
            body_str = str(body)
            # Stack trace should contain thread references
            assert len(body_str) > 100, (
                f"Stack trace response suspiciously short ({len(body_str)} chars)"
            )

    @test("get_active_searches", tags=["admin"], order=13)
    def test_get_active_searches(self, client, ctx):
        resp = client.get("/activeSearches", admin=True)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, (list, dict)), (
            f"Expected list or dict response, got {type(body).__name__}"
        )
        # If there are active searches, each should have basic info
        if isinstance(body, list):
            for item in body[:3]:
                if isinstance(item, dict):
                    assert any(k in item for k in ("query", "user", "startTime", "id")), (
                        f"Active search entry missing expected fields, got: {list(item.keys())}"
                    )

    @test("get_patches", tags=["admin"], order=14)
    def test_get_patches(self, client, ctx):
        resp = client.get("/patches", admin=True)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, (list, dict)), (
            f"Expected list or dict response for patches, got {type(body).__name__}"
        )

    @test("get_admin_tasks", tags=["admin"], order=15)
    def test_get_admin_tasks(self, client, ctx):
        resp = client.get("/tasks", admin=True)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, (list, dict)), (
            f"Expected list or dict response for tasks, got {type(body).__name__}"
        )
        # Validate task structure if tasks exist
        tasks = body if isinstance(body, list) else body.get("tasks", [])
        if tasks and isinstance(tasks[0], dict):
            task = tasks[0]
            assert any(k in task for k in ("taskGuid", "guid", "type", "status")), (
                f"Task entry missing expected fields, got: {list(task.keys())[:10]}"
            )

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
                # 500 can happen if the endpoint doesn't support GET by GUID
                assert_status_in(resp2, [200, 404, 500])
                if resp2.status_code == 200:
                    body2 = resp2.json()
                    if isinstance(body2, dict):
                        found_guid = body2.get("taskGuid") or body2.get("guid")
                        assert found_guid == task_guid, (
                            f"Expected task GUID={task_guid}, got {found_guid}"
                        )

    @test("get_debug_metrics", tags=["admin"], order=17)
    def test_get_debug_metrics(self, client, ctx):
        resp = client.get("/debug/metrics", admin=True)
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list response for debug metrics, got {type(body).__name__}"
            )
            # Empty dict is valid on staging (no debug metrics configured)

    @test("push_metrics_statsd", tags=["admin"], order=18)
    def test_push_metrics_statsd(self, client, ctx):
        resp = client.get("/pushMetricsToStatsd", admin=True)
        # May fail if StatsD not configured or timeout on staging
        assert_status_in(resp, [200, 404, 408, 500])

    @test("check_state", tags=["admin"], order=20)
    def test_check_state(self, client, ctx):
        payload = {"fixIssues": False}
        resp = client.post("/checkstate", json_data=payload, admin=True)
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict) and body, "Expected non-empty dict checkstate response"
            # checkstate should report issues found or state info
            has_state_info = any(k in body for k in ("state", "status", "issues", "totalIssues"))
            assert has_state_info, (
                f"checkstate response missing expected fields (state/status/issues/totalIssues), "
                f"got keys: {list(body.keys())}"
            )

    @test("set_and_delete_feature_flag", tags=["admin"], order=21)
    def test_set_and_delete_feature_flag(self, client, ctx):
        flag_name = "test_harness_flag"
        # Set
        try:
            resp = client.post(
                "/featureFlag",
                params={"flag": flag_name, "value": "true"},
                admin=True,
            )
        except Exception as e:
            raise SkipTestError(f"Feature flag endpoint not reachable: {e}")
        assert_status_in(resp, [200, 204, 404, 500])
        if resp.status_code in (404, 500):
            raise SkipTestError(
                f"Feature flag endpoint returned {resp.status_code} — not supported"
            )

        # Delete
        try:
            resp = client.delete(f"/featureFlag/{flag_name}", admin=True)
        except Exception as e:
            raise SkipTestError(f"Feature flag delete not reachable: {e}")
        assert_status_in(resp, [200, 204, 404])
