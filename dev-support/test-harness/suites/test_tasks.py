"""Task search/retry/delete tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("tasks", depends_on_suites=["entity_crud"],
       description="Task management endpoints")
class TasksSuite:

    @test("search_tasks", tags=["tasks"], order=1)
    def test_search_tasks(self, client, ctx):
        resp = client.post("/task/search", json_data={
            "limit": 10,
            "offset": 0,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (list, dict)), (
                f"Expected list or dict response, got {type(body).__name__}"
            )

    @test("search_tasks_with_filter", tags=["tasks"], order=2)
    def test_search_tasks_with_filter(self, client, ctx):
        resp = client.post("/task/search", json_data={
            "limit": 10,
            "offset": 0,
            "status": "COMPLETE",
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (list, dict)), (
                f"Expected list or dict response, got {type(body).__name__}"
            )

    @test("retry_nonexistent_task", tags=["tasks"], order=3)
    def test_retry_nonexistent_task(self, client, ctx):
        resp = client.put("/task/retry/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [200, 404, 400])
        if resp.status_code in [400, 404]:
            body = resp.json()
            if isinstance(body, dict):
                assert "errorMessage" in body or "errorCode" in body or "message" in body, (
                    f"Expected error details in response, got keys: {list(body.keys())}"
                )
