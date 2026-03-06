"""DLQ replay status tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("dlq", description="DLQ replay status endpoint")
class DlqSuite:

    @test("get_dlq_status", tags=["smoke", "dlq"], order=1)
    def test_get_dlq_status(self, client, ctx):
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
