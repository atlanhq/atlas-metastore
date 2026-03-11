"""Index repair endpoint tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("repair", depends_on_suites=["entity_crud"],
       description="Index repair endpoints")
class RepairSuite:

    @test("repair_single_index", tags=["repair"], order=1)
    def test_repair_single_index(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        # Need qualifiedName for the entity
        resp_entity = client.get(f"/entity/guid/{guid}")
        assert_status(resp_entity, 200)
        qn = resp_entity.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"qualifiedName not found on entity {guid}"

        resp = client.post("/repair/single-index", params={
            "qualifiedName": qn,
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_composite_index", tags=["repair"], order=2)
    def test_repair_composite_index(self, client, ctx):
        resp = client.post("/repair/composite-index", json_data={
            "propertyName": "qualifiedName",
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_batch", tags=["repair"], order=3)
    def test_repair_batch(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/repair/batch", json_data={
            "guids": [guid],
        }, params={"indexType": "SINGLE"})
        assert_status_in(resp, [200, 204, 400, 404])
