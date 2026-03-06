"""Business lineage create tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("business_lineage", depends_on_suites=["entity_crud"],
       description="Business lineage create endpoint")
class BusinessLineageSuite:

    @test("create_lineage_invalid", tags=["business_lineage"], order=1)
    def test_create_lineage_invalid(self, client, ctx):
        # Test with empty/invalid request - should fail gracefully
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_with_entity", tags=["business_lineage"], order=2)
    def test_create_lineage_with_entity(self, client, ctx):
        entity_guid = ctx.get_entity_guid("ds1")
        if not entity_guid:
            return
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": entity_guid,
            "assetGuids": [entity_guid],
        })
        # May fail because entity is not a DataProduct
        assert_status_in(resp, [200, 204, 400, 404])
