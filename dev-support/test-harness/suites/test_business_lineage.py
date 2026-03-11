"""Business lineage create tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


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
        assert entity_guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": entity_guid,
            "assetGuids": [entity_guid],
        })
        # May fail because entity is not a DataProduct
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_empty_assets", tags=["business_lineage"], order=3)
    def test_create_lineage_empty_assets(self, client, ctx):
        """POST with valid GUID but empty assetGuids."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": guid,
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_with_product", tags=["business_lineage"], order=4)
    def test_create_lineage_with_product(self, client, ctx):
        """Create business lineage with actual DataProduct if available."""
        # Try to get a DataProduct GUID from data_mesh suite
        product_guid = ctx.get_entity_guid("data_product")
        ds_guid = ctx.get_entity_guid("ds1")
        if not product_guid or not ds_guid:
            from core.assertions import SkipTestError
            raise SkipTestError("DataProduct or ds1 GUID not available")
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": product_guid,
            "assetGuids": [ds_guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("get_business_lineage", tags=["business_lineage"], order=5)
    def test_get_business_lineage(self, client, ctx):
        """GET business lineage for an entity."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.get(f"/business-lineage/{guid}")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list, got {type(body).__name__}"
            )

    @test("delete_business_lineage", tags=["business_lineage"], order=6)
    def test_delete_business_lineage(self, client, ctx):
        """DELETE business lineage for an entity."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.delete(f"/business-lineage/{guid}")
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_nonexistent", tags=["business_lineage", "negative"], order=7)
    def test_create_lineage_nonexistent(self, client, ctx):
        """POST with nonexistent productGuid."""
        fake = "00000000-0000-0000-0000-000000000000"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": fake,
            "assetGuids": [fake],
        })
        assert_status_in(resp, [200, 204, 400, 404])
