"""Data mesh - domain link/unlink tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("data_mesh", depends_on_suites=["entity_crud", "glossary"],
       description="Domain link/unlink operations")
class DataMeshSuite:

    @test("link_domain_no_domain", tags=["data_mesh"], order=1)
    def test_link_domain_no_domain(self, client, ctx):
        # Test with nonexistent domain - should fail gracefully
        entity_guid = ctx.get_entity_guid("ds1")
        if not entity_guid:
            return
        resp = client.post("/mesh-asset-link/link-domain", json_data={
            "domainGuid": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [entity_guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("unlink_domain_no_domain", tags=["data_mesh"], order=2)
    def test_unlink_domain_no_domain(self, client, ctx):
        entity_guid = ctx.get_entity_guid("ds1")
        if not entity_guid:
            return
        resp = client.post("/mesh-asset-link/unlink-domain", json_data={
            "domainGuid": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [entity_guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])
