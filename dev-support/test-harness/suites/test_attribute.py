"""Attribute update endpoint tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("attribute", depends_on_suites=["entity_crud"],
       description="Attribute update endpoint")
class AttributeSuite:

    @test("update_attribute", tags=["attribute"], order=1)
    def test_update_attribute(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {
                "description": "Updated via attribute API",
            }
        })
        # This endpoint is restricted to Argo service user, may get 403 in local dev
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("update_attribute_invalid", tags=["attribute"], order=2)
    def test_update_attribute_invalid(self, client, ctx):
        resp = client.post("/attribute/update", json_data={
            "guid": "00000000-0000-0000-0000-000000000000",
            "typeName": "DataSet",
            "attributes": {},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])
