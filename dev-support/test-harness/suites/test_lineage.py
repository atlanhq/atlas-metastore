"""Lineage query tests."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present


@suite("lineage", depends_on_suites=["entity_crud"],
       description="Lineage query endpoints")
class LineageSuite:

    def setup(self, client, ctx):
        # Wait for graph to settle after entity_crud created Process
        time.sleep(2)

    @test("get_lineage_by_guid", tags=["smoke", "lineage"], order=1)
    def test_get_lineage_by_guid(self, client, ctx):
        # Use ds1 which should have lineage via process1
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity; depends on entity_crud suite")
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_present(resp, "baseEntityGuid")

    @test("get_lineage_input", tags=["lineage"], order=2)
    def test_get_lineage_input(self, client, ctx):
        guid = ctx.get_entity_guid("ds2")
        if not guid:
            return
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "INPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])

    @test("get_lineage_output", tags=["lineage"], order=3)
    def test_get_lineage_output(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            return
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "OUTPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])

    @test("post_lineage_on_demand", tags=["lineage"], order=4)
    def test_post_lineage_on_demand(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            return
        resp = client.post(f"/lineage/{guid}", json_data={
            "direction": "BOTH",
            "inputRelationsLimit": 10,
            "outputRelationsLimit": 10,
            "depth": 3,
        })
        assert_status_in(resp, [200, 400])

    @test("post_lineage_list", tags=["lineage"], order=5)
    def test_post_lineage_list(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            return
        resp = client.post("/lineage/list", json_data={
            "guid": guid,
            "size": 10,
            "depth": 3,
            "direction": "BOTH",
        })
        assert_status_in(resp, [200, 400, 404])
