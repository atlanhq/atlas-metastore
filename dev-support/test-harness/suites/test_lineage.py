"""Lineage query tests."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present, assert_field_equals, SkipTestError
from core.data_factory import build_dataset_entity, unique_qn, unique_name


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
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            assert "guidEntityMap" in body or "relations" in body, (
                "Expected 'guidEntityMap' or 'relations' in lineage response"
            )
            # baseEntityGuid should be in the guidEntityMap
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap, got: {list(guid_map.keys())[:5]}"
                )

    @test("get_lineage_input", tags=["lineage"], order=2)
    def test_get_lineage_input(self, client, ctx):
        guid = ctx.get_entity_guid("ds2")
        assert guid, "ds2 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "INPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap"
                )
            # INPUT direction: relations should reference fromEntityId pointing to base
            relations = body.get("relations", [])
            if relations:
                for rel in relations:
                    assert "fromEntityId" in rel and "toEntityId" in rel, (
                        f"Relation missing fromEntityId/toEntityId: {list(rel.keys())}"
                    )

    @test("get_lineage_output", tags=["lineage"], order=3)
    def test_get_lineage_output(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "OUTPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap"
                )
            relations = body.get("relations", [])
            if relations:
                for rel in relations:
                    assert "fromEntityId" in rel and "toEntityId" in rel, (
                        f"Relation missing fromEntityId/toEntityId: {list(rel.keys())}"
                    )

    @test("post_lineage_on_demand", tags=["lineage"], order=4)
    def test_post_lineage_on_demand(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post(f"/lineage/{guid}", json_data={
            "direction": "BOTH",
            "inputRelationsLimit": 10,
            "outputRelationsLimit": 10,
            "depth": 3,
        })
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            body = resp.json()
            assert "baseEntityGuid" in body, "Expected 'baseEntityGuid' in on-demand lineage response"
            assert body["baseEntityGuid"] == guid, (
                f"Expected baseEntityGuid={guid}, got {body['baseEntityGuid']}"
            )
            # On-demand should have guidEntityMap or relations or searchParameters
            assert "guidEntityMap" in body or "relations" in body or "searchParameters" in body, (
                f"On-demand lineage missing expected fields, got keys: {list(body.keys())}"
            )

    @test("post_lineage_list", tags=["lineage"], order=5)
    def test_post_lineage_list(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/lineage/list", json_data={
            "guid": guid,
            "size": 10,
            "depth": 3,
            "direction": "BOTH",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list response from lineage list, got {type(body).__name__}"
            )
            # If dict, should have entities or searchResult
            if isinstance(body, dict):
                has_data = any(k in body for k in (
                    "entities", "searchResult", "searchParameters",
                    "hasMoreUpstreamVertices", "hasMoreDownstreamVertices",
                ))
                assert has_data, (
                    f"Lineage list response missing expected fields, got keys: {list(body.keys())}"
                )

    @test("lineage_isolated_entity", tags=["lineage"], order=6)
    def test_lineage_isolated_entity(self, client, ctx):
        # Create entity with no Process links -> lineage should be empty
        qn = unique_qn("lineage-isolated")
        entity = build_dataset_entity(qn=qn, name=unique_name("lineage-iso"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

        resp2 = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        assert_status_in(resp2, [200, 404])
        if resp2.status_code == 200:
            body2 = resp2.json()
            relations = body2.get("relations", [])
            assert len(relations) == 0, (
                f"Expected no lineage relations for isolated entity, got {len(relations)}"
            )
            # guidEntityMap should only contain the base entity itself
            guid_map = body2.get("guidEntityMap", {})
            if guid_map:
                assert len(guid_map) <= 1, (
                    f"Isolated entity should have at most 1 entry in guidEntityMap, got {len(guid_map)}"
                )

    @test("lineage_depth_limited", tags=["lineage"], order=7)
    def test_lineage_depth_limited(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 1,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            # With depth=1, lineageDepth should be <= 1
            reported_depth = body.get("lineageDepth", -1)
            if reported_depth >= 0:
                assert reported_depth <= 1, (
                    f"Expected lineageDepth <= 1, got {reported_depth}"
                )

    @test("lineage_hide_process", tags=["lineage"], order=8)
    def test_lineage_hide_process(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
            "hideProcess": "true",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            # If hideProcess works, no Process entities in guidEntityMap
            for entity_guid, entity_data in guid_map.items():
                if isinstance(entity_data, dict):
                    type_name = entity_data.get("typeName", "")
                    assert type_name != "Process", (
                        f"Expected no Process entities with hideProcess=true, found {entity_guid}"
                    )
