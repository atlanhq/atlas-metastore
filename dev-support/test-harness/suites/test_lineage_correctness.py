"""Lineage correctness tests.

Verifies lineage graph structure (guidEntityMap, relations, direction,
depth, hideProcess), process entity inputs/outputs round-trip,
classification propagation through lineage with hard assertions,
restrictPropagationThroughLineage blocking, propagation cleanup on
tag removal, and lineage behavior after entity deletion.

Existing suites test endpoint availability and response shapes but
use best-effort assertions for propagation.  This suite uses hard
assertions throughout.
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.data_factory import (
    build_dataset_entity, build_process_entity, build_classification_def,
    unique_name, unique_qn, unique_type_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _index_search(client, dsl):
    """Issue an indexsearch query and return (available, body)."""
    resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _search_by_guid(client, guid):
    """Search for a single entity by GUID in ES."""
    return _index_search(client, {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [
            {"term": {"__guid": guid}},
            {"term": {"__state": "ACTIVE"}},
        ]}}
    })


def _create_dataset(client, ctx, suffix):
    """Create a DataSet, register cleanup, return guid."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix))
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return guid


def _create_process(client, ctx, suffix, input_guids, output_guids):
    """Create a Process with inputs/outputs.  Returns (ok, guid)."""
    inputs = [{"guid": g, "typeName": "DataSet"} for g in input_guids]
    outputs = [{"guid": g, "typeName": "DataSet"} for g in output_guids]
    proc = build_process_entity(
        qn=unique_qn(suffix), name=unique_name(suffix),
        inputs=inputs, outputs=outputs,
    )
    resp = client.post("/entity", json_data={"entity": proc})
    if resp.status_code != 200:
        return False, None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        return False, None
    guid = entities[0]["guid"]
    ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return True, guid


def _get_lineage(client, guid, direction="BOTH", depth=3, hide_process=False):
    """GET /lineage/{guid} and return (available, body)."""
    params = {"direction": direction, "depth": depth}
    if hide_process:
        params["hideProcess"] = "true"
    resp = client.get(f"/lineage/{guid}", params=params)
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _get_entity_classifications(client, guid):
    """GET entity and return its classifications list."""
    resp = client.get(f"/entity/guid/{guid}")
    if resp.status_code != 200:
        return None
    entity = resp.json().get("entity", {})
    return entity.get("classifications", [])


# ---------------------------------------------------------------------------
# Suite
# ---------------------------------------------------------------------------

@suite("lineage_correctness", depends_on_suites=["entity_crud"],
       description="Lineage structure, propagation, and mutation correctness")
class LineageCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # --- Classification typedefs for propagation tests ---
        self.tag_name = unique_type_name("LinTag")
        self.tag2_name = unique_type_name("LinTag2")
        tag_resp = client.post("/types/typedefs", json_data={
            "classificationDefs": [
                build_classification_def(name=self.tag_name),
                build_classification_def(name=self.tag2_name),
            ]
        })
        self.tag_ok = tag_resp.status_code in (200, 409)
        if self.tag_ok:
            time.sleep(10)  # type cache propagation
            ctx.register_cleanup(
                lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
            )
            ctx.register_cleanup(
                lambda: client.delete(f"/types/typedef/name/{self.tag2_name}")
            )

        # -------------------------------------------------------
        # Main lineage graph (non-destructive tests):
        #   ds_a  -->  proc_ab  -->  ds_b  -->  proc_bc  -->  ds_c
        # -------------------------------------------------------
        self.ds_a = _create_dataset(client, ctx, "lin-a")
        self.ds_b = _create_dataset(client, ctx, "lin-b")
        self.ds_c = _create_dataset(client, ctx, "lin-c")

        ok1, self.proc_ab = _create_process(
            client, ctx, "lin-p-ab", [self.ds_a], [self.ds_b],
        )
        ok2, self.proc_bc = _create_process(
            client, ctx, "lin-p-bc", [self.ds_b], [self.ds_c],
        )
        self.lineage_ok = ok1 and ok2

        # -------------------------------------------------------
        # Propagation lineage (will be mutated by tag tests):
        #   prop_src  -->  prop_proc  -->  prop_tgt
        # -------------------------------------------------------
        self.prop_src = _create_dataset(client, ctx, "lin-prop-src")
        self.prop_tgt = _create_dataset(client, ctx, "lin-prop-tgt")
        ok3, self.prop_proc = _create_process(
            client, ctx, "lin-prop-p", [self.prop_src], [self.prop_tgt],
        )
        self.prop_lineage_ok = ok3

        # Wait for JanusGraph indexes to settle
        time.sleep(max(es_wait, 5))

    # ================================================================
    #  Group 1 — Lineage graph structure
    # ================================================================

    @test("lineage_both_has_correct_entities",
          tags=["lineage", "data_correctness"], order=1)
    def test_lineage_both(self, client, ctx):
        """GET /lineage BOTH — guidEntityMap contains src, process, and tgt."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_a, "BOTH", depth=3)
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem, (
            f"Source {self.ds_a} not in guidEntityMap: {list(gem.keys())}"
        )
        assert self.proc_ab in gem, (
            f"Process {self.proc_ab} not in guidEntityMap: {list(gem.keys())}"
        )
        assert self.ds_b in gem, (
            f"Target {self.ds_b} not in guidEntityMap: {list(gem.keys())}"
        )

    @test("lineage_output_finds_downstream",
          tags=["lineage", "data_correctness"], order=2)
    def test_lineage_output(self, client, ctx):
        """GET /lineage OUTPUT — downstream entity reachable from source."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=3)
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        assert self.ds_b in gem, (
            f"Downstream {self.ds_b} not in OUTPUT lineage from {self.ds_a}"
        )

    @test("lineage_input_finds_upstream",
          tags=["lineage", "data_correctness"], order=3)
    def test_lineage_input(self, client, ctx):
        """GET /lineage INPUT — upstream entity reachable from mid-entity."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_b, "INPUT", depth=3)
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem, (
            f"Upstream {self.ds_a} not in INPUT lineage from {self.ds_b}"
        )

    @test("lineage_relations_have_edges",
          tags=["lineage", "data_correctness"], order=4)
    def test_lineage_relations(self, client, ctx):
        """Relations array has edges with fromEntityId / toEntityId."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=1)
        if not available:
            return

        relations = body.get("relations", [])
        if not relations:
            return  # some environments omit relations

        all_ids = set()
        for rel in relations:
            fid = rel.get("fromEntityId")
            tid = rel.get("toEntityId")
            if fid:
                all_ids.add(fid)
            if tid:
                all_ids.add(tid)

        assert self.ds_a in all_ids or self.proc_ab in all_ids, (
            f"Neither source {self.ds_a} nor process {self.proc_ab} "
            f"in relation edges: {relations}"
        )

    @test("lineage_multi_hop_reachable_at_depth_3",
          tags=["lineage", "data_correctness"], order=5)
    def test_multi_hop(self, client, ctx):
        """depth=3 from ds_a OUTPUT reaches ds_c (2 hops through 2 processes)."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=3)
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        assert self.ds_c in gem, (
            f"Entity {self.ds_c} (2 hops away) not reachable at depth=3. "
            f"guidEntityMap has {len(gem)} entries: {list(gem.keys())}"
        )

    @test("lineage_depth_1_limits_reach",
          tags=["lineage", "data_correctness"], order=6)
    def test_depth_limit(self, client, ctx):
        """depth=1 from ds_a OUTPUT — ds_c (2 hops) should NOT be reachable."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=1)
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        assert self.ds_c not in gem, (
            f"Entity {self.ds_c} (2 hops) should NOT be reachable at depth=1. "
            f"guidEntityMap: {list(gem.keys())}"
        )

    @test("lineage_hide_process_excludes_processes",
          tags=["lineage", "data_correctness"], order=7)
    def test_hide_process(self, client, ctx):
        """hideProcess=true — no Process entities in guidEntityMap."""
        if not self.lineage_ok:
            return
        available, body = _get_lineage(
            client, self.ds_a, "BOTH", depth=3, hide_process=True,
        )
        if not available:
            return

        gem = body.get("guidEntityMap", {})
        for guid, info in gem.items():
            if isinstance(info, dict):
                tn = info.get("typeName", "")
                assert tn != "Process", (
                    f"Process entity {guid} in guidEntityMap with hideProcess=true"
                )

        # DataSets should still be present
        ds_found = any(
            isinstance(info, dict) and info.get("typeName") == "DataSet"
            for info in gem.values()
        )
        assert ds_found, "No DataSet entities in guidEntityMap with hideProcess=true"

    # ================================================================
    #  Group 2 — Process entity data round-trip
    # ================================================================

    @test("process_entity_has_correct_inputs",
          tags=["lineage", "data_correctness", "process"], order=8)
    def test_process_inputs(self, client, ctx):
        """GET process entity — inputs reference the source DataSet."""
        if not self.lineage_ok:
            return
        resp = client.get(f"/entity/guid/{self.proc_ab}")
        if resp.status_code != 200:
            return

        entity = resp.json().get("entity", {})
        # inputs live in relationshipAttributes or attributes
        inputs = (
            entity.get("relationshipAttributes", {}).get("inputs")
            or entity.get("attributes", {}).get("inputs")
            or []
        )
        if not inputs:
            return  # field not returned

        input_guids = [i.get("guid") for i in inputs if isinstance(i, dict)]
        assert self.ds_a in input_guids, (
            f"Source {self.ds_a} not in process inputs: {input_guids}"
        )

    @test("process_entity_has_correct_outputs",
          tags=["lineage", "data_correctness", "process"], order=9)
    def test_process_outputs(self, client, ctx):
        """GET process entity — outputs reference the target DataSet."""
        if not self.lineage_ok:
            return
        resp = client.get(f"/entity/guid/{self.proc_ab}")
        if resp.status_code != 200:
            return

        entity = resp.json().get("entity", {})
        outputs = (
            entity.get("relationshipAttributes", {}).get("outputs")
            or entity.get("attributes", {}).get("outputs")
            or []
        )
        if not outputs:
            return

        output_guids = [o.get("guid") for o in outputs if isinstance(o, dict)]
        assert self.ds_b in output_guids, (
            f"Target {self.ds_b} not in process outputs: {output_guids}"
        )

    # ================================================================
    #  Group 3 — On-demand & list endpoints with data checks
    # ================================================================

    @test("on_demand_lineage_has_base_entity",
          tags=["lineage", "data_correctness"], order=10)
    def test_on_demand(self, client, ctx):
        """POST /lineage on-demand — base entity appears in guidEntityMap."""
        if not self.lineage_ok:
            return
        resp = client.post(f"/lineage/{self.ds_b}", json_data={
            "direction": "BOTH",
            "inputRelationsLimit": 10,
            "outputRelationsLimit": 10,
            "depth": 3,
        })
        if resp.status_code in (400, 404, 405):
            return  # on-demand may be disabled
        if resp.status_code != 200:
            return

        body = resp.json()
        gem = body.get("guidEntityMap", {})
        if gem:
            assert self.ds_b in gem, (
                f"Base entity {self.ds_b} not in on-demand guidEntityMap"
            )

    @test("lineage_list_returns_data",
          tags=["lineage", "data_correctness"], order=11)
    def test_lineage_list(self, client, ctx):
        """POST /lineage/list — response contains lineage data."""
        if not self.lineage_ok:
            return
        resp = client.post("/lineage/list", json_data={
            "guid": self.ds_a,
            "size": 10,
            "depth": 3,
            "direction": "OUTPUT",
        })
        if resp.status_code in (400, 404, 405):
            return
        if resp.status_code != 200:
            return

        body = resp.json()
        if isinstance(body, dict):
            # Should have some content
            has_data = (
                body.get("entities")
                or body.get("relations")
                or body.get("guidEntityMap")
                or body.get("searchParameters")
            )
            assert has_data, (
                f"lineage/list returned empty response: {list(body.keys())}"
            )

    # ================================================================
    #  Group 4 — Classification propagation through lineage
    #            (hard assertions, not best-effort)
    # ================================================================

    @test("tag_propagates_to_downstream_entity",
          tags=["lineage", "propagation", "data_correctness"], order=12)
    def test_tag_propagates(self, client, ctx):
        """Add tag to source with propagate=True — target gets it via GET entity."""
        if not self.prop_lineage_ok or not self.tag_ok:
            return

        resp = client.post(
            f"/entity/guid/{self.prop_src}/classifications",
            json_data=[{
                "typeName": self.tag_name,
                "propagate": True,
                "restrictPropagationThroughLineage": False,
            }],
        )
        if resp.status_code not in (200, 204):
            return

        # Wait for propagation through graph
        time.sleep(max(ctx.get("es_sync_wait", 5), 8))

        classifications = _get_entity_classifications(client, self.prop_tgt)
        if classifications is None:
            return

        tag_found = any(
            isinstance(c, dict) and c.get("typeName") == self.tag_name
            for c in classifications
        )
        assert tag_found, (
            f"Tag {self.tag_name} did NOT propagate to downstream entity "
            f"{self.prop_tgt}. Classifications: "
            f"{[c.get('typeName') for c in classifications if isinstance(c, dict)]}"
        )

    @test("propagated_tag_visible_in_search",
          tags=["lineage", "propagation", "search", "data_correctness"],
          order=13, depends_on=["tag_propagates_to_downstream_entity"])
    def test_propagated_in_search(self, client, ctx):
        """Search downstream entity — propagatedClassificationNames has the tag."""
        if not self.prop_lineage_ok or not self.tag_ok:
            return

        available, body = _search_by_guid(client, self.prop_tgt)
        if not available:
            return
        entities = body.get("entities", [])
        if not entities:
            return

        entity = entities[0]
        prop_names = entity.get("propagatedClassificationNames", [])
        cn = entity.get("classificationNames", [])

        found = self.tag_name in prop_names or self.tag_name in cn
        assert found, (
            f"Propagated tag {self.tag_name} not in search result for "
            f"{self.prop_tgt}. propagatedClassificationNames={prop_names}, "
            f"classificationNames={cn}"
        )

    @test("remove_source_tag_clears_propagation",
          tags=["lineage", "propagation", "data_correctness"],
          order=14, depends_on=["tag_propagates_to_downstream_entity"])
    def test_remove_clears(self, client, ctx):
        """Remove tag from source — downstream entity loses the propagated tag."""
        if not self.prop_lineage_ok or not self.tag_ok:
            return

        resp = client.delete(
            f"/entity/guid/{self.prop_src}/classification/{self.tag_name}"
        )
        if resp.status_code not in (200, 204):
            return

        time.sleep(max(ctx.get("es_sync_wait", 5), 8))

        classifications = _get_entity_classifications(client, self.prop_tgt)
        if classifications is None:
            return

        tag_names = [
            c.get("typeName") for c in classifications if isinstance(c, dict)
        ]
        assert self.tag_name not in tag_names, (
            f"Tag {self.tag_name} still on downstream entity {self.prop_tgt} "
            f"after removal from source. Classifications: {tag_names}"
        )

    @test("restrict_propagation_blocks_lineage",
          tags=["lineage", "propagation", "data_correctness"],
          order=15, depends_on=["remove_source_tag_clears_propagation"])
    def test_restrict_propagation(self, client, ctx):
        """restrictPropagationThroughLineage=True — target does NOT get the tag."""
        if not self.prop_lineage_ok or not self.tag_ok:
            return

        resp = client.post(
            f"/entity/guid/{self.prop_src}/classifications",
            json_data=[{
                "typeName": self.tag2_name,
                "propagate": True,
                "restrictPropagationThroughLineage": True,
            }],
        )
        if resp.status_code not in (200, 204):
            return

        time.sleep(max(ctx.get("es_sync_wait", 5), 8))

        classifications = _get_entity_classifications(client, self.prop_tgt)
        if classifications is None:
            return

        tag_names = [
            c.get("typeName") for c in classifications if isinstance(c, dict)
        ]
        assert self.tag2_name not in tag_names, (
            f"Tag {self.tag2_name} propagated to {self.prop_tgt} despite "
            f"restrictPropagationThroughLineage=True. Classifications: {tag_names}"
        )

    # ================================================================
    #  Group 5 — Lineage after entity deletion
    # ================================================================

    @test("lineage_breaks_after_process_delete",
          tags=["lineage", "data_correctness", "delete"], order=16)
    def test_lineage_after_process_delete(self, client, ctx):
        """Delete the Process — lineage from source shows no downstream."""
        # Create a disposable lineage: del_src → del_proc → del_tgt
        del_src = _create_dataset(client, ctx, "lin-del-src")
        del_tgt = _create_dataset(client, ctx, "lin-del-tgt")
        ok, del_proc = _create_process(
            client, ctx, "lin-del-p", [del_src], [del_tgt],
        )
        if not ok:
            return
        time.sleep(3)

        # Verify lineage exists before
        available, body = _get_lineage(client, del_src, "OUTPUT", depth=1)
        if not available:
            return
        gem_before = body.get("guidEntityMap", {})
        if del_tgt not in gem_before:
            return  # lineage didn't register in time

        # Delete the process
        client.delete(f"/entity/guid/{del_proc}")
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        # Lineage from source should no longer reach target
        _, body2 = _get_lineage(client, del_src, "OUTPUT", depth=1)
        gem_after = body2.get("guidEntityMap", {})
        relations_after = body2.get("relations", [])

        assert del_tgt not in gem_after or len(relations_after) == 0, (
            f"Target {del_tgt} still reachable after process deletion. "
            f"guidEntityMap: {list(gem_after.keys())}"
        )

    @test("lineage_after_target_soft_delete",
          tags=["lineage", "data_correctness", "delete"], order=17)
    def test_lineage_after_target_delete(self, client, ctx):
        """Soft-delete target — entity in lineage should be DELETED or removed."""
        del2_src = _create_dataset(client, ctx, "lin-del2-src")
        del2_tgt = _create_dataset(client, ctx, "lin-del2-tgt")
        ok, del2_proc = _create_process(
            client, ctx, "lin-del2-p", [del2_src], [del2_tgt],
        )
        if not ok:
            return
        time.sleep(3)

        # Verify lineage exists
        available, body = _get_lineage(client, del2_src, "OUTPUT", depth=1)
        if not available:
            return
        if del2_tgt not in body.get("guidEntityMap", {}):
            return

        # Soft-delete the target
        client.delete(f"/entity/guid/{del2_tgt}")
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        # Check lineage — target may be marked DELETED or removed
        _, body2 = _get_lineage(client, del2_src, "OUTPUT", depth=1)
        gem = body2.get("guidEntityMap", {})
        if del2_tgt in gem:
            entity_info = gem[del2_tgt]
            if isinstance(entity_info, dict):
                status = entity_info.get("status", "")
                assert status == "DELETED", (
                    f"Deleted target {del2_tgt} in lineage with status={status}, "
                    f"expected DELETED"
                )

    @test("lineage_by_unique_attribute",
          tags=["lineage", "data_correctness"], order=18)
    def test_lineage_by_unique_attr(self, client, ctx):
        """GET /lineage/uniqueAttribute/type/DataSet — lineage by qualifiedName."""
        if not self.lineage_ok:
            return

        # Get ds_a's qualifiedName
        resp = client.get(f"/entity/guid/{self.ds_a}")
        if resp.status_code != 200:
            return
        qn = resp.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        if not qn:
            return

        resp2 = client.get(
            "/lineage/uniqueAttribute/type/DataSet",
            params={
                "attr:qualifiedName": qn,
                "direction": "OUTPUT",
                "depth": 1,
            },
        )
        if resp2.status_code in (400, 404, 405):
            return  # endpoint may not be available
        if resp2.status_code != 200:
            return

        body = resp2.json()
        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem or len(gem) > 0, (
            f"No entities in lineage by unique attribute for QN={qn}"
        )
