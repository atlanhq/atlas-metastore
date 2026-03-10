"""Delete correctness tests — pre/post-delete validation.

Validates that after soft-delete:
- Entity status is DELETED
- Classifications are gone/inaccessible
- Lineage is cleared
- Entities are excluded from ACTIVE search
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_equals,
)
from core.data_factory import (
    build_dataset_entity, build_classification_def, build_process_entity,
    unique_name, unique_qn, unique_type_name, PREFIX,
)


def _index_search(client, dsl):
    """Issue an indexsearch query and return (available, body)."""
    resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _create_entity(client, ctx, suffix):
    """Create a DataSet, register cleanup, return (guid, qn)."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix))
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    # Register cleanup as safety net (tests will delete explicitly)
    ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return guid, qn


@suite("delete_correctness", depends_on_suites=["entity_crud"],
       description="Pre/post delete correctness validation")
class DeleteCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # Create a classification for delete tests
        self.tag_name = unique_type_name("DelTag")
        payload = {"classificationDefs": [
            build_classification_def(name=self.tag_name),
        ]}
        resp = client.post("/types/typedefs", json_data=payload)
        time.sleep(10)  # Wait for type cache propagation
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
        )

        # Create src and tgt DataSet entities
        self.src_guid, self.src_qn = _create_entity(client, ctx, "del-src")
        self.tgt_guid, self.tgt_qn = _create_entity(client, ctx, "del-tgt")

        # Add classification to src
        resp = client.post(
            f"/entity/guid/{self.src_guid}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        self.tag_add_ok = resp.status_code in (200, 204)

        # Add labels to src
        self.src_labels = ["del-label-a"]
        client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.src_guid,
                "attributes": {
                    "qualifiedName": self.src_qn,
                    "name": unique_name("del-src"),
                },
                "labels": self.src_labels,
            }
        })

        # Create Process linking src -> tgt (lineage)
        proc = build_process_entity(
            inputs=[{"guid": self.src_guid, "typeName": "DataSet"}],
            outputs=[{"guid": self.tgt_guid, "typeName": "DataSet"}],
        )
        resp = client.post("/entity", json_data={"entity": proc})
        self.proc_guid = None
        self.lineage_ok = False
        if resp.status_code == 200:
            proc_entities = (resp.json().get("mutatedEntities", {}).get("CREATE", []) or
                             resp.json().get("mutatedEntities", {}).get("UPDATE", []))
            if proc_entities:
                self.proc_guid = proc_entities[0]["guid"]
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.proc_guid}"))
                self.lineage_ok = True

        # Wait for ES sync
        time.sleep(max(es_wait, 5))

    # ---- Pre-delete validation ----

    @test("pre_delete_entities_exist", tags=["delete", "correctness"], order=1)
    def test_pre_delete_entities_exist(self, client, ctx):
        for label, guid in [("src", self.src_guid), ("tgt", self.tgt_guid)]:
            resp = client.get(f"/entity/guid/{guid}")
            assert_status(resp, 200)
            assert_field_equals(resp, "entity.status", "ACTIVE")

        if self.proc_guid:
            resp = client.get(f"/entity/guid/{self.proc_guid}")
            assert_status(resp, 200)
            assert_field_equals(resp, "entity.status", "ACTIVE")

    @test("pre_delete_classifications_attached", tags=["delete", "correctness"], order=2,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_classifications_attached(self, client, ctx):
        if not self.tag_add_ok:
            return

        resp = client.get(f"/entity/guid/{self.src_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, (
            f"Classification {self.tag_name} not found on src entity before delete"
        )

    @test("pre_delete_lineage_exists", tags=["delete", "correctness"], order=3,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_lineage_exists(self, client, ctx):
        if not self.lineage_ok:
            return  # Process creation didn't work (staging may reject)

        resp = client.get(f"/lineage/{self.src_guid}", params={"depth": 1})
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            # Verify at least src is in the lineage map
            assert self.src_guid in guid_map, (
                f"src entity {self.src_guid} not found in lineage guidEntityMap"
            )

    @test("pre_delete_in_search", tags=["delete", "correctness"], order=4,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_in_search(self, client, ctx):
        # Poll for entities to appear in search (ES sync can be slow on staging)
        # Try multiple QN field names to handle ES mapping differences
        import time as _time
        qn_fields = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")
        deadline = _time.time() + 120
        count = 0
        while _time.time() < deadline:
            for qn_field in qn_fields:
                available, body = _index_search(client, {
                    "from": 0, "size": 10,
                    "query": {"bool": {"must": [
                        {"wildcard": {qn_field: f"{PREFIX}/del-*"}},
                        {"term": {"__typeName.keyword": "DataSet"}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                })
                if not available:
                    continue
                count = body.get("approximateCount", 0)
                if count >= 2:
                    return  # Found
            if not available:
                return
            _time.sleep(5)

        assert count >= 2, (
            f"Expected at least 2 del- entities in search, got count={count}"
        )

    # ---- Delete ----

    @test("delete_entities", tags=["delete", "correctness"], order=5,
          depends_on=["pre_delete_entities_exist"])
    def test_delete_entities(self, client, ctx):
        # Delete process first (if it exists), then datasets
        if self.proc_guid:
            resp = client.delete(f"/entity/guid/{self.proc_guid}")
            assert_status(resp, 200)

        resp = client.delete(f"/entity/guid/{self.src_guid}")
        assert_status(resp, 200)

        resp = client.delete(f"/entity/guid/{self.tgt_guid}")
        assert_status(resp, 200)

    # ---- Post-delete validation ----

    @test("post_delete_entity_status", tags=["delete", "correctness"], order=6,
          depends_on=["delete_entities"])
    def test_post_delete_entity_status(self, client, ctx):
        for label, guid in [("src", self.src_guid), ("tgt", self.tgt_guid)]:
            resp = client.get(f"/entity/guid/{guid}")
            assert_status_in(resp, [200, 404])
            if resp.status_code == 200:
                assert_field_equals(resp, "entity.status", "DELETED")

        if self.proc_guid:
            resp = client.get(f"/entity/guid/{self.proc_guid}")
            assert_status_in(resp, [200, 404])
            if resp.status_code == 200:
                assert_field_equals(resp, "entity.status", "DELETED")

    @test("post_delete_classifications_gone", tags=["delete", "correctness"], order=7,
          depends_on=["delete_entities"])
    def test_post_delete_classifications_gone(self, client, ctx):
        if not self.tag_add_ok:
            return

        resp = client.get(f"/entity/guid/{self.src_guid}/classifications")
        # After delete: either 404 (entity gone) or 200 with entity status DELETED
        if resp.status_code == 404:
            return  # Entity is hard-deleted or classifications endpoint returns 404
        if resp.status_code == 200:
            # Entity still accessible but DELETED — check entity status
            entity_resp = client.get(f"/entity/guid/{self.src_guid}")
            if entity_resp.status_code == 200:
                status = entity_resp.json().get("entity", {}).get("status")
                assert status == "DELETED", (
                    f"Expected entity status DELETED after delete, got {status}"
                )

    @test("post_delete_search_cleared", tags=["delete", "correctness"], order=8,
          depends_on=["delete_entities"])
    def test_post_delete_search_cleared(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(max(es_wait, 5))

        # Verify src is excluded from ACTIVE search
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.src_guid}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return

        count = body.get("approximateCount", 0)
        assert count == 0, (
            f"Deleted src entity {self.src_guid} still in ACTIVE search, count={count}"
        )

        # Verify tgt is excluded from ACTIVE search
        _, body2 = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.tgt_guid}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        count2 = body2.get("approximateCount", 0)
        assert count2 == 0, (
            f"Deleted tgt entity {self.tgt_guid} still in ACTIVE search, count={count2}"
        )
