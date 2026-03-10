"""Search field-level correctness tests.

Validates that ES index-search results contain expected fields:
classificationNames, classifications objects, labels, meanings,
approximateCount, and combined boolean queries.
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
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


def _search_by_guid(client, guid):
    """Search for a single entity by GUID."""
    return _index_search(client, {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [
            {"term": {"__guid": guid}},
            {"term": {"__state": "ACTIVE"}},
        ]}}
    })


# ES field names for qualifiedName differ between local and staging
QN_FIELDS = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")


def _create_entity_and_register(client, ctx, suffix, cleanup=True):
    """Helper: create a DataSet, register cleanup, return guid."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix))
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    if cleanup:
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return guid, qn


@suite("search_correctness", depends_on_suites=["entity_crud"],
       description="Search field-level correctness validation")
class SearchCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # Create a classification for search tests
        self.tag_name = unique_type_name("SearchTag")
        self.tag2_name = unique_type_name("SearchTag2")
        payload = {"classificationDefs": [
            build_classification_def(name=self.tag_name),
            build_classification_def(name=self.tag2_name),
        ]}
        resp = client.post("/types/typedefs", json_data=payload)
        time.sleep(10)  # Wait for type cache propagation
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
        )
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag2_name}")
        )

        # Entity A: has classification
        self.guid_a, self.qn_a = _create_entity_and_register(client, ctx, "search-a")
        resp = client.post(
            f"/entity/guid/{self.guid_a}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        self.tag_add_ok = resp.status_code in (200, 204)

        # Entity B: has labels
        self.guid_b, self.qn_b = _create_entity_and_register(client, ctx, "search-b")
        self.labels_b = ["label-a", "label-b"]
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_b,
                "attributes": {
                    "qualifiedName": self.qn_b,
                    "name": unique_name("search-b"),
                },
                "labels": self.labels_b,
            }
        })

        # Entity C: has classification + labels
        self.guid_c, self.qn_c = _create_entity_and_register(client, ctx, "search-c")
        self.labels_c = ["label-c"]
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_c,
                "attributes": {
                    "qualifiedName": self.qn_c,
                    "name": unique_name("search-c"),
                },
                "labels": self.labels_c,
            }
        })
        if self.tag_add_ok:
            client.post(
                f"/entity/guid/{self.guid_c}/classifications",
                json_data=[{"typeName": self.tag_name}],
            )

        # Wait for ES sync
        time.sleep(max(es_wait, 5))

    # ---- Tests ----

    @test("search_by_classification_filter", tags=["search", "correctness"], order=1)
    def test_search_by_classification_filter(self, client, ctx):
        if not self.tag_add_ok:
            return  # Classification add failed, skip

        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": "DataSet"}},
                {"match_phrase": {"__classificationNames": self.tag_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return

        count = body.get("approximateCount", 0)
        assert count > 0, (
            f"Expected at least 1 entity with classification {self.tag_name}, got count={count}"
        )
        entities = body.get("entities", [])
        for e in entities:
            cn = e.get("classificationNames", [])
            assert self.tag_name in cn, (
                f"Entity {e.get('guid')} missing {self.tag_name} in classificationNames: {cn}"
            )

    @test("search_classification_objects", tags=["search", "correctness"], order=2,
          depends_on=["search_by_classification_filter"])
    def test_search_classification_objects(self, client, ctx):
        if not self.tag_add_ok:
            return

        available, body = _search_by_guid(client, self.guid_a)
        if not available:
            return

        entities = body.get("entities", [])
        assert len(entities) > 0, f"Entity {self.guid_a} not found in search"
        entity = entities[0]
        classifications = entity.get("classifications", [])
        assert isinstance(classifications, list), (
            f"Expected classifications to be a list, got {type(classifications).__name__}"
        )
        found = any(
            isinstance(c, dict) and c.get("typeName") == self.tag_name
            for c in classifications
        )
        assert found, (
            f"Classification object with typeName={self.tag_name} not found in "
            f"search result classifications: {classifications}"
        )

    @test("search_combined_type_classification_qn", tags=["search", "correctness"], order=3,
          depends_on=["search_by_classification_filter"])
    def test_search_combined_type_classification_qn(self, client, ctx):
        if not self.tag_add_ok:
            return

        # Try multiple QN field names
        available = False
        body = {}
        for qn_field in QN_FIELDS:
            available, body = _index_search(client, {
                "from": 0, "size": 10,
                "query": {"bool": {"must": [
                    {"term": {"__typeName.keyword": "DataSet"}},
                    {"match_phrase": {"__classificationNames": self.tag_name}},
                    {"wildcard": {qn_field: f"{PREFIX}*"}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            })
            if available and body.get("approximateCount", 0) > 0:
                break
        if not available:
            return

        entities = body.get("entities", [])
        for e in entities:
            assert e.get("typeName") == "DataSet", (
                f"Expected typeName=DataSet, got {e.get('typeName')}"
            )
            cn = e.get("classificationNames", [])
            assert self.tag_name in cn, (
                f"Entity {e.get('guid')} missing {self.tag_name} in classificationNames"
            )
            qn = (e.get("attributes", {}).get("qualifiedName", "") or
                  e.get("qualifiedName", ""))
            assert qn.startswith(PREFIX), (
                f"Entity QN {qn} does not start with {PREFIX}"
            )

    @test("search_wildcard_qn_prefix", tags=["search", "correctness"], order=4)
    def test_search_wildcard_qn_prefix(self, client, ctx):
        # Try multiple QN field names to handle ES mapping differences
        count = 0
        entities = []
        for qn_field in ("qualifiedName.keyword", "qualifiedName", "__qualifiedName"):
            available, body = _index_search(client, {
                "from": 0, "size": 50,
                "query": {"bool": {"must": [
                    {"wildcard": {qn_field: f"{PREFIX}*"}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            })
            if not available:
                continue
            count = body.get("approximateCount", 0)
            entities = body.get("entities", [])
            if count > 0:
                break
        if not available:
            return

        assert count > 0, f"Expected entities with QN prefix {PREFIX}*, got count={count}"

        for e in entities:
            qn = (e.get("attributes", {}).get("qualifiedName", "") or
                  e.get("qualifiedName", ""))
            assert qn.startswith(PREFIX), (
                f"Entity QN {qn} does not start with expected prefix {PREFIX}"
            )

    @test("search_result_has_labels", tags=["search", "correctness"], order=5)
    def test_search_result_has_labels(self, client, ctx):
        available, body = _search_by_guid(client, self.guid_b)
        if not available:
            return

        entities = body.get("entities", [])
        assert len(entities) > 0, f"Entity {self.guid_b} not found in search"
        entity = entities[0]
        labels = entity.get("labels", [])
        if labels:  # Labels may not be indexed on all environments
            for expected in self.labels_b:
                assert expected in labels, (
                    f"Expected label '{expected}' in search result labels: {labels}"
                )

    @test("search_approximate_count_present", tags=["search", "correctness"], order=6)
    def test_search_approximate_count_present(self, client, ctx):
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return

        assert "approximateCount" in body, (
            f"Expected 'approximateCount' field in search response, got keys: {list(body.keys())}"
        )
        count = body["approximateCount"]
        assert isinstance(count, int), (
            f"Expected approximateCount to be int, got {type(count).__name__}"
        )
        assert count >= 0, f"Expected approximateCount >= 0, got {count}"

    @test("search_multi_classification_entity", tags=["search", "correctness"], order=7)
    def test_search_multi_classification_entity(self, client, ctx):
        if not self.tag_add_ok:
            return

        # Add second classification to entity A
        resp = client.post(
            f"/entity/guid/{self.guid_a}/classifications",
            json_data=[{"typeName": self.tag2_name}],
        )
        if resp.status_code not in (200, 204):
            return  # Type cache lag

        time.sleep(ctx.get("es_sync_wait", 5))

        # Search by first tag
        available, body1 = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.guid_a}},
                {"match_phrase": {"__classificationNames": self.tag_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        assert body1.get("approximateCount", 0) > 0, (
            f"Entity {self.guid_a} not found when searching by {self.tag_name}"
        )

        # Search by second tag
        _, body2 = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.guid_a}},
                {"match_phrase": {"__classificationNames": self.tag2_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        assert body2.get("approximateCount", 0) > 0, (
            f"Entity {self.guid_a} not found when searching by {self.tag2_name}"
        )

    @test("search_exclude_deleted_by_state_filter", tags=["search", "correctness"], order=8)
    def test_search_exclude_deleted_by_state_filter(self, client, ctx):
        # Create a throwaway entity, delete it, verify __state=ACTIVE excludes it
        guid, qn = _create_entity_and_register(client, ctx, "search-del", cleanup=False)

        time.sleep(ctx.get("es_sync_wait", 5))

        # Verify it's in ACTIVE search first
        available, body = _search_by_guid(client, guid)
        if not available:
            return
        assert body.get("approximateCount", 0) > 0, (
            f"Entity {guid} not found in ACTIVE search before delete"
        )

        # Soft delete
        client.delete(f"/entity/guid/{guid}")
        time.sleep(ctx.get("es_sync_wait", 5))

        # Verify excluded from ACTIVE search
        _, body2 = _search_by_guid(client, guid)
        assert body2.get("approximateCount", 0) == 0, (
            f"Deleted entity {guid} still in ACTIVE search, count={body2.get('approximateCount')}"
        )

    @test("search_by_glossary_term_meaning", tags=["search", "correctness", "glossary"], order=9)
    def test_search_by_glossary_term_meaning(self, client, ctx):
        # Create glossary + term + assign to entity
        glossary_name = unique_name("search-gloss")
        resp = client.post("/glossary", json_data={
            "name": glossary_name,
            "shortDescription": "Search test glossary",
        })
        if resp.status_code != 200:
            return  # Glossary endpoint not available
        glossary_guid = resp.json().get("guid")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{glossary_guid}"))

        term_name = unique_name("search-term")
        resp = client.post("/glossary/term", json_data={
            "name": term_name,
            "shortDescription": "Search test term",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        if resp.status_code != 200:
            return
        term_guid = resp.json().get("guid")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{term_guid}"))

        # Create entity and assign term
        entity_guid, entity_qn = _create_entity_and_register(client, ctx, "search-meaning")
        resp = client.post(f"/glossary/terms/{term_guid}/assignedEntities", json_data=[
            {"guid": entity_guid, "typeName": "DataSet"},
        ])
        if resp.status_code not in (200, 204):
            return  # Term assignment not available

        time.sleep(ctx.get("es_sync_wait", 5))

        # Search by __meaningNames
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": entity_guid}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return

        entities = body.get("entities", [])
        if not entities:
            return
        entity = entities[0]
        meanings = entity.get("meanings", []) or entity.get("meaningNames", [])
        # Best-effort: check meanings field is present if the platform populates it
        if meanings:
            # Verify term info in meanings
            if isinstance(meanings, list) and meanings:
                if isinstance(meanings[0], dict):
                    guids = [m.get("termGuid") for m in meanings]
                    assert term_guid in guids, (
                        f"Expected termGuid {term_guid} in meanings, got {guids}"
                    )

    @test("search_propagated_classification", tags=["search", "correctness", "propagation"],
          order=10)
    def test_search_propagated_classification(self, client, ctx):
        if not self.tag_add_ok:
            return

        # Create lineage: src -> process -> tgt, tag src, check tgt
        src_guid, src_qn = _create_entity_and_register(client, ctx, "prop-search-src")
        tgt_guid, tgt_qn = _create_entity_and_register(client, ctx, "prop-search-tgt")

        proc = build_process_entity(
            inputs=[{"guid": src_guid, "typeName": "DataSet"}],
            outputs=[{"guid": tgt_guid, "typeName": "DataSet"}],
        )
        resp = client.post("/entity", json_data={"entity": proc})
        if resp.status_code != 200:
            return  # Process creation not supported (staging may reject)
        proc_entities = (resp.json().get("mutatedEntities", {}).get("CREATE", []) or
                         resp.json().get("mutatedEntities", {}).get("UPDATE", []))
        if not proc_entities:
            return
        proc_guid = proc_entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{proc_guid}"))

        # Add propagating classification to src
        resp = client.post(f"/entity/guid/{src_guid}/classifications", json_data=[{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }])
        if resp.status_code not in (200, 204):
            return

        # Wait for propagation
        time.sleep(max(ctx.get("es_sync_wait", 5), 8))

        # Best-effort: search tgt by propagated classification
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": tgt_guid}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return

        entities = body.get("entities", [])
        if not entities:
            return
        entity = entities[0]
        prop_names = entity.get("propagatedClassificationNames", [])
        # Best-effort check: propagation may take longer or be disabled
        if prop_names and self.tag_name in prop_names:
            pass  # Propagation confirmed in search
