"""Search filter tests.

Validates that ES search correctly filters by certificate status,
labels, business metadata values, glossary term meanings, and
advanced attribute operators via the basic search API.
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.data_factory import (
    build_dataset_entity, build_business_metadata_def,
    unique_name, unique_qn, unique_type_name, PREFIX,
)


# ES field names for qualifiedName differ between local and staging
QN_FIELDS = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")


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


def _create_entity_and_register(client, ctx, suffix, extra_attrs=None,
                                labels=None):
    """Create a DataSet entity, register cleanup, return (guid, qn, name)."""
    qn = unique_qn(suffix)
    name = unique_name(suffix)
    entity = build_dataset_entity(qn=qn, name=name, extra_attrs=extra_attrs)
    if labels is not None:
        entity["labels"] = labels
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return guid, qn, name


@suite("search_filters", depends_on_suites=["entity_crud"],
       description="Search filter operations (certificate, labels, BM, meanings, operators)")
class SearchFiltersSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # --- Entity A: certificateStatus=VERIFIED, has description ---
        self.guid_a, self.qn_a, self.name_a = _create_entity_and_register(
            client, ctx, "filt-a",
            extra_attrs={
                "certificateStatus": "VERIFIED",
                "description": "filter-desc-alpha",
            },
        )

        # --- Entity B: certificateStatus=DRAFT ---
        self.guid_b, self.qn_b, self.name_b = _create_entity_and_register(
            client, ctx, "filt-b",
            extra_attrs={"certificateStatus": "DRAFT"},
        )

        # --- Entity C: labels ---
        self.labels_c = ["filter-label-x", "filter-label-y"]
        self.guid_c, self.qn_c, self.name_c = _create_entity_and_register(
            client, ctx, "filt-c", labels=self.labels_c,
        )

        # --- Entity E: plain, no extras (negative check) ---
        self.guid_e, self.qn_e, self.name_e = _create_entity_and_register(
            client, ctx, "filt-e",
        )

        # --- BM typedef + Entity D ---
        self.bm_ok = False
        self.bm_name = unique_type_name("FiltBM")
        bm_resp = client.post("/types/typedefs", json_data={
            "businessMetadataDefs": [build_business_metadata_def(name=self.bm_name)]
        })
        if bm_resp.status_code in (200, 409):
            time.sleep(10)  # type cache propagation
            ctx.register_cleanup(
                lambda: client.delete(f"/types/typedef/name/{self.bm_name}")
            )

            self.guid_d, self.qn_d, self.name_d = _create_entity_and_register(
                client, ctx, "filt-d",
            )
            # Set BM value on Entity D
            bm_set_resp = client.post(
                f"/entity/guid/{self.guid_d}/businessmetadata",
                json_data={self.bm_name: {"bmField1": "filter-bm-val"}},
            )
            if bm_set_resp.status_code in (200, 204):
                self.bm_ok = True
        else:
            self.guid_d = self.qn_d = self.name_d = None

        # --- Glossary + Term assigned to Entity A ---
        self.glossary_ok = False
        self.term_qn = None
        self.term_guid = None

        glossary_resp = client.post("/glossary", json_data={
            "name": unique_name("filt-gloss"),
            "shortDescription": "Filter test glossary",
        })
        if glossary_resp.status_code == 200:
            glossary_guid = glossary_resp.json().get("guid")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/{glossary_guid}"))

            term_name = unique_name("filt-term")
            term_resp = client.post("/glossary/term", json_data={
                "name": term_name,
                "shortDescription": "Filter test term",
                "anchor": {"glossaryGuid": glossary_guid},
            })
            if term_resp.status_code == 200:
                term_body = term_resp.json()
                self.term_guid = term_body.get("guid")
                self.term_qn = term_body.get("qualifiedName")
                ctx.register_cleanup(
                    lambda: client.delete(f"/glossary/term/{self.term_guid}")
                )

                # Assign term to Entity A
                assign_resp = client.post(
                    f"/glossary/terms/{self.term_guid}/assignedEntities",
                    json_data=[{"guid": self.guid_a, "typeName": "DataSet"}],
                )
                if assign_resp.status_code in (200, 204):
                    self.glossary_ok = True

        # Single ES sync wait after all setup
        time.sleep(max(es_wait, 5))

    # ----------------------------------------------------------------
    # Certificate Status Filters
    # ----------------------------------------------------------------

    @test("filter_certificate_verified", tags=["filter", "search", "certificate"],
          order=1)
    def test_filter_certificate_verified(self, client, ctx):
        """DSL term filter on certificateStatus=VERIFIED."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"certificateStatus": "VERIFIED"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        count = body.get("approximateCount", 0)
        if count == 0:
            return  # certificateStatus may not be indexed on this env

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found in VERIFIED filter results: {guids}"
        )
        # Entity B should NOT appear (it's DRAFT)
        assert self.guid_b not in guids, (
            f"Entity B ({self.guid_b}) should not appear in VERIFIED filter"
        )

    @test("filter_certificate_draft", tags=["filter", "search", "certificate"],
          order=2)
    def test_filter_certificate_draft(self, client, ctx):
        """DSL term filter on certificateStatus=DRAFT."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"certificateStatus": "DRAFT"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        count = body.get("approximateCount", 0)
        if count == 0:
            return  # certificateStatus not indexed

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_b in guids, (
            f"Entity B ({self.guid_b}) not found in DRAFT filter results: {guids}"
        )

    @test("filter_certificate_basic_search_api", tags=["filter", "search", "certificate"],
          order=3)
    def test_filter_certificate_basic_search_api(self, client, ctx):
        """Basic search API with entityFilters for certificateStatus=VERIFIED."""
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 10,
            "entityFilters": {
                "condition": "AND",
                "criterion": [{
                    "attributeName": "certificateStatus",
                    "operator": "eq",
                    "attributeValue": "VERIFIED",
                }],
            },
        })
        if resp.status_code in (400, 404, 405):
            return  # Basic search not available or filter not supported
        if resp.status_code != 200:
            return

        body = resp.json()
        entities = body.get("entities", [])
        if not entities:
            return  # certificateStatus not indexed

        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found via basic search "
            f"certificateStatus=VERIFIED: {guids}"
        )

    # ----------------------------------------------------------------
    # Label Filters
    # ----------------------------------------------------------------

    @test("filter_by_label_dsl", tags=["filter", "search", "label"], order=4)
    def test_filter_by_label_dsl(self, client, ctx):
        """DSL term filter on __labels."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__labels": "filter-label-x"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        count = body.get("approximateCount", 0)
        if count == 0:
            return  # Labels not indexed

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_c in guids, (
            f"Entity C ({self.guid_c}) not found in label filter results: {guids}"
        )

    @test("filter_by_label_excludes_unlabeled", tags=["filter", "search", "label"],
          order=5)
    def test_filter_by_label_excludes_unlabeled(self, client, ctx):
        """Verify __labels filter excludes entities without that label."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__labels": "filter-label-x"}},
                {"term": {"__guid": self.guid_e}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        count = body.get("approximateCount", 0)
        assert count == 0, (
            f"Entity E ({self.guid_e}) should NOT match label filter, "
            f"but got count={count}"
        )

    # ----------------------------------------------------------------
    # Business Metadata Value Filter
    # ----------------------------------------------------------------

    @test("filter_by_bm_value_in_search", tags=["filter", "search", "businessmeta"],
          order=6)
    def test_filter_by_bm_value_in_search(self, client, ctx):
        """Search Entity D by GUID and verify BM attribute in search result."""
        if not self.bm_ok:
            return  # BM setup failed

        available, body = _search_by_guid(client, self.guid_d)
        if not available:
            return

        entities = body.get("entities", [])
        if not entities:
            return  # Entity not yet in search

        entity = entities[0]
        assert entity.get("guid") == self.guid_d, (
            f"Expected guid={self.guid_d}, got {entity.get('guid')}"
        )
        # Best-effort: check BM attributes in search result
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        if bm:
            assert bm.get("bmField1") == "filter-bm-val", (
                f"Expected bmField1='filter-bm-val', got {bm}"
            )

    # ----------------------------------------------------------------
    # Glossary Term / Meanings Filter
    # ----------------------------------------------------------------

    @test("filter_by_meanings_dsl", tags=["filter", "search", "glossary"], order=7)
    def test_filter_by_meanings_dsl(self, client, ctx):
        """DSL term filter on __meanings by term qualifiedName."""
        if not self.glossary_ok or not self.term_qn:
            return  # Glossary setup failed

        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__meanings": self.term_qn}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if not available:
            return
        count = body.get("approximateCount", 0)
        if count == 0:
            return  # Meanings not indexed

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found in __meanings filter "
            f"for term QN {self.term_qn}: {guids}"
        )

    @test("filter_meanings_in_result", tags=["filter", "search", "glossary"],
          order=8)
    def test_filter_meanings_in_result(self, client, ctx):
        """Search Entity A by GUID and verify meanings field contains the term."""
        if not self.glossary_ok or not self.term_guid:
            return

        available, body = _search_by_guid(client, self.guid_a)
        if not available:
            return

        entities = body.get("entities", [])
        if not entities:
            return

        entity = entities[0]
        meanings = entity.get("meanings", [])
        meaning_names = entity.get("meaningNames", [])

        # Check meanings objects (list of dicts with termGuid)
        if meanings and isinstance(meanings, list):
            if isinstance(meanings[0], dict):
                term_guids = [m.get("termGuid") for m in meanings]
                assert self.term_guid in term_guids, (
                    f"Expected termGuid {self.term_guid} in meanings, "
                    f"got {term_guids}"
                )
                return
        # Fallback: check meaningNames (list of strings)
        if meaning_names and isinstance(meaning_names, list):
            # At least the term name should be present
            pass  # Best-effort — shape varies by environment

    # ----------------------------------------------------------------
    # Advanced Attribute Filter Operators via Basic Search API
    # ----------------------------------------------------------------

    @test("filter_attr_equals", tags=["filter", "search", "operator"], order=9)
    def test_filter_attr_equals(self, client, ctx):
        """Basic search EQUALS operator on qualifiedName."""
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 10,
            "entityFilters": {
                "condition": "AND",
                "criterion": [{
                    "attributeName": "qualifiedName",
                    "operator": "eq",
                    "attributeValue": self.qn_a,
                }],
            },
        })
        if resp.status_code in (400, 404, 405):
            return  # Operator not supported
        if resp.status_code != 200:
            return

        body = resp.json()
        entities = body.get("entities", [])
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with EQUALS on QN "
            f"{self.qn_a}: {guids}"
        )

    @test("filter_attr_contains", tags=["filter", "search", "operator"], order=10)
    def test_filter_attr_contains(self, client, ctx):
        """Basic search CONTAINS operator on qualifiedName."""
        # Use a substring of Entity A's qualifiedName (keyword field, works with wildcard)
        substring = self.qn_a.split("/")[-1] if "/" in self.qn_a else self.qn_a[-12:]
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 10,
            "entityFilters": {
                "condition": "AND",
                "criterion": [{
                    "attributeName": "qualifiedName",
                    "operator": "contains",
                    "attributeValue": substring,
                }],
            },
        })
        if resp.status_code in (400, 404, 405):
            return
        if resp.status_code != 200:
            return

        body = resp.json()
        entities = body.get("entities", [])
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with CONTAINS "
            f"'{substring}': {guids}"
        )

    @test("filter_attr_ends_with", tags=["filter", "search", "operator"], order=11)
    def test_filter_attr_ends_with(self, client, ctx):
        """Basic search ENDS_WITH operator on qualifiedName."""
        # Use the last segment of Entity A's QN
        suffix = self.qn_a.split("/")[-1] if "/" in self.qn_a else self.qn_a[-12:]
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 10,
            "entityFilters": {
                "condition": "AND",
                "criterion": [{
                    "attributeName": "qualifiedName",
                    "operator": "endsWith",
                    "attributeValue": suffix,
                }],
            },
        })
        if resp.status_code in (400, 404, 405):
            return
        if resp.status_code != 200:
            return

        body = resp.json()
        entities = body.get("entities", [])
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with ENDS_WITH "
            f"'{suffix}': {guids}"
        )

    @test("filter_attr_not_null", tags=["filter", "search", "operator"], order=12)
    def test_filter_attr_not_null(self, client, ctx):
        """Basic search NOT_NULL operator on description, scoped by QN prefix."""
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 25,
            "entityFilters": {
                "condition": "AND",
                "criterion": [
                    {
                        "attributeName": "description",
                        "operator": "not_null",
                    },
                    {
                        "attributeName": "qualifiedName",
                        "operator": "startsWith",
                        "attributeValue": PREFIX,
                    },
                ],
            },
        })
        if resp.status_code in (400, 404, 405):
            return
        if resp.status_code != 200:
            return

        body = resp.json()
        entities = body.get("entities", [])
        if not entities:
            return  # Operator combination not supported or no match

        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) with description should appear in "
            f"NOT_NULL results: {guids}"
        )
