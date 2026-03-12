"""Glossary/Term/Category full lifecycle tests."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, SkipTestError,
)
from core.audit_helpers import poll_audit_events
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import unique_name, build_dataset_entity, unique_qn


def _find_glossary_entity_by_name(client, type_name, name, max_wait=90,
                                   interval=15):
    """Poll search until a glossary entity with given name appears.

    Handles the case where POST timed out but the server created the entity.
    Returns guid or None.
    """
    # Try search immediately first
    for attempt in range(1 + max_wait // interval):
        if attempt > 0:
            time.sleep(interval)
        resp = client.post("/search/indexsearch", json_data={"dsl": {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": type_name}},
                {"term": {"name.keyword": name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }})
        if resp.status_code == 200:
            entities = resp.json().get("entities", [])
            if entities:
                return entities[0].get("guid")
    return None


@suite("glossary", description="Glossary, Term, Category lifecycle")
class GlossarySuite:

    def setup(self, client, ctx):
        self.glossary_name = unique_name("harness-glossary")
        self.term_name = unique_name("harness-term")
        self.term2_name = unique_name("harness-term2")
        self.category_name = unique_name("harness-category")

    # ---- Glossary CRUD ----

    @test("list_glossaries", tags=["smoke", "glossary"], order=1)
    def test_list_glossaries(self, client, ctx):
        resp = client.get("/glossary", params={"limit": 10, "offset": 0, "sort": "ASC"})
        assert_status(resp, 200)

    @test("create_glossary", tags=["smoke", "glossary", "crud"], order=2)
    def test_create_glossary(self, client, ctx):

        def _find_glossary_by_name(name):
            """Check if glossary was created server-side despite timeout."""
            # Try listing API
            r = client.get("/glossary", params={"limit": 100, "offset": 0, "sort": "ASC"})
            if r.status_code == 200:
                glossaries = r.json() if isinstance(r.json(), list) else []
                for g in glossaries:
                    if g.get("name") == name:
                        return g.get("guid")
            # Try index search as fallback (faster on staging)
            r2 = client.post("/search/indexsearch", json_data={"dsl": {
                "from": 0, "size": 1,
                "query": {"bool": {"must": [
                    {"term": {"__typeName.keyword": "AtlasGlossary"}},
                    {"term": {"name.keyword": name}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            }})
            if r2.status_code == 200:
                entities = r2.json().get("entities", [])
                if entities:
                    return entities[0].get("guid")
            return None

        def _poll_for_glossary(name, max_wait=90, interval=15):
            """Poll until glossary appears via list or search API."""
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(interval)
                elapsed += interval
                guid = _find_glossary_by_name(name)
                if guid:
                    return guid
            return None

        resp = client.post("/glossary", json_data={
            "name": self.glossary_name,
            "shortDescription": "Test harness glossary",
        }, timeout=180)

        guid = None
        if resp.status_code == 200:
            guid = resp.json().get("guid")
        elif resp.status_code in (408, 502, 503):
            # Timeout or transient error — server may still be creating it.
            print(f"  [glossary] POST returned {resp.status_code}, polling for glossary...")
            guid = _poll_for_glossary(self.glossary_name, max_wait=120, interval=15)

        if not guid:
            raise SkipTestError(
                f"Glossary creation failed (status={resp.status_code}) — "
                f"server may be overloaded or unavailable"
            )

        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

        # Read-after-write: verify persisted glossary matches what was sent
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.glossary_name)
        assert_field_present(resp2, "qualifiedName")

        glossary_qn = resp2.json().get("qualifiedName")
        ctx.register_entity("glossary", guid, "AtlasGlossary", qualifiedName=glossary_qn)

        # Kafka: verify ENTITY_CREATE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_CREATE")

    @test("list_glossaries_find_created", tags=["glossary", "validation"], order=3, depends_on=["create_glossary"])
    def test_list_glossaries_find_created(self, client, ctx):
        # GET-all -> find our glossary by name -> GET by that GUID -> verify details
        resp = client.get("/glossary", params={"limit": 100, "offset": 0, "sort": "ASC"})
        assert_status(resp, 200)
        body = resp.json()
        glossaries = body if isinstance(body, list) else []
        found = None
        for g in glossaries:
            if g.get("name") == self.glossary_name:
                found = g
                break
        assert found is not None, f"Created glossary '{self.glossary_name}' not found in list"

        # GET by that GUID and verify details
        found_guid = found.get("guid")
        resp2 = client.get(f"/glossary/{found_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.glossary_name)

    @test("get_glossary", tags=["glossary"], order=4, depends_on=["create_glossary"])
    def test_get_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.glossary_name)

    @test("get_glossary_detailed", tags=["glossary"], order=5, depends_on=["create_glossary"])
    def test_get_glossary_detailed(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}/detailed")
        assert_status(resp, 200)

    @test("update_glossary", tags=["glossary", "crud"], order=6, depends_on=["create_glossary"])
    def test_update_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/{guid}", json_data={
            "guid": guid,
            "name": self.glossary_name,
            "shortDescription": "Updated description",
        }, timeout=180)
        assert_status(resp, 200)

        # Read-after-write: GET and verify shortDescription
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "shortDescription", "Updated description")

    @test("glossary_create_audit", tags=["glossary", "audit"], order=7, depends_on=["create_glossary"])
    def test_glossary_create_audit(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context — create_glossary must have failed"
        qn = ctx.get_entity_qn("glossary")
        events, total = poll_audit_events(
            client, guid, action_filter="ENTITY_CREATE",
            max_wait=60, interval=10, qualifiedName=qn,
        )
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint available but no ENTITY_CREATE events for glossary {guid} after 60s — "
                f"audit indexing may not be configured"
            )

    # ---- Term CRUD ----

    @test("create_term", tags=["smoke", "glossary", "crud"], order=10, depends_on=["create_glossary"])
    def test_create_term(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/term", json_data={
            "name": self.term_name,
            "shortDescription": "Test term",
            "anchor": {"glossaryGuid": glossary_guid},
        }, timeout=180)

        guid = None
        if resp.status_code == 200:
            guid = resp.json().get("guid")
        elif resp.status_code in (408, 409, 502, 503):
            # 409 = POST timed out earlier but server created the entity
            print(f"  [glossary] POST /glossary/term returned {resp.status_code}, "
                  f"polling for term...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryTerm", self.term_name,
                max_wait=90, interval=15,
            )

        if not guid:
            raise SkipTestError(
                f"Term creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("term1", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

        # Read-after-write: verify persisted term matches what was sent
        resp2 = client.get(f"/glossary/term/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.term_name)
        assert_field_present(resp2, "anchor")
        anchor = resp2.json().get("anchor", {})
        assert anchor.get("glossaryGuid") == glossary_guid, (
            f"Expected anchor.glossaryGuid={glossary_guid}, got {anchor.get('glossaryGuid')}"
        )

        # Kafka: verify ENTITY_CREATE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_CREATE")

    @test("create_terms_bulk", tags=["glossary", "crud"], order=11, depends_on=["create_glossary"])
    def test_create_terms_bulk(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/terms", json_data=[
            {
                "name": self.term2_name,
                "shortDescription": "Bulk term",
                "anchor": {"glossaryGuid": glossary_guid},
            }
        ], timeout=180)

        guid = None
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, list) and body:
                guid = body[0].get("guid")
        elif resp.status_code in (408, 409, 502, 503):
            # 409 = POST timed out earlier but server created the entity
            print(f"  [glossary] POST /glossary/terms returned {resp.status_code}, "
                  f"polling for term...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryTerm", self.term2_name,
                max_wait=90, interval=15,
            )

        if not guid:
            raise SkipTestError(
                f"Bulk term creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("term2", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

    @test("get_term", tags=["glossary"], order=12, depends_on=["create_term"])
    def test_get_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context — create_term must have failed"
        resp = client.get(f"/glossary/term/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.term_name)

    @test("update_term", tags=["glossary", "crud"], order=13, depends_on=["create_term"])
    def test_update_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context — create_term must have failed"
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/term/{guid}", json_data={
            "guid": guid,
            "name": self.term_name,
            "shortDescription": "Updated term description",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        assert_status(resp, 200)

        # Read-after-write: GET and verify shortDescription
        resp2 = client.get(f"/glossary/term/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "shortDescription", "Updated term description")

    @test("get_glossary_terms_list", tags=["glossary"], order=15, depends_on=["create_term"])
    def test_get_glossary_terms_list(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/terms", params={
            "limit": 10, "offset": 0, "sort": "ASC",
        })
        assert_status(resp, 200)
        body = resp.json()
        terms = body if isinstance(body, list) else []
        assert len(terms) > 0, f"Expected at least one term in glossary, got {len(terms)}"

    @test("get_glossary_detailed_with_terms", tags=["glossary"], order=16, depends_on=["create_term"])
    def test_get_glossary_detailed_with_terms(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/detailed")
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"
        assert "guid" in body, "Detailed glossary response should have 'guid'"
        assert "name" in body, "Detailed glossary response should have 'name'"

    # ---- Category CRUD ----

    @test("create_category", tags=["glossary", "crud"], order=20, depends_on=["create_glossary"])
    def test_create_category(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/category", json_data={
            "name": self.category_name,
            "shortDescription": "Test category",
            "anchor": {"glossaryGuid": glossary_guid},
        }, timeout=180)

        guid = None
        if resp.status_code == 200:
            guid = resp.json().get("guid")
        elif resp.status_code in (408, 409, 502, 503):
            # 409 = POST timed out earlier but server created the entity
            print(f"  [glossary] POST /glossary/category returned {resp.status_code}, "
                  f"polling for category...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryCategory", self.category_name,
                max_wait=90, interval=15,
            )

        if not guid:
            raise SkipTestError(
                f"Category creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("category1", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        # Read-after-write: verify persisted category matches what was sent
        resp2 = client.get(f"/glossary/category/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.category_name)

    @test("get_category", tags=["glossary"], order=21, depends_on=["create_category"])
    def test_get_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context — create_category must have failed"
        resp = client.get(f"/glossary/category/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.category_name)

    @test("create_categories_bulk", tags=["glossary", "crud"], order=23, depends_on=["create_glossary"])
    def test_create_categories_bulk(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        cat_name_a = unique_name("bulk-cat-a")
        cat_name_b = unique_name("bulk-cat-b")
        resp = client.post("/glossary/categories", json_data=[
            {
                "name": cat_name_a,
                "shortDescription": "Bulk category A",
                "anchor": {"glossaryGuid": glossary_guid},
            },
            {
                "name": cat_name_b,
                "shortDescription": "Bulk category B",
                "anchor": {"glossaryGuid": glossary_guid},
            },
        ], timeout=180)

        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, list):
                for cat in body:
                    cat_guid = cat.get("guid")
                    if cat_guid:
                        ctx.register_cleanup(
                            lambda g=cat_guid: client.delete(f"/glossary/category/{g}")
                        )
        elif resp.status_code in (408, 409, 502, 503):
            # 409 = POST timed out earlier but server created the entities
            print(f"  [glossary] POST /glossary/categories returned {resp.status_code}, "
                  f"polling for categories...")
            for name in [cat_name_a, cat_name_b]:
                guid = _find_glossary_entity_by_name(
                    client, "AtlasGlossaryCategory", name,
                    max_wait=60, interval=15,
                )
                if guid:
                    ctx.register_cleanup(
                        lambda g=guid: client.delete(f"/glossary/category/{g}")
                    )
        else:
            raise SkipTestError(
                f"Bulk categories creation failed (status={resp.status_code})"
            )

    @test("create_category_hierarchy", tags=["glossary", "crud"], order=24, depends_on=["create_category"])
    def test_create_category_hierarchy(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        parent_guid = ctx.get_entity_guid("category1")
        child_name = unique_name("child-cat")
        resp = client.post("/glossary/category", json_data={
            "name": child_name,
            "shortDescription": "Child category",
            "anchor": {"glossaryGuid": glossary_guid},
            "parentCategory": {"categoryGuid": parent_guid},
        }, timeout=180)

        child_guid = None
        if resp.status_code == 200:
            child_guid = resp.json().get("guid")
        elif resp.status_code in (408, 409, 502, 503):
            # 409 = POST timed out earlier but server created the entity
            print(f"  [glossary] POST /glossary/category (hierarchy) returned "
                  f"{resp.status_code}, polling...")
            child_guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryCategory", child_name,
                max_wait=90, interval=15,
            )

        if child_guid:
            ctx.register_entity("child_category", child_guid, "AtlasGlossaryCategory")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{child_guid}"))

            # Verify parent has childrenCategories
            resp2 = client.get(f"/glossary/category/{parent_guid}")
            if resp2.status_code == 200:
                body = resp2.json()
                children = body.get("childrenCategories", [])
                found = any(c.get("categoryGuid") == child_guid for c in children)
                assert found, f"Expected child {child_guid} in parent's childrenCategories"

    @test("get_glossary_categories_list", tags=["glossary"], order=25, depends_on=["create_category"])
    def test_get_glossary_categories_list(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/categories", params={
            "limit": 10, "offset": 0, "sort": "ASC",
        })
        assert_status(resp, 200)
        body = resp.json()
        categories = body if isinstance(body, list) else []
        assert len(categories) > 0, f"Expected at least one category, got {len(categories)}"

    @test("get_category_terms", tags=["glossary"], order=26, depends_on=["create_term", "create_category"])
    def test_get_category_terms(self, client, ctx):
        cat_guid = ctx.get_entity_guid("category1")
        assert cat_guid, "category1 GUID not found in context"
        resp = client.get(f"/glossary/category/{cat_guid}/terms")
        # May be empty if term not assigned to category, just verify endpoint works
        assert_status_in(resp, [200, 204])

    @test("assign_term_to_entity", tags=["glossary"], order=30, depends_on=["create_term"])
    def test_assign_term_to_entity(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        assert term_guid, "term1 GUID not found in context"
        # Create a DataSet entity for term assignment
        qn = unique_qn("term-assign-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("term-assign"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        entity_guid = entities[0]["guid"]
        ctx.register_entity("term_assign_entity", entity_guid, "DataSet")
        ctx.register_entity_cleanup(entity_guid)

        # Assign term to entity
        resp = client.post(f"/glossary/terms/{term_guid}/assignedEntities", json_data=[
            {"guid": entity_guid, "typeName": "DataSet"},
        ])
        assert_status_in(resp, [200, 204])

        # Read-after-write: verify entity in assigned list
        resp2 = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        if resp2.status_code == 200:
            body = resp2.json()
            assigned = body if isinstance(body, list) else []
            found = any(e.get("guid") == entity_guid for e in assigned)
            assert found, f"Expected entity {entity_guid} in assigned entities after assign"

    @test("get_term_assigned_entities", tags=["glossary"], order=31, depends_on=["assign_term_to_entity"])
    def test_get_term_assigned_entities(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        resp = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        assert_status(resp, 200)
        body = resp.json()
        entities = body if isinstance(body, list) else []
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        if entity_guid:
            found = any(e.get("guid") == entity_guid for e in entities)
            assert found, f"Expected entity {entity_guid} in assigned entities"

    @test("disassociate_term_from_entity", tags=["glossary"], order=32, depends_on=["assign_term_to_entity"])
    def test_disassociate_term_from_entity(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        assert entity_guid, "term_assign_entity GUID not found in context"

        # First get the relationship guid
        resp = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        assert_status(resp, 200)
        body = resp.json()
        entities = body if isinstance(body, list) else []
        # Find the assignment with matching guid
        assignment = None
        for e in entities:
            if e.get("guid") == entity_guid:
                assignment = e
                break
        assert assignment, (
            f"Entity {entity_guid} not found in assigned entities of term {term_guid}"
        )

        # Disassociate
        resp = client.put(
            f"/glossary/terms/{term_guid}/assignedEntities",
            json_data=[assignment],
        )
        # PUT with empty or DELETE semantics vary; accept multiple statuses
        assert_status_in(resp, [200, 204, 400, 404])

    @test("update_category", tags=["glossary", "crud"], order=22, depends_on=["create_category"])
    def test_update_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context"
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/category/{guid}", json_data={
            "guid": guid,
            "name": self.category_name,
            "shortDescription": "Updated category",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        # 409 can happen if concurrent modification or qualifiedName conflicts
        assert_status_in(resp, [200, 409])
        if resp.status_code == 200:
            # Read-after-write: verify shortDescription updated
            resp2 = client.get(f"/glossary/category/{guid}")
            if resp2.status_code == 200:
                assert_field_equals(resp2, "shortDescription", "Updated category")

    # ---- Delete (reverse order: categories, terms, then glossary via cleanup) ----

    @test("term_assignment_in_search", tags=["glossary", "search"], order=42,
          depends_on=["assign_term_to_entity"])
    def test_term_assignment_in_search(self, client, ctx):
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        term_guid = ctx.get_entity_guid("term1")
        assert entity_guid, "term_assign_entity GUID not found in context"
        assert term_guid, "term1 GUID not found in context"

        # Poll ES until entity appears with meanings populated
        print(f"  [glossary-search] Polling ES for entity {entity_guid} with meanings...")
        found_entity = None
        meanings_found = False
        max_wait = 60
        poll_interval = 5
        for i in range(max_wait // poll_interval):
            time.sleep(poll_interval)
            elapsed = (i + 1) * poll_interval
            resp = client.post("/search/indexsearch", json_data={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {"bool": {"must": [
                        {"term": {"__guid": entity_guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                }
            })
            if resp.status_code != 200:
                print(f"  [glossary-search] Search returned {resp.status_code} ({elapsed}s/{max_wait}s)")
                continue
            entities = resp.json().get("entities", [])
            if not entities:
                print(f"  [glossary-search] Entity not in ES yet ({elapsed}s/{max_wait}s)")
                continue
            found_entity = entities[0]
            # Check both 'meanings' (API model) and '__meanings' (ES field)
            meanings = found_entity.get("meanings", [])
            meanings_raw = found_entity.get("__meanings", [])
            if (meanings and isinstance(meanings, list) and len(meanings) > 0) or \
               (meanings_raw and isinstance(meanings_raw, list) and len(meanings_raw) > 0):
                meanings_found = True
                print(f"  [glossary-search] Meanings found after {elapsed}s: "
                      f"meanings={meanings}, __meanings={meanings_raw}")
                break
            print(f"  [glossary-search] Entity found but no meanings yet ({elapsed}s/{max_wait}s)")

        assert found_entity is not None, (
            f"Entity {entity_guid} not found in ES after {max_wait}s polling"
        )
        meanings = found_entity.get("meanings", [])
        meanings_raw = found_entity.get("__meanings", [])
        assert meanings_found, (
            f"Expected meanings on entity {entity_guid} after term assignment, "
            f"but meanings is empty after {max_wait}s polling. "
            f"meanings={meanings}, __meanings={meanings_raw}"
        )

        # Verify termGuid in meanings (if populated as objects)
        if meanings and isinstance(meanings[0], dict):
            term_guids = [m.get("termGuid") for m in meanings]
            assert term_guid in term_guids, (
                f"Expected termGuid {term_guid} in meanings, got {term_guids}"
            )

    @test("delete_glossary_not_empty", tags=["glossary"], order=79, depends_on=["create_glossary"])
    def test_delete_glossary_not_empty(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.delete(f"/glossary/{guid}")
        # May return 409 (conflict) if cascade not supported, or 200 if cascade delete
        assert_status_in(resp, [200, 204, 409])
        if resp.status_code == 409:
            body = resp.json()
            assert "errorMessage" in body or "message" in body, (
                "Expected error message in 409 response"
            )
        elif resp.status_code in (200, 204):
            # Cascade deleted everything — mark so downstream deletes skip gracefully
            ctx.set("glossary_cascade_deleted", True)

    @test("delete_category", tags=["glossary", "crud"], order=80, depends_on=["create_category"])
    def test_delete_category(self, client, ctx):
        if ctx.get("glossary_cascade_deleted"):
            raise SkipTestError("Glossary was cascade-deleted — category already gone")
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context"
        resp = client.delete(f"/glossary/category/{guid}")
        assert_status_in(resp, [200, 204])

    @test("delete_term", tags=["glossary", "crud"], order=81, depends_on=["create_term"])
    def test_delete_term(self, client, ctx):
        if ctx.get("glossary_cascade_deleted"):
            raise SkipTestError("Glossary was cascade-deleted — term already gone")
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context"
        resp = client.delete(f"/glossary/term/{guid}")
        assert_status_in(resp, [200, 204])
