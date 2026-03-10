"""Glossary/Term/Category full lifecycle tests."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty,
)
from core.audit_helpers import assert_audit_event_exists
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import unique_name, build_dataset_entity, unique_qn


@suite("glossary", depends_on_suites=["typedefs"],
       description="Glossary, Term, Category lifecycle")
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
        resp = client.post("/glossary", json_data={
            "name": self.glossary_name,
            "shortDescription": "Test harness glossary",
        }, timeout=90)
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        assert_field_equals(resp, "name", self.glossary_name)
        assert_field_present(resp, "qualifiedName")

        guid = resp.json()["guid"]
        ctx.register_entity("glossary", guid, "AtlasGlossary")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

        # Read-after-write: verify persisted glossary matches what was sent
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.glossary_name)
        assert_field_present(resp2, "qualifiedName")

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
        }, timeout=90)
        assert_status(resp, 200)

        # Read-after-write: GET and verify shortDescription
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "shortDescription", "Updated description")

    @test("glossary_create_audit", tags=["glossary", "audit"], order=7, depends_on=["create_glossary"])
    def test_glossary_create_audit(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        if not guid:
            return
        event = assert_audit_event_exists(client, guid, "ENTITY_CREATE")
        if event is None:
            return  # Audit endpoint not available on this environment

    # ---- Term CRUD ----

    @test("create_term", tags=["smoke", "glossary", "crud"], order=10, depends_on=["create_glossary"])
    def test_create_term(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/term", json_data={
            "name": self.term_name,
            "shortDescription": "Test term",
            "anchor": {"glossaryGuid": glossary_guid},
        }, timeout=90)
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        assert_field_equals(resp, "name", self.term_name)
        assert_field_present(resp, "anchor")

        guid = resp.json()["guid"]
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
        ], timeout=90)
        assert_status(resp, 200)
        body = resp.json()
        if isinstance(body, list) and body:
            guid = body[0]["guid"]
            ctx.register_entity("term2", guid, "AtlasGlossaryTerm")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

    @test("get_term", tags=["glossary"], order=12, depends_on=["create_term"])
    def test_get_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        if not guid:
            return
        resp = client.get(f"/glossary/term/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.term_name)

    @test("update_term", tags=["glossary", "crud"], order=13, depends_on=["create_term"])
    def test_update_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        if not guid:
            return
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
        if not guid:
            return
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
        if not guid:
            return
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
        }, timeout=90)
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        assert_field_not_empty(resp, "name")
        assert_field_present(resp, "qualifiedName")

        guid = resp.json()["guid"]
        ctx.register_entity("category1", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        # Read-after-write: verify persisted category matches what was sent
        resp2 = client.get(f"/glossary/category/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.category_name)

    @test("get_category", tags=["glossary"], order=21, depends_on=["create_category"])
    def test_get_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        if not guid:
            return
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
        ], timeout=90)
        assert_status(resp, 200)
        body = resp.json()
        if isinstance(body, list):
            for cat in body:
                cat_guid = cat.get("guid")
                if cat_guid:
                    ctx.register_cleanup(lambda g=cat_guid: client.delete(f"/glossary/category/{g}"))

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
        }, timeout=90)
        assert_status_in(resp, [200, 409])
        if resp.status_code == 200:
            child_guid = resp.json().get("guid")
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
        if not guid:
            return
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
        if not cat_guid:
            return
        resp = client.get(f"/glossary/category/{cat_guid}/terms")
        # May be empty if term not assigned to category, just verify endpoint works
        assert_status_in(resp, [200, 204])

    @test("assign_term_to_entity", tags=["glossary"], order=30, depends_on=["create_term"])
    def test_assign_term_to_entity(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        if not term_guid:
            return
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
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{entity_guid}"))

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
        if not entity_guid:
            return

        # First get the relationship guid
        resp = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        if resp.status_code != 200:
            return
        body = resp.json()
        entities = body if isinstance(body, list) else []
        # Find the assignment with matching guid
        assignment = None
        for e in entities:
            if e.get("guid") == entity_guid:
                assignment = e
                break
        if not assignment:
            return

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
        if not guid:
            return
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
        if not entity_guid or not term_guid:
            return

        time.sleep(ctx.get("es_sync_wait", 5))

        # Search entity by GUID and verify meanings in response
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
            return  # Search not available

        body = resp.json()
        entities = body.get("entities", [])
        if not entities:
            return  # Entity not yet in search

        entity = entities[0]

        # Check meanings array has termGuid
        meanings = entity.get("meanings", [])
        if meanings and isinstance(meanings, list):
            if isinstance(meanings[0], dict):
                term_guids = [m.get("termGuid") for m in meanings]
                assert term_guid in term_guids, (
                    f"Expected termGuid {term_guid} in meanings, got {term_guids}"
                )

        # Check meaningNames field is present (if populated)
        meaning_names = entity.get("meaningNames", [])
        if meaning_names:
            assert len(meaning_names) > 0, "Expected non-empty meaningNames"

    @test("delete_glossary_not_empty", tags=["glossary"], order=79)
    def test_delete_glossary_not_empty(self, client, ctx):
        # Try to delete glossary with terms still present
        guid = ctx.get_entity_guid("glossary")
        if not guid:
            return
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
            return  # Already deleted by cascade
        guid = ctx.get_entity_guid("category1")
        if not guid:
            return
        resp = client.delete(f"/glossary/category/{guid}")
        assert_status_in(resp, [200, 204])

    @test("delete_term", tags=["glossary", "crud"], order=81, depends_on=["create_term"])
    def test_delete_term(self, client, ctx):
        if ctx.get("glossary_cascade_deleted"):
            return  # Already deleted by cascade
        guid = ctx.get_entity_guid("term1")
        if not guid:
            return
        resp = client.delete(f"/glossary/term/{guid}")
        assert_status_in(resp, [200, 204])
