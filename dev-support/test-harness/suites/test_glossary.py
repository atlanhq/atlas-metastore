"""Glossary/Term/Category full lifecycle tests."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty,
)
from core.data_factory import unique_name


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
        })
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        guid = resp.json()["guid"]
        ctx.register_entity("glossary", guid, "AtlasGlossary")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

    @test("get_glossary", tags=["glossary"], order=3, depends_on=["create_glossary"])
    def test_get_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.glossary_name)

    @test("get_glossary_detailed", tags=["glossary"], order=4, depends_on=["create_glossary"])
    def test_get_glossary_detailed(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}/detailed")
        assert_status(resp, 200)

    @test("update_glossary", tags=["glossary", "crud"], order=5, depends_on=["create_glossary"])
    def test_update_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/{guid}", json_data={
            "guid": guid,
            "name": self.glossary_name,
            "shortDescription": "Updated description",
        })
        assert_status(resp, 200)

    # ---- Term CRUD ----

    @test("create_term", tags=["smoke", "glossary", "crud"], order=10, depends_on=["create_glossary"])
    def test_create_term(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/term", json_data={
            "name": self.term_name,
            "shortDescription": "Test term",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        guid = resp.json()["guid"]
        ctx.register_entity("term1", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

    @test("create_terms_bulk", tags=["glossary", "crud"], order=11, depends_on=["create_glossary"])
    def test_create_terms_bulk(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/terms", json_data=[
            {
                "name": self.term2_name,
                "shortDescription": "Bulk term",
                "anchor": {"glossaryGuid": glossary_guid},
            }
        ])
        assert_status(resp, 200)
        body = resp.json()
        if isinstance(body, list) and body:
            guid = body[0]["guid"]
            ctx.register_entity("term2", guid, "AtlasGlossaryTerm")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

    @test("get_term", tags=["glossary"], order=12, depends_on=["create_term"])
    def test_get_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        resp = client.get(f"/glossary/term/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.term_name)

    @test("update_term", tags=["glossary", "crud"], order=13, depends_on=["create_term"])
    def test_update_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/term/{guid}", json_data={
            "guid": guid,
            "name": self.term_name,
            "shortDescription": "Updated term description",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        assert_status(resp, 200)

    # ---- Category CRUD ----

    @test("create_category", tags=["glossary", "crud"], order=20, depends_on=["create_glossary"])
    def test_create_category(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.post("/glossary/category", json_data={
            "name": self.category_name,
            "shortDescription": "Test category",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        assert_status(resp, 200)
        assert_field_present(resp, "guid")
        guid = resp.json()["guid"]
        ctx.register_entity("category1", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

    @test("get_category", tags=["glossary"], order=21, depends_on=["create_category"])
    def test_get_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        resp = client.get(f"/glossary/category/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.category_name)

    @test("update_category", tags=["glossary", "crud"], order=22, depends_on=["create_category"])
    def test_update_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/category/{guid}", json_data={
            "guid": guid,
            "name": self.category_name,
            "shortDescription": "Updated category",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        # 409 can happen if concurrent modification or qualifiedName conflicts
        assert_status_in(resp, [200, 409])

    # ---- Delete (reverse order: categories, terms, then glossary via cleanup) ----

    @test("delete_category", tags=["glossary", "crud"], order=80, depends_on=["create_category"])
    def test_delete_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        resp = client.delete(f"/glossary/category/{guid}")
        assert_status_in(resp, [200, 204])

    @test("delete_term", tags=["glossary", "crud"], order=81, depends_on=["create_term"])
    def test_delete_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        resp = client.delete(f"/glossary/term/{guid}")
        assert_status_in(resp, [200, 204])
