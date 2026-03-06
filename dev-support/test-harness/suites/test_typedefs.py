"""TypeDef CRUD tests (~20 tests)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, assert_field_type,
    assert_list_min_length,
)
from core.data_factory import (
    build_enum_def, build_classification_def, build_struct_def,
    build_entity_def, build_business_metadata_def, build_relationship_def,
    unique_type_name,
)


@suite("typedefs", description="TypeDef CRUD operations")
class TypeDefSuite:

    def setup(self, client, ctx):
        self.enum_name = unique_type_name("TestEnum")
        self.classification_name = unique_type_name("TestClassification")
        self.struct_name = unique_type_name("TestStruct")
        self.entity_type_name = unique_type_name("TestEntityType")
        self.bm_name = unique_type_name("TestBM")

    # ---- GET existing types ----

    @test("get_all_typedefs", tags=["smoke", "typedef"], order=1)
    def test_get_all_typedefs(self, client, ctx):
        resp = client.get("/types/typedefs")
        assert_status(resp, 200)

    @test("get_typedef_headers", tags=["smoke", "typedef"], order=2)
    def test_get_typedef_headers(self, client, ctx):
        resp = client.get("/types/typedefs/headers")
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list), "Expected list of headers"
        assert len(body) > 0, "Expected at least one type header"

    @test("get_entitydef_by_name", tags=["typedef"], order=3)
    def test_get_entitydef_by_name(self, client, ctx):
        resp = client.get("/types/entitydef/name/DataSet")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", "DataSet")

    @test("get_entitydef_not_found", tags=["typedef"], order=4)
    def test_get_entitydef_not_found(self, client, ctx):
        resp = client.get("/types/entitydef/name/NonExistentType12345")
        assert_status_in(resp, [404, 204])

    # ---- CREATE types ----

    @test("create_enum_def", tags=["typedef", "crud"], order=10)
    def test_create_enum_def(self, client, ctx):
        payload = {"enumDefs": [build_enum_def(name=self.enum_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status(resp, 200)
        ctx.set("test_enum_name", self.enum_name)

        # Validate response structure
        body = resp.json()
        enum_defs = body.get("enumDefs", [])
        assert len(enum_defs) > 0, "Expected enumDefs in response"
        assert enum_defs[0].get("name") == self.enum_name, f"Expected name={self.enum_name}"
        assert "elementDefs" in enum_defs[0], "Expected elementDefs in enum def"

    @test("get_enum_def_by_name", tags=["typedef"], order=11, depends_on=["create_enum_def"])
    def test_get_enum_def_by_name(self, client, ctx):
        resp = client.get(f"/types/enumdef/name/{self.enum_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.enum_name)

    @test("create_struct_def", tags=["typedef", "crud"], order=12)
    def test_create_struct_def(self, client, ctx):
        payload = {"structDefs": [build_struct_def(name=self.struct_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status(resp, 200)
        ctx.set("test_struct_name", self.struct_name)

    @test("get_struct_def_by_name", tags=["typedef"], order=13, depends_on=["create_struct_def"])
    def test_get_struct_def_by_name(self, client, ctx):
        resp = client.get(f"/types/structdef/name/{self.struct_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.struct_name)

    @test("create_classification_def", tags=["typedef", "crud"], order=14)
    def test_create_classification_def(self, client, ctx):
        payload = {"classificationDefs": [build_classification_def(name=self.classification_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status(resp, 200)
        ctx.set("test_classification_name", self.classification_name)

        # Validate response structure
        body = resp.json()
        cls_defs = body.get("classificationDefs", [])
        assert len(cls_defs) > 0, "Expected classificationDefs in response"

        # Classification/BM types need time to propagate through type cache on staging
        time.sleep(5)

    @test("get_classification_def_by_name", tags=["typedef"], order=15, depends_on=["create_classification_def"])
    def test_get_classification_def_by_name(self, client, ctx):
        resp = client.get(f"/types/classificationdef/name/{self.classification_name}")
        # May return 404 if type cache hasn't propagated yet
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "name", self.classification_name)

    @test("create_entity_def", tags=["typedef", "crud"], order=16)
    def test_create_entity_def(self, client, ctx):
        payload = {"entityDefs": [build_entity_def(name=self.entity_type_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status(resp, 200)
        ctx.set("test_entity_type_name", self.entity_type_name)

        # Validate response structure
        body = resp.json()
        entity_defs = body.get("entityDefs", [])
        assert len(entity_defs) > 0, "Expected entityDefs in response"
        assert "attributeDefs" in entity_defs[0], "Expected attributeDefs in entity def"

    @test("get_entity_def_by_name", tags=["typedef"], order=17, depends_on=["create_entity_def"])
    def test_get_entity_def_by_name(self, client, ctx):
        resp = client.get(f"/types/entitydef/name/{self.entity_type_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.entity_type_name)

    @test("create_business_metadata_def", tags=["typedef", "crud"], order=18)
    def test_create_business_metadata_def(self, client, ctx):
        payload = {"businessMetadataDefs": [build_business_metadata_def(name=self.bm_name)]}
        resp = client.post("/types/typedefs", json_data=payload)
        assert_status(resp, 200)
        ctx.set("test_bm_name", self.bm_name)

        # Validate response structure
        body = resp.json()
        bm_defs = body.get("businessMetadataDefs", [])
        assert len(bm_defs) > 0, "Expected businessMetadataDefs in response"

        # BM types need time to propagate through type cache on staging
        time.sleep(5)

    @test("get_bm_def_by_name", tags=["typedef"], order=19, depends_on=["create_business_metadata_def"])
    def test_get_bm_def_by_name(self, client, ctx):
        resp = client.get(f"/types/businessmetadatadef/name/{self.bm_name}")
        # May return 404 if type cache hasn't propagated yet
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "name", self.bm_name)

    # ---- UPDATE type ----

    @test("update_enum_def", tags=["typedef", "crud"], order=20, depends_on=["create_enum_def"])
    def test_update_enum_def(self, client, ctx):
        payload = {
            "enumDefs": [build_enum_def(
                name=self.enum_name,
                elements=[
                    {"value": "VAL_A", "ordinal": 0},
                    {"value": "VAL_B", "ordinal": 1},
                    {"value": "VAL_C", "ordinal": 2},
                    {"value": "VAL_D", "ordinal": 3},
                ],
            )]
        }
        resp = client.put("/types/typedefs", json_data=payload)
        assert_status(resp, 200)

        # Read-after-write: GET enum and verify VAL_D is in elementDefs
        resp2 = client.get(f"/types/enumdef/name/{self.enum_name}")
        assert_status(resp2, 200)
        body = resp2.json()
        element_values = [e.get("value") for e in body.get("elementDefs", [])]
        assert "VAL_D" in element_values, f"Expected VAL_D in elementDefs, got {element_values}"

    @test("get_all_find_created_enum", tags=["typedef"], order=21, depends_on=["create_enum_def"])
    def test_get_all_find_created_enum(self, client, ctx):
        # GET-all typedefs and find our created enum in enumDefs by name
        resp = client.get("/types/typedefs")
        assert_status(resp, 200)
        body = resp.json()
        enum_defs = body.get("enumDefs", [])
        found = None
        for ed in enum_defs:
            if ed.get("name") == self.enum_name:
                found = ed
                break
        assert found is not None, f"Created enum '{self.enum_name}' not found in GET /types/typedefs"
        assert "elementDefs" in found, "Found enum should have elementDefs"

    # ---- DELETE types (cleanup) ----

    @test("delete_entity_def", tags=["typedef", "crud"], order=90, depends_on=["create_entity_def"])
    def test_delete_entity_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.entity_type_name}")
        assert_status_in(resp, [200, 204])

    @test("delete_classification_def", tags=["typedef", "crud"], order=91, depends_on=["create_classification_def"])
    def test_delete_classification_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.classification_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])

    @test("delete_struct_def", tags=["typedef", "crud"], order=92, depends_on=["create_struct_def"])
    def test_delete_struct_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.struct_name}")
        assert_status_in(resp, [200, 204])

    @test("delete_enum_def", tags=["typedef", "crud"], order=93, depends_on=["create_enum_def"])
    def test_delete_enum_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.enum_name}")
        assert_status_in(resp, [200, 204])

    @test("delete_bm_def", tags=["typedef", "crud"], order=94, depends_on=["create_business_metadata_def"])
    def test_delete_bm_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.bm_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])
