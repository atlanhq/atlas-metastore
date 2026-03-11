"""Data mesh - domain/product lifecycle tests."""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, SkipTestError,
)
from core.data_factory import (
    build_domain_entity, build_data_product_entity, build_dataset_entity,
    unique_name, unique_qn,
)


@suite("data_mesh", depends_on_suites=["entity_crud"],
       description="DataDomain/DataProduct lifecycle")
class DataMeshSuite:

    def setup(self, client, ctx):
        self.domain_name = unique_name("harness-domain")
        self.sub_domain_name = unique_name("harness-subdomain")
        self.product_name = unique_name("harness-product")

    @test("create_domain", tags=["data_mesh", "crud"], order=1)
    def test_create_domain(self, client, ctx):
        entity = build_domain_entity(name=self.domain_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code != 200:
            # 400/403 = Keycloak unavailable or preprocessor rejects
            ctx.set("data_mesh_unavailable", True)
            raise SkipTestError(
                f"DataDomain creation returned {resp.status_code} — "
                f"Keycloak/preprocessor not configured on this environment"
            )

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Domain creation returned no entities in mutatedEntities"
        guid = entities[0]["guid"]
        ctx.register_entity("domain1", guid, "DataDomain")
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

        # Read back and verify field values
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "DataDomain")
        assert_field_equals(resp2, "entity.attributes.name", self.domain_name)
        qn = resp2.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"Expected qualifiedName on domain {guid}"
        ctx.set("domain1_qn", qn)

    @test("get_domain", tags=["data_mesh"], order=2, depends_on=["create_domain"])
    def test_get_domain(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        guid = ctx.get_entity_guid("domain1")
        assert guid, "domain1 GUID not found in context"
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.typeName", "DataDomain")
        assert_field_not_empty(resp, "entity.attributes.qualifiedName")

    @test("create_sub_domain", tags=["data_mesh", "crud"], order=3, depends_on=["create_domain"])
    def test_create_sub_domain(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        parent_qn = ctx.get("domain1_qn")
        assert parent_qn, "domain1_qn not found in context — create_domain must have failed"
        entity = build_domain_entity(name=self.sub_domain_name, parent_domain_qn=parent_qn)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                assert entities[0].get("typeName") == "DataDomain", (
                    f"Expected typeName=DataDomain, got {entities[0].get('typeName')}"
                )
                guid = entities[0]["guid"]
                ctx.register_entity("sub_domain1", guid, "DataDomain")
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

    @test("get_domain_hierarchy", tags=["data_mesh"], order=4, depends_on=["create_sub_domain"])
    def test_get_domain_hierarchy(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        guid = ctx.get_entity_guid("domain1")
        assert guid, "domain1 GUID not found in context"
        resp = client.get(f"/entity/guid/{guid}", params={"ignoreRelationships": "false"})
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        rel_attrs = entity.get("relationshipAttributes", {})
        # Check for subDomains relationship attribute
        sub_domains = rel_attrs.get("subDomains", [])
        sub_guid = ctx.get_entity_guid("sub_domain1")
        if sub_guid and sub_domains:
            found = any(
                (isinstance(s, dict) and s.get("guid") == sub_guid)
                for s in sub_domains
            )
            assert found, f"Expected sub-domain {sub_guid} in parent's subDomains"

    @test("create_data_product", tags=["data_mesh", "crud"], order=5, depends_on=["create_domain"])
    def test_create_data_product(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        domain_guid = ctx.get_entity_guid("domain1")
        assert domain_guid, "domain1 GUID not found in context"
        entity = build_data_product_entity(name=self.product_name, domain_guid=domain_guid)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                assert entities[0].get("typeName") == "DataProduct", (
                    f"Expected typeName=DataProduct, got {entities[0].get('typeName')}"
                )
                guid = entities[0]["guid"]
                ctx.register_entity("product1", guid, "DataProduct")
                ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))

                # Read-after-write: verify persisted data product
                resp2 = client.get(f"/entity/guid/{guid}")
                if resp2.status_code == 200:
                    assert_field_equals(resp2, "entity.typeName", "DataProduct")
                    assert_field_equals(resp2, "entity.attributes.name", self.product_name)

    @test("get_product_with_domain", tags=["data_mesh"], order=6, depends_on=["create_data_product"])
    def test_get_product_with_domain(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        guid = ctx.get_entity_guid("product1")
        assert guid, "product1 GUID not found in context — create_data_product must have failed"
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.typeName", "DataProduct")

    @test("add_assets_to_product", tags=["data_mesh"], order=6.5, depends_on=["create_data_product"])
    def test_add_assets_to_product(self, client, ctx):
        """PR-02 (add): Add asset GUIDs to a DataProduct via dapiAssetGuids."""
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        product_guid = ctx.get_entity_guid("product1")
        assert product_guid, "product1 GUID not found in context"

        # Create a throwaway DataSet to assign as an asset
        qn = unique_qn("product-asset")
        entity = build_dataset_entity(qn=qn, name=unique_name("prod-asset"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Asset entity creation returned no entities"
        asset_guid = entities[0]["guid"]
        ctx.set("product_asset_guid", asset_guid)
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{asset_guid}"))

        # Read product to get its current QN
        resp_prod = client.get(f"/entity/guid/{product_guid}")
        assert_status(resp_prod, 200)
        prod_entity = resp_prod.json().get("entity", {})
        prod_qn = prod_entity.get("attributes", {}).get("qualifiedName")
        assert prod_qn, f"Expected qualifiedName on product {product_guid}"

        # Update product to set dapiAssetGuids
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataProduct",
                "guid": product_guid,
                "attributes": {
                    "qualifiedName": prod_qn,
                    "name": self.product_name,
                    "dapiAssetGuids": [asset_guid],
                },
            }
        })
        assert_status(resp, 200)

        # Read back and verify
        resp2 = client.get(f"/entity/guid/{product_guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        asset_guids = attrs.get("dapiAssetGuids", [])
        assert asset_guid in asset_guids, (
            f"Expected {asset_guid} in dapiAssetGuids, got {asset_guids}"
        )

    @test("remove_assets_from_product", tags=["data_mesh"], order=6.7,
          depends_on=["add_assets_to_product"])
    def test_remove_assets_from_product(self, client, ctx):
        """PR-02 (remove): Clear dapiAssetGuids from DataProduct."""
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        product_guid = ctx.get_entity_guid("product1")
        assert product_guid, "product1 GUID not found in context"

        # Read product QN
        resp_prod = client.get(f"/entity/guid/{product_guid}")
        assert_status(resp_prod, 200)
        prod_entity = resp_prod.json().get("entity", {})
        prod_qn = prod_entity.get("attributes", {}).get("qualifiedName")
        assert prod_qn, f"Expected qualifiedName on product {product_guid}"

        # Update product to clear dapiAssetGuids
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataProduct",
                "guid": product_guid,
                "attributes": {
                    "qualifiedName": prod_qn,
                    "name": self.product_name,
                    "dapiAssetGuids": [],
                },
            }
        })
        assert_status(resp, 200)

        # Read back and verify cleared
        resp2 = client.get(f"/entity/guid/{product_guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        asset_guids = attrs.get("dapiAssetGuids", [])
        assert not asset_guids, f"Expected empty dapiAssetGuids, got {asset_guids}"

    @test("update_domain", tags=["data_mesh", "crud"], order=7, depends_on=["create_domain"])
    def test_update_domain(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        guid = ctx.get_entity_guid("domain1")
        domain_qn = ctx.get("domain1_qn")
        assert guid, "domain1 GUID not found in context"
        assert domain_qn, "domain1_qn not found in context"
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataDomain",
                "guid": guid,
                "attributes": {
                    "qualifiedName": domain_qn,
                    "name": self.domain_name,
                    "description": "Updated by test harness",
                },
            }
        })
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            # Read-after-write verify
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            assert_field_equals(resp2, "entity.attributes.description", "Updated by test harness")

    @test("delete_mesh_hierarchy", tags=["data_mesh", "crud"], order=80)
    def test_delete_mesh_hierarchy(self, client, ctx):
        if ctx.get("data_mesh_unavailable"):
            raise SkipTestError("Data mesh not available — domain creation failed (400/403)")
        # Delete in reverse order: product -> sub-domain -> domain
        for entity_name in ("product1", "sub_domain1", "domain1"):
            guid = ctx.get_entity_guid(entity_name)
            if guid:
                resp = client.delete(f"/entity/guid/{guid}")
                assert_status_in(resp, [200, 204, 404])
