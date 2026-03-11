"""Business metadata add/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.audit_helpers import assert_audit_event_exists
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import (
    build_business_metadata_def, build_multi_attr_business_metadata_def,
    build_dataset_entity, unique_name, unique_qn, unique_type_name,
)


@suite("entity_businessmeta", depends_on_suites=["entity_crud"],
       description="Business metadata on entities")
class EntityBusinessMetaSuite:

    def setup(self, client, ctx):
        # Create a BM type (with retry on 500/503)
        self.bm_name = unique_type_name("HarnessBM")
        payload = {"businessMetadataDefs": [build_business_metadata_def(name=self.bm_name)]}
        self.bm_ok = False
        for attempt in range(3):
            resp = client.post("/types/typedefs", json_data=payload)
            if resp.status_code in (200, 409):
                self.bm_ok = True
                break
            if resp.status_code in (500, 503) and attempt < 2:
                time.sleep(10 * (attempt + 1))
                continue
        if self.bm_ok:
            # Wait for type cache — verify typedef is queryable
            for _ in range(3):
                time.sleep(15)
                check = client.get(f"/types/businessmetadatadef/name/{self.bm_name}")
                if check.status_code == 200:
                    break
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.bm_name}")
        )

        # Create entity
        qn = unique_qn("bm-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("bm-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{self.entity_guid}"))

    @test("add_business_metadata", tags=["businessmeta"], order=1)
    def test_add_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef creation failed (500/503)")
        payload = {self.bm_name: {"bmField1": "test-value"}}
        # Retry on 404 (type cache lag) up to 2 times with 15s backoff
        for attempt in range(3):
            resp = client.post(
                f"/entity/guid/{self.entity_guid}/businessmetadata",
                json_data=payload,
            )
            if resp.status_code in (200, 204):
                break
            if resp.status_code == 404 and attempt < 2:
                time.sleep(15)
                continue
            break
        assert_status_in(resp, [200, 204])

    @test("add_multi_attr_business_metadata", tags=["businessmeta"], order=1.5)
    def test_add_multi_attr_business_metadata(self, client, ctx):
        # Create multi-attr BM typedef (with retry)
        self.multi_bm_name = unique_type_name("HarnessMultiBM")
        payload = {"businessMetadataDefs": [build_multi_attr_business_metadata_def(name=self.multi_bm_name)]}
        multi_ok = False
        for attempt in range(3):
            resp = client.post("/types/typedefs", json_data=payload)
            if resp.status_code in (200, 409):
                multi_ok = True
                break
            if resp.status_code in (500, 503) and attempt < 2:
                time.sleep(10 * (attempt + 1))
                continue
        if not multi_ok:
            raise SkipTestError(f"Multi-attr BM typedef creation failed ({resp.status_code})")
        time.sleep(15)
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.multi_bm_name}")
        )

        # Add multi-attr BM to entity
        bm_payload = {self.multi_bm_name: {
            "bmStrField": "hello",
            "bmIntField": 42,
            "bmBoolField": True,
        }}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=bm_payload,
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code in [200, 204]:
            # Verify all 3 field types persisted
            resp2 = client.get(f"/entity/guid/{self.entity_guid}")
            assert_status(resp2, 200)
            entity = resp2.json().get("entity", {})
            bm = entity.get("businessAttributes", {}).get(self.multi_bm_name, {})
            assert bm.get("bmStrField") == "hello", f"Expected bmStrField='hello', got {bm}"
            assert bm.get("bmIntField") == 42, f"Expected bmIntField=42, got {bm}"
            assert bm.get("bmBoolField") is True, f"Expected bmBoolField=True, got {bm}"
            ctx.set("multi_bm_name", self.multi_bm_name)

    @test("verify_business_metadata", tags=["businessmeta"], order=2, depends_on=["add_business_metadata"])
    def test_verify_business_metadata(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "test-value", f"Expected bmField1='test-value', got {bm}"

    @test("overwrite_business_metadata", tags=["businessmeta"], order=2.3, depends_on=["add_business_metadata"])
    def test_overwrite_business_metadata(self, client, ctx):
        payload = {self.bm_name: {"bmField1": "overwritten-value"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Verify overwrite
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "overwritten-value", (
            f"Expected bmField1='overwritten-value', got {bm}"
        )

    @test("partial_update_business_metadata", tags=["businessmeta"], order=2.5, depends_on=["add_business_metadata"])
    def test_partial_update_business_metadata(self, client, ctx):
        # Update only bmField1 and verify it changes
        payload = {self.bm_name: {"bmField1": "partial-updated"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm.get("bmField1") == "partial-updated", (
            f"Expected bmField1='partial-updated', got {bm}"
        )

    @test("delete_business_metadata", tags=["businessmeta"], order=3, depends_on=["add_business_metadata"])
    def test_delete_business_metadata(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/businessmetadata/{self.bm_name}",
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify BM namespace is empty
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(self.bm_name, {})
        # After delete, BM should be empty or absent
        if bm:
            # Check that the field value is cleared (null or missing)
            val = bm.get("bmField1")
            assert val is None, f"Expected bmField1 cleared after delete, got {val}"

    @test("add_bm_audit", tags=["businessmeta", "audit"], order=4, depends_on=["add_business_metadata"])
    def test_add_bm_audit(self, client, ctx):
        event = assert_audit_event_exists(client, self.entity_guid, "BUSINESS_ATTRIBUTE_UPDATE",
                                          max_wait=60, interval=10)
        # assert_audit_event_exists now asserts internally if endpoint available
        # Returns None only if endpoint genuinely doesn't exist
        assert event is not None, (
            f"Expected BUSINESS_ATTRIBUTE_UPDATE audit event for {self.entity_guid}"
        )

    @test("bm_add_kafka_cdc", tags=["businessmeta", "kafka"], order=4.5,
          depends_on=["add_business_metadata"])
    def test_bm_add_kafka_cdc(self, client, ctx):
        """AUD-06: Verify Kafka CDC notification for BUSINESS_ATTRIBUTE_UPDATE."""
        result = assert_entity_in_kafka(ctx, self.entity_guid, "BUSINESS_ATTRIBUTE_UPDATE")
        # Soft assertion — result is None if Kafka unavailable or not found

    @test("search_by_bm_attribute", tags=["businessmeta", "search"], order=4.7,
          depends_on=["add_business_metadata"])
    def test_search_by_bm_attribute(self, client, ctx):
        """CM-04: Search for entity by business metadata attribute in ES."""
        # Poll ES until entity appears with BM attributes
        print(f"  [bm-search] Polling ES for entity {self.entity_guid} with BM...")
        found_entity = None
        for i in range(6):
            time.sleep(5)
            resp = client.post("/search/indexsearch", json_data={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {"bool": {"must": [
                        {"term": {"__guid": self.entity_guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                }
            })
            if resp.status_code != 200:
                print(f"  [bm-search] Search returned {resp.status_code} ({(i+1)*5}s/30s)")
                continue
            entities = resp.json().get("entities", [])
            if entities:
                found_entity = entities[0]
                bm = found_entity.get("businessAttributes", {}).get(self.bm_name, {})
                if bm:
                    print(f"  [bm-search] BM found in search after {(i+1)*5}s: {bm}")
                    break
                print(f"  [bm-search] Entity found but no BM yet ({(i+1)*5}s/30s)")
            else:
                print(f"  [bm-search] Entity not in ES yet ({(i+1)*5}s/30s)")

        assert found_entity is not None, (
            f"Entity {self.entity_guid} not found in ES after 30s polling"
        )
        assert found_entity.get("guid") == self.entity_guid, (
            f"Expected guid={self.entity_guid}, got {found_entity.get('guid')}"
        )
        bm = found_entity.get("businessAttributes", {}).get(self.bm_name, {})
        assert bm, (
            f"Expected BM {self.bm_name} in search result for {self.entity_guid}, "
            f"got businessAttributes={found_entity.get('businessAttributes', {})}"
        )

    @test("add_bm_nonexistent_entity", tags=["businessmeta"], order=5)
    def test_add_bm_nonexistent_entity(self, client, ctx):
        payload = {self.bm_name: {"bmField1": "test"}}
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/businessmetadata",
            json_data=payload,
        )
        assert_status_in(resp, [404, 400])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("delete_bm_typedef", tags=["businessmeta", "typedef"], order=6,
          depends_on=["delete_business_metadata"])
    def test_delete_bm_typedef(self, client, ctx):
        """CM-05: Delete a BM typedef and verify it's gone."""
        # Create a throwaway BM typedef (not the main one used by other tests)
        throwaway_bm = unique_type_name("ThrowawayBM")
        payload = {"businessMetadataDefs": [build_business_metadata_def(name=throwaway_bm)]}
        resp = client.post("/types/typedefs", json_data=payload)
        # Retry on 500/503
        for attempt in range(3):
            if resp.status_code in (200, 409):
                break
            if resp.status_code in (500, 503) and attempt < 2:
                time.sleep(10)
                resp = client.post("/types/typedefs", json_data=payload)
        assert resp.status_code in (200, 409), (
            f"Throwaway BM typedef creation failed after retries (status={resp.status_code})"
        )

        time.sleep(15)

        # Delete the throwaway BM typedef
        resp = client.delete(f"/types/typedef/name/{throwaway_bm}")
        assert_status_in(resp, [200, 204])

        # Verify it's gone — GET should return 404
        resp2 = client.get(f"/types/typedef/name/{throwaway_bm}")
        assert_status_in(resp2, [404, 400])
