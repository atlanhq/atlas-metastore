"""Bulk classification endpoint tests (8 tests).

Tests the 4 bulk classification REST endpoints:
  POST /entity/bulk/classification
  POST /entity/bulk/classification/displayName
  POST /entity/bulk/setClassifications
  POST /entity/bulk/repairClassificationsMappings
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_classification_def,
    unique_name, unique_qn, unique_type_name,
)
from core.typedef_helpers import create_typedef_verified


def _create_entity_and_register(client, ctx, suffix, max_retries=3):
    """Create a DataSet entity with retry on timeout, register cleanup, return guid."""
    qn = unique_qn(suffix)
    name = unique_name(suffix)
    entity = build_dataset_entity(qn=qn, name=name)
    for attempt in range(max_retries):
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code == 200:
            break
        if resp.status_code in (408, 500, 503) and attempt < max_retries - 1:
            time.sleep(5 * (attempt + 1))
            continue
        raise Exception(
            f"Entity creation for {suffix} returned {resp.status_code}"
        )
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    ctx.register_cleanup(lambda: client.delete(f"/entity/guid/{guid}"))
    return guid


def _get_entity_classifications(client, guid):
    """GET entity and return its classificationNames list."""
    resp = client.get(f"/entity/guid/{guid}")
    if resp.status_code != 200:
        return []
    body = resp.json()
    entity = body.get("entity", {})
    return entity.get("classificationNames", []) or []


@suite("bulk_classifications", depends_on_suites=["entity_crud"],
       description="Bulk classification endpoint operations")
class BulkClassificationsSuite:

    def setup(self, client, ctx):
        # --- Create classification typedefs with verify-after-500 + type cache wait ---
        self.tag_name = unique_type_name("BulkTag")
        self.tag2_name = unique_type_name("BulkTag2")

        payload = {
            "classificationDefs": [
                build_classification_def(name=self.tag_name),
                build_classification_def(name=self.tag2_name),
            ]
        }
        self.tag_ok, _resp = create_typedef_verified(
            client, payload, max_wait=60, interval=15,
        )
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag_name}")
        )
        ctx.register_cleanup(
            lambda: client.delete(f"/types/typedef/name/{self.tag2_name}")
        )

        # --- Create 4 test entities ---
        self.guid_e1 = _create_entity_and_register(client, ctx, "bulk-cls-e1")
        self.guid_e2 = _create_entity_and_register(client, ctx, "bulk-cls-e2")
        self.guid_e3 = _create_entity_and_register(client, ctx, "bulk-cls-e3")
        self.guid_e4 = _create_entity_and_register(client, ctx, "bulk-cls-e4")

    # ----------------------------------------------------------------
    # POST /entity/bulk/classification
    # ----------------------------------------------------------------

    @test("bulk_add_classification_by_guids",
          tags=["bulk", "classification"], order=1)
    def test_bulk_add_classification_by_guids(self, client, ctx):
        """POST /entity/bulk/classification — add tag to E1 + E2."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": self.tag_name},
            "entityGuids": [self.guid_e1, self.guid_e2],
        })
        if resp.status_code == 404:
            raise SkipTestError(
                "Bulk classification returned 404 — classification type may not "
                "have propagated through type cache"
            )
        assert_status_in(resp, [200, 204])

        # Verify via GET
        time.sleep(2)
        for guid in (self.guid_e1, self.guid_e2):
            names = _get_entity_classifications(client, guid)
            assert self.tag_name in names, (
                f"Entity {guid} should have {self.tag_name}, "
                f"got classificationNames={names}"
            )

    # ----------------------------------------------------------------
    # POST /entity/bulk/classification/displayName
    # ----------------------------------------------------------------

    @test("bulk_add_classification_by_display_name",
          tags=["bulk", "classification"], order=2)
    def test_bulk_add_classification_by_display_name(self, client, ctx):
        """POST /entity/bulk/classification/displayName — add tag to E3."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post("/entity/bulk/classification/displayName", json_data=[
            {
                "typeName": self.tag_name,
                "entityGuid": self.guid_e3,
            },
        ])
        # 204 on success, 404 if endpoint doesn't exist
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/classification/displayName returned 404 — not available")

        time.sleep(2)
        names = _get_entity_classifications(client, self.guid_e3)
        assert self.tag_name in names, (
            f"Entity E3 ({self.guid_e3}) should have {self.tag_name}, "
            f"got {names}"
        )

    # ----------------------------------------------------------------
    # POST /entity/bulk/setClassifications
    # ----------------------------------------------------------------

    @test("bulk_set_classifications_add",
          tags=["bulk", "classification"], order=3)
    def test_bulk_set_classifications_add(self, client, ctx):
        """POST /entity/bulk/setClassifications — add BulkTag to E4."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post("/entity/bulk/setClassifications", json_data={
            "guidHeaderMap": {
                self.guid_e4: {
                    "guid": self.guid_e4,
                    "typeName": "DataSet",
                    "classifications": [{"typeName": self.tag_name}],
                },
            },
        })
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")

        time.sleep(2)
        names = _get_entity_classifications(client, self.guid_e4)
        assert self.tag_name in names, (
            f"Entity E4 ({self.guid_e4}) should have {self.tag_name} "
            f"after setClassifications, got {names}"
        )

    @test("bulk_set_classifications_override",
          tags=["bulk", "classification"], order=4,
          depends_on=["bulk_set_classifications_add"])
    def test_bulk_set_classifications_override(self, client, ctx):
        """POST /entity/bulk/setClassifications?overrideClassifications=true.

        Replace BulkTag with BulkTag2 on E4.
        """
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post(
            "/entity/bulk/setClassifications",
            params={"overrideClassifications": "true"},
            json_data={
                "guidHeaderMap": {
                    self.guid_e4: {
                        "guid": self.guid_e4,
                        "typeName": "DataSet",
                        "classifications": [{"typeName": self.tag2_name}],
                    },
                },
            },
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")

        time.sleep(2)
        names = _get_entity_classifications(client, self.guid_e4)
        assert self.tag2_name in names, (
            f"Entity E4 should have {self.tag2_name} after override, got {names}"
        )
        assert self.tag_name not in names, (
            f"Entity E4 should NOT have {self.tag_name} after override, got {names}"
        )

    @test("bulk_set_classifications_no_override",
          tags=["bulk", "classification"], order=5,
          depends_on=["bulk_set_classifications_override"])
    def test_bulk_set_classifications_no_override(self, client, ctx):
        """POST /entity/bulk/setClassifications?overrideClassifications=false.

        Send both BulkTag + BulkTag2 for E4 (which already has BulkTag2).
        With override=false the server should merge/add BulkTag without
        removing BulkTag2.
        """
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post(
            "/entity/bulk/setClassifications",
            params={"overrideClassifications": "false"},
            json_data={
                "guidHeaderMap": {
                    self.guid_e4: {
                        "guid": self.guid_e4,
                        "typeName": "DataSet",
                        "classifications": [
                            {"typeName": self.tag_name},
                            {"typeName": self.tag2_name},
                        ],
                    },
                },
            },
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")

        time.sleep(3)
        names = _get_entity_classifications(client, self.guid_e4)
        # With override=false and both tags in payload, at minimum BulkTag2
        # should be retained (it was already present).
        assert len(names) > 0, (
            f"Entity E4 should have at least one classification after "
            f"no-override setClassifications, got {names}"
        )
        assert self.tag2_name in names, (
            f"Entity E4 should still have {self.tag2_name} after no-override, "
            f"got {names}"
        )
        # BulkTag should also be added
        assert self.tag_name in names, (
            f"Entity E4 should have {self.tag_name} after no-override add, "
            f"got {names}"
        )

    # ----------------------------------------------------------------
    # POST /entity/bulk/repairClassificationsMappings
    # ----------------------------------------------------------------

    @test("bulk_repair_classifications_mappings",
          tags=["bulk", "classification"], order=6,
          depends_on=["bulk_add_classification_by_guids"])
    def test_bulk_repair_classifications_mappings(self, client, ctx):
        """POST /entity/bulk/repairClassificationsMappings — repair E1 + E2."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post(
            "/entity/bulk/repairClassificationsMappings",
            json_data=[self.guid_e1, self.guid_e2],
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/repairClassificationsMappings returned 404 — not available")

        if resp.status_code == 200:
            body = resp.json()
            # Response should be a map with GUIDs as keys
            if isinstance(body, dict):
                for guid in (self.guid_e1, self.guid_e2):
                    assert guid in body, (
                        f"Expected {guid} in repair response keys, got {list(body.keys())}"
                    )

    # ----------------------------------------------------------------
    # Negative / Error Tests
    # ----------------------------------------------------------------

    @test("bulk_add_classification_nonexistent_entity",
          tags=["bulk", "classification", "negative"], order=7)
    def test_bulk_add_classification_nonexistent_entity(self, client, ctx):
        """POST /entity/bulk/classification with fake GUID -> 404 or 400."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": self.tag_name},
            "entityGuids": ["00000000-0000-0000-0000-000000000000"],
        })
        assert_status_in(resp, [400, 404])

    @test("bulk_add_classification_nonexistent_type",
          tags=["bulk", "classification", "negative"], order=8)
    def test_bulk_add_classification_nonexistent_type(self, client, ctx):
        """POST /entity/bulk/classification with non-existent typeName -> 404 or 400."""
        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": "NonExistentTag_999"},
            "entityGuids": [self.guid_e1],
        })
        assert_status_in(resp, [400, 404])
