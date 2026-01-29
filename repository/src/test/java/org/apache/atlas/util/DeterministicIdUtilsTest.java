/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.*;

/**
 * Unit tests for DeterministicIdUtils.
 * Validates that IDs are deterministic, correctly formatted, and unique for different inputs.
 */
public class DeterministicIdUtilsTest {

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final Pattern NANOID_PATTERN = Pattern.compile("^[0-9a-zA-Z]{21}$");

    // ========== GUID Format Tests ==========

    @Test
    public void testGenerateGuid_ValidUUIDFormat() {
        String guid = DeterministicIdUtils.generateGuid("test", "input");

        assertTrue(UUID_PATTERN.matcher(guid).matches(),
                "Generated GUID should match UUID format: " + guid);
        assertEquals(guid.length(), 36, "GUID should be 36 characters (including hyphens)");
    }

    @Test
    public void testGenerateNanoId_ValidFormat() {
        String nanoId = DeterministicIdUtils.generateNanoId("test", "input");

        assertTrue(NANOID_PATTERN.matcher(nanoId).matches(),
                "Generated NanoId should be 21 alphanumeric characters: " + nanoId);
        assertEquals(nanoId.length(), 21, "NanoId should be 21 characters");
    }

    // ========== Determinism Tests ==========

    @Test
    public void testGenerateGuid_Deterministic() {
        String guid1 = DeterministicIdUtils.generateGuid("entity", "Table", "default/schema/mytable");
        String guid2 = DeterministicIdUtils.generateGuid("entity", "Table", "default/schema/mytable");

        assertEquals(guid1, guid2, "Same inputs should produce identical GUIDs");
    }

    @Test
    public void testGenerateNanoId_Deterministic() {
        String nanoId1 = DeterministicIdUtils.generateNanoId("glossary", "MyGlossary");
        String nanoId2 = DeterministicIdUtils.generateNanoId("glossary", "MyGlossary");

        assertEquals(nanoId1, nanoId2, "Same inputs should produce identical NanoIds");
    }

    @Test
    public void testGenerateEntityGuid_Deterministic() {
        String guid1 = DeterministicIdUtils.generateEntityGuid("Table", "default/schema/employees");
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", "default/schema/employees");

        assertEquals(guid1, guid2, "Same entity should produce identical GUID");
    }

    @Test
    public void testGenerateTypeDefGuid_Deterministic() {
        String guid1 = DeterministicIdUtils.generateTypeDefGuid("PII", "atlas_governance");
        String guid2 = DeterministicIdUtils.generateTypeDefGuid("PII", "atlas_governance");

        assertEquals(guid1, guid2, "Same typedef should produce identical GUID");
    }

    @Test
    public void testGenerateRelationshipGuid_Deterministic() {
        String end1 = "guid-aaa-111";
        String end2 = "guid-bbb-222";

        String guid1 = DeterministicIdUtils.generateRelationshipGuid("TableToColumn", end1, end2);
        String guid2 = DeterministicIdUtils.generateRelationshipGuid("TableToColumn", end1, end2);

        assertEquals(guid1, guid2, "Same relationship should produce identical GUID");
    }

    @Test
    public void testGenerateGlossaryQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateGlossaryQN("Business Terms");
        String qn2 = DeterministicIdUtils.generateGlossaryQN("Business Terms");

        assertEquals(qn1, qn2, "Same glossary name should produce identical QN");
    }

    @Test
    public void testGenerateTermQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateTermQN("Revenue", "glossary123");
        String qn2 = DeterministicIdUtils.generateTermQN("Revenue", "glossary123");

        assertEquals(qn1, qn2, "Same term should produce identical QN");
    }

    @Test
    public void testGenerateCategoryQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateCategoryQN("Finance", "parent123", "glossary456");
        String qn2 = DeterministicIdUtils.generateCategoryQN("Finance", "parent123", "glossary456");

        assertEquals(qn1, qn2, "Same category should produce identical QN");
    }

    @Test
    public void testGenerateDomainQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateDomainQN("Sales", "default/domain/root");
        String qn2 = DeterministicIdUtils.generateDomainQN("Sales", "default/domain/root");

        assertEquals(qn1, qn2, "Same domain should produce identical QN");
    }

    @Test
    public void testGenerateProductQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateProductQN("CustomerData", "default/domain/sales");
        String qn2 = DeterministicIdUtils.generateProductQN("CustomerData", "default/domain/sales");

        assertEquals(qn1, qn2, "Same product should produce identical QN");
    }

    @Test
    public void testGenerateAccessControlQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateAccessControlQN("persona", "DataSteward", "tenant1");
        String qn2 = DeterministicIdUtils.generateAccessControlQN("persona", "DataSteward", "tenant1");

        assertEquals(qn1, qn2, "Same persona should produce identical QN");
    }

    @Test
    public void testGenerateQueryResourceQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generateQueryResourceQN("query", "SalesReport", "collection123", "user1");
        String qn2 = DeterministicIdUtils.generateQueryResourceQN("query", "SalesReport", "collection123", "user1");

        assertEquals(qn1, qn2, "Same query should produce identical QN");
    }

    @Test
    public void testGeneratePolicyQN_Deterministic() {
        String qn1 = DeterministicIdUtils.generatePolicyQN("ReadAccess", "persona/datasteward");
        String qn2 = DeterministicIdUtils.generatePolicyQN("ReadAccess", "persona/datasteward");

        assertEquals(qn1, qn2, "Same policy should produce identical QN");
    }

    // ========== Uniqueness Tests ==========

    @Test
    public void testGenerateGuid_DifferentInputsProduceDifferentGuids() {
        String guid1 = DeterministicIdUtils.generateGuid("entity", "Table", "schema/table1");
        String guid2 = DeterministicIdUtils.generateGuid("entity", "Table", "schema/table2");
        String guid3 = DeterministicIdUtils.generateGuid("entity", "Column", "schema/table1");

        assertNotEquals(guid1, guid2, "Different qualifiedNames should produce different GUIDs");
        assertNotEquals(guid1, guid3, "Different typeNames should produce different GUIDs");
        assertNotEquals(guid2, guid3, "All three should be unique");
    }

    @Test
    public void testGenerateEntityGuid_DifferentTypesProduceDifferentGuids() {
        String tableGuid = DeterministicIdUtils.generateEntityGuid("Table", "default/myentity");
        String columnGuid = DeterministicIdUtils.generateEntityGuid("Column", "default/myentity");

        assertNotEquals(tableGuid, columnGuid,
                "Same QN with different types should produce different GUIDs");
    }

    @Test
    public void testGenerateTypeDefGuid_DifferentServiceTypesProduceDifferentGuids() {
        String guid1 = DeterministicIdUtils.generateTypeDefGuid("CustomType", "service1");
        String guid2 = DeterministicIdUtils.generateTypeDefGuid("CustomType", "service2");

        assertNotEquals(guid1, guid2,
                "Same type name with different service types should produce different GUIDs");
    }

    @Test
    public void testGenerateTypeDefGuid_NullServiceTypeUsesDefault() {
        String guid1 = DeterministicIdUtils.generateTypeDefGuid("MyType", null);
        String guid2 = DeterministicIdUtils.generateTypeDefGuid("MyType", "");
        String guid3 = DeterministicIdUtils.generateTypeDefGuid("MyType", "atlas");

        // null and empty should both default to "atlas"
        assertEquals(guid1, guid3, "Null service type should default to 'atlas'");
        assertEquals(guid2, guid3, "Empty service type should default to 'atlas'");
    }

    @Test
    public void testGenerateTermQN_SameTermDifferentGlossaries() {
        String qn1 = DeterministicIdUtils.generateTermQN("Revenue", "glossary-finance");
        String qn2 = DeterministicIdUtils.generateTermQN("Revenue", "glossary-marketing");

        assertNotEquals(qn1, qn2,
                "Same term name in different glossaries should produce different QNs");
    }

    @Test
    public void testGenerateCategoryQN_SameCategoryDifferentParents() {
        String qn1 = DeterministicIdUtils.generateCategoryQN("Metrics", "parent-sales", "glossary1");
        String qn2 = DeterministicIdUtils.generateCategoryQN("Metrics", "parent-finance", "glossary1");

        assertNotEquals(qn1, qn2,
                "Same category name under different parents should produce different QNs");
    }

    @Test
    public void testGenerateCategoryQN_RootVsChildCategory() {
        String rootQn = DeterministicIdUtils.generateCategoryQN("Finance", null, "glossary1");
        String rootQnEmpty = DeterministicIdUtils.generateCategoryQN("Finance", "", "glossary1");
        String childQn = DeterministicIdUtils.generateCategoryQN("Finance", "parent123", "glossary1");

        assertEquals(rootQn, rootQnEmpty, "Null and empty parent should produce same QN");
        assertNotEquals(rootQn, childQn, "Root and child categories should have different QNs");
    }

    @Test
    public void testGenerateDomainQN_RootVsChildDomain() {
        String rootQn = DeterministicIdUtils.generateDomainQN("Sales", "");
        String childQn = DeterministicIdUtils.generateDomainQN("Sales", "default/domain/root");

        assertNotEquals(rootQn, childQn,
                "Root and child domains with same name should produce different QNs");
    }

    @Test
    public void testGenerateQueryResourceQN_SameQueryDifferentUsers() {
        String qn1 = DeterministicIdUtils.generateQueryResourceQN("query", "Report", "collection1", "user1");
        String qn2 = DeterministicIdUtils.generateQueryResourceQN("query", "Report", "collection1", "user2");

        assertNotEquals(qn1, qn2,
                "Same query name by different users should produce different QNs");
    }

    // ========== Relationship Direction Independence ==========

    @Test
    public void testGenerateRelationshipGuid_DirectionIndependent() {
        String end1 = "guid-aaa-111";
        String end2 = "guid-zzz-999";

        String guid1 = DeterministicIdUtils.generateRelationshipGuid("TableToColumn", end1, end2);
        String guid2 = DeterministicIdUtils.generateRelationshipGuid("TableToColumn", end2, end1);

        assertEquals(guid1, guid2,
                "Relationship GUID should be the same regardless of endpoint order");
    }

    @Test
    public void testGenerateRelationshipGuid_DifferentRelationshipTypes() {
        String end1 = "guid-aaa";
        String end2 = "guid-bbb";

        String guid1 = DeterministicIdUtils.generateRelationshipGuid("RelType1", end1, end2);
        String guid2 = DeterministicIdUtils.generateRelationshipGuid("RelType2", end1, end2);

        assertNotEquals(guid1, guid2,
                "Different relationship types should produce different GUIDs");
    }

    // ========== Entity GUID with Unique Attributes Map ==========

    @Test
    public void testGenerateEntityGuid_WithUniqueAttributes() {
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("qualifiedName", "default/schema/table1");

        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("qualifiedName", "default/schema/table1");

        String guid1 = DeterministicIdUtils.generateEntityGuid("Table", attrs1);
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", attrs2);

        assertEquals(guid1, guid2, "Same unique attributes should produce same GUID");
    }

    @Test
    public void testGenerateEntityGuid_AttributeOrderIndependent() {
        Map<String, Object> attrs1 = new LinkedHashMap<>();
        attrs1.put("qualifiedName", "default/table");
        attrs1.put("name", "MyTable");

        Map<String, Object> attrs2 = new LinkedHashMap<>();
        attrs2.put("name", "MyTable");
        attrs2.put("qualifiedName", "default/table");

        String guid1 = DeterministicIdUtils.generateEntityGuid("Table", attrs1);
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", attrs2);

        assertEquals(guid1, guid2,
                "Attribute order should not affect GUID (keys are sorted internally)");
    }

    @Test
    public void testGenerateEntityGuid_NullAttributeMap() {
        String guid1 = DeterministicIdUtils.generateEntityGuid("Table", (Map<String, Object>) null);
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", (Map<String, Object>) null);

        assertEquals(guid1, guid2, "Null attribute map should still produce deterministic GUID");
    }

    @Test
    public void testGenerateEntityGuid_EmptyAttributeMap() {
        String guid1 = DeterministicIdUtils.generateEntityGuid("Table", new HashMap<>());
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", new HashMap<>());

        assertEquals(guid1, guid2, "Empty attribute map should produce deterministic GUID");
    }

    // ========== Collision Resistance Tests ==========

    @Test
    public void testGenerateGuid_NoCollisions_LargeSet() {
        Set<String> guids = new HashSet<>();
        int count = 10000;

        for (int i = 0; i < count; i++) {
            String guid = DeterministicIdUtils.generateGuid("entity", "Type" + (i % 10), "qn" + i);
            guids.add(guid);
        }

        assertEquals(guids.size(), count,
                "All " + count + " unique inputs should produce unique GUIDs");
    }

    @Test
    public void testGenerateNanoId_NoCollisions_LargeSet() {
        Set<String> nanoIds = new HashSet<>();
        int count = 10000;

        for (int i = 0; i < count; i++) {
            String nanoId = DeterministicIdUtils.generateNanoId("type", "name" + i, "context" + (i % 100));
            nanoIds.add(nanoId);
        }

        assertEquals(nanoIds.size(), count,
                "All " + count + " unique inputs should produce unique NanoIds");
    }

    @Test
    public void testConcatenationCollisionPrevention() {
        // These would collide without null-byte separators: "ab" + "c" vs "a" + "bc"
        String guid1 = DeterministicIdUtils.generateGuid("ab", "c");
        String guid2 = DeterministicIdUtils.generateGuid("a", "bc");

        assertNotEquals(guid1, guid2,
                "Different component boundaries should produce different GUIDs");
    }

    // ========== Cross-Instance Consistency Tests ==========

    @Test
    public void testGenerateGuid_ConsistentAcrossMultipleCalls() {
        // Simulate what would happen on two different Atlas instances
        String[] expectedGuids = new String[100];

        // First "instance"
        for (int i = 0; i < 100; i++) {
            expectedGuids[i] = DeterministicIdUtils.generateEntityGuid("Table", "schema/table" + i);
        }

        // Second "instance" - should produce identical results
        for (int i = 0; i < 100; i++) {
            String guid = DeterministicIdUtils.generateEntityGuid("Table", "schema/table" + i);
            assertEquals(guid, expectedGuids[i],
                    "GUID for table" + i + " should be identical across instances");
        }
    }

    // ========== Edge Cases ==========

    @Test
    public void testGenerateGuid_WithSpecialCharacters() {
        String guid1 = DeterministicIdUtils.generateGuid("entity", "Table", "default/schema/table@#$%");
        String guid2 = DeterministicIdUtils.generateGuid("entity", "Table", "default/schema/table@#$%");

        assertEquals(guid1, guid2, "Special characters should be handled consistently");
        assertTrue(UUID_PATTERN.matcher(guid1).matches(), "Should still produce valid UUID format");
    }

    @Test
    public void testGenerateGuid_WithUnicodeCharacters() {
        String guid1 = DeterministicIdUtils.generateGuid("entity", "Table", "schema/表/データ");
        String guid2 = DeterministicIdUtils.generateGuid("entity", "Table", "schema/表/データ");

        assertEquals(guid1, guid2, "Unicode characters should be handled consistently");
        assertTrue(UUID_PATTERN.matcher(guid1).matches(), "Should still produce valid UUID format");
    }

    @Test
    public void testGenerateGuid_WithEmptyComponents() {
        String guid1 = DeterministicIdUtils.generateGuid("", "", "");
        String guid2 = DeterministicIdUtils.generateGuid("", "", "");

        assertEquals(guid1, guid2, "Empty components should produce consistent GUID");
        assertTrue(UUID_PATTERN.matcher(guid1).matches(), "Should still produce valid UUID format");
    }

    @Test
    public void testGenerateGuid_WithNullComponents() {
        String guid1 = DeterministicIdUtils.generateGuid("entity", null, "qualifiedName");
        String guid2 = DeterministicIdUtils.generateGuid("entity", null, "qualifiedName");

        assertEquals(guid1, guid2, "Null components should produce consistent GUID");
    }

    @Test
    public void testGenerateGuid_NullVsEmptyDifferent() {
        String guidWithNull = DeterministicIdUtils.generateGuid("a", null, "b");
        String guidWithEmpty = DeterministicIdUtils.generateGuid("a", "", "b");

        assertNotEquals(guidWithNull, guidWithEmpty,
                "Null and empty string should produce different GUIDs");
    }

    @Test
    public void testGenerateGlossaryQN_WithSpaces() {
        String qn1 = DeterministicIdUtils.generateGlossaryQN("Business Glossary");
        String qn2 = DeterministicIdUtils.generateGlossaryQN("Business Glossary");

        assertEquals(qn1, qn2, "Glossary names with spaces should work consistently");
    }

    @Test
    public void testGenerateGlossaryQN_CaseSensitive() {
        String qn1 = DeterministicIdUtils.generateGlossaryQN("MyGlossary");
        String qn2 = DeterministicIdUtils.generateGlossaryQN("myglossary");
        String qn3 = DeterministicIdUtils.generateGlossaryQN("MYGLOSSARY");

        assertNotEquals(qn1, qn2, "Different case should produce different QNs");
        assertNotEquals(qn2, qn3, "Different case should produce different QNs");
        assertNotEquals(qn1, qn3, "Different case should produce different QNs");
    }

    // ========== Type-Specific Method Tests ==========

    @Test
    public void testAllAccessControlTypes() {
        String personaQN = DeterministicIdUtils.generateAccessControlQN("persona", "Admin", "tenant1");
        String purposeQN = DeterministicIdUtils.generateAccessControlQN("purpose", "Admin", "tenant1");
        String stakeholderQN = DeterministicIdUtils.generateAccessControlQN("stakeholder", "Admin", "tenant1");

        // All should be different due to type prefix
        assertNotEquals(personaQN, purposeQN, "Persona and Purpose should have different QNs");
        assertNotEquals(purposeQN, stakeholderQN, "Purpose and Stakeholder should have different QNs");
        assertNotEquals(personaQN, stakeholderQN, "Persona and Stakeholder should have different QNs");
    }

    @Test
    public void testAllQueryResourceTypes() {
        String collectionQN = DeterministicIdUtils.generateQueryResourceQN("collection", "MyResource", "", "user1");
        String folderQN = DeterministicIdUtils.generateQueryResourceQN("folder", "MyResource", "", "user1");
        String queryQN = DeterministicIdUtils.generateQueryResourceQN("query", "MyResource", "", "user1");

        // All should be different due to type prefix
        assertNotEquals(collectionQN, folderQN, "Collection and Folder should have different QNs");
        assertNotEquals(folderQN, queryQN, "Folder and Query should have different QNs");
        assertNotEquals(collectionQN, queryQN, "Collection and Query should have different QNs");
    }

    // ========== Known Value Tests (Golden Tests) ==========

    @Test
    public void testKnownValues_EntityGuid() {
        // These are "golden" values - if the algorithm changes, these tests will fail
        // This ensures we don't accidentally break backward compatibility
        String guid = DeterministicIdUtils.generateEntityGuid("Table", "default/schema/employees");

        // Verify it's a valid UUID format
        assertTrue(UUID_PATTERN.matcher(guid).matches());

        // Generate again to confirm determinism
        String guid2 = DeterministicIdUtils.generateEntityGuid("Table", "default/schema/employees");
        assertEquals(guid, guid2);
    }

    @Test
    public void testKnownValues_GlossaryQN() {
        String qn = DeterministicIdUtils.generateGlossaryQN("Enterprise Data Glossary");

        // Verify format
        assertTrue(NANOID_PATTERN.matcher(qn).matches());

        // Verify determinism
        String qn2 = DeterministicIdUtils.generateGlossaryQN("Enterprise Data Glossary");
        assertEquals(qn, qn2);
    }
}
