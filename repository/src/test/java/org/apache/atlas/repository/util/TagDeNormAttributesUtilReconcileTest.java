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
package org.apache.atlas.repository.util;

import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link TagDeNormAttributesUtil#reconcileDenormAttributes}.
 *
 * <p>This method is the reconciliation-safe replacement for the incremental denorm
 * computation that was prone to bugs — most critically, the fallback in
 * {@code updateDenormAttributesForPropagatedTags} that re-introduced a deleted
 * propagated tag into ES when no other propagated tags remained on the vertex.</p>
 *
 * <h3>What these tests verify</h3>
 * <ul>
 *   <li>All 5 ES denorm attributes are always set (never partially populated)</li>
 *   <li>Direct vs propagated segregation uses {@code Tag.isPropagated()} from Cassandra</li>
 *   <li>The critical delete-propagation scenario produces correct empty propagated lists</li>
 *   <li>Mixed tag states (direct + propagated) are handled correctly</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TagDeNormAttributesUtilReconcileTest {

    private final AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
    private final IFullTextMapper fullTextMapper = mock(IFullTextMapper.class);

    @BeforeEach
    void setUpMocks() {
        when(typeRegistry.getClassificationTypeByName(anyString()))
                .thenReturn(mock(AtlasClassificationType.class));
    }

    // =========================================================================
    //  Empty / null input — all 5 attributes must be set to empty defaults
    // =========================================================================

    @Test
    void nullTagList_allAttributesEmpty() throws Exception {
        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                null, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, Collections.emptyList());
        assertPropagatedAttributes(attrs, Collections.emptyList());
        assertEquals("", attrs.get(CLASSIFICATION_TEXT_KEY));
    }

    @Test
    void emptyTagList_allAttributesEmpty() throws Exception {
        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                Collections.emptyList(), typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, Collections.emptyList());
        assertPropagatedAttributes(attrs, Collections.emptyList());
        assertEquals("", attrs.get(CLASSIFICATION_TEXT_KEY));
    }

    // =========================================================================
    //  Single tag scenarios
    // =========================================================================

    @Test
    void singleDirectTag_onlyDirectAttributesPopulated() throws Exception {
        List<Tag> tags = List.of(
                directTag("PII")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, List.of("PII"));
        assertPropagatedAttributes(attrs, Collections.emptyList());
        assertClassificationTextContains(attrs, "PII");
    }

    @Test
    void singlePropagatedTag_onlyPropagatedAttributesPopulated() throws Exception {
        List<Tag> tags = List.of(
                propagatedTag("Confidential", "source-vertex-1")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, Collections.emptyList());
        assertPropagatedAttributes(attrs, List.of("Confidential"));
        assertClassificationTextContains(attrs, "Confidential");
    }

    // =========================================================================
    //  THE CRITICAL BUG SCENARIO
    //
    //  A column has 1 direct tag ("PII") + 1 propagated tag ("Sensitive").
    //  The propagated tag is deleted from Cassandra by a task.
    //  After deletion, Cassandra returns only the direct tag.
    //
    //  The OLD incremental code (updateDenormAttributesForPropagatedTags) would
    //  fall into its else-branch and re-introduce "Sensitive" into ES because
    //  finalPropagatedTags was empty and the fallback used the deleted tag.
    //
    //  The reconciliation method must produce:
    //    __traitNames = ["PII"]
    //    __propagatedTraitNames = []     ← must be EMPTY, not ["Sensitive"]
    // =========================================================================

    @Test
    void directTagRemainsAfterPropagatedDeleted_propagatedAttributesMustBeEmpty() throws Exception {
        // After the propagated tag is deleted from Cassandra, only the direct tag remains
        List<Tag> tagsAfterDeletion = List.of(
                directTag("PII")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tagsAfterDeletion, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, List.of("PII"));

        // This is the critical assertion: propagated must be empty, not contain "Sensitive"
        assertPropagatedAttributes(attrs, Collections.emptyList());
    }

    // =========================================================================
    //  Mixed direct + propagated tags
    // =========================================================================

    @Test
    void mixedDirectAndPropagated_bothListsPopulatedCorrectly() throws Exception {
        List<Tag> tags = List.of(
                directTag("PII"),
                directTag("Internal"),
                propagatedTag("Confidential", "source-1"),
                propagatedTag("Sensitive", "source-2")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, List.of("PII", "Internal"));
        assertPropagatedAttributes(attrs, List.of("Confidential", "Sensitive"));
        assertClassificationTextContains(attrs, "PII", "Internal", "Confidential", "Sensitive");
    }

    @Test
    void multiplePropagatedFromDifferentSources_allPresent() throws Exception {
        List<Tag> tags = List.of(
                propagatedTag("TagA", "source-table-1"),
                propagatedTag("TagB", "source-table-2"),
                propagatedTag("TagC", "source-table-1")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        assertAllFiveKeysPresent(attrs);
        assertDirectAttributes(attrs, Collections.emptyList());
        assertPropagatedAttributes(attrs, List.of("TagA", "TagB", "TagC"));
    }

    // =========================================================================
    //  Delimited string format verification
    // =========================================================================

    @Test
    void classificationNamesKey_usesCorrectDelimitedFormat() throws Exception {
        // __classificationNames uses pipe-delimited format: |Tag1|Tag2|
        List<Tag> tags = List.of(
                directTag("PII"),
                directTag("Internal")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        String classificationNames = (String) attrs.get(CLASSIFICATION_NAMES_KEY);
        assertTrue(classificationNames.startsWith("|"), "Should start with delimiter");
        assertTrue(classificationNames.endsWith("|"), "Should end with delimiter");
        assertTrue(classificationNames.contains("|PII|"), "Should contain PII");
        assertTrue(classificationNames.contains("|Internal|"), "Should contain Internal");
    }

    @Test
    void propagatedClassificationNamesKey_usesCorrectDelimitedFormat() throws Exception {
        // __propagatedClassificationNames uses: |Tag1|Tag2 (leading delimiter only)
        List<Tag> tags = List.of(
                propagatedTag("Confidential", "s1"),
                propagatedTag("Sensitive", "s2")
        );

        Map<String, Object> attrs = TagDeNormAttributesUtil.reconcileDenormAttributes(
                tags, typeRegistry, fullTextMapper);

        String propClassificationNames = (String) attrs.get(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        assertTrue(propClassificationNames.startsWith("|"), "Should start with delimiter");
        assertTrue(propClassificationNames.contains("|Confidential"), "Should contain Confidential");
        assertTrue(propClassificationNames.contains("|Sensitive"), "Should contain Sensitive");
    }

    // =========================================================================
    //  Helpers: Tag construction
    // =========================================================================

    /**
     * Builds a Tag representing a direct (non-propagated) classification attachment.
     * The tagMetaJson contains the minimum fields needed for Jackson to convert
     * to an AtlasClassification (just the typeName).
     */
    private static Tag directTag(String typeName) {
        Tag tag = new Tag();
        tag.setVertexId("self-vertex");
        tag.setTagTypeName(typeName);
        tag.setPropagated(false);
        tag.setSourceVertexId("self-vertex");
        tag.setTagMetaJson(Map.of("typeName", typeName));
        return tag;
    }

    /**
     * Builds a Tag representing a propagated classification attachment from a different source.
     */
    private static Tag propagatedTag(String typeName, String sourceVertexId) {
        Tag tag = new Tag();
        tag.setVertexId("self-vertex");
        tag.setTagTypeName(typeName);
        tag.setPropagated(true);
        tag.setSourceVertexId(sourceVertexId);
        tag.setTagMetaJson(Map.of("typeName", typeName));
        return tag;
    }

    // =========================================================================
    //  Helpers: Assertions
    // =========================================================================

    /**
     * Verifies that all 5 denorm attribute keys are present in the result map.
     * A partially populated map is always a bug — the reconciliation method must
     * set all 5 attributes regardless of the tag state.
     */
    private static void assertAllFiveKeysPresent(Map<String, Object> attrs) {
        assertNotNull(attrs.get(TRAIT_NAMES_PROPERTY_KEY), "__traitNames must be present");
        assertNotNull(attrs.get(CLASSIFICATION_NAMES_KEY), "__classificationNames must be present");
        assertNotNull(attrs.get(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY), "__propagatedTraitNames must be present");
        assertNotNull(attrs.get(PROPAGATED_CLASSIFICATION_NAMES_KEY), "__propagatedClassificationNames must be present");
        assertTrue(attrs.containsKey(CLASSIFICATION_TEXT_KEY), "__classificationsText must be present");
    }

    /**
     * Asserts that the direct tag attributes contain exactly the expected type names.
     */
    @SuppressWarnings("unchecked")
    private static void assertDirectAttributes(Map<String, Object> attrs, List<String> expectedTypeNames) {
        List<String> traitNames = (List<String>) attrs.get(TRAIT_NAMES_PROPERTY_KEY);
        assertEquals(expectedTypeNames, traitNames, "__traitNames mismatch");

        String classificationNames = (String) attrs.get(CLASSIFICATION_NAMES_KEY);
        if (expectedTypeNames.isEmpty()) {
            assertEquals("", classificationNames, "__classificationNames should be empty when no direct tags");
        } else {
            for (String name : expectedTypeNames) {
                assertTrue(classificationNames.contains("|" + name + "|"),
                        "__classificationNames should contain " + name);
            }
        }
    }

    /**
     * Asserts that the propagated tag attributes contain exactly the expected type names.
     */
    @SuppressWarnings("unchecked")
    private static void assertPropagatedAttributes(Map<String, Object> attrs, List<String> expectedTypeNames) {
        List<String> propagatedTraitNames = (List<String>) attrs.get(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        assertEquals(expectedTypeNames, propagatedTraitNames, "__propagatedTraitNames mismatch");

        String propClassificationNames = (String) attrs.get(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        if (expectedTypeNames.isEmpty()) {
            assertEquals("", propClassificationNames,
                    "__propagatedClassificationNames should be empty when no propagated tags");
        } else {
            for (String name : expectedTypeNames) {
                assertTrue(propClassificationNames.contains("|" + name),
                        "__propagatedClassificationNames should contain " + name);
            }
        }
    }

    /**
     * Asserts that the classification text contains all expected type names.
     * The classification text is a space-delimited full-text search field that
     * includes both direct and propagated tag names.
     */
    private static void assertClassificationTextContains(Map<String, Object> attrs, String... expectedTypeNames) {
        String text = (String) attrs.get(CLASSIFICATION_TEXT_KEY);
        for (String name : expectedTypeNames) {
            assertTrue(text.contains(name),
                    "__classificationsText should contain " + name + ", actual: " + text);
        }
    }
}
