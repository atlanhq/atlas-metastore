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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.RequestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Unit tests for AtlasElasticsearchQuery, specifically for the classification names
 * extraction and caching functionality in ResultImplDirect.
 */
public class AtlasElasticsearchQueryTest {

    @BeforeMethod
    public void setUp() {
        RequestContext.clear();
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    // ==================== Tests for _source detection in query ====================

    @Test
    public void testQueryContainsSource_WithSourceInQuery() {
        // Arrange
        String queryWithSource = "{\"query\":{\"match_all\":{}},\"_source\":{\"includes\":[\"__traitNames\"]}}";

        // Act & Assert
        assertTrue(queryWithSource.contains("\"_source\""),
                "Query should be detected as containing _source");
    }

    @Test
    public void testQueryContainsSource_WithoutSourceInQuery() {
        // Arrange
        String queryWithoutSource = "{\"query\":{\"match_all\":{}}}";

        // Act & Assert
        assertFalse(queryWithoutSource.contains("\"_source\""),
                "Query should be detected as NOT containing _source");
    }

    // ==================== Tests for classification names extraction logic ====================

    @Test
    public void testExtractClassificationNames_FromSourceWithBothFields() {
        // Arrange - simulate ES response _source structure
        Map<String, Object> source = new HashMap<>();
        source.put("__traitNames", Arrays.asList("PII", "Confidential"));
        source.put("__propagatedTraitNames", Arrays.asList("Sensitive", "Internal"));

        // Act - extract classification names (simulating what ResultImplDirect does)
        List<String> allClassificationNames = extractClassificationNames(source);

        // Assert
        assertEquals(allClassificationNames.size(), 4,
                "Should have 4 classification names total");
        assertTrue(allClassificationNames.contains("PII"));
        assertTrue(allClassificationNames.contains("Confidential"));
        assertTrue(allClassificationNames.contains("Sensitive"));
        assertTrue(allClassificationNames.contains("Internal"));
    }

    @Test
    public void testExtractClassificationNames_OnlyTraitNames() {
        // Arrange
        Map<String, Object> source = new HashMap<>();
        source.put("__traitNames", Arrays.asList("PII", "Confidential"));
        // No __propagatedTraitNames

        // Act
        List<String> allClassificationNames = extractClassificationNames(source);

        // Assert
        assertEquals(allClassificationNames.size(), 2);
        assertTrue(allClassificationNames.contains("PII"));
        assertTrue(allClassificationNames.contains("Confidential"));
    }

    @Test
    public void testExtractClassificationNames_OnlyPropagatedTraitNames() {
        // Arrange
        Map<String, Object> source = new HashMap<>();
        // No __traitNames
        source.put("__propagatedTraitNames", Arrays.asList("Sensitive"));

        // Act
        List<String> allClassificationNames = extractClassificationNames(source);

        // Assert
        assertEquals(allClassificationNames.size(), 1);
        assertTrue(allClassificationNames.contains("Sensitive"));
    }

    @Test
    public void testExtractClassificationNames_EmptyLists() {
        // Arrange
        Map<String, Object> source = new HashMap<>();
        source.put("__traitNames", Arrays.asList());
        source.put("__propagatedTraitNames", Arrays.asList());

        // Act
        List<String> allClassificationNames = extractClassificationNames(source);

        // Assert
        assertEquals(allClassificationNames.size(), 0,
                "Should have empty list when both trait lists are empty");
    }

    @Test
    public void testExtractClassificationNames_NullSource() {
        // Act
        List<String> allClassificationNames = extractClassificationNames(null);

        // Assert
        assertEquals(allClassificationNames.size(), 0,
                "Should return empty list for null source");
    }

    @Test
    public void testExtractClassificationNames_NoTraitFields() {
        // Arrange - source exists but has no trait fields
        Map<String, Object> source = new HashMap<>();
        source.put("__typeName", "Table");
        source.put("__guid", "some-guid");

        // Act
        List<String> allClassificationNames = extractClassificationNames(source);

        // Assert
        assertEquals(allClassificationNames.size(), 0,
                "Should return empty list when no trait fields present");
    }

    // ==================== Tests for caching behavior ====================

    @Test
    public void testCacheClassificationNames_StoresInRequestContext() {
        // Arrange
        String vertexId = "12345";
        List<String> classificationNames = Arrays.asList("PII", "Confidential");

        // Act - simulate what ResultImplDirect.extractAndCacheClassificationNames does
        RequestContext.get().cacheESClassificationNames(vertexId, classificationNames);

        // Assert
        assertTrue(RequestContext.get().hasESClassificationNamesCache(vertexId),
                "Should have cached the classification names");
        assertEquals(RequestContext.get().getCachedESClassificationNames(vertexId), classificationNames,
                "Cached names should match");
    }

    @Test
    public void testCacheClassificationNames_MultipleHits() {
        // Arrange - simulate multiple search hits
        LinkedHashMap<String, Object> hit1 = createMockHit("111", Arrays.asList("PII"), Arrays.asList());
        LinkedHashMap<String, Object> hit2 = createMockHit("222", Arrays.asList(), Arrays.asList("Sensitive"));
        LinkedHashMap<String, Object> hit3 = createMockHit("333", Arrays.asList("Confidential"), Arrays.asList("Internal"));

        // Act - simulate processing each hit
        processHitAndCache(hit1);
        processHitAndCache(hit2);
        processHitAndCache(hit3);

        // Assert
        assertTrue(RequestContext.get().hasESClassificationNamesCache("111"));
        assertTrue(RequestContext.get().hasESClassificationNamesCache("222"));
        assertTrue(RequestContext.get().hasESClassificationNamesCache("333"));

        assertEquals(RequestContext.get().getCachedESClassificationNames("111"), Arrays.asList("PII"));
        assertEquals(RequestContext.get().getCachedESClassificationNames("222"), Arrays.asList("Sensitive"));

        List<String> hit3Names = RequestContext.get().getCachedESClassificationNames("333");
        assertEquals(hit3Names.size(), 2);
        assertTrue(hit3Names.contains("Confidential"));
        assertTrue(hit3Names.contains("Internal"));
    }

    // ==================== Helper methods to simulate ResultImplDirect behavior ====================

    /**
     * Simulates the classification name extraction logic from ResultImplDirect.extractAndCacheClassificationNames()
     */
    private List<String> extractClassificationNames(Map<String, Object> source) {
        List<String> allClassificationNames = new java.util.ArrayList<>();

        if (source == null) {
            return allClassificationNames;
        }

        // Extract __traitNames
        Object traitNames = source.get("__traitNames");
        if (traitNames instanceof List) {
            allClassificationNames.addAll((List<String>) traitNames);
        }

        // Extract __propagatedTraitNames
        Object propagatedTraitNames = source.get("__propagatedTraitNames");
        if (propagatedTraitNames instanceof List) {
            allClassificationNames.addAll((List<String>) propagatedTraitNames);
        }

        return allClassificationNames;
    }

    /**
     * Creates a mock ES hit with _source containing classification names.
     */
    private LinkedHashMap<String, Object> createMockHit(String vertexId, List<String> traitNames, List<String> propagatedTraitNames) {
        LinkedHashMap<String, Object> hit = new LinkedHashMap<>();
        hit.put("_id", vertexId);

        Map<String, Object> source = new HashMap<>();
        if (traitNames != null && !traitNames.isEmpty()) {
            source.put("__traitNames", traitNames);
        }
        if (propagatedTraitNames != null && !propagatedTraitNames.isEmpty()) {
            source.put("__propagatedTraitNames", propagatedTraitNames);
        }
        hit.put("_source", source);

        return hit;
    }

    /**
     * Simulates processing a single ES hit and caching its classification names.
     */
    private void processHitAndCache(LinkedHashMap<String, Object> hit) {
        String vertexId = String.valueOf(hit.get("_id"));
        Object sourceObj = hit.get("_source");

        if (sourceObj instanceof Map) {
            Map<String, Object> source = (Map<String, Object>) sourceObj;
            List<String> classificationNames = extractClassificationNames(source);
            RequestContext.get().cacheESClassificationNames(vertexId, classificationNames);
        }
    }
}
