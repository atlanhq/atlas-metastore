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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Unit tests for EntityGraphRetriever classification cache lookup logic.
 *
 * These tests verify the decision logic for when to use cached classification names
 * from Elasticsearch vs. fetching from Cassandra.
 *
 * The optimization conditions are:
 * 1. includeClassifications = false (don't need full classification objects)
 * 2. includeClassificationNames = true (need classification names)
 * 3. Cache has entry for the vertex ID
 *
 * When all conditions are met, classification names are retrieved from the ES cache
 * instead of making a Cassandra call.
 */
public class EntityGraphRetrieverClassificationCacheTest {

    @BeforeMethod
    public void setUp() {
        RequestContext.clear();
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    // ==================== Cache Lookup Decision Logic Tests ====================

    @Test
    public void testShouldUseCacheWhenAllConditionsMet() {
        // Arrange
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);  // Don't need full objects
        context.setIncludeClassificationNames(true); // Need names

        String vertexId = "12345";
        List<String> cachedNames = Arrays.asList("PII", "Confidential");
        context.cacheESClassificationNames(vertexId, cachedNames);

        // Act - simulate the decision logic from EntityGraphRetriever
        boolean includeClassifications = context.includeClassifications();
        boolean includeClassificationNames = context.isIncludeClassificationNames();
        boolean shouldUseCache = !includeClassifications
                                 && includeClassificationNames
                                 && context.hasESClassificationNamesCache(vertexId);

        // Assert
        assertTrue(shouldUseCache,
                "Should use cache when: includeClassifications=false, includeClassificationNames=true, and cache has entry");
    }

    @Test
    public void testShouldNotUseCacheWhenIncludeClassificationsTrue() {
        // Arrange - need full classification objects, so can't use cache
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(true);  // Need full objects
        context.setIncludeClassificationNames(true);

        String vertexId = "12345";
        context.cacheESClassificationNames(vertexId, Arrays.asList("PII"));

        // Act
        boolean includeClassifications = context.includeClassifications();
        boolean includeClassificationNames = context.isIncludeClassificationNames();
        boolean shouldUseCache = !includeClassifications
                                 && includeClassificationNames
                                 && context.hasESClassificationNamesCache(vertexId);

        // Assert
        assertFalse(shouldUseCache,
                "Should NOT use cache when includeClassifications=true (need full objects from Cassandra)");
    }

    @Test
    public void testShouldNotUseCacheWhenIncludeClassificationNamesFalse() {
        // Arrange - don't need classification names at all
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);
        context.setIncludeClassificationNames(false); // Don't need names

        String vertexId = "12345";
        context.cacheESClassificationNames(vertexId, Arrays.asList("PII"));

        // Act
        boolean includeClassifications = context.includeClassifications();
        boolean includeClassificationNames = context.isIncludeClassificationNames();
        boolean shouldUseCache = !includeClassifications
                                 && includeClassificationNames
                                 && context.hasESClassificationNamesCache(vertexId);

        // Assert
        assertFalse(shouldUseCache,
                "Should NOT use cache when includeClassificationNames=false");
    }

    @Test
    public void testShouldNotUseCacheWhenCacheMiss() {
        // Arrange - vertex not in cache
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);
        context.setIncludeClassificationNames(true);

        String vertexId = "12345";
        // NOT caching anything for this vertex

        // Act
        boolean includeClassifications = context.includeClassifications();
        boolean includeClassificationNames = context.isIncludeClassificationNames();
        boolean shouldUseCache = !includeClassifications
                                 && includeClassificationNames
                                 && context.hasESClassificationNamesCache(vertexId);

        // Assert
        assertFalse(shouldUseCache,
                "Should NOT use cache when vertex ID not in cache");
    }

    // ==================== Classification Names Retrieval Tests ====================

    @Test
    public void testRetrieveCachedClassificationNames() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> expectedNames = Arrays.asList("PII", "Confidential", "Internal");
        context.cacheESClassificationNames(vertexId, expectedNames);

        // Act
        List<String> retrievedNames = context.getCachedESClassificationNames(vertexId);

        // Assert
        assertEquals(retrievedNames, expectedNames,
                "Retrieved classification names should match cached names");
    }

    @Test
    public void testRetrieveCachedEmptyClassificationNames() {
        // Arrange - entity has no classifications
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> emptyNames = Collections.emptyList();
        context.cacheESClassificationNames(vertexId, emptyNames);

        // Act
        List<String> retrievedNames = context.getCachedESClassificationNames(vertexId);

        // Assert
        assertNotNull(retrievedNames, "Should return empty list, not null");
        assertTrue(retrievedNames.isEmpty(), "Should return empty list for entity with no classifications");
    }

    // ==================== Full Flow Simulation Tests ====================

    @Test
    public void testFullFlowWithCacheHit() {
        // This test simulates the complete flow:
        // 1. ES query returns results with _source containing __traitNames
        // 2. Classification names are cached in RequestContext
        // 3. EntityGraphRetriever checks cache and uses cached names

        // Step 1 & 2: Simulate ES response processing (what AtlasElasticsearchQuery does)
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);
        context.setIncludeClassificationNames(true);

        String vertexId1 = "111";
        String vertexId2 = "222";
        context.cacheESClassificationNames(vertexId1, Arrays.asList("PII"));
        context.cacheESClassificationNames(vertexId2, Arrays.asList("Confidential", "Sensitive"));

        // Step 3: Simulate EntityGraphRetriever decision for each vertex
        for (String vertexId : Arrays.asList(vertexId1, vertexId2)) {
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();

            List<String> classificationNames;
            boolean usedCache = false;

            if (!includeClassifications && includeClassificationNames && context.hasESClassificationNamesCache(vertexId)) {
                // Use cache - this is the optimized path
                classificationNames = context.getCachedESClassificationNames(vertexId);
                usedCache = true;
            } else {
                // Would call Cassandra in real code
                classificationNames = null; // Simulating that we would call Cassandra
            }

            // Assert
            assertTrue(usedCache, "Should have used cache for vertex " + vertexId);
            assertNotNull(classificationNames, "Should have retrieved classification names from cache");
        }
    }

    @Test
    public void testFullFlowWithCacheMiss() {
        // Test the fallback path when cache doesn't have the entry

        // Arrange
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);
        context.setIncludeClassificationNames(true);

        String cachedVertexId = "111";
        String uncachedVertexId = "222"; // This one won't be cached

        context.cacheESClassificationNames(cachedVertexId, Arrays.asList("PII"));
        // NOT caching uncachedVertexId

        // Act & Assert for cached vertex
        assertTrue(context.hasESClassificationNamesCache(cachedVertexId));

        // Act & Assert for uncached vertex
        assertFalse(context.hasESClassificationNamesCache(uncachedVertexId),
                "Cache should not have entry for uncached vertex");

        // In real code, EntityGraphRetriever would call Cassandra for uncachedVertexId
    }

    @Test
    public void testMultipleSearchesInSameRequest() {
        // Test that cache persists across multiple "searches" in the same request context

        // Arrange
        RequestContext context = RequestContext.get();
        context.setIncludeClassifications(false);
        context.setIncludeClassificationNames(true);

        // First search caches some vertices
        context.cacheESClassificationNames("v1", Arrays.asList("Tag1"));
        context.cacheESClassificationNames("v2", Arrays.asList("Tag2"));

        // Second search caches more vertices
        context.cacheESClassificationNames("v3", Arrays.asList("Tag3"));
        context.cacheESClassificationNames("v4", Arrays.asList("Tag4"));

        // Assert - all should be accessible
        assertTrue(context.hasESClassificationNamesCache("v1"));
        assertTrue(context.hasESClassificationNamesCache("v2"));
        assertTrue(context.hasESClassificationNamesCache("v3"));
        assertTrue(context.hasESClassificationNamesCache("v4"));

        assertEquals(context.getCachedESClassificationNames("v1"), Arrays.asList("Tag1"));
        assertEquals(context.getCachedESClassificationNames("v4"), Arrays.asList("Tag4"));
    }

    // ==================== Edge Cases ====================

    @Test
    public void testCacheWithDuplicateClassificationNames() {
        // Test handling of duplicate names (shouldn't happen in practice, but good to verify)
        RequestContext context = RequestContext.get();
        String vertexId = "12345";

        // If ES returns duplicates (from both __traitNames and __propagatedTraitNames)
        List<String> namesWithDuplicates = Arrays.asList("PII", "Confidential", "PII");
        context.cacheESClassificationNames(vertexId, namesWithDuplicates);

        List<String> retrieved = context.getCachedESClassificationNames(vertexId);

        // The cache should store exactly what was put in
        assertEquals(retrieved, namesWithDuplicates,
                "Cache should preserve the list as-is, including duplicates");
    }

    @Test
    public void testCacheWithSpecialCharactersInNames() {
        // Test that classification names with special characters are handled correctly
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> specialNames = Arrays.asList(
                "Classification With Spaces",
                "Classification-With-Dashes",
                "Classification_With_Underscores",
                "Classification.With.Dots"
        );
        context.cacheESClassificationNames(vertexId, specialNames);

        List<String> retrieved = context.getCachedESClassificationNames(vertexId);
        assertEquals(retrieved, specialNames,
                "Cache should preserve classification names with special characters");
    }
}
