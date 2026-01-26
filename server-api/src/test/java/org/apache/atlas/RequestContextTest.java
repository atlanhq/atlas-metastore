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
package org.apache.atlas;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Unit tests for RequestContext, specifically for ES classification names caching functionality.
 */
public class RequestContextTest {

    @BeforeMethod
    public void setUp() {
        // Clear any existing context before each test
        RequestContext.clear();
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    // ==================== ES Classification Names Cache Tests ====================

    @Test
    public void testCacheESClassificationNames_BasicCaching() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> classificationNames = Arrays.asList("PII", "Confidential", "Internal");

        // Act
        context.cacheESClassificationNames(vertexId, classificationNames);

        // Assert
        assertTrue(context.hasESClassificationNamesCache(vertexId),
                "Cache should contain entry for vertexId");
        assertEquals(context.getCachedESClassificationNames(vertexId), classificationNames,
                "Cached classification names should match");
    }

    @Test
    public void testCacheESClassificationNames_EmptyList() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> emptyList = Collections.emptyList();

        // Act
        context.cacheESClassificationNames(vertexId, emptyList);

        // Assert
        assertTrue(context.hasESClassificationNamesCache(vertexId),
                "Cache should contain entry even for empty list");
        assertEquals(context.getCachedESClassificationNames(vertexId), emptyList,
                "Cached empty list should be returned");
    }

    @Test
    public void testCacheESClassificationNames_MultipleVertices() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId1 = "111";
        String vertexId2 = "222";
        String vertexId3 = "333";
        List<String> names1 = Arrays.asList("PII");
        List<String> names2 = Arrays.asList("Confidential", "Sensitive");
        List<String> names3 = Collections.emptyList();

        // Act
        context.cacheESClassificationNames(vertexId1, names1);
        context.cacheESClassificationNames(vertexId2, names2);
        context.cacheESClassificationNames(vertexId3, names3);

        // Assert
        assertTrue(context.hasESClassificationNamesCache(vertexId1));
        assertTrue(context.hasESClassificationNamesCache(vertexId2));
        assertTrue(context.hasESClassificationNamesCache(vertexId3));
        assertEquals(context.getCachedESClassificationNames(vertexId1), names1);
        assertEquals(context.getCachedESClassificationNames(vertexId2), names2);
        assertEquals(context.getCachedESClassificationNames(vertexId3), names3);
    }

    @Test
    public void testCacheESClassificationNames_NullVertexId() {
        // Arrange
        RequestContext context = RequestContext.get();
        List<String> classificationNames = Arrays.asList("PII");

        // Act - should not throw, just ignore
        context.cacheESClassificationNames(null, classificationNames);

        // Assert - null key should not be cached
        assertFalse(context.hasESClassificationNamesCache(null),
                "Null vertexId should not be cached");
    }

    @Test
    public void testCacheESClassificationNames_NullClassificationNames() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";

        // Act - should not throw, just ignore
        context.cacheESClassificationNames(vertexId, null);

        // Assert - entry should not be cached when names are null
        assertFalse(context.hasESClassificationNamesCache(vertexId),
                "Entry with null classification names should not be cached");
    }

    @Test
    public void testGetCachedESClassificationNames_NonExistentVertex() {
        // Arrange
        RequestContext context = RequestContext.get();
        String nonExistentVertexId = "99999";

        // Act
        List<String> result = context.getCachedESClassificationNames(nonExistentVertexId);

        // Assert
        assertNull(result, "Should return null for non-existent vertex");
    }

    @Test
    public void testHasESClassificationNamesCache_NonExistentVertex() {
        // Arrange
        RequestContext context = RequestContext.get();
        String nonExistentVertexId = "99999";

        // Act & Assert
        assertFalse(context.hasESClassificationNamesCache(nonExistentVertexId),
                "Should return false for non-existent vertex");
    }

    @Test
    public void testCacheESClassificationNames_OverwriteExisting() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> originalNames = Arrays.asList("PII");
        List<String> updatedNames = Arrays.asList("Confidential", "Internal");

        // Act
        context.cacheESClassificationNames(vertexId, originalNames);
        context.cacheESClassificationNames(vertexId, updatedNames);

        // Assert - should have the updated value
        assertEquals(context.getCachedESClassificationNames(vertexId), updatedNames,
                "Cache should contain updated classification names");
    }

    @Test
    public void testCacheClearedOnContextClear() {
        // Arrange
        RequestContext context = RequestContext.get();
        String vertexId = "12345";
        List<String> classificationNames = Arrays.asList("PII");
        context.cacheESClassificationNames(vertexId, classificationNames);

        // Verify it's cached
        assertTrue(context.hasESClassificationNamesCache(vertexId));

        // Act - clear the context
        RequestContext.clear();

        // Assert - get a new context and verify cache is empty
        RequestContext newContext = RequestContext.get();
        assertFalse(newContext.hasESClassificationNamesCache(vertexId),
                "Cache should be cleared after RequestContext.clear()");
    }

    // ==================== Integration with includeClassificationNames flag ====================

    @Test
    public void testIncludeClassificationNamesFlag() {
        // Arrange
        RequestContext context = RequestContext.get();

        // Assert default value
        assertFalse(context.isIncludeClassificationNames(),
                "Default value should be false");

        // Act
        context.setIncludeClassificationNames(true);

        // Assert
        assertTrue(context.isIncludeClassificationNames(),
                "Should be true after setting");

        // Act - reset
        context.setIncludeClassificationNames(false);

        // Assert
        assertFalse(context.isIncludeClassificationNames(),
                "Should be false after resetting");
    }

    @Test
    public void testCacheUsageWithIncludeClassificationNamesFlag() {
        // This test verifies the intended usage pattern:
        // 1. Set includeClassificationNames = true
        // 2. Cache classification names from ES
        // 3. Retrieve them later

        // Arrange
        RequestContext context = RequestContext.get();
        context.setIncludeClassificationNames(true);

        String vertexId = "12345";
        List<String> classificationNames = Arrays.asList("PII", "Confidential");

        // Act - simulate what AtlasElasticsearchQuery.ResultImplDirect does
        if (context.isIncludeClassificationNames()) {
            context.cacheESClassificationNames(vertexId, classificationNames);
        }

        // Assert - simulate what EntityGraphRetriever does
        if (context.isIncludeClassificationNames() && context.hasESClassificationNamesCache(vertexId)) {
            List<String> cachedNames = context.getCachedESClassificationNames(vertexId);
            assertEquals(cachedNames, classificationNames,
                    "Should retrieve the same classification names that were cached");
        } else {
            fail("Should have found cached classification names");
        }
    }
}
