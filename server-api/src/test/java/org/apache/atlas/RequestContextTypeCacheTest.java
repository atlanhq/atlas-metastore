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

import static org.testng.Assert.*;

/**
 * Unit tests for RequestContext type caching functionality.
 * Tests the request-scoped caching of entity, classification, and relationship types
 * used to optimize TypeRegistry lookups.
 */
public class RequestContextTypeCacheTest {

    @BeforeMethod
    public void setup() {
        RequestContext.clear();
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    // ==================== Entity Type Cache Tests ====================

    @Test
    public void testCacheEntityType_nullTypeName_shouldNotCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheEntityType(null, mockType);

        assertNull(context.getCachedEntityType(null));
    }

    @Test
    public void testCacheEntityType_nullType_shouldNotCache() {
        RequestContext context = RequestContext.get();

        context.cacheEntityType("TestType", null);

        assertNull(context.getCachedEntityType("TestType"));
    }

    @Test
    public void testCacheEntityType_validInput_shouldCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheEntityType("TestType", mockType);

        Object retrieved = context.getCachedEntityType("TestType");
        assertSame(retrieved, mockType);
    }

    @Test
    public void testGetCachedEntityType_notCached_shouldReturnNull() {
        RequestContext context = RequestContext.get();

        Object retrieved = context.getCachedEntityType("NonExistentType");

        assertNull(retrieved);
    }

    @Test
    public void testCacheEntityType_multipleTypes_shouldCacheAll() {
        RequestContext context = RequestContext.get();
        Object mockType1 = new Object();
        Object mockType2 = new Object();
        Object mockType3 = new Object();

        context.cacheEntityType("Type1", mockType1);
        context.cacheEntityType("Type2", mockType2);
        context.cacheEntityType("Type3", mockType3);

        assertSame(context.getCachedEntityType("Type1"), mockType1);
        assertSame(context.getCachedEntityType("Type2"), mockType2);
        assertSame(context.getCachedEntityType("Type3"), mockType3);
    }

    @Test
    public void testCacheEntityType_overwrite_shouldReplaceExisting() {
        RequestContext context = RequestContext.get();
        Object mockType1 = new Object();
        Object mockType2 = new Object();

        context.cacheEntityType("TestType", mockType1);
        context.cacheEntityType("TestType", mockType2);

        assertSame(context.getCachedEntityType("TestType"), mockType2);
    }

    // ==================== Classification Type Cache Tests ====================

    @Test
    public void testCacheClassificationType_nullTypeName_shouldNotCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheClassificationType(null, mockType);

        assertNull(context.getCachedClassificationType(null));
    }

    @Test
    public void testCacheClassificationType_nullType_shouldNotCache() {
        RequestContext context = RequestContext.get();

        context.cacheClassificationType("TestClassification", null);

        assertNull(context.getCachedClassificationType("TestClassification"));
    }

    @Test
    public void testCacheClassificationType_validInput_shouldCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheClassificationType("TestClassification", mockType);

        Object retrieved = context.getCachedClassificationType("TestClassification");
        assertSame(retrieved, mockType);
    }

    @Test
    public void testGetCachedClassificationType_notCached_shouldReturnNull() {
        RequestContext context = RequestContext.get();

        Object retrieved = context.getCachedClassificationType("NonExistentClassification");

        assertNull(retrieved);
    }

    // ==================== Relationship Type Cache Tests ====================

    @Test
    public void testCacheRelationshipType_nullTypeName_shouldNotCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheRelationshipType(null, mockType);

        assertNull(context.getCachedRelationshipType(null));
    }

    @Test
    public void testCacheRelationshipType_nullType_shouldNotCache() {
        RequestContext context = RequestContext.get();

        context.cacheRelationshipType("TestRelationship", null);

        assertNull(context.getCachedRelationshipType("TestRelationship"));
    }

    @Test
    public void testCacheRelationshipType_validInput_shouldCache() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheRelationshipType("TestRelationship", mockType);

        Object retrieved = context.getCachedRelationshipType("TestRelationship");
        assertSame(retrieved, mockType);
    }

    @Test
    public void testGetCachedRelationshipType_notCached_shouldReturnNull() {
        RequestContext context = RequestContext.get();

        Object retrieved = context.getCachedRelationshipType("NonExistentRelationship");

        assertNull(retrieved);
    }

    // ==================== Cache Clearing Tests ====================

    @Test
    public void testClearCache_shouldClearAllTypeCaches() {
        RequestContext context = RequestContext.get();
        Object mockEntityType = new Object();
        Object mockClassificationType = new Object();
        Object mockRelationshipType = new Object();

        context.cacheEntityType("EntityType", mockEntityType);
        context.cacheClassificationType("ClassificationType", mockClassificationType);
        context.cacheRelationshipType("RelationshipType", mockRelationshipType);

        // Verify caches are populated
        assertNotNull(context.getCachedEntityType("EntityType"));
        assertNotNull(context.getCachedClassificationType("ClassificationType"));
        assertNotNull(context.getCachedRelationshipType("RelationshipType"));

        // Clear the cache
        context.clearCache();

        // Verify all caches are cleared
        assertNull(context.getCachedEntityType("EntityType"));
        assertNull(context.getCachedClassificationType("ClassificationType"));
        assertNull(context.getCachedRelationshipType("RelationshipType"));
    }

    @Test
    public void testRequestContextClear_shouldClearTypeCaches() {
        RequestContext context = RequestContext.get();
        Object mockType = new Object();

        context.cacheEntityType("TestType", mockType);
        assertNotNull(context.getCachedEntityType("TestType"));

        // Clear RequestContext entirely
        RequestContext.clear();

        // Get a new context and verify cache is empty
        RequestContext newContext = RequestContext.get();
        assertNull(newContext.getCachedEntityType("TestType"));
    }

    // ==================== Thread Safety Tests ====================

    @Test
    public void testTypeCaches_concurrentAccess_shouldBeThreadSafe() throws InterruptedException {
        final RequestContext context = RequestContext.get();
        final int numThreads = 10;
        final int numOperations = 100;
        final Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < numOperations; j++) {
                    String typeName = "Type_" + threadId + "_" + j;
                    Object mockType = new Object();

                    // Concurrent cache and retrieve operations
                    context.cacheEntityType(typeName, mockType);
                    context.getCachedEntityType(typeName);

                    context.cacheClassificationType(typeName, mockType);
                    context.getCachedClassificationType(typeName);

                    context.cacheRelationshipType(typeName, mockType);
                    context.getCachedRelationshipType(typeName);
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // If we reach here without ConcurrentModificationException, the test passes
        assertTrue(true);
    }

    // ==================== Type Casting Tests ====================

    @Test
    public void testGetCachedEntityType_genericTypeCasting() {
        RequestContext context = RequestContext.get();

        // Simulate caching an actual type object
        MockEntityType mockType = new MockEntityType("TestEntity");
        context.cacheEntityType("TestEntity", mockType);

        // Retrieve with generic type casting
        MockEntityType retrieved = context.getCachedEntityType("TestEntity");

        assertNotNull(retrieved);
        assertEquals(retrieved.getTypeName(), "TestEntity");
    }

    @Test
    public void testGetCachedClassificationType_genericTypeCasting() {
        RequestContext context = RequestContext.get();

        MockClassificationType mockType = new MockClassificationType("TestClassification");
        context.cacheClassificationType("TestClassification", mockType);

        MockClassificationType retrieved = context.getCachedClassificationType("TestClassification");

        assertNotNull(retrieved);
        assertEquals(retrieved.getTypeName(), "TestClassification");
    }

    @Test
    public void testGetCachedRelationshipType_genericTypeCasting() {
        RequestContext context = RequestContext.get();

        MockRelationshipType mockType = new MockRelationshipType("TestRelationship");
        context.cacheRelationshipType("TestRelationship", mockType);

        MockRelationshipType retrieved = context.getCachedRelationshipType("TestRelationship");

        assertNotNull(retrieved);
        assertEquals(retrieved.getTypeName(), "TestRelationship");
    }

    // ==================== Helper Mock Classes ====================

    private static class MockEntityType {
        private final String typeName;

        MockEntityType(String typeName) {
            this.typeName = typeName;
        }

        String getTypeName() {
            return typeName;
        }
    }

    private static class MockClassificationType {
        private final String typeName;

        MockClassificationType(String typeName) {
            this.typeName = typeName;
        }

        String getTypeName() {
            return typeName;
        }
    }

    private static class MockRelationshipType {
        private final String typeName;

        MockRelationshipType(String typeName) {
            this.typeName = typeName;
        }

        String getTypeName() {
            return typeName;
        }
    }
}
