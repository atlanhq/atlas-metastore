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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for EntityGraphRetriever type caching functionality.
 * Tests the request-scoped caching of TypeRegistry lookups to verify
 * that repeated type lookups within a request use cached values.
 */
public class EntityGraphRetrieverTypeCacheTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasEntityType mockEntityType;

    @Mock
    private AtlasClassificationType mockClassificationType;

    @Mock
    private AtlasRelationshipType mockRelationshipType;

    private EntityGraphRetriever retriever;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        retriever = new EntityGraphRetriever(graph, typeRegistry);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    // ==================== Entity Type Caching Tests ====================

    @Test
    public void testGetEntityTypeCached_firstCall_shouldCallTypeRegistry() throws Exception {
        String typeName = "TestEntityType";
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasEntityType result = invokeGetEntityTypeCached(typeName);

        assertSame(result, mockEntityType);
        verify(typeRegistry, times(1)).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetEntityTypeCached_secondCall_shouldUseCachedValue() throws Exception {
        String typeName = "TestEntityType";
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // First call - should call typeRegistry
        AtlasEntityType result1 = invokeGetEntityTypeCached(typeName);

        // Second call - should use cached value
        AtlasEntityType result2 = invokeGetEntityTypeCached(typeName);

        assertSame(result1, mockEntityType);
        assertSame(result2, mockEntityType);
        // TypeRegistry should only be called once
        verify(typeRegistry, times(1)).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetEntityTypeCached_multipleTypes_shouldCacheEach() throws Exception {
        String typeName1 = "EntityType1";
        String typeName2 = "EntityType2";
        AtlasEntityType mockType1 = mock(AtlasEntityType.class);
        AtlasEntityType mockType2 = mock(AtlasEntityType.class);

        when(typeRegistry.getEntityTypeByName(typeName1)).thenReturn(mockType1);
        when(typeRegistry.getEntityTypeByName(typeName2)).thenReturn(mockType2);

        // Call for each type twice
        AtlasEntityType result1a = invokeGetEntityTypeCached(typeName1);
        AtlasEntityType result1b = invokeGetEntityTypeCached(typeName1);
        AtlasEntityType result2a = invokeGetEntityTypeCached(typeName2);
        AtlasEntityType result2b = invokeGetEntityTypeCached(typeName2);

        assertSame(result1a, mockType1);
        assertSame(result1b, mockType1);
        assertSame(result2a, mockType2);
        assertSame(result2b, mockType2);

        // Each type should only trigger one typeRegistry call
        verify(typeRegistry, times(1)).getEntityTypeByName(typeName1);
        verify(typeRegistry, times(1)).getEntityTypeByName(typeName2);
    }

    @Test
    public void testGetEntityTypeCached_nullTypeName_shouldReturnNull() throws Exception {
        AtlasEntityType result = invokeGetEntityTypeCached(null);

        assertNull(result);
        verify(typeRegistry, never()).getEntityTypeByName(any());
    }

    @Test
    public void testGetEntityTypeCached_typeNotFound_shouldReturnNullAndNotCache() throws Exception {
        String typeName = "NonExistentType";
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(null);

        AtlasEntityType result1 = invokeGetEntityTypeCached(typeName);
        AtlasEntityType result2 = invokeGetEntityTypeCached(typeName);

        assertNull(result1);
        assertNull(result2);
        // Since null is not cached, typeRegistry should be called each time
        verify(typeRegistry, times(2)).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetEntityTypeCached_afterClearCache_shouldCallTypeRegistryAgain() throws Exception {
        String typeName = "TestEntityType";
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // First call
        invokeGetEntityTypeCached(typeName);

        // Clear cache
        RequestContext.get().clearCache();

        // Second call after cache clear
        invokeGetEntityTypeCached(typeName);

        // TypeRegistry should be called twice (once before clear, once after)
        verify(typeRegistry, times(2)).getEntityTypeByName(typeName);
    }

    // ==================== Classification Type Caching Tests ====================

    @Test
    public void testGetClassificationTypeCached_firstCall_shouldCallTypeRegistry() throws Exception {
        String typeName = "TestClassification";
        when(typeRegistry.getClassificationTypeByName(typeName)).thenReturn(mockClassificationType);

        AtlasClassificationType result = invokeGetClassificationTypeCached(typeName);

        assertSame(result, mockClassificationType);
        verify(typeRegistry, times(1)).getClassificationTypeByName(typeName);
    }

    @Test
    public void testGetClassificationTypeCached_secondCall_shouldUseCachedValue() throws Exception {
        String typeName = "TestClassification";
        when(typeRegistry.getClassificationTypeByName(typeName)).thenReturn(mockClassificationType);

        // First call
        AtlasClassificationType result1 = invokeGetClassificationTypeCached(typeName);

        // Second call - should use cached value
        AtlasClassificationType result2 = invokeGetClassificationTypeCached(typeName);

        assertSame(result1, mockClassificationType);
        assertSame(result2, mockClassificationType);
        verify(typeRegistry, times(1)).getClassificationTypeByName(typeName);
    }

    @Test
    public void testGetClassificationTypeCached_nullTypeName_shouldReturnNull() throws Exception {
        AtlasClassificationType result = invokeGetClassificationTypeCached(null);

        assertNull(result);
        verify(typeRegistry, never()).getClassificationTypeByName(any());
    }

    // ==================== Relationship Type Caching Tests ====================

    @Test
    public void testGetRelationshipTypeCached_firstCall_shouldCallTypeRegistry() throws Exception {
        String typeName = "TestRelationship";
        when(typeRegistry.getRelationshipTypeByName(typeName)).thenReturn(mockRelationshipType);

        AtlasRelationshipType result = invokeGetRelationshipTypeCached(typeName);

        assertSame(result, mockRelationshipType);
        verify(typeRegistry, times(1)).getRelationshipTypeByName(typeName);
    }

    @Test
    public void testGetRelationshipTypeCached_secondCall_shouldUseCachedValue() throws Exception {
        String typeName = "TestRelationship";
        when(typeRegistry.getRelationshipTypeByName(typeName)).thenReturn(mockRelationshipType);

        // First call
        AtlasRelationshipType result1 = invokeGetRelationshipTypeCached(typeName);

        // Second call - should use cached value
        AtlasRelationshipType result2 = invokeGetRelationshipTypeCached(typeName);

        assertSame(result1, mockRelationshipType);
        assertSame(result2, mockRelationshipType);
        verify(typeRegistry, times(1)).getRelationshipTypeByName(typeName);
    }

    @Test
    public void testGetRelationshipTypeCached_nullTypeName_shouldReturnNull() throws Exception {
        AtlasRelationshipType result = invokeGetRelationshipTypeCached(null);

        assertNull(result);
        verify(typeRegistry, never()).getRelationshipTypeByName(any());
    }

    // ==================== Cross-Request Isolation Tests ====================

    @Test
    public void testTypeCaching_differentRequests_shouldHaveSeparateCaches() throws Exception {
        String typeName = "TestEntityType";
        AtlasEntityType mockType1 = mock(AtlasEntityType.class);
        AtlasEntityType mockType2 = mock(AtlasEntityType.class);

        // First request
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockType1);
        AtlasEntityType result1 = invokeGetEntityTypeCached(typeName);
        assertSame(result1, mockType1);

        // Simulate new request
        RequestContext.clear();

        // Second request with different mock
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockType2);
        AtlasEntityType result2 = invokeGetEntityTypeCached(typeName);
        assertSame(result2, mockType2);

        // TypeRegistry should be called twice (once per request)
        verify(typeRegistry, times(2)).getEntityTypeByName(typeName);
    }

    // ==================== Performance Simulation Tests ====================

    @Test
    public void testTypeCaching_manyRepeatedCalls_shouldOnlyCallTypeRegistryOnce() throws Exception {
        String typeName = "FrequentlyUsedType";
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // Simulate 100 repeated calls (as might happen in bulk operations)
        for (int i = 0; i < 100; i++) {
            AtlasEntityType result = invokeGetEntityTypeCached(typeName);
            assertSame(result, mockEntityType);
        }

        // TypeRegistry should only be called once despite 100 lookups
        verify(typeRegistry, times(1)).getEntityTypeByName(typeName);
    }

    @Test
    public void testTypeCaching_mixedTypeLookups_shouldCacheIndependently() throws Exception {
        String entityTypeName = "TestEntity";
        String classificationTypeName = "TestClassification";
        String relationshipTypeName = "TestRelationship";

        when(typeRegistry.getEntityTypeByName(entityTypeName)).thenReturn(mockEntityType);
        when(typeRegistry.getClassificationTypeByName(classificationTypeName)).thenReturn(mockClassificationType);
        when(typeRegistry.getRelationshipTypeByName(relationshipTypeName)).thenReturn(mockRelationshipType);

        // Multiple calls for each type
        for (int i = 0; i < 10; i++) {
            invokeGetEntityTypeCached(entityTypeName);
            invokeGetClassificationTypeCached(classificationTypeName);
            invokeGetRelationshipTypeCached(relationshipTypeName);
        }

        // Each type registry method should be called only once
        verify(typeRegistry, times(1)).getEntityTypeByName(entityTypeName);
        verify(typeRegistry, times(1)).getClassificationTypeByName(classificationTypeName);
        verify(typeRegistry, times(1)).getRelationshipTypeByName(relationshipTypeName);
    }

    // ==================== Helper Methods for Reflection-based Testing ====================

    /**
     * Invokes the private getEntityTypeCached method using reflection.
     */
    private AtlasEntityType invokeGetEntityTypeCached(String typeName) throws Exception {
        Method method = EntityGraphRetriever.class.getDeclaredMethod("getEntityTypeCached", String.class);
        method.setAccessible(true);
        return (AtlasEntityType) method.invoke(retriever, typeName);
    }

    /**
     * Invokes the private getClassificationTypeCached method using reflection.
     */
    private AtlasClassificationType invokeGetClassificationTypeCached(String typeName) throws Exception {
        Method method = EntityGraphRetriever.class.getDeclaredMethod("getClassificationTypeCached", String.class);
        method.setAccessible(true);
        return (AtlasClassificationType) method.invoke(retriever, typeName);
    }

    /**
     * Invokes the private getRelationshipTypeCached method using reflection.
     */
    private AtlasRelationshipType invokeGetRelationshipTypeCached(String typeName) throws Exception {
        Method method = EntityGraphRetriever.class.getDeclaredMethod("getRelationshipTypeCached", String.class);
        method.setAccessible(true);
        return (AtlasRelationshipType) method.invoke(retriever, typeName);
    }
}
