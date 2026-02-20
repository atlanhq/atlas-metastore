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
package org.apache.atlas.service;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * Unit tests for FeatureFlagCacheStore.
 * Tests the two-tier caching system with primary (TTL) and fallback (no TTL) caches.
 */
@Test
public class FeatureFlagCacheStoreTest {

    private FeatureFlagCacheStore cacheStore;
    private FeatureFlagConfig mockConfig;

    @BeforeMethod
    public void setUp() {
        mockConfig = Mockito.mock(FeatureFlagConfig.class);
        when(mockConfig.getPrimaryCacheTtlMinutes()).thenReturn(30);
        cacheStore = new FeatureFlagCacheStore(mockConfig);
    }

    @Test
    public void testPutAndGetFromPrimaryCache() {
        cacheStore.putInBothCaches("testKey", "testValue");

        String value = cacheStore.getFromPrimaryCache("testKey");
        assertEquals(value, "testValue");
    }

    @Test
    public void testPutAndGetFromFallbackCache() {
        cacheStore.putInBothCaches("testKey", "testValue");

        String value = cacheStore.getFromFallbackCache("testKey");
        assertEquals(value, "testValue");
    }

    @Test
    public void testGetFromPrimaryCacheMiss() {
        String value = cacheStore.getFromPrimaryCache("nonexistent");
        assertNull(value);
    }

    @Test
    public void testGetFromFallbackCacheMiss() {
        String value = cacheStore.getFromFallbackCache("nonexistent");
        assertNull(value);
    }

    @Test
    public void testPutInFallbackCacheOnly() {
        cacheStore.putInFallbackCache("testKey", "testValue");

        // Should not be in primary cache
        assertNull(cacheStore.getFromPrimaryCache("testKey"));

        // Should be in fallback cache
        assertEquals(cacheStore.getFromFallbackCache("testKey"), "testValue");
    }

    @Test
    public void testRemoveFromBothCaches() {
        cacheStore.putInBothCaches("testKey", "testValue");

        // Verify both caches have the value
        assertEquals(cacheStore.getFromPrimaryCache("testKey"), "testValue");
        assertEquals(cacheStore.getFromFallbackCache("testKey"), "testValue");

        // Remove from both
        cacheStore.removeFromBothCaches("testKey");

        // Verify removal
        assertNull(cacheStore.getFromPrimaryCache("testKey"));
        assertNull(cacheStore.getFromFallbackCache("testKey"));
    }

    @Test
    public void testRemoveNonExistentKey() {
        // Should not throw exception
        cacheStore.removeFromBothCaches("nonexistent");
    }

    @Test
    public void testOverwriteValue() {
        cacheStore.putInBothCaches("testKey", "value1");
        assertEquals(cacheStore.getFromPrimaryCache("testKey"), "value1");

        cacheStore.putInBothCaches("testKey", "value2");
        assertEquals(cacheStore.getFromPrimaryCache("testKey"), "value2");
        assertEquals(cacheStore.getFromFallbackCache("testKey"), "value2");
    }

    @Test
    public void testMultipleKeys() {
        cacheStore.putInBothCaches("key1", "value1");
        cacheStore.putInBothCaches("key2", "value2");
        cacheStore.putInBothCaches("key3", "value3");

        assertEquals(cacheStore.getFromPrimaryCache("key1"), "value1");
        assertEquals(cacheStore.getFromPrimaryCache("key2"), "value2");
        assertEquals(cacheStore.getFromPrimaryCache("key3"), "value3");

        assertEquals(cacheStore.getFromFallbackCache("key1"), "value1");
        assertEquals(cacheStore.getFromFallbackCache("key2"), "value2");
        assertEquals(cacheStore.getFromFallbackCache("key3"), "value3");
    }

    @Test
    public void testNullValue() {
        cacheStore.putInBothCaches("testKey", null);

        // Null values should be cacheable
        assertNull(cacheStore.getFromPrimaryCache("testKey"));
        assertNull(cacheStore.getFromFallbackCache("testKey"));
    }

    @Test
    public void testEmptyStringValue() {
        cacheStore.putInBothCaches("testKey", "");

        assertEquals(cacheStore.getFromPrimaryCache("testKey"), "");
        assertEquals(cacheStore.getFromFallbackCache("testKey"), "");
    }

    @Test
    public void testFeatureFlagKeyPatterns() {
        // Test with actual feature flag key patterns
        cacheStore.putInBothCaches("ENABLE_JANUS_OPTIMISATION", "true");
        cacheStore.putInBothCaches("ENABLE_PERSONA_HIERARCHY_FILTER", "false");
        cacheStore.putInBothCaches("DISABLE_WRITE_FLAG", "false");

        assertEquals(cacheStore.getFromPrimaryCache("ENABLE_JANUS_OPTIMISATION"), "true");
        assertEquals(cacheStore.getFromPrimaryCache("ENABLE_PERSONA_HIERARCHY_FILTER"), "false");
        assertEquals(cacheStore.getFromPrimaryCache("DISABLE_WRITE_FLAG"), "false");
    }

    @Test
    public void testFallbackCacheAsInsurance() {
        // Simulate scenario where primary cache TTL expires but fallback retains value
        // Note: This is a conceptual test - actual TTL behavior is tested in SimpleCacheTest

        // Put value only in fallback (simulating primary cache miss after TTL)
        cacheStore.putInFallbackCache("testKey", "fallbackValue");

        // Primary miss
        assertNull(cacheStore.getFromPrimaryCache("testKey"));

        // Fallback hit
        assertEquals(cacheStore.getFromFallbackCache("testKey"), "fallbackValue");
    }

    @Test
    public void testCacheIndependence() {
        // Put different values in each cache
        cacheStore.putInBothCaches("key1", "bothValue");
        cacheStore.putInFallbackCache("key2", "fallbackOnly");

        // Remove from primary conceptually by invalidating and re-adding to fallback only
        cacheStore.removeFromBothCaches("key1");
        cacheStore.putInFallbackCache("key1", "newFallbackValue");

        // Primary should be null, fallback should have new value
        assertNull(cacheStore.getFromPrimaryCache("key1"));
        assertEquals(cacheStore.getFromFallbackCache("key1"), "newFallbackValue");
    }

    @Test
    public void testConfiguredTtl() {
        // Verify that config is used for TTL
        FeatureFlagConfig customConfig = Mockito.mock(FeatureFlagConfig.class);
        when(customConfig.getPrimaryCacheTtlMinutes()).thenReturn(60);

        FeatureFlagCacheStore customCacheStore = new FeatureFlagCacheStore(customConfig);

        // Just verify construction doesn't throw
        customCacheStore.putInBothCaches("testKey", "testValue");
        assertEquals(customCacheStore.getFromPrimaryCache("testKey"), "testValue");
    }
}
