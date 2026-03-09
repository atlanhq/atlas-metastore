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

import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

/**
 * Unit tests for SimpleCache with TTL and jitter support.
 */
@Test
public class SimpleCacheTest {

    @Test
    public void testBasicPutAndGet() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L); // 60 second TTL

        cache.put("key1", "value1");
        cache.put("key2", "value2");

        assertEquals(cache.getIfPresent("key1"), "value1");
        assertEquals(cache.getIfPresent("key2"), "value2");
    }

    @Test
    public void testGetNonExistentKey() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L);

        assertNull(cache.getIfPresent("nonexistent"));
    }

    @Test
    public void testInvalidate() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        cache.invalidate("key1");
        assertNull(cache.getIfPresent("key1"));
    }

    @Test
    public void testInvalidateNonExistentKey() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L);

        // Should not throw exception
        cache.invalidate("nonexistent");
        assertNull(cache.getIfPresent("nonexistent"));
    }

    @Test
    public void testOverwriteValue() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        cache.put("key1", "value2");
        assertEquals(cache.getIfPresent("key1"), "value2");
    }

    @Test
    public void testTtlExpiration() throws InterruptedException {
        // Use a very short TTL (100ms) for testing
        SimpleCache<String, String> cache = new SimpleCache<>(100L);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        // Wait for TTL to expire (accounting for jitter, wait longer)
        Thread.sleep(200);

        assertNull(cache.getIfPresent("key1"), "Value should have expired");
    }

    @Test
    public void testNoTtlWithNullValue() {
        SimpleCache<String, String> cache = new SimpleCache<>(null);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        // Value should never expire
        assertEquals(cache.getIfPresent("key1"), "value1");
    }

    @Test
    public void testNoTtlWithZeroValue() {
        SimpleCache<String, String> cache = new SimpleCache<>(0L);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        // Value should never expire
        assertEquals(cache.getIfPresent("key1"), "value1");
    }

    @Test
    public void testNoTtlWithNegativeValue() {
        SimpleCache<String, String> cache = new SimpleCache<>(-100L);

        cache.put("key1", "value1");
        assertEquals(cache.getIfPresent("key1"), "value1");

        // Value should never expire
        assertEquals(cache.getIfPresent("key1"), "value1");
    }

    @Test
    public void testNullValues() {
        SimpleCache<String, String> cache = new SimpleCache<>(60000L);

        // Putting null value is allowed
        cache.put("key1", null);
        assertNull(cache.getIfPresent("key1"));
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        SimpleCache<String, Integer> cache = new SimpleCache<>(60000L);
        int threadCount = 10;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + threadId + "-" + i;
                        cache.put(key, i);
                        Integer value = cache.getIfPresent(key);
                        if (value != null && value.equals(i)) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Test timed out");
        executor.shutdown();

        // All operations should succeed
        assertEquals(successCount.get(), threadCount * operationsPerThread);
    }

    @Test
    public void testDifferentKeyTypes() {
        SimpleCache<Integer, String> cache = new SimpleCache<>(60000L);

        cache.put(1, "one");
        cache.put(2, "two");

        assertEquals(cache.getIfPresent(1), "one");
        assertEquals(cache.getIfPresent(2), "two");
    }

    @Test
    public void testJitterDistribution() throws InterruptedException {
        // Test that jitter spreads expirations
        // Use a 1000ms TTL with 20% jitter = +/- 200ms
        // Entries should expire between 800ms and 1200ms
        SimpleCache<String, String> cache = new SimpleCache<>(1000L);

        // Put many entries at the same time
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, "value" + i);
        }

        // After 600ms (before any should expire), all should still be present
        Thread.sleep(600);
        int presentAt600ms = 0;
        for (int i = 0; i < 100; i++) {
            if (cache.getIfPresent("key" + i) != null) {
                presentAt600ms++;
            }
        }
        assertEquals(presentAt600ms, 100, "All entries should still be present at 600ms");

        // After 1400ms (after all should expire), none should be present
        Thread.sleep(800);
        int presentAt1400ms = 0;
        for (int i = 0; i < 100; i++) {
            if (cache.getIfPresent("key" + i) != null) {
                presentAt1400ms++;
            }
        }
        assertEquals(presentAt1400ms, 0, "All entries should have expired by 1400ms");
    }

    @Test
    public void testLazyExpiration() throws InterruptedException {
        SimpleCache<String, String> cache = new SimpleCache<>(100L);

        cache.put("key1", "value1");
        cache.put("key2", "value2");

        // Wait for expiration
        Thread.sleep(200);

        // First access triggers lazy removal for key1
        assertNull(cache.getIfPresent("key1"));

        // key2 should also be expired on next access
        assertNull(cache.getIfPresent("key2"));
    }
}
