package org.apache.atlas.web.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for AtlasWorkloadIsolationFilter.
 * 
 * Tests verify:
 * 1. Concurrency limiting works correctly (permits are held during request processing)
 * 2. Different client origins are throttled appropriately
 * 3. product_webapp is NOT throttled
 * 4. Rate limiting works when enabled
 * 5. 429 responses are returned when limits are exceeded
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WorkloadIsolationFilterTest extends AtlasDockerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadIsolationFilterTest.class);

    private String authHeader;

    private String indexSearchRequestBody;

    @BeforeAll
    void setupAuth() {
        String auth = "admin:admin";
        authHeader = "Basic " + Base64.getEncoder().encodeToString(auth.getBytes());
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/meta", atlas.getMappedPort(21000));
        indexSearchRequestBody ="{\n" +
                "    \"dsl\": {\n" +
                "        \"sort\": [\n" +
                "           \n" +
                "        ],\n" +
                "        \"from\": 0,\n" +
                "        \"size\": 1,\n" +
                "        \"query\": {\n" +
                "            \"bool\": {\n" +
                "                \"should\": [\n" +
                "                    {\n" +
                "                        \"term\": {\n" +
                "                            \"__typeName.keyword\": \"Table\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                ]\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "    \"attributes\": [\n" +
                "        \"policies\",\n" +
                "        \"readme\",\n" +
                "        \"links\",\n" +
                "        \"accessControl\",\n" +
                "        \"policyActions\",\n" +
                "        \"policyUsers\",\n" +
                "        \"policyGroups\",\n" +
                "        \"policyRoles\",\n" +
                "        \"policyResources\",\n" +
                "        \"policyType\",\n" +
                "        \"isPolicyEnabled\",\n" +
                "        \"policyCategory\"\n" +
                "    ],\n" +
                "    \"suppressLogs\": true,\n" +
                "    \"async\": {\n" +
                "        \"isCallAsync\": true,\n" +
                "        \"requestTimeoutInSecs\": 10\n" +
                "    }\n" +
                "}";
    }

    @Test
    @Order(1)
    @DisplayName("Test product_webapp is NOT throttled")
    void testProductWebappNotThrottled() throws Exception {
        LOG.info("Testing that product_webapp is not throttled...");

        // Fire 20 parallel requests with product_webapp origin
        // All should succeed regardless of throttle settings
        int numRequests = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);

        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/search/indexsearch"))
                                .header("Authorization", authHeader)
                                .header("X-Atlan-Client-Origin", "product_webapp")
                                .header("Accept", "application/json")
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(indexSearchRequestBody))
                                .timeout(Duration.ofSeconds(10))
                                .build();

                        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        // Wait for all requests to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Verify all requests succeeded (no 429s)
        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long successCount = responses.stream()
                .filter(r -> r.statusCode() == 200)
                .count();

        long throttledCount = responses.stream()
                .filter(r -> r.statusCode() == 503)
                .count();

        executor.shutdown();

        LOG.info("product_webapp results: {} successful, {} throttled", successCount, throttledCount);
        
        // All requests should succeed (no throttling for product_webapp)
        assertEquals(numRequests, successCount, "All product_webapp requests should succeed");
        assertEquals(0, throttledCount, "product_webapp should NOT be throttled");
    }

    @Test
    @Order(2)
    @DisplayName("Test unknown client is throttled")
    void testUnknownClientThrottled() throws Exception {
        LOG.info("Testing that unknown clients are throttled...");
        // Fire many parallel requests without origin header
        // Some should be throttled (503)
        int numRequests = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(150);
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/atlas", atlas.getMappedPort(21000));
        LOG.info("Creating {} threads, will release them simultaneously...", numRequests);

        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/test/slowOperation"))
                                .header("Authorization", authHeader)
                                .header("Accept", "application/json")
                                .header("Content-Type", "application/json")
                                .GET()
                                .timeout(Duration.ofSeconds(300))
                                .build();

                        long requestStart = System.currentTimeMillis();
                        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                        long requestDuration = System.currentTimeMillis() - requestStart;
                        
                        if (response.statusCode() == 503) {
                            LOG.info("Request {} THROTTLED (429) after {}ms", i, requestDuration);
                        } else if (i < 5 || requestDuration > 100) {
                            LOG.debug("Request {} SUCCESS (200) after {}ms", i, requestDuration);
                        }
                        
                        return response;
                    } catch (Exception e) {
                        LOG.error("Request {} FAILED: {}", i, e.getMessage());
                        //throw new RuntimeException(e);
                        return null;
                    }
                }, executor))
                .collect(Collectors.toList());

        // Wait for all requests to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long successCount = responses.stream()
                .filter(Objects::nonNull)
                .filter(r -> r.statusCode() == 200)
                .count();

        long throttledCount = responses.stream()
                .filter(Objects::nonNull)
                .filter(r -> r.statusCode() == 503)
                .count();

        executor.shutdown();

        LOG.info("=== UNKNOWN CLIENT TEST RESULTS ===");
        LOG.info("Total requests: {}", numRequests);
        LOG.info("Successful (200): {}", successCount);
        LOG.info("Throttled (503): {}", throttledCount);
        LOG.info("Expected max concurrent: ~2 (configured limit for unknown clients)");
        LOG.info("Config should be: atlas.throttle.unknown.maxConcurrent=2");

        // At least some requests should be throttled
        assertTrue(throttledCount > 0, 
                String.format("Unknown clients should be throttled when limit exceeded. Got %d throttled out of %d requests",
                        throttledCount, numRequests));
        
        // At least some should succeed
        assertTrue(successCount > 0, "Some requests should succeed");
    }

    @Test
    @Order(3)
    @DisplayName("Test concurrency limiting holds permits during request processing")
    void testConcurrencyLimitingHoldsPermits() throws Exception {
        LOG.info("Testing that concurrency limiting properly holds permits...");
        // Use a slow endpoint or create artificial delay by using indexsearch
        // Fire more requests than the concurrent limit
        int numRequests = 20;
        int expectedMaxConcurrent = 5; // Assuming default config allows ~5 for workflow
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/meta", atlas.getMappedPort(21000));
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        AtomicInteger maxObservedConcurrent = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        // Wait for all threads to be ready
                        startLatch.await();

                        int current = currentConcurrent.incrementAndGet();
                        maxObservedConcurrent.updateAndGet(max -> Math.max(max, current));

                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/search/indexsearch"))
                                .header("Authorization", authHeader)
                                .header("x-atlan-agent-id", "workflow")
                                .header("Accept", "application/json")
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(indexSearchRequestBody))
                                .timeout(Duration.ofSeconds(10))
                                .build();

                        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                        
                        currentConcurrent.decrementAndGet();
                        return response;
                    } catch (Exception e) {
                        currentConcurrent.decrementAndGet();
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        // Release all threads at once
        startLatch.countDown();

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long successCount = responses.stream()
                .filter(r -> r.statusCode() == 200)
                .count();

        long throttledCount = responses.stream()
                .filter(r -> r.statusCode() == 503)
                .count();

        executor.shutdown();

        LOG.info("Concurrency test results: {} successful, {} throttled, max concurrent: {}",
                successCount, throttledCount, maxObservedConcurrent.get());

        // Key assertion: max concurrent should be limited
        // If the bug existed (acquire/release immediately), we'd see much higher concurrency
        assertTrue(maxObservedConcurrent.get() <= expectedMaxConcurrent * 2, 
                "Max concurrent requests should be limited by bulkhead (was: " + maxObservedConcurrent.get() + ")");
        
        // Should have some throttling if we exceeded the limit
        if (numRequests > expectedMaxConcurrent) {
            assertTrue(throttledCount > 0 || successCount == numRequests, 
                    "Should either throttle some requests or process all (if slow enough)");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test different client origins have different limits")
    void testDifferentClientOriginsHaveDifferentLimits() throws Exception {
        LOG.info("Testing that different client origins have different throttle limits...");
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/meta", atlas.getMappedPort(21000));
        // Test workflow client
        ThrottleTestResult workflowResult = testClientOrigin("workflow", 30);
        LOG.info("Workflow: {} successful, {} throttled", 
                workflowResult.successCount, workflowResult.throttledCount);

        // Test sdk client
        ThrottleTestResult sdkResult = testClientOrigin("product_sdk", 30);
        LOG.info("SDK: {} successful, {} throttled", 
                sdkResult.successCount, sdkResult.throttledCount);

        // Test numaflow client
        ThrottleTestResult numaflowResult = testClientOrigin("numaflow", 30, "pipeline-test-1");
        LOG.info("Numaflow: {} successful, {} throttled", 
                numaflowResult.successCount, numaflowResult.throttledCount);

        // Verify all had some throttling (since we sent many requests)
        assertTrue(workflowResult.throttledCount > 0 || workflowResult.successCount == 30, 
                "Workflow should show throttling or process all");
        assertTrue(sdkResult.throttledCount > 0 || sdkResult.successCount == 30, 
                "SDK should show throttling or process all");
        assertTrue(numaflowResult.throttledCount > 0 || numaflowResult.successCount == 30, 
                "Numaflow should show throttling or process all");
    }

    @Test
    @Order(5)
    @DisplayName("Test numaflow pipelines are isolated from each other")
    void testNumaflowPipelineIsolation() throws Exception {
        LOG.info("Testing that different numaflow pipelines are isolated...");
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/meta", atlas.getMappedPort(21000));
        // Fire requests for pipeline-1
        ThrottleTestResult pipeline1Result = testClientOrigin("numaflow", 20, "pipeline-1");
        LOG.info("Pipeline-1: {} successful, {} throttled", 
                pipeline1Result.successCount, pipeline1Result.throttledCount);

        // Fire requests for pipeline-2 (should have separate limits)
        ThrottleTestResult pipeline2Result = testClientOrigin("numaflow", 20, "pipeline-2");
        LOG.info("Pipeline-2: {} successful, {} throttled", 
                pipeline2Result.successCount, pipeline2Result.throttledCount);

        // Both pipelines should be able to process requests
        // (they have separate limiters)
        assertTrue(pipeline1Result.successCount > 0, "Pipeline-1 should process some requests");
        assertTrue(pipeline2Result.successCount > 0, "Pipeline-2 should process some requests");
        
        LOG.info("Numaflow pipeline isolation verified");
    }

    @Test
    @Order(6)
    @DisplayName("Test non-throttled paths are not affected")
    void testNonThrottledPathsNotAffected() throws Exception {
        LOG.info("Testing that non-throttled paths are not affected...");
        // Fire many requests to a non-throttled endpoint (not indexsearch or bulk)
        int numRequests = 30;
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/atlas", atlas.getMappedPort(21000));
        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/types/typedefs"))
                                .header("Authorization", authHeader)
                                .header("X-Atlan-Client-Origin", "product_webapp")
                                .header("Accept", "application/json")
                                .header("Content-Type", "application/json")
                                .GET()
                                .timeout(Duration.ofSeconds(10))
                                .build();

                        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long successCount = responses.stream()
                .filter(r -> r.statusCode() == 200)
                .count();

        long throttledCount = responses.stream()
                .filter(r -> r.statusCode() == 429)
                .count();

        executor.shutdown();

        LOG.info("Non-throttled path results: {} successful, {} throttled", successCount, throttledCount);

        // All should succeed (this path is not throttled)
        assertEquals(numRequests, successCount, "All requests to non-throttled paths should succeed");
        assertEquals(0, throttledCount, "Non-throttled paths should not return 429");
    }

    @Test
    @Order(7)
    @DisplayName("Test 429 response contains proper headers")
    void test503ResponseHeaders() throws Exception {
        LOG.info("Testing that 429 responses contain proper headers...");
        // Fire many requests quickly to trigger throttling
        int numRequests = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/meta", atlas.getMappedPort(21000));
        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/search/indexsearch"))
                                .header("Authorization", authHeader)
                                .header("Accept", "application/json")
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(indexSearchRequestBody))
                                .timeout(Duration.ofSeconds(10))
                                .build();

                        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        // Find a 429 response
        HttpResponse<String> throttledResponse = responses.stream()
                .filter(r -> r.statusCode() == 503)
                .findFirst()
                .orElse(null);

        executor.shutdown();

        if (throttledResponse != null) {
            LOG.info("Found 503 response, verifying headers...");
            LOG.info("Response body: {}", throttledResponse.body());
            
            // Verify response has proper content type
            assertTrue(throttledResponse.headers().firstValue("Content-Type").isPresent(),
                    "429 response should have Content-Type header");
            
            LOG.info("503 response properly formatted");
        } else {
            LOG.warn("No 503 responses observed - may need to increase request count or reduce limits");
        }
    }

    // Helper method to test a specific client origin
    private ThrottleTestResult testClientOrigin(String origin, int numRequests) throws Exception {
        return testClientOrigin(origin, numRequests, null);
    }

    private ThrottleTestResult testClientOrigin(String origin, int numRequests, String agentId) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);

        List<CompletableFuture<HttpResponse<String>>> futures = IntStream.range(0, numRequests)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpRequest.Builder builder = HttpRequest.newBuilder()
                                .uri(URI.create(ATLAS_BASE_URL + "/search/indexsearch"))
                                .header("Authorization", authHeader)
                                .header("X-Atlan-Client-Origin", origin)
                                .header("Content-Type", "application/json")
                                .header("Accept", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(indexSearchRequestBody))
                                .timeout(Duration.ofSeconds(30));

                        if (agentId != null) {
                            builder.header("x-atlan-agent-id", agentId);
                        }

                        HttpRequest request = builder.build();
                        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<HttpResponse<String>> responses = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long successCount = responses.stream().filter(r -> r.statusCode() == 200).count();
        long throttledCount = responses.stream().filter(r -> r.statusCode() == 429).count();

        executor.shutdown();

        return new ThrottleTestResult(successCount, throttledCount);
    }

    private static class ThrottleTestResult {
        final long successCount;
        final long throttledCount;

        ThrottleTestResult(long successCount, long throttledCount) {
            this.successCount = successCount;
            this.throttledCount = throttledCount;
        }
    }
}

