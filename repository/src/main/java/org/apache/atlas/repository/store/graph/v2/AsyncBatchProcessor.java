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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class for async batch processing of entities and search results.
 * Provides parallel processing with configurable batch sizes and timeout handling.
 */
public class AsyncBatchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncBatchProcessor.class);

    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int DEFAULT_MIN_FOR_PARALLEL = 5;
    private static final long DEFAULT_TIMEOUT_MS = 30000;

    private static final ExecutorService EXECUTOR;

    static {
        int poolSize = AtlasConfiguration.ASYNC_EXECUTOR_CORE_POOL_SIZE.getInt();
        EXECUTOR = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "atlas-batch-processor-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * Check if async execution is enabled.
     */
    public static boolean isAsyncEnabled() {
        try {
            return FeatureFlagStore.evaluate(FeatureFlag.ENABLE_ASYNC_EXECUTION.getKey(), "true");
        } catch (Exception e) {
            LOG.debug("Failed to evaluate async feature flag, defaulting to false", e);
            return false;
        }
    }

    /**
     * Process a list of items in parallel batches.
     *
     * @param items       List of items to process
     * @param processor   Function to process each item
     * @param metricName  Name for performance metrics
     * @param <T>         Input type
     * @param <R>         Result type
     * @return List of results in the same order as input
     */
    public static <T, R> List<R> processBatchParallel(
            List<T> items,
            Function<T, R> processor,
            String metricName) {

        if (items == null || items.isEmpty()) {
            return new ArrayList<>();
        }

        int minForParallel = AtlasConfiguration.ASYNC_MIN_ENTITIES_FOR_PARALLEL.getInt();

        // For small lists, process sequentially
        if (items.size() < minForParallel || !isAsyncEnabled()) {
            return processSequential(items, processor);
        }

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord(metricName + ".parallel");
        try {
            return processParallel(items, processor, metricName);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Process items in batches with parallel execution within each batch.
     *
     * @param items       List of items to process
     * @param processor   Function to process each item
     * @param batchSize   Size of each batch
     * @param metricName  Name for performance metrics
     * @param <T>         Input type
     * @param <R>         Result type
     * @return List of results in the same order as input
     */
    public static <T, R> List<R> processBatchesParallel(
            List<T> items,
            Function<T, R> processor,
            int batchSize,
            String metricName) {

        if (items == null || items.isEmpty()) {
            return new ArrayList<>();
        }

        if (!isAsyncEnabled()) {
            return processSequential(items, processor);
        }

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord(metricName + ".batched");
        try {
            List<R> results = new ArrayList<>(items.size());

            // Process in batches
            for (int i = 0; i < items.size(); i += batchSize) {
                int end = Math.min(i + batchSize, items.size());
                List<T> batch = items.subList(i, end);

                // Process batch in parallel
                List<R> batchResults = processParallel(batch, processor, metricName + ".batch");
                results.addAll(batchResults);
            }

            return results;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Process items sequentially.
     */
    private static <T, R> List<R> processSequential(List<T> items, Function<T, R> processor) {
        List<R> results = new ArrayList<>(items.size());
        for (T item : items) {
            try {
                R result = processor.apply(item);
                results.add(result);
            } catch (Exception e) {
                LOG.warn("Error processing item sequentially: {}", e.getMessage());
                results.add(null);
            }
        }
        return results;
    }

    /**
     * Process items in parallel using CompletableFuture.
     * Propagates both MDC and RequestContext to worker threads.
     */
    private static <T, R> List<R> processParallel(
            List<T> items,
            Function<T, R> processor,
            String metricName) {

        // Capture contexts for propagation
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        RequestContext parentContext = RequestContext.get();
        RequestContext asyncContext = parentContext.copyForAsync();

        // Create futures for all items
        List<CompletableFuture<R>> futures = items.stream()
            .map(item -> CompletableFuture.supplyAsync(() -> {
                // Propagate MDC context
                Map<String, String> previousMdc = MDC.getCopyOfContextMap();
                try {
                    if (mdcContext != null) {
                        MDC.setContextMap(mdcContext);
                    }
                    // Propagate RequestContext
                    RequestContext.setCurrentContext(asyncContext.copyForAsync());

                    return processor.apply(item);
                } catch (Exception e) {
                    LOG.warn("Error processing item in parallel: {}", e.getMessage());
                    return null;
                } finally {
                    // Clean up contexts
                    RequestContext.clear();
                    if (previousMdc != null) {
                        MDC.setContextMap(previousMdc);
                    } else {
                        MDC.clear();
                    }
                }
            }, EXECUTOR))
            .collect(Collectors.toList());

        // Wait for all futures with timeout
        long timeoutMs = AtlasConfiguration.ASYNC_EXECUTOR_DEFAULT_TIMEOUT_MS.getLong();
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );

        try {
            allFutures.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.warn("Timeout or error waiting for parallel processing in {}: {}", metricName, e.getMessage());
        }

        // Collect results in order
        List<R> results = new ArrayList<>(items.size());
        for (CompletableFuture<R> future : futures) {
            try {
                R result = future.getNow(null);
                results.add(result);
            } catch (Exception e) {
                results.add(null);
            }
        }

        return results;
    }

    /**
     * Partition a list into batches of specified size.
     *
     * @param list      List to partition
     * @param batchSize Size of each batch
     * @param <T>       Element type
     * @return List of batches
     */
    public static <T> List<List<T>> partition(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        if (list == null || list.isEmpty()) {
            return batches;
        }

        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(new ArrayList<>(list.subList(i, end)));
        }

        return batches;
    }

    /**
     * Get the configured batch size for entity processing.
     */
    public static int getEntityBatchSize() {
        return AtlasConfiguration.ASYNC_ENTITY_BATCH_SIZE.getInt();
    }

    /**
     * Get the configured batch size for header processing.
     */
    public static int getHeaderBatchSize() {
        return AtlasConfiguration.ASYNC_HEADER_BATCH_SIZE.getInt();
    }
}
