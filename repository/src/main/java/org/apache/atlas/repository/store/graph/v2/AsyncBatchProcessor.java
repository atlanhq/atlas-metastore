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
import org.apache.atlas.async.AsyncExecutorService;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class for async batch processing of entities and search results.
 * Provides parallel processing with configurable batch sizes and timeout handling.
 */
public class AsyncBatchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncBatchProcessor.class);

    private static volatile AsyncExecutorService sharedExecutorService;

    /**
     * Initialize with the Spring-managed AsyncExecutorService.
     * Should be called once during application startup.
     */
    public static void init(AsyncExecutorService executorService) {
        sharedExecutorService = executorService;
    }

    /**
     * Check if async execution is enabled via dynamic config store.
     */
    public static boolean isAsyncEnabled() {
        try {
            return DynamicConfigStore.isAsyncExecutionEnabled();
        } catch (Exception e) {
            LOG.debug("Failed to evaluate async execution flag, defaulting to false", e);
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
     * Falls back to sequential processing if the shared executor is not initialized.
     */
    private static <T, R> List<R> processParallel(
            List<T> items,
            Function<T, R> processor,
            String metricName) {

        AsyncExecutorService executor = sharedExecutorService;
        if (executor == null) {
            LOG.warn("AsyncBatchProcessor: sharedExecutorService not initialized, falling back to sequential processing");
            return processSequential(items, processor);
        }

        // Capture contexts for propagation
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        RequestContext parentContext = RequestContext.get();
        RequestContext asyncContext = parentContext.copyForAsync();

        // Create futures for all items
        List<CompletableFuture<R>> futures = items.stream()
            .map(item -> executor.supplyAsync(() -> {
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
            }, metricName))
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
}
