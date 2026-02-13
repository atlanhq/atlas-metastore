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
package org.apache.atlas.async;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Shared async executor service for running blocking operations asynchronously.
 * Provides context propagation (MDC) and timeout handling.
 */
@Component
public class AsyncExecutorService {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutorService.class);

    // Configuration property names
    private static final String PROP_CORE_POOL_SIZE = "atlas.async.executor.core.pool.size";
    private static final String PROP_MAX_POOL_SIZE = "atlas.async.executor.max.pool.size";
    private static final String PROP_QUEUE_CAPACITY = "atlas.async.executor.queue.capacity";
    private static final String PROP_KEEP_ALIVE_SECONDS = "atlas.async.executor.keep.alive.seconds";
    private static final String PROP_DEFAULT_TIMEOUT_MS = "atlas.async.executor.default.timeout.ms";

    // Default values
    private static final int DEFAULT_CORE_POOL_SIZE = 10;
    private static final int DEFAULT_MAX_POOL_SIZE = 50;
    private static final int DEFAULT_QUEUE_CAPACITY = 200;
    private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60;
    private static final long DEFAULT_TIMEOUT_MS = 30000;

    private ExecutorService executor;
    private long defaultTimeoutMs;

    @PostConstruct
    public void init() {
        try {
            Configuration config = ApplicationProperties.get();
            int corePoolSize = config.getInt(PROP_CORE_POOL_SIZE, DEFAULT_CORE_POOL_SIZE);
            int maxPoolSize = config.getInt(PROP_MAX_POOL_SIZE, DEFAULT_MAX_POOL_SIZE);
            int queueCapacity = config.getInt(PROP_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY);
            long keepAliveSeconds = config.getLong(PROP_KEEP_ALIVE_SECONDS, DEFAULT_KEEP_ALIVE_SECONDS);
            defaultTimeoutMs = config.getLong(PROP_DEFAULT_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);

            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueCapacity);

            executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveSeconds, TimeUnit.SECONDS,
                queue,
                new AtlasAsyncThreadFactory("atlas-async"),
                new ThreadPoolExecutor.CallerRunsPolicy()  // Back-pressure: caller thread executes if queue full
            );

            LOG.info("AsyncExecutorService initialized: core={}, max={}, queue={}, keepAlive={}s, defaultTimeout={}ms",
                     corePoolSize, maxPoolSize, queueCapacity, keepAliveSeconds, defaultTimeoutMs);
        } catch (Exception e) {
            LOG.error("Failed to initialize AsyncExecutorService, using defaults", e);
            executor = new ThreadPoolExecutor(
                DEFAULT_CORE_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY),
                new AtlasAsyncThreadFactory("atlas-async"),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            defaultTimeoutMs = DEFAULT_TIMEOUT_MS;
        }
    }

    /**
     * Custom thread factory for creating daemon threads with a specific name format.
     */
    private static class AtlasAsyncThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        AtlasAsyncThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + "-" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    /**
     * Execute a supplier asynchronously with MDC context propagation.
     *
     * NOTE: RequestContext propagation must be handled by the caller if needed,
     * as RequestContext is in server-api module which common module cannot depend on.
     * The caller should use RequestContext.copyForAsync() and RequestContext.setCurrentContext()
     * within their supplier if RequestContext access is needed.
     *
     * @param supplier The blocking operation to execute
     * @param opName   Operation name for logging/metrics
     * @return CompletableFuture with the result
     */
    public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, String opName) {
        // Capture MDC context from calling thread
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();

        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            Map<String, String> previousMdc = MDC.getCopyOfContextMap();
            try {
                // Propagate MDC context
                if (mdcContext != null) {
                    MDC.setContextMap(mdcContext);
                } else {
                    MDC.clear();
                }

                return supplier.get();
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("AsyncExecutorService.supplyAsync[{}] completed in {}ms", opName, duration);
                }
                // Restore previous MDC context
                if (previousMdc != null) {
                    MDC.setContextMap(previousMdc);
                } else {
                    MDC.clear();
                }
            }
        }, executor);
    }

    /**
     * Wrap a CompletableFuture with timeout handling.
     * On timeout, completes exceptionally with TimeoutException.
     *
     * @param future  The future to wrap
     * @param timeout Timeout duration
     * @param opName  Operation name for logging
     * @return Future that will timeout if not completed within duration
     */
    public <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, Duration timeout, String opName) {
        return future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (ex instanceof TimeoutException) {
                    LOG.warn("AsyncExecutorService.withTimeout[{}] timed out after {}ms", opName, timeout.toMillis());
                }
            });
    }

    /**
     * Wrap a CompletableFuture with the default configured timeout.
     *
     * @param future The future to wrap
     * @param opName Operation name for logging
     * @return Future that will timeout if not completed within default duration
     */
    public <T> CompletableFuture<T> withDefaultTimeout(CompletableFuture<T> future, String opName) {
        return withTimeout(future, Duration.ofMillis(defaultTimeoutMs), opName);
    }

    /**
     * Wrap a CompletableFuture with timeout, returning default value on timeout.
     *
     * @param future       The future to wrap
     * @param timeout      Timeout duration
     * @param defaultValue Value to return on timeout
     * @param opName       Operation name for logging
     * @return Future that completes with defaultValue on timeout
     */
    public <T> CompletableFuture<T> withTimeoutOrDefault(CompletableFuture<T> future, Duration timeout, T defaultValue, String opName) {
        return future.completeOnTimeout(defaultValue, timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (result == defaultValue && ex == null) {
                    LOG.warn("AsyncExecutorService.withTimeoutOrDefault[{}] timed out, returning default", opName);
                }
            });
    }

    /**
     * Get the default timeout in milliseconds.
     *
     * @return Default timeout in milliseconds
     */
    public long getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    /**
     * Get executor stats for monitoring endpoints.
     *
     * @return Map of executor statistics
     */
    public Map<String, Object> getStats() {
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
            Map<String, Object> stats = new HashMap<>();
            stats.put("activeCount", tpe.getActiveCount());
            stats.put("poolSize", tpe.getPoolSize());
            stats.put("queueSize", tpe.getQueue().size());
            stats.put("completedTaskCount", tpe.getCompletedTaskCount());
            stats.put("taskCount", tpe.getTaskCount());
            return stats;
        }
        return new HashMap<>();
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down AsyncExecutorService...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("AsyncExecutorService did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
