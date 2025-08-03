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
package org.apache.atlas.services;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.discovery.VertexEdgeCache;
import org.apache.atlas.glossary.GlossaryTermUtils;
import org.apache.atlas.repository.audit.EntityAuditListenerV2;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

/**
 * MEMORY LEAK FIX: Central service for coordinating memory cleanup across the Atlas application
 *
 * This service addresses memory leak issues commonly seen in multi-pod environments by:
 * 1. Periodically cleaning ThreadLocal caches that accumulate over time
 * 2. Limiting cache sizes to prevent unbounded growth
 * 3. Providing explicit cleanup during application shutdown
 * 4. Coordinating cleanup across different components
 *
 * The service is especially important for:
 * - Long-running threads in servlet containers
 * - Multi-pod deployments with frequent restarts
 * - High-volume environments where caches can grow rapidly
 */
@Service
public class AtlasMemoryCleanupService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasMemoryCleanupService.class);

    private final VertexEdgeCache vertexEdgeCache;
    private final EntityAuditListenerV2 auditListener;

    // Track cleanup statistics for monitoring
    private volatile long lastCleanupTime = System.currentTimeMillis();
    private volatile long cleanupCount = 0;
    private volatile boolean shutdownInProgress = false;

    @Inject
    public AtlasMemoryCleanupService(VertexEdgeCache vertexEdgeCache, EntityAuditListenerV2 auditListener) {
        this.vertexEdgeCache = vertexEdgeCache;
        this.auditListener = auditListener;
        LOG.info("AtlasMemoryCleanupService initialized - periodic cleanup enabled");
    }

    /**
     * Periodic cleanup every 5 minutes to prevent memory accumulation
     * This runs regardless of request activity to handle idle threads
     */
    @Scheduled(fixedRate = 300000) // 5 minutes
    public void periodicCleanup() {
        if (shutdownInProgress) {
            return;
        }

        try {
            LOG.debug("==> AtlasMemoryCleanupService.periodicCleanup()");

            long startTime = System.currentTimeMillis();

            // Clean up all ThreadLocal caches
            cleanupThreadLocalCaches();

            // Limit cache sizes to prevent unbounded growth
            limitCacheSizes();

            // Clean up static caches with TTL/size limits
            cleanupStaticCaches();

            long duration = System.currentTimeMillis() - startTime;
            lastCleanupTime = System.currentTimeMillis();
            cleanupCount++;

            LOG.debug("<== AtlasMemoryCleanupService.periodicCleanup() completed in {}ms (cleanup #{}})", duration, cleanupCount);

        } catch (Exception e) {
            LOG.warn("Error during periodic memory cleanup - continuing operation", e);
        }
    }

    /**
     * Clean up ThreadLocal caches across all components
     * This is the most important cleanup as ThreadLocals are the primary source of memory leaks
     */
    public void cleanupThreadLocalCaches() {
        try {
            // GraphTransactionInterceptor - most critical
            GraphTransactionInterceptor.cleanupThreadLocalCaches();

            // VertexEdgeCache cleanup
            if (vertexEdgeCache != null) {
                vertexEdgeCache.clearCache();
            }

            // EntityAuditListenerV2 cleanup
            EntityAuditListenerV2.clearAuditBuffer();

            // GlossaryTermUtils cleanup
            GlossaryTermUtils.clearGlossaryCaches();

        } catch (Exception e) {
            LOG.warn("Error cleaning ThreadLocal caches", e);
        }
    }

    /**
     * Limit cache sizes to prevent unbounded growth within active threads
     */
    public void limitCacheSizes() {
        try {
            // Limit glossary cache sizes
            GlossaryTermUtils.limitCacheSizes();

            // Periodic cleanup for graph transaction caches
            GraphTransactionInterceptor.periodicCacheCleanup();

            // Limit TagDAO paging state cache size
            TagDAOCassandraImpl.limitPagingStateCacheSize();

        } catch (Exception e) {
            LOG.warn("Error limiting cache sizes", e);
        }
    }

    /**
     * Clean up static caches that may accumulate over time
     */
    public void cleanupStaticCaches() {
        try {
            // Clear audit excluded attributes cache periodically
            if (auditListener != null) {
                auditListener.clearAuditExcludedAttributesCache();
            }

        } catch (Exception e) {
            LOG.warn("Error cleaning static caches", e);
        }
    }

    /**
     * Force garbage collection if memory pressure is detected
     * This is aggressive but necessary in environments with poor GC performance
     */
    public void forceGarbageCollection() {
        try {
            LOG.info("Forcing garbage collection due to memory pressure");

            Runtime runtime = Runtime.getRuntime();
            long beforeGC = runtime.totalMemory() - runtime.freeMemory();

            // Perform comprehensive cleanup first
            comprehensiveCleanup();

            // Force GC
            System.gc();
            Thread.sleep(100); // Give GC time to work
            System.runFinalization();

            long afterGC = runtime.totalMemory() - runtime.freeMemory();
            long freedMemory = beforeGC - afterGC;

            LOG.info("Garbage collection completed - freed approximately {} MB", freedMemory / (1024 * 1024));

        } catch (Exception e) {
            LOG.warn("Error during forced garbage collection", e);
        }
    }

    /**
     * Comprehensive cleanup of all memory resources
     * Used during shutdown or when memory pressure is detected
     */
    public void comprehensiveCleanup() {
        try {
            LOG.info("==> AtlasMemoryCleanupService.comprehensiveCleanup()");

            cleanupThreadLocalCaches();
            cleanupStaticCaches();

            // Clear TagDAO paging state cache
            TagDAOCassandraImpl.clearPagingStateCache();

            LOG.info("<== AtlasMemoryCleanupService.comprehensiveCleanup() completed");

        } catch (Exception e) {
            LOG.error("Error during comprehensive cleanup", e);
        }
    }

    /**
     * Get cleanup statistics for monitoring
     */
    public String getCleanupStats() {
        return String.format("Last cleanup: %d ms ago, Total cleanups: %d",
                System.currentTimeMillis() - lastCleanupTime, cleanupCount);
    }

    /**
     * Check if memory cleanup is needed based on current memory usage
     */
    public boolean isMemoryCleanupNeeded() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        // Trigger cleanup if using more than 80% of allocated memory
        double memoryUsagePercent = (double) usedMemory / totalMemory;
        return memoryUsagePercent > 0.8;
    }

    /**
     * Emergency cleanup triggered by memory pressure
     */
    public void emergencyCleanup() {
        LOG.warn("EMERGENCY MEMORY CLEANUP TRIGGERED");
        comprehensiveCleanup();
        forceGarbageCollection();
    }

    @PreDestroy
    public void shutdown() {
        shutdownInProgress = true;
        LOG.info("AtlasMemoryCleanupService shutting down - performing final cleanup");
        comprehensiveCleanup();
    }
} 