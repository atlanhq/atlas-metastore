package org.apache.atlas.service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;

/**
 * Background scheduler that periodically syncs configuration from Cassandra into the local cache.
 *
 * Design principles:
 * - Runs every 60 seconds (configurable via atlas.config.store.sync.interval.ms)
 * - Replaces entire cache atomically on each sync
 * - On failure: logs warning and continues with stale cache (graceful degradation)
 * - Only active when atlas.config.store.cassandra.enabled=true
 *
 * This ensures that config changes propagate to all pods within 60 seconds.
 */
@Configuration
@EnableScheduling
public class ConfigSyncScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigSyncScheduler.class);

    private final DynamicConfigStore configStore;
    private final DynamicConfigStoreConfig config;

    private long lastSyncTime = 0;
    private long syncCount = 0;
    private long failureCount = 0;

    @Inject
    public ConfigSyncScheduler(DynamicConfigStore configStore, DynamicConfigStoreConfig config) {
        this.configStore = configStore;
        this.config = config;
        LOG.info("ConfigSyncScheduler initialized - sync interval: {}ms", config.getSyncIntervalMs());
    }

    /**
     * Scheduled task that syncs config from Cassandra to local cache.
     *
     * This runs at a fixed rate (default 60 seconds).
     * On failure, the task logs a warning and continues using the stale cache.
     */
    @Scheduled(fixedRateString = "${atlas.config.store.sync.interval.ms:60000}")
    public void syncFromCassandra() {
        // Skip if config store is disabled
        if (!config.isEnabled()) {
            return;
        }

        if (!configStore.isInitialized()) {
            LOG.debug("ConfigSyncScheduler: Store not initialized, skipping sync");
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            // Check if Cassandra is healthy before attempting sync
            if (!configStore.isCassandraHealthy()) {
                LOG.warn("ConfigSyncScheduler: Cassandra unhealthy, skipping sync (using stale cache)");
                failureCount++;
                return;
            }

            // Reload all configs from Cassandra into cache
            configStore.loadAllConfigsIntoCache();

            // Update stats
            lastSyncTime = System.currentTimeMillis();
            syncCount++;
            long duration = lastSyncTime - startTime;

            if (LOG.isDebugEnabled()) {
                LOG.debug("ConfigSyncScheduler: Sync completed in {}ms - {} configs in cache (syncCount: {})",
                        duration, configStore.getCacheStore().size(), syncCount);
            }

        } catch (Exception e) {
            failureCount++;
            LOG.warn("ConfigSyncScheduler: Sync failed (using stale cache) - failureCount: {}, error: {}",
                    failureCount, e.getMessage());
            // Don't rethrow - graceful degradation using stale cache
        }
    }

    /**
     * Get the timestamp of the last successful sync.
     * @return timestamp in milliseconds, 0 if never synced
     */
    public long getLastSyncTime() {
        return lastSyncTime;
    }

    /**
     * Get the total number of successful syncs.
     * @return sync count
     */
    public long getSyncCount() {
        return syncCount;
    }

    /**
     * Get the total number of failed syncs.
     * @return failure count
     */
    public long getFailureCount() {
        return failureCount;
    }
}
