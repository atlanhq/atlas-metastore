package org.apache.atlas.service.config;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamic Configuration Store backed by Cassandra.
 *
 * This is the main coordinator that:
 * - Provides static methods for getting/setting configs
 * - Manages the in-memory cache
 * - Coordinates with CassandraConfigDAO for persistence
 *
 * Design principles:
 * - Reads ALWAYS come from cache (never hit Cassandra on read path)
 * - Writes update Cassandra first, then update local cache
 * - Background sync (ConfigSyncScheduler) refreshes cache periodically
 * - If Cassandra is disabled, operations are no-ops (fall back to existing behavior)
 *
 * Configuration:
 * - atlas.config.store.cassandra.enabled=true to enable
 * - If disabled, all operations are no-ops
 */
@Component
public class DynamicConfigStore implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigStore.class);

    private static ApplicationContext context;

    private final DynamicConfigStoreConfig config;
    private final DynamicConfigCacheStore cacheStore;

    private volatile boolean initialized = false;
    private volatile boolean cassandraAvailable = false;

    @Inject
    public DynamicConfigStore(DynamicConfigStoreConfig config, DynamicConfigCacheStore cacheStore) {
        this.config = Objects.requireNonNull(config, "DynamicConfigStoreConfig cannot be null");
        this.cacheStore = Objects.requireNonNull(cacheStore, "DynamicConfigCacheStore cannot be null");

        LOG.info("DynamicConfigStore created - enabled: {}", config.isEnabled());
    }

    @PostConstruct
    public void initialize() {
        if (!config.isEnabled()) {
            LOG.info("Dynamic config store is disabled (atlas.config.store.cassandra.enabled=false)");
            initialized = true;
            return;
        }

        LOG.info("Initializing DynamicConfigStore with Cassandra backend...");
        long startTime = System.currentTimeMillis();

        try {
            // Initialize Cassandra DAO
            CassandraConfigDAO.initialize(config);
            cassandraAvailable = true;

            // Load initial data into cache
            loadAllConfigsIntoCache();

            initialized = true;
            long duration = System.currentTimeMillis() - startTime;
            LOG.info("DynamicConfigStore initialization completed in {}ms - {} configs loaded",
                    duration, cacheStore.size());

        } catch (Exception e) {
            LOG.error("Failed to initialize DynamicConfigStore - Cassandra config store will be unavailable", e);
            // Fail-fast if Cassandra is enabled but unavailable
            throw new RuntimeException("DynamicConfigStore initialization failed - Cassandra unavailable", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (config.isEnabled()) {
            CassandraConfigDAO.shutdown();
            LOG.info("DynamicConfigStore shutdown complete");
        }
    }

    /**
     * Load all configs from Cassandra into cache.
     * Called during initialization and by the sync scheduler.
     */
    public void loadAllConfigsIntoCache() throws AtlasBaseException {
        if (!config.isEnabled() || !cassandraAvailable) {
            return;
        }

        try {
            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
            Map<String, ConfigEntry> configs = dao.getAllConfigs();
            cacheStore.replaceAll(configs);
            LOG.debug("Loaded {} configs into cache", configs.size());
        } catch (Exception e) {
            LOG.error("Failed to load configs from Cassandra", e);
            throw e;
        }
    }

    /**
     * Check if Cassandra backend is healthy.
     * @return true if healthy, false otherwise
     */
    public boolean isCassandraHealthy() {
        if (!config.isEnabled() || !cassandraAvailable) {
            return false;
        }

        try {
            return CassandraConfigDAO.getInstance().isHealthy();
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed", e);
            return false;
        }
    }

    // ================== Static API Methods ==================

    /**
     * Get a config value.
     * Returns from cache if available, otherwise returns default value.
     *
     * @param key the config key
     * @return the config value or default if not found
     */
    public static String getConfig(String key) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            return getDefaultValue(key);
        }

        return store.getConfigInternal(key);
    }

    /**
     * Get a config value as boolean.
     *
     * @param key the config key
     * @return true if value is "true" (case-insensitive), false otherwise
     */
    public static boolean getConfigAsBoolean(String key) {
        String value = getConfig(key);
        return "true".equalsIgnoreCase(value);
    }

    /**
     * Set a config value.
     *
     * @param key the config key
     * @param value the config value
     * @param updatedBy who is making the update
     */
    public static void setConfig(String key, String value, String updatedBy) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            LOG.warn("Cannot set config - DynamicConfigStore not available");
            return;
        }

        store.setConfigInternal(key, value, updatedBy);
    }

    /**
     * Delete a config (reset to default).
     *
     * @param key the config key
     */
    public static void deleteConfig(String key) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            LOG.warn("Cannot delete config - DynamicConfigStore not available");
            return;
        }

        store.deleteConfigInternal(key);
    }

    /**
     * Get all cached configs.
     *
     * @return map of all config entries
     */
    public static Map<String, ConfigEntry> getAllConfigs() {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            return Map.of();
        }

        return store.cacheStore.getAll();
    }

    /**
     * Check if the store is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public static boolean isEnabled() {
        DynamicConfigStore store = getInstance();
        return store != null && store.config.isEnabled();
    }

    /**
     * Check if the store is initialized.
     *
     * @return true if initialized, false otherwise
     */
    public static boolean isReady() {
        DynamicConfigStore store = getInstance();
        return store != null && store.initialized;
    }

    // ================== Internal Methods ==================

    String getConfigInternal(String key) {
        if (!initialized) {
            LOG.warn("DynamicConfigStore not initialized, returning default for key: {}", key);
            return getDefaultValue(key);
        }

        // Always read from cache
        ConfigEntry entry = cacheStore.get(key);
        if (entry != null) {
            return entry.getValue();
        }

        // Return default value if not in cache
        return getDefaultValue(key);
    }

    void setConfigInternal(String key, String value, String updatedBy) {
        if (!initialized || !cassandraAvailable) {
            LOG.warn("Cannot set config - store not ready");
            return;
        }

        // Validate key
        if (!ConfigKey.isValidKey(key)) {
            LOG.warn("Invalid config key: {}. Only predefined keys are allowed.", key);
            return;
        }

        try {
            // Write to Cassandra first
            CassandraConfigDAO.getInstance().putConfig(key, value, updatedBy);

            // Then update local cache
            cacheStore.put(key, value, updatedBy);

            LOG.info("Config set - key: {}, value: {}, by: {}", key, value, updatedBy);

        } catch (Exception e) {
            LOG.error("Failed to set config - key: {}, value: {}", key, value, e);
        }
    }

    void deleteConfigInternal(String key) {
        if (!initialized || !cassandraAvailable) {
            LOG.warn("Cannot delete config - store not ready");
            return;
        }

        try {
            // Delete from Cassandra first
            CassandraConfigDAO.getInstance().deleteConfig(key);

            // Then remove from local cache
            cacheStore.remove(key);

            LOG.info("Config deleted - key: {}", key);

        } catch (Exception e) {
            LOG.error("Failed to delete config - key: {}", key, e);
        }
    }

    private static String getDefaultValue(String key) {
        ConfigKey configKey = ConfigKey.fromKey(key);
        if (configKey != null) {
            return configKey.getDefaultValue();
        }
        return null;
    }

    private static DynamicConfigStore getInstance() {
        if (context == null) {
            LOG.debug("ApplicationContext not available");
            return null;
        }

        try {
            return context.getBean(DynamicConfigStore.class);
        } catch (Exception e) {
            LOG.debug("DynamicConfigStore bean not available: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    // ================== Getters for testing ==================

    public DynamicConfigStoreConfig getConfig() {
        return config;
    }

    public DynamicConfigCacheStore getCacheStore() {
        return cacheStore;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isCassandraAvailable() {
        return cassandraAvailable;
    }
}
