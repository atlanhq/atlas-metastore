package org.apache.atlas.service.config;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Static Configuration Store backed by Cassandra.
 *
 * Reads configs from Cassandra ONCE at startup into an immutable map.
 * No cache layer, no background refresh, no runtime updates.
 * A restart is required to pick up changes seeded via the admin endpoint.
 *
 * Fail-fast behavior:
 * - Cassandra reachable, row found      -> use value from Cassandra
 * - Cassandra reachable, no row         -> fallback to atlas-application.properties
 * - Cassandra UNREACHABLE (after retry) -> KILL PROCESS (System.exit)
 *
 * Depends on DynamicConfigStore to ensure CassandraConfigDAO is initialized first.
 */
@Component("staticConfigStore")
@DependsOn("dynamicConfigStore")
public class StaticConfigStore {
    private static final Logger LOG = LoggerFactory.getLogger(StaticConfigStore.class);

    private static volatile StaticConfigStore INSTANCE;

    private final StaticConfigStoreConfig config;

    private volatile Map<String, String> configs = Collections.emptyMap();
    private volatile boolean ready = false;

    @Inject
    public StaticConfigStore(StaticConfigStoreConfig config) {
        this.config = config;
        INSTANCE = this;
        LOG.info("StaticConfigStore created - enabled: {}, appName: {}", config.isEnabled(), config.getAppName());
    }

    @PostConstruct
    public void initialize() {
        if (!config.isEnabled()) {
            LOG.info("Static config store is disabled (atlas.static.config.store.enabled=false). Falling back to application properties.");
            configs = buildFallbackMap();
            ready = true;
            return;
        }

        LOG.info("Initializing StaticConfigStore - reading from Cassandra partition '{}'...", config.getAppName());
        long startTime = System.currentTimeMillis();

        try {
            // Read all configs for the static partition from Cassandra.
            // CassandraConfigDAO retry logic handles transient failures (3 retries with backoff).
            // If still fails after retries -> AtlasBaseException is thrown -> process KILLED.
            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
            Map<String, ConfigEntry> cassandraConfigs = dao.getAllConfigsForApp(config.getAppName());

            // Load application properties for fallback when Cassandra has no entry
            Configuration appProps = ApplicationProperties.get();

            // Build config map: for each StaticConfigKey, use Cassandra value or fall back to application properties
            Map<String, String> configMap = new HashMap<>();
            StringBuilder logBuilder = new StringBuilder();
            logBuilder.append("Static config values loaded at startup:\n");

            for (StaticConfigKey staticKey : StaticConfigKey.values()) {
                String key = staticKey.getKey();
                ConfigEntry entry = cassandraConfigs.get(key);

                String value;
                String source;
                if (entry != null && entry.getValue() != null) {
                    value = entry.getValue();
                    source = "cassandra";
                } else {
                    // Fallback to atlas-application.properties
                    value = appProps.getString(key, null);
                    source = value != null ? "application.properties" : "not-set";
                }

                if (value != null) {
                    configMap.put(key, value);
                }

                logBuilder.append("  ").append(key).append(" = ").append(value)
                          .append(" [source=").append(source).append("]\n");
            }

            configs = Collections.unmodifiableMap(configMap);
            ready = true;

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("StaticConfigStore initialization completed in {}ms - {} configs loaded", duration, configs.size());
            LOG.info(logBuilder.toString());

        } catch (Exception e) {
            // FAIL-FAST: Cassandra unreachable -> kill the process
            LOG.error("FATAL: StaticConfigStore failed to read from Cassandra. KILLING PROCESS.", e);
            exitProcess(1);
        }
    }

    /**
     * Build fallback map from atlas-application.properties.
     * Used when static config store is disabled.
     */
    private Map<String, String> buildFallbackMap() {
        Map<String, String> fallback = new HashMap<>();
        try {
            Configuration appProps = ApplicationProperties.get();
            for (StaticConfigKey staticKey : StaticConfigKey.values()) {
                String value = appProps.getString(staticKey.getKey(), null);
                if (value != null) {
                    fallback.put(staticKey.getKey(), value);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to load application properties for fallback, static configs will be empty", e);
        }
        return Collections.unmodifiableMap(fallback);
    }

    // ================== Static API ==================

    /**
     * Get a static config value.
     * @param key the config key
     * @return the value or null if not found
     */
    public static String getConfig(String key) {
        StaticConfigStore store = INSTANCE;
        if (store == null || !store.ready) {
            return getPropertyFallback(key);
        }
        return store.configs.get(key);
    }

    /**
     * Get a static config value as boolean.
     * @param key the config key
     * @return true if value is "true" (case-insensitive), false otherwise
     */
    public static boolean getConfigAsBoolean(String key) {
        String value = getConfig(key);
        return "true".equalsIgnoreCase(value);
    }

    /**
     * Get the configured graph backend value.
     * @return "janus" or "cassandra"
     */
    public static String getGraphBackend() {
        return getConfig(StaticConfigKey.GRAPH_BACKEND.getKey());
    }

    /**
     * Check if the graph backend is configured to use CassandraGraph.
     * @return true if GRAPH_BACKEND == "cassandra"
     */
    public static boolean isCassandraGraphBackend() {
        return "cassandra".equalsIgnoreCase(getGraphBackend());
    }

    /**
     * Check if the store has completed initialization.
     * @return true after @PostConstruct completes successfully
     */
    public static boolean isReady() {
        StaticConfigStore store = INSTANCE;
        return store != null && store.ready;
    }

    /**
     * Get all static configs as an unmodifiable map.
     * @return unmodifiable map of all static config key-value pairs
     */
    public static Map<String, String> getAllConfigs() {
        StaticConfigStore store = INSTANCE;
        if (store == null || !store.ready) {
            return Collections.emptyMap();
        }
        return store.configs;
    }

    /**
     * Seed a static config value into Cassandra.
     * This writes to Cassandra but does NOT update the in-memory map.
     * A restart is required for the new value to take effect.
     *
     * @param key the config key
     * @param value the config value
     * @param updatedBy who is making the update
     * @throws AtlasBaseException if the write fails
     */
    public static void seedConfig(String key, String value, String updatedBy) throws AtlasBaseException {
        StaticConfigStore store = INSTANCE;
        if (store == null) {
            throw new AtlasBaseException("StaticConfigStore not initialized");
        }

        CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
        dao.putConfigForApp(store.config.getAppName(), key, value, updatedBy);
        LOG.info("Static config seeded in Cassandra - key: {}, value: {}, updatedBy: {} (restart required to take effect)",
                key, value, updatedBy);
    }

    /**
     * Kill the process. Extracted into a method so tests can override it.
     */
    void exitProcess(int status) {
        System.exit(status);
    }

    // ================== Internal helpers ==================

    private static String getPropertyFallback(String key) {
        try {
            return ApplicationProperties.get().getString(key, null);
        } catch (Exception e) {
            LOG.warn("Failed to read application property for key: {}", key, e);
            return null;
        }
    }

    // For testing
    StaticConfigStoreConfig getStoreConfig() {
        return config;
    }
}
