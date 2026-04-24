package org.apache.atlas.service.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.time.Duration;
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
 * <h3>Resolution strategy (per key)</h3>
 * <ol>
 *   <li>Cassandra reachable, table exists, row found → use value from Cassandra</li>
 *   <li>Cassandra reachable, table exists, no row   → fallback to atlas-application.properties</li>
 *   <li>Cassandra reachable, table missing           → try create; if fails, fallback to atlas-application.properties</li>
 *   <li>Cassandra UNREACHABLE (after retry)          → KILL PROCESS (System.exit)</li>
 * </ol>
 */
@Component("staticConfigStore")
public class StaticConfigStore {
    private static final Logger LOG = LoggerFactory.getLogger(StaticConfigStore.class);

    private static volatile StaticConfigStore INSTANCE;

    private final boolean enabled;
    private final String appName;
    private final String keyspace;
    private final String table;
    private final int replicationFactor;

    private volatile Map<String, String> configs = Collections.emptyMap();
    private volatile boolean ready = false;

    public StaticConfigStore() {
        boolean enabledVal = true;
        String appNameVal = "atlas_static";
        String keyspaceVal = "config_store";
        String tableVal = "static_configs";
        int replicationFactorVal = 3;
        try {
            Configuration props = ApplicationProperties.get();
            enabledVal = props.getBoolean("atlas.static.config.store.enabled", true);
            appNameVal = props.getString("atlas.static.config.store.app.name", "atlas_static");
            keyspaceVal = props.getString("atlas.config.store.cassandra.keyspace", "config_store");
            tableVal = props.getString("atlas.static.config.store.table", "static_configs");
            replicationFactorVal = props.getInt("atlas.config.store.cassandra.replication.factor", 3);
        } catch (Exception e) {
            LOG.warn("Failed to read static config store properties, using defaults", e);
        }
        this.enabled = enabledVal;
        this.appName = appNameVal;
        this.keyspace = keyspaceVal;
        this.table = tableVal;
        this.replicationFactor = replicationFactorVal;
        INSTANCE = this;
        LOG.info("StaticConfigStore created - enabled: {}, appName: {}, table: {}.{}", enabled, appName, keyspace, table);
    }

    // Test-only constructor
    StaticConfigStore(boolean enabled, String appName, String keyspace, String table) {
        this.enabled = enabled;
        this.appName = appName;
        this.keyspace = keyspace;
        this.table = table;
        this.replicationFactor = 1;
        INSTANCE = this;
    }

    /**
     * Early overlay: reads static configs from Cassandra and overlays onto ApplicationProperties
     * BEFORE Spring context starts. Must be called from Atlas.main() to ensure graph backend and
     * ES prefix are set correctly before Constants class loading and AtlasGraphProvider bean creation.
     *
     * Best-effort: if Cassandra is unreachable or table doesn't exist, falls back silently.
     * The @PostConstruct initialize() will handle fail-fast later.
     */
    public static void earlyOverlay() {
        long start = System.currentTimeMillis();
        CqlSession session = null;
        try {
            Configuration props = ApplicationProperties.get();

            boolean enabled = props.getBoolean("atlas.static.config.store.enabled", true);
            if (!enabled) {
                LOG.info("Static config store disabled, skipping early overlay");
                return;
            }

            String hostname = props.getString("atlas.config.store.cassandra.hostname", null);
            if (hostname == null || hostname.isEmpty()) {
                hostname = props.getString("atlas.graph.storage.hostname", "localhost");
            }
            int port = props.getInt("atlas.config.store.cassandra.port", -1);
            if (port <= 0) {
                port = props.getInt("atlas.graph.storage.cql.port",
                        props.getInt("atlas.graph.storage.port", 9042));
            }
            String datacenter = props.getString("atlas.config.store.cassandra.datacenter", null);
            if (datacenter == null || datacenter.isEmpty()) {
                datacenter = props.getString("atlas.graph.storage.cql.local-datacenter", "datacenter1");
            }
            String keyspace = props.getString("atlas.config.store.cassandra.keyspace", "config_store");
            String table = props.getString("atlas.static.config.store.table", "static_configs");
            String appName = props.getString("atlas.static.config.store.app.name", "atlas_static");

            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1)
                    .build();

            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, port))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(datacenter)
                    .build();

            String cql = String.format(
                    "SELECT config_key, config_value FROM %s.%s WHERE app_name = ?",
                    keyspace, table);
            SimpleStatement stmt = SimpleStatement.builder(cql)
                    .addPositionalValue(appName)
                    .build();
            ResultSet rs = session.execute(stmt);

            int overlayCount = 0;
            for (Row row : rs) {
                String key = row.getString("config_key");
                String value = row.getString("config_value");
                if (key != null && value != null && StaticConfigKey.isValidKey(key)) {
                    String existing = props.getString(key, null);
                    props.setProperty(key, value);
                    LOG.info("Early overlay: {} = {} (was: {})", key, value, existing);
                    overlayCount++;
                }
            }

            long duration = System.currentTimeMillis() - start;
            LOG.info("StaticConfigStore early overlay completed in {}ms - {} configs overlaid", duration, overlayCount);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            LOG.warn("StaticConfigStore early overlay failed ({}ms) - will retry in @PostConstruct: {}",
                    duration, e.getMessage());
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close early overlay Cassandra session", e);
                }
            }
        }
    }

    @PostConstruct
    public void initialize() {
        if (!enabled) {
            LOG.info("Static config store is disabled. Falling back to application properties.");
            configs = buildFallbackMap();
            ready = true;
            return;
        }

        LOG.info("Initializing StaticConfigStore - reading from {}.{} partition '{}'...", keyspace, table, appName);
        long startTime = System.currentTimeMillis();

        try {
            // Ensure CassandraConfigDAO is initialized (no-op if already initialized).
            initializeDAO();

            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();

            // Try to ensure the static config table exists.
            // If creation fails (e.g., permissions), fall back to application.properties.
            boolean tableReady = dao.ensureTableExists(keyspace, table, replicationFactor);
            if (!tableReady) {
                LOG.warn("Static config table {}.{} could not be created. Falling back to application properties.", keyspace, table);
                configs = buildFallbackMap();
                ready = true;
                return;
            }

            // Read all configs for the static partition from Cassandra.
            Map<String, ConfigEntry> cassandraConfigs = dao.getAllConfigsFromTable(keyspace, table, appName);

            // Build config map: for each StaticConfigKey, use Cassandra value or fall back to application properties
            configs = buildConfigMap(cassandraConfigs);

            // Overlay Cassandra-backed values onto ApplicationProperties so that downstream
            // consumers in other modules (e.g., CassandraSessionProvider, CassandraGraphDatabase)
            // automatically pick them up without needing a direct dependency on StaticConfigStore.
            overlayOntoApplicationProperties();

            ready = true;

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("StaticConfigStore initialization completed in {}ms - {} configs loaded", duration, configs.size());

        } catch (Exception e) {
            // FAIL-FAST: Cassandra unreachable -> kill the process
            LOG.error("FATAL: StaticConfigStore failed to read from Cassandra. KILLING PROCESS.", e);
            exitProcess(1);
        }
    }

    /**
     * Initialize CassandraConfigDAO using shared Cassandra connection properties.
     * No-op if the DAO was already initialized.
     */
    private void initializeDAO() throws Exception {
        Configuration props = ApplicationProperties.get();

        String hostname = props.getString("atlas.config.store.cassandra.hostname", null);
        if (hostname == null || hostname.isEmpty()) {
            hostname = props.getString("atlas.graph.storage.hostname", "localhost");
        }

        int port = props.getInt("atlas.config.store.cassandra.port", -1);
        if (port <= 0) {
            port = props.getInt("atlas.graph.storage.cql.port",
                    props.getInt("atlas.graph.storage.port", 9042));
        }

        String datacenter = props.getString("atlas.config.store.cassandra.datacenter", null);
        if (datacenter == null || datacenter.isEmpty()) {
            datacenter = props.getString("atlas.graph.storage.cql.local-datacenter", "datacenter1");
        }

        String dynamicAppName = props.getString("atlas.config.store.app.name", "atlas");

        CassandraConfigDAO.doInitialize(
                props.getString("atlas.config.store.cassandra.keyspace", "config_store"),
                props.getString("atlas.config.store.cassandra.table", "configs"),
                dynamicAppName,
                hostname,
                port,
                datacenter,
                replicationFactor,
                props.getString("atlas.config.store.cassandra.consistency.level", "LOCAL_QUORUM")
        );
    }

    private Map<String, String> buildConfigMap(Map<String, ConfigEntry> cassandraConfigs) {
        Map<String, String> configMap = new HashMap<>();
        StringBuilder logBuilder = new StringBuilder("Static config values loaded at startup:\n");

        Configuration appProps = null;
        try {
            appProps = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.warn("Failed to load application properties for fallback", e);
        }

        for (StaticConfigKey staticKey : StaticConfigKey.values()) {
            String key = staticKey.getKey();
            ConfigEntry entry = cassandraConfigs.get(key);

            String value;
            String source;
            if (entry != null && entry.getValue() != null) {
                value = entry.getValue();
                source = "cassandra";
            } else if (appProps != null) {
                value = appProps.getString(key, null);
                source = value != null ? "application.properties" : "not-set";
            } else {
                value = null;
                source = "not-set";
            }

            if (value != null) {
                configMap.put(key, value);
            }

            logBuilder.append("  ").append(key).append(" = ").append(value)
                      .append(" [source=").append(source).append("]\n");
        }

        LOG.info(logBuilder.toString());
        return Collections.unmodifiableMap(configMap);
    }

    /**
     * Overlay static config values onto ApplicationProperties.
     * This allows downstream modules (graphdb/cassandra) that read from ApplicationProperties
     * to automatically get Cassandra-backed values without a direct dependency on StaticConfigStore.
     * Only values that came from Cassandra (not fallback defaults) are overlaid.
     */
    private void overlayOntoApplicationProperties() {
        try {
            Configuration appProps = ApplicationProperties.get();
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String existingValue = appProps.getString(entry.getKey(), null);
                if (existingValue == null || !existingValue.equals(entry.getValue())) {
                    appProps.setProperty(entry.getKey(), entry.getValue());
                    LOG.info("ApplicationProperties overlaid: {} = {} (was: {})",
                            entry.getKey(), entry.getValue(), existingValue);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to overlay static configs onto ApplicationProperties", e);
        }
    }

    /**
     * Build fallback map from atlas-application.properties.
     * Used when static config store is disabled or table creation fails.
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

    public static String getConfig(String key) {
        StaticConfigStore store = INSTANCE;
        if (store == null || !store.ready) {
            return getPropertyFallback(key);
        }
        return store.configs.get(key);
    }

    public static boolean getConfigAsBoolean(String key) {
        String value = getConfig(key);
        return "true".equalsIgnoreCase(value);
    }

    public static String getGraphBackend() {
        return getConfig(StaticConfigKey.GRAPH_BACKEND.getKey());
    }

    public static boolean isCassandraGraphBackend() {
        return "cassandra".equalsIgnoreCase(getGraphBackend());
    }

    public static boolean isReady() {
        StaticConfigStore store = INSTANCE;
        return store != null && store.ready;
    }

    public static Map<String, String> getAllConfigs() {
        StaticConfigStore store = INSTANCE;
        if (store == null || !store.ready) {
            return Collections.emptyMap();
        }
        return store.configs;
    }

    /**
     * Seed a static config value into Cassandra.
     * Writes to Cassandra but does NOT update the in-memory map.
     * A restart is required for the new value to take effect.
     */
    public static void seedConfig(String key, String value, String updatedBy) throws AtlasBaseException {
        StaticConfigStore store = INSTANCE;
        if (store == null) {
            throw new AtlasBaseException("StaticConfigStore not initialized");
        }

        CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
        dao.putConfigToTable(store.keyspace, store.table, store.appName, key, value, updatedBy);
        LOG.info("Static config seeded in Cassandra - key: {}, value: {}, updatedBy: {} (restart required to take effect)",
                key, value, updatedBy);
    }

    /**
     * Kill the process. Extracted into a method so tests can override it.
     */
    void exitProcess(int status) {
        System.exit(status);
    }

    private static String getPropertyFallback(String key) {
        try {
            return ApplicationProperties.get().getString(key, null);
        } catch (Exception e) {
            LOG.warn("Failed to read application property for key: {}", key, e);
            return null;
        }
    }
}
