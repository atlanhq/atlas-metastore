package org.apache.atlas.service.featureflag;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;

/**
 * Data Access Object for feature flag storage in Cassandra.
 *
 * This DAO provides feature flag persistence independent of Redis,
 * ensuring Atlas can start even when Redis is unavailable.
 *
 * Design principles:
 * - Singleton pattern with lazy initialization
 * - Auto-creates keyspace and table on first use
 * - Uses LOCAL_QUORUM consistency for reads and writes
 * - Retry with exponential backoff for transient failures
 * - No dependency on Redis or DynamicConfigStore
 *
 * Schema:
 * CREATE TABLE feature_flags.flags (
 *     flag_name    text PRIMARY KEY,
 *     flag_value   text,
 *     updated_by   text,
 *     updated_at   timestamp
 * );
 */
public class CassandraFeatureFlagDAO implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraFeatureFlagDAO.class);

    // Configuration property keys
    private static final String PROP_HOSTNAME = "atlas.graph.storage.hostname";
    private static final String PROP_PORT = "atlas.graph.storage.port";
    private static final String PROP_DATACENTER = "atlas.graph.storage.cql.local-datacenter";
    private static final String PROP_KEYSPACE = "atlas.feature.flag.cassandra.keyspace";
    private static final String PROP_TABLE = "atlas.feature.flag.cassandra.table";
    private static final String PROP_REPLICATION_FACTOR = "atlas.feature.flag.cassandra.replication.factor";

    // Default values
    private static final String DEFAULT_KEYSPACE = "feature_flags";
    private static final String DEFAULT_TABLE = "flags";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_DATACENTER = "datacenter1";
    private static final int DEFAULT_PORT = 9042;
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(100);

    // Connection configuration
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    // Singleton instance
    private static volatile CassandraFeatureFlagDAO INSTANCE;
    private static volatile boolean initialized = false;
    private static volatile Exception initializationException = null;
    private static final Object INIT_LOCK = new Object();

    private final CqlSession session;
    private final String keyspace;
    private final String table;

    // Prepared statements
    private final PreparedStatement selectStmt;
    private final PreparedStatement upsertStmt;
    private final PreparedStatement deleteStmt;
    private final PreparedStatement healthCheckStmt;

    /**
     * Get the singleton instance, initializing if necessary.
     * This method is safe to call during Spring context initialization.
     *
     * @return the singleton instance
     * @throws RuntimeException if initialization fails
     */
    public static CassandraFeatureFlagDAO getInstance() {
        if (!initialized) {
            synchronized (INIT_LOCK) {
                if (!initialized) {
                    try {
                        INSTANCE = new CassandraFeatureFlagDAO();
                        initialized = true;
                        initializationException = null;
                        LOG.info("CassandraFeatureFlagDAO singleton initialized successfully");
                    } catch (Exception e) {
                        initializationException = e;
                        LOG.error("Failed to initialize CassandraFeatureFlagDAO", e);
                        throw new RuntimeException("Failed to initialize CassandraFeatureFlagDAO", e);
                    }
                }
            }
        }

        if (initializationException != null) {
            throw new RuntimeException("CassandraFeatureFlagDAO initialization failed previously", initializationException);
        }

        return INSTANCE;
    }

    /**
     * Check if the DAO is initialized.
     * @return true if initialized, false otherwise
     */
    public static boolean isInitialized() {
        return initialized;
    }

    private CassandraFeatureFlagDAO() {
        try {
            Configuration config = ApplicationProperties.get();

            String hostname = config.getString(PROP_HOSTNAME, DEFAULT_HOSTNAME);
            int port = config.getInt(PROP_PORT, DEFAULT_PORT);
            String datacenter = config.getString(PROP_DATACENTER, DEFAULT_DATACENTER);
            this.keyspace = config.getString(PROP_KEYSPACE, DEFAULT_KEYSPACE);
            this.table = config.getString(PROP_TABLE, DEFAULT_TABLE);
            int replicationFactor = config.getInt(PROP_REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR);

            LOG.info("Initializing CassandraFeatureFlagDAO - hostname: {}, port: {}, keyspace: {}, table: {}",
                    hostname, port, keyspace, table);

            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 2)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1)
                    .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)
                    .build();

            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, port))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(datacenter)
                    .build();

            // Initialize schema
            initializeSchema(replicationFactor);

            // Prepare statements
            selectStmt = prepare(String.format(
                    "SELECT flag_value FROM %s.%s WHERE flag_name = ?",
                    keyspace, table));

            upsertStmt = prepare(String.format(
                    "INSERT INTO %s.%s (flag_name, flag_value, updated_by, updated_at) VALUES (?, ?, ?, ?)",
                    keyspace, table));

            deleteStmt = prepare(String.format(
                    "DELETE FROM %s.%s WHERE flag_name = ?",
                    keyspace, table));

            healthCheckStmt = prepare("SELECT release_version FROM system.local");

            LOG.info("CassandraFeatureFlagDAO initialized successfully");

        } catch (Exception e) {
            LOG.error("Failed to initialize CassandraFeatureFlagDAO", e);
            throw new RuntimeException("Failed to initialize CassandraFeatureFlagDAO", e);
        }
    }

    private PreparedStatement prepare(String cql) {
        return session.prepare(SimpleStatement.builder(cql)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build());
    }

    private void initializeSchema(int replicationFactor) {
        // Create keyspace
        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '%d'} AND durable_writes = true",
                keyspace, replicationFactor);
        executeWithRetry(SimpleStatement.builder(createKeyspaceQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured keyspace {} exists", keyspace);

        // Create table
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "flag_name text PRIMARY KEY, " +
                        "flag_value text, " +
                        "updated_by text, " +
                        "updated_at timestamp" +
                        ")",
                keyspace, table);
        executeWithRetry(SimpleStatement.builder(createTableQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured table {}.{} exists", keyspace, table);
    }

    /**
     * Get a feature flag value.
     *
     * @param flagName the flag name
     * @return the flag value, or null if not found
     */
    public String getValue(String flagName) {
        try {
            BoundStatement bound = selectStmt.bind(flagName);
            ResultSet rs = executeWithRetry(bound);
            Row row = rs.one();

            if (row == null) {
                LOG.debug("Feature flag not found: {}", flagName);
                return null;
            }

            return row.getString("flag_value");

        } catch (Exception e) {
            LOG.error("Error fetching feature flag: {}", flagName, e);
            throw new RuntimeException("Error fetching feature flag: " + flagName, e);
        }
    }

    /**
     * Set a feature flag value.
     *
     * @param flagName the flag name
     * @param value the flag value
     */
    public void putValue(String flagName, String value) {
        try {
            String updatedBy = System.getenv().getOrDefault("HOSTNAME", "unknown");
            BoundStatement bound = upsertStmt.bind(flagName, value, updatedBy, Instant.now());
            executeWithRetry(bound);
            LOG.info("Feature flag updated: {} = {}", flagName, value);

        } catch (Exception e) {
            LOG.error("Error updating feature flag: {} = {}", flagName, value, e);
            throw new RuntimeException("Error updating feature flag: " + flagName, e);
        }
    }

    /**
     * Delete a feature flag.
     *
     * @param flagName the flag name
     */
    public void removeValue(String flagName) {
        try {
            BoundStatement bound = deleteStmt.bind(flagName);
            executeWithRetry(bound);
            LOG.info("Feature flag deleted: {}", flagName);

        } catch (Exception e) {
            LOG.error("Error deleting feature flag: {}", flagName, e);
            throw new RuntimeException("Error deleting feature flag: " + flagName, e);
        }
    }

    /**
     * Check if Cassandra is healthy and accessible.
     *
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            ResultSet rs = session.execute(healthCheckStmt.bind());
            return rs.iterator().hasNext();
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed: {}", e.getMessage());
            return false;
        }
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(Statement<T> statement) {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < MAX_RETRIES) {
            try {
                return session.execute(statement);
            } catch (DriverTimeoutException e) {
                lastException = e;
                retryCount++;
                LOG.warn("Retry attempt {} for statement execution due to: {}", retryCount, e.getMessage());

                if (retryCount < MAX_RETRIES) {
                    try {
                        long backoff = INITIAL_BACKOFF.toMillis() * (long) Math.pow(2, retryCount - 1);
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry backoff", ie);
                    }
                }
            } catch (Exception e) {
                // Non-retryable exception
                throw new RuntimeException("Non-retryable exception during statement execution", e);
            }
        }

        LOG.error("Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new RuntimeException("Failed to execute statement after " + MAX_RETRIES + " retries", lastException);
    }

    @Override
    public void close() {
        if (session != null && !session.isClosed()) {
            session.close();
            LOG.info("CassandraFeatureFlagDAO session closed");
        }
    }

    /**
     * Shutdown the singleton instance.
     */
    public static synchronized void shutdown() {
        if (INSTANCE != null) {
            INSTANCE.close();
            INSTANCE = null;
            initialized = false;
            LOG.info("CassandraFeatureFlagDAO singleton shut down");
        }
    }
}
