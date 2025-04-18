package org.apache.atlas.repository.cassaandra;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enhanced Cassandra implementation for vertex data repository with advanced
 * features like connection pooling, retry mechanisms, and better error handling.
 */
class EnhancedCassandraRepository implements VertexDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedCassandraRepository.class);

    // Maximum number of items in an IN clause for Cassandra
    private static final int MAX_IN_CLAUSE_ITEMS = 100;

    // Maximum number of retry attempts for failed queries
    private static final int MAX_RETRY_ATTEMPTS = 3;

    // Base delay for exponential backoff (in milliseconds)
    private static final long BASE_RETRY_DELAY_MS = 100;

    private final CqlSession session;
    private final String keyspace;
    private final String tableName;
    private final Map<Integer, PreparedStatement> batchSizeToStatement = new ConcurrentHashMap<>();
  //  private final ConnectionHealthMonitor healthMonitor;

    /**
     * Creates a new enhanced Cassandra repository.
     *
     * @param session   The Cassandra session
     * @param keyspace  The keyspace containing vertex data
     * @param tableName The table name for vertex data
     */
    @Inject
    public EnhancedCassandraRepository(CqlSession session, String keyspace, String tableName) {
        this.session = session;
        this.keyspace = keyspace;
        this.tableName = tableName;
       // this.healthMonitor = new ConnectionHealthMonitor(session);

        // Start health monitoring
      //  this.healthMonitor.start();
    }

    /**
     * Fetches vertex JSON data as strings.
     */
    @Override
    public Map<String, String> fetchVerticesJsonData(List<String> vertexIds) throws AtlasBaseException {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Filter out blank IDs
        List<String> sanitizedIds = new ArrayList<>();
        for (String id : vertexIds) {
            if (StringUtils.isNotBlank(id)) {
                sanitizedIds.add(id);
            }
        }

        if (sanitizedIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Split large batches into smaller ones to avoid Cassandra limitations
        if (sanitizedIds.size() > MAX_IN_CLAUSE_ITEMS) {
            return fetchLargeBatch(sanitizedIds);
        } else {
            return fetchSingleBatch(sanitizedIds, 0);
        }
    }

    /**
     * Fetches vertex data as parsed JsonElements instead of raw strings.
     * This is more efficient when the caller needs to work with the JSON directly.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to parsed JsonElement
     */
    @Override
    public Map<String, JsonElement> fetchVerticesAsJsonElements(List<String> vertexIds) throws AtlasBaseException {
        Map<String, String> jsonStrings = fetchVerticesJsonData(vertexIds);
        Map<String, JsonElement> jsonElements = new HashMap<>(jsonStrings.size());

        JsonParser parser = new JsonParser();
        for (Map.Entry<String, String> entry : jsonStrings.entrySet()) {
            try {
                JsonElement element = parser.parse(entry.getValue());
                jsonElements.put(entry.getKey(), element);
            } catch (JsonSyntaxException e) {
                LOG.warn("Failed to parse JSON for vertex ID {}: {}", entry.getKey(), e.getMessage());
                // Skip invalid JSON entries
            }
        }

        return jsonElements;
    }

    /**
     * Fetches a large batch of vertices by splitting into smaller batches.
     */
    private Map<String, String> fetchLargeBatch(List<String> vertexIds) {
        Map<String, String> results = new HashMap<>();
        List<List<String>> batches = Lists.partition(vertexIds, MAX_IN_CLAUSE_ITEMS);

        // Process each batch
        for (List<String> batch : batches) {
            try {
                Map<String, String> batchResults = fetchSingleBatch(batch, 0);
                results.putAll(batchResults);
            } catch (Exception e) {
                LOG.error("Error fetching batch of vertex data", e);
                // Continue with other batches even if one fails
            }
        }

        return results;
    }

    /**
     * Fetches a single batch of vertices with retry logic.
     * Parses JSON data into JsonElement trees using Gson.
     */
    private Map<String, String> fetchSingleBatch(List<String> vertexIds, int retryAttempt) throws AtlasBaseException {
        try {
            // Check if connection is healthy
            // !healthMonitor.isHealthy() &&
            if (retryAttempt < MAX_RETRY_ATTEMPTS) {
                // Connection issues detected, delay and retry
                long delayMs = calculateExponentialBackoff(retryAttempt);
                LOG.warn("Cassandra connection unhealthy, retrying after {}ms (attempt {}/{})",
                        delayMs, retryAttempt + 1, MAX_RETRY_ATTEMPTS);

                Thread.sleep(delayMs);
                return fetchSingleBatch(vertexIds, retryAttempt + 1);
            }

            // Get or prepare the statement for this batch size
            PreparedStatement statement = getPreparedStatementForBatchSize(vertexIds.size());

            // Bind values
            BoundStatement boundStatement = statement.bind();
            for (int i = 0; i < vertexIds.size(); i++) {
                boundStatement = boundStatement.setString(i, vertexIds.get(i));
            }

            // Set query timeout and other options
            boundStatement = boundStatement
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            // Execute the query
            ResultSet resultSet = (ResultSet) session.execute(String.valueOf(boundStatement));

            // Process the results and parse JSON with Gson
            Map<String, String> results = new HashMap<>();
            JsonParser jsonParser = new JsonParser();

            for (Row row : resultSet) {
                String id = row.getString("id");
                String jsonData = row.getString("json_data");

                if (StringUtils.isNotBlank(jsonData)) {
                    try {
                        // Parse JSON to validate it's proper JSON before storing
                        JsonElement jsonElement = jsonParser.parse(jsonData);
                        LOG.debug("Successfully parsed JSON for vertex ID: {}", id);

                        // Store the validated JSON string
                        results.put(id, jsonData);
                    } catch (JsonSyntaxException e) {
                        LOG.warn("Invalid JSON data for vertex ID {}: {}", id, e.getMessage());
                        // Option: store malformed JSON with a warning, or skip it
                        // Here we're choosing to store it with a warning
                        results.put(id, jsonData);
                    }
                }
            }

            return results;

        } catch (NoHostAvailableException | QueryExecutionException e) {
            // These are potentially recoverable errors
            if (retryAttempt < MAX_RETRY_ATTEMPTS) {
                long delayMs = calculateExponentialBackoff(retryAttempt);
                LOG.warn("Recoverable error, retrying after {}ms (attempt {}/{}): {}",
                        delayMs, retryAttempt + 1, MAX_RETRY_ATTEMPTS, e.getMessage());

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AtlasBaseException("Interrupted during retry delay", ie);
                }

                return fetchSingleBatch(vertexIds, retryAttempt + 1);
            } else {
                throw new AtlasBaseException("Max retry attempts exceeded", e);
            }

        } catch (QueryValidationException e) {
            // These are non-recoverable errors with the query itself
            LOG.error("Invalid query error fetching vertex data: {}", e.getMessage());
            throw new AtlasBaseException("Invalid query: " + e.getMessage(), e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AtlasBaseException("Interrupted during retry delay", e);

        } catch (Exception e) {
            // For unexpected errors
            LOG.error("Unexpected error fetching vertex data", e);
            throw new AtlasBaseException("Failed to fetch vertex data: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a prepared statement for a specific batch size, creating it if needed.
     */
    private PreparedStatement getPreparedStatementForBatchSize(int batchSize) {
        return batchSizeToStatement.computeIfAbsent(batchSize, this::prepareStatementForBatchSize);
    }

    /**
     * Prepares a statement for a specific batch size.
     */
    private PreparedStatement prepareStatementForBatchSize(int batchSize) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT id, json_data FROM ")
                .append(keyspace)
                .append(".")
                .append(tableName)
                .append(" WHERE id IN (");

        for (int i = 0; i < batchSize; i++) {
            if (i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("?");
        }
        queryBuilder.append(")");

        return (PreparedStatement) session.prepare(queryBuilder.toString());
    }

    /**
     * Calculates exponential backoff delay for retries.
     */
    private long calculateExponentialBackoff(int retryAttempt) {
        return (long) (BASE_RETRY_DELAY_MS * Math.pow(2, retryAttempt)) +
                ThreadLocalRandom.current().nextLong(50); // Add jitter
    }

    /**
     * Shutdown the repository and its resources.
     */
    public void shutdown() {
        //healthMonitor.stop();
    }

    /**
     * Monitors Cassandra connection health in the background.
     */
    private static class ConnectionHealthMonitor {
        private static final Logger LOG = LoggerFactory.getLogger(ConnectionHealthMonitor.class);

        private final CqlSession session;
        private final ScheduledExecutorService scheduler;
        private final AtomicBoolean isHealthy = new AtomicBoolean(true);
        private ScheduledFuture<?> monitorTask;

        @Inject
        public ConnectionHealthMonitor(CqlSession session) {
            this.session = session;
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "cassandra-health-monitor");
                        t.setDaemon(true);
                        return t;
                    }
            );
        }

        /**
         * Starts the health monitoring.
         */
        public void start() {
            monitorTask = scheduler.scheduleAtFixedRate(
                    this::checkHealth, 0, 30, TimeUnit.SECONDS
            );
        }

        /**
         * Stops the health monitoring.
         */
        public void stop() {
            if (monitorTask != null) {
                monitorTask.cancel(true);
            }
            scheduler.shutdown();
        }

        /**
         * Checks if the connection is healthy.
         */
        public boolean isHealthy() {
            return isHealthy.get();
        }

        /**
         * Checks the health of the Cassandra connection.
         */
        private void checkHealth() {
            try {
                // Simple health check query
                ResultSet resultSet = (ResultSet) session.execute("SELECT release_version FROM system.local");
                Row row = resultSet.one();
                if (row != null) {
                    String version = row.getString("release_version");
                    LOG.debug("Cassandra connection healthy, version: {}", version);
                    isHealthy.set(true);
                } else {
                    LOG.warn("Cassandra health check returned no rows");
                    isHealthy.set(false);
                }
            } catch (Exception e) {
                LOG.warn("Cassandra connection health check failed: {}", e.getMessage());
                isHealthy.set(false);
            }
        }
    }


}