package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Lists;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Enhanced Cassandra implementation for vertex data repository with advanced
 * features like connection pooling, retry mechanisms, and better error handling.
 */
class CassandraVertexDataRepository implements VertexDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraVertexDataRepository.class);

    // Maximum number of items in an IN clause for Cassandra
    // make it configurable
    private static final int MAX_IN_CLAUSE_ITEMS = 100;
    private final CqlSession session;
    private final String keyspace;
    private final String tableName;
    private final Map<Integer, PreparedStatement> batchSizeToStatement = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    private static String INSERT_VERTEX = "INSERT into %s.%s (bucket, id, json_data, updated_at) values (%s, '%s', '%s', %s)";

    private static String DROP_VERTEX = "DELETE from %s.%s where bucket = %s AND id = '%s'";

    /**
     * Creates a new enhanced Cassandra repository.
     *
     * @param session   The Cassandra session
     */
    @Inject
    public CassandraVertexDataRepository(CqlSession session) {
        this.session = session;
        this.keyspace = AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString();
        this.tableName = AtlasConfiguration.ATLAS_CASSANDRA_VERTEX_TABLE.getString();

        this.objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("NumbersAsStringModule");
        module.addDeserializer(Object.class, new NumbersAsStringObjectDeserializer());
        objectMapper.registerModule(module);

        // Initialize required Cassandra resources (tables) on startup
        createResources();
    }

    @Override
    public void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("insertVertices");

        try {
            StringBuilder batchQuery = new StringBuilder();
            batchQuery.append("BEGIN BATCH ");

            for (String vertexId : serialisedVertices.keySet()) {
                int bucket = calculateBucket(vertexId);
                String insert = String.format(INSERT_VERTEX,
                        keyspace,
                        tableName,
                        bucket,
                        vertexId,
                        serialisedVertices.get(vertexId),
                        RequestContext.get().getRequestTime());
                batchQuery.append(insert).append(";");
            }

            batchQuery.append("APPLY BATCH;");
            try {
                session.execute(batchQuery.toString());
            } catch (InvalidQueryException iqe) {
                if (isUnconfiguredTableError(iqe)) {
                    LOG.warn("Vertex table not found on insert. Attempting to create resources and retry. Error={}", iqe.getMessage());
                    // Best-effort ensure the table exists, then retry once
                    createResources();
                    session.execute(batchQuery.toString());
                } else {
                    throw iqe;
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void dropVertices(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("dropVertices");
        try {
            StringBuilder batchQuery = new StringBuilder();
            batchQuery.append("BEGIN BATCH ");
            for (String vertexId : vertexIds) {
                int bucket = calculateBucket(vertexId);
                String insert = String.format(DROP_VERTEX,
                        keyspace,
                        tableName,
                        bucket,
                        vertexId);
                batchQuery.append(insert).append(";");
            }
            batchQuery.append("APPLY BATCH;");
            try {
                session.execute(batchQuery.toString());
            } catch (InvalidQueryException iqe) {
                if (isUnconfiguredTableError(iqe)) {
                    LOG.warn("Vertex table not found on delete. Attempting to create resources and retry. Error={}", iqe.getMessage());
                    createResources();
                    session.execute(batchQuery.toString());
                } else {
                    throw iqe;
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
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

        // fix this code to use bucket id
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
        queryBuilder.append(" ALLOW FILTERING");

        return session.prepare(queryBuilder.toString());
    }

    /**
     * Initialize Cassandra resources required by this repository.
     * Creates the vertex table if it does not already exist using configured keyspace and table name.
     */
    private void createResources() {
        String cql = "CREATE TABLE IF NOT EXISTS %s.%s (\n" +
                "    id text,\n" +
                "    bucket int,\n" +
                "    json_data text,\n" +
                "    updated_at timestamp,\n" +
                "    PRIMARY KEY ((bucket), id)\n" +
                ") WITH compaction = {" +
                "'class': 'SizeTieredCompactionStrategy', " +
                "'min_threshold': 4, " +
                "'max_threshold': 32" +
                "};";

        try {
            String formatted = String.format(cql, keyspace, tableName);
            SimpleStatement stmt = SimpleStatement.builder(formatted)
                    .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                    .build();
            executeSchemaChangeWithRetry(stmt, 5, Duration.ofMillis(100));
            waitForTableAvailability(Duration.ofSeconds(5));
            if (LOG.isInfoEnabled()) {
                LOG.info("Ensured Cassandra table exists: {}.{}", keyspace, tableName);
            }
        } catch (Exception e) {
            LOG.error("Failed to create or verify Cassandra table {}.{}: {}", keyspace, tableName, e.getMessage(), e);
        }
    }

    private boolean isUnconfiguredTableError(Throwable t) {
        String msg = t != null && t.getMessage() != null ? t.getMessage().toLowerCase(Locale.ROOT) : "";
        return msg.contains("unconfigured table") || msg.contains("unknown table") || msg.contains("does not exist");
    }

    private void executeSchemaChangeWithRetry(SimpleStatement stmt, int maxRetries, Duration initialBackoff) {
        int attempt = 0;
        while (true) {
            try {
                session.execute(stmt);
                return;
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    throw e;
                }
                long backoff = (long) (initialBackoff.toMillis() * Math.pow(2, attempt - 1));
                LOG.warn("Schema change failed (attempt {}/{}). Retrying in {} ms. Error={}", attempt, maxRetries, backoff, e.getMessage());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during schema change retry backoff", ie);
                }
            }
        }
    }

    private void waitForTableAvailability(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                Optional<com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata> ks = session.getMetadata().getKeyspace(keyspace);
                if (ks.isPresent() && ks.get().getTable(tableName).isPresent()) {
                    return;
                }
            } catch (Exception ignored) {
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        LOG.warn("Timed out waiting for table {}.{} to become available in metadata.", keyspace, tableName);
    }

    private int calculateBucket(String vertexId) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("calculateBucket");

        try {
            int numBuckets = 2 << 5; // 2^5=32
            return Math.abs(vertexId.hashCode() % numBuckets);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Fetches vertices directly as DynamicVertex objects without intermediate JSON serialization/deserialization.
     * This is the most efficient method for retrieving vertices from the database.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to DynamicVertex object
     */
    @Override
    public Map<String, DynamicVertex> fetchVerticesDirectly(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("fetchVerticesDirectly");
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

        Map<String, DynamicVertex> results = new HashMap<>();

        // Split large batches into smaller ones to avoid Cassandra limitations
        if (sanitizedIds.size() > MAX_IN_CLAUSE_ITEMS) {
            List<List<String>> batches = Lists.partition(sanitizedIds, MAX_IN_CLAUSE_ITEMS);

            // Process each batch
            for (List<String> batch : batches) {
                try {
                    Map<String, DynamicVertex> batchResults = fetchSingleBatchDirectly(batch);
                    results.putAll(batchResults);
                } catch (Exception e) {
                    LOG.error("Error fetching batch of vertex data directly", e);
                    // Continue with other batches even if one fails
                }
            }
        } else {
            results = fetchSingleBatchDirectly(sanitizedIds);
        }

        RequestContext.get().endMetricRecord(recorder);

        return results;
    }

    /**
     * Fetches a single batch of vertices directly as DynamicVertex objects using a single Cassandra call,
     * even if IDs span multiple buckets.
     * Uses "WHERE bucket_id IN (...) AND id IN (...)" approach.
     */
    private Map<String, DynamicVertex> fetchSingleBatchDirectly(List<String> vertexIdsInBatch) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder mainRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly_SingleCallStrategy");
        Map<String, DynamicVertex> results = new HashMap<>();

        if (vertexIdsInBatch == null || vertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        // 1. Determine unique bucket_ids and unique, sanitized vertex_ids for the current batch.
        //    Also, store the original mapping to validate/use results.
        Map<String, Integer> vertexIdToItsBucketMap = new HashMap<>();
        Set<Integer> uniqueBucketIdsInBatch = new HashSet<>();
        List<String> uniqueSanitizedVertexIdsInBatch = new ArrayList<>(); // Order matters for binding to IN clause

        for (String vertexId : vertexIdsInBatch) {
            if (StringUtils.isNotBlank(vertexId)) {
                try {
                    int bucket = calculateBucket(vertexId);
                    if (!vertexIdToItsBucketMap.containsKey(vertexId)) { // Ensure each original vertexId is processed once for uniqueness
                        vertexIdToItsBucketMap.put(vertexId, bucket);
                        uniqueSanitizedVertexIdsInBatch.add(vertexId); // Add to list for IN clause
                        uniqueBucketIdsInBatch.add(bucket);             // Add to set for IN clause
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Skipping vertexId '{}' as it's not a valid long for bucket calculation (single call strategy).", vertexId);
                }
            }
        }

        if (uniqueSanitizedVertexIdsInBatch.isEmpty() || uniqueBucketIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        try {
            // 2. Construct the dynamic query string
            StringBuilder queryBuilder = new StringBuilder("SELECT id, json_data FROM ")
                    .append(keyspace).append(".").append(tableName)
                    .append(" WHERE bucket IN (");
            for (int i = 0; i < uniqueBucketIdsInBatch.size(); i++) {
                queryBuilder.append(i == 0 ? "?" : ", ?");
            }
            queryBuilder.append(") AND id IN (");
            for (int i = 0; i < uniqueSanitizedVertexIdsInBatch.size(); i++) {
                queryBuilder.append(i == 0 ? "?" : ", ?");
            }
            queryBuilder.append(")");

            String cqlQuery = queryBuilder.toString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing single Cassandra call for batch: Query={}, Buckets={}, IDs={}", cqlQuery, uniqueBucketIdsInBatch.size(), uniqueSanitizedVertexIdsInBatch.size());
            }

            // Prepare the statement (dynamically, less efficient for reuse than cached)
            PreparedStatement preparedStatement = session.prepare(cqlQuery);
            BoundStatement boundStatement = preparedStatement.bind();

            int bindIndex = 0;
            for (Integer bucketId : uniqueBucketIdsInBatch) {
                boundStatement = boundStatement.setInt(bindIndex++, bucketId);
            }
            for (String vertexId : uniqueSanitizedVertexIdsInBatch) {
                boundStatement = boundStatement.setString(bindIndex++, vertexId);
            }

            boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            ResultSet resultSet = session.execute(boundStatement);

            // 3. Process results.
            for (Row row : resultSet) {
                String id = row.getString("id");

                if (vertexIdToItsBucketMap.containsKey(id)) {
                    String jsonData = row.getString("json_data");
                    try {
                        AtlasPerfMetrics.MetricRecorder deserializeRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly_DeserializeJson");
                        Map<String, Object> props = objectMapper.readValue(jsonData, Map.class);
                        RequestContext.get().endMetricRecord(deserializeRecorder);

                        DynamicVertex vertex = new DynamicVertex(props);
                        if (!vertex.hasProperty("id")) { // Ensure ID is present
                            vertex.setProperty("id", id);
                        }
                        results.put(id, vertex);
                    } catch (JsonProcessingException e) {
                        LOG.warn("Failed to parse JSON for DynamicVertex ID {}: {}", id, e.getMessage());
                    } catch (Exception e) { // Catch broader exceptions during DynamicVertex creation
                        LOG.warn("Failed to convert or process data for DynamicVertex ID {}: {}", id, e.getMessage());
                    }
                } else {
                    LOG.warn("Fetched vertex ID {} which was not in the original request map for this specific batch processing. Ignoring.", id);
                }
            }
            return results;

        } catch (QueryValidationException e) {
            LOG.error("Invalid query error during single call batch fetch strategy:  Error=\'{}\'.", e.getMessage(), e);
            throw new AtlasBaseException("Invalid query for single call batch fetch: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error during single call batch fetch strategy", e);
            throw new AtlasBaseException("Failed to fetch vertex data in single call strategy: " + e.getMessage(), e);
        } finally {
            RequestContext.get().endMetricRecord(mainRecorder);
        }
    }
}