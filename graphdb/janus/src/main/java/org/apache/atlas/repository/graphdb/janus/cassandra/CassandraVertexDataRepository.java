package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Lists;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.atlas.utils.AtlasEntityUtil.calculateBucket; // LEGACY: remove when all tenants migrated off bucket schema

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
    private final PreparedStatement insertVertexStatement;
    private final PreparedStatement deleteVertexStatement;
    private final ObjectMapper objectMapper;

    // LEGACY: remove when all tenants migrated off bucket schema
    private final boolean hasBucketColumn;

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
        this.hasBucketColumn = detectBucketColumn(session, keyspace, tableName);

        if (hasBucketColumn) {
            LOG.info("Detected LEGACY bucket-based schema for table {}.{} - using bucket-aware queries", keyspace, tableName);
            this.insertVertexStatement = session.prepare(String.format(
                    "INSERT INTO %s.%s (bucket, id, json_data, updated_at) VALUES (?, ?, ?, ?)",
                    keyspace, tableName));
            this.deleteVertexStatement = session.prepare(String.format(
                    "DELETE FROM %s.%s WHERE bucket = ? AND id = ?",
                    keyspace, tableName));
        } else {
            LOG.info("Detected new schema (no bucket column) for table {}.{} - using direct id queries", keyspace, tableName);
            this.insertVertexStatement = session.prepare(String.format(
                    "INSERT INTO %s.%s (id, json_data, updated_at) VALUES (?, ?, ?)",
                    keyspace, tableName));
            this.deleteVertexStatement = session.prepare(String.format(
                    "DELETE FROM %s.%s WHERE id = ?",
                    keyspace, tableName));
        }

        this.objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("NumbersAsStringModule");
        module.addDeserializer(Object.class, new NumbersAsStringObjectDeserializer());
        objectMapper.registerModule(module);
    }

    /**
     * Detects whether the assets table has a 'bucket' column in its schema.
     * Uses CqlSession cached metadata (no network call).
     * Returns false (new schema) if detection fails, as a safe default for new tenants.
     *
     * LEGACY: Remove this method when all tenants are migrated off the bucket schema.
     */
    private static boolean detectBucketColumn(CqlSession session, String keyspace, String tableName) {
        try {
            return session.getMetadata()
                    .getKeyspace(CqlIdentifier.fromCql(keyspace))
                    .flatMap(ks -> ks.getTable(CqlIdentifier.fromCql(tableName)))
                    .flatMap(table -> table.getColumn(CqlIdentifier.fromCql("bucket")))
                    .isPresent();
        } catch (Exception e) {
            LOG.warn("Failed to detect schema for table {}.{}, defaulting to no-bucket schema: {}",
                    keyspace, tableName, e.getMessage());
            return false;
        }
    }

    @Override
    public void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("insertVertices");

        try {
            if (serialisedVertices == null || serialisedVertices.isEmpty()) {
                return;
            }

            if (hasBucketColumn) {
                insertVerticesWithBucket(serialisedVertices);
            } else {
                insertVerticesNoBucket(serialisedVertices);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void insertVerticesNoBucket(Map<String, String> serialisedVertices) {
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        Instant updatedAt = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
        for (Map.Entry<String, String> entry : serialisedVertices.entrySet()) {
            BoundStatement boundStatement = insertVertexStatement.bind(entry.getKey(), entry.getValue(), updatedAt);
            batchBuilder.addStatement(boundStatement);
        }
        session.execute(batchBuilder.build());
    }

    /**
     * LEGACY: Insert vertices using bucket-partitioned schema.
     * Remove when all tenants are migrated off the bucket schema.
     */
    private void insertVerticesWithBucket(Map<String, String> serialisedVertices) {
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        Instant updatedAt = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
        for (Map.Entry<String, String> entry : serialisedVertices.entrySet()) {
            int bucket = calculateBucket(entry.getKey());
            BoundStatement boundStatement = insertVertexStatement.bind(bucket, entry.getKey(), entry.getValue(), updatedAt);
            batchBuilder.addStatement(boundStatement);
        }
        session.execute(batchBuilder.build());
    }

    @Override
    public void dropVertices(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("dropVertices");
        try {
            if (vertexIds == null || vertexIds.isEmpty()) {
                return;
            }

            if (hasBucketColumn) {
                dropVerticesWithBucket(vertexIds);
            } else {
                dropVerticesNoBucket(vertexIds);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void dropVerticesNoBucket(List<String> vertexIds) {
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        for (String vertexId : vertexIds) {
            batchBuilder.addStatement(deleteVertexStatement.bind(vertexId));
        }
        session.execute(batchBuilder.build());
    }

    /**
     * LEGACY: Drop vertices using bucket-partitioned schema.
     * Remove when all tenants are migrated off the bucket schema.
     */
    private void dropVerticesWithBucket(List<String> vertexIds) {
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        for (String vertexId : vertexIds) {
            int bucket = calculateBucket(vertexId);
            batchBuilder.addStatement(deleteVertexStatement.bind(bucket, vertexId));
        }
        session.execute(batchBuilder.build());
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

        return session.prepare(queryBuilder.toString());
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

        try {
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
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return results;
    }

    /**
     * Fetches a single batch of vertices directly as DynamicVertex objects using a single Cassandra call.
     */
    private Map<String, DynamicVertex> fetchSingleBatchDirectly(List<String> vertexIdsInBatch) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder mainRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly");
        Map<String, DynamicVertex> results = new HashMap<>();

        if (vertexIdsInBatch == null || vertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        List<String> uniqueSanitizedVertexIdsInBatch = new ArrayList<>();
        Set<String> seenIds = new HashSet<>();

        for (String vertexId : vertexIdsInBatch) {
            if (StringUtils.isNotBlank(vertexId)) {
                if (seenIds.add(vertexId)) {
                    uniqueSanitizedVertexIdsInBatch.add(vertexId);
                }
            }
        }

        if (uniqueSanitizedVertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        try {
            if (hasBucketColumn) {
                return fetchSingleBatchWithBucket(uniqueSanitizedVertexIdsInBatch);
            } else {
                return fetchSingleBatchNoBucket(uniqueSanitizedVertexIdsInBatch);
            }
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

    private Map<String, DynamicVertex> fetchSingleBatchNoBucket(List<String> vertexIds) {
        Map<String, DynamicVertex> results = new HashMap<>();

        PreparedStatement preparedStatement = getPreparedStatementForBatchSize(vertexIds.size());
        BoundStatement boundStatement = preparedStatement.bind();

        int bindIndex = 0;
        for (String vertexId : vertexIds) {
            boundStatement = boundStatement.setString(bindIndex++, vertexId);
        }

        boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = session.execute(boundStatement);

        processResultRows(resultSet, results);
        return results;
    }

    /**
     * LEGACY: Fetch vertices using bucket-partitioned schema.
     * Builds a dynamic query: SELECT id, json_data FROM table WHERE bucket IN (?,?,...) AND id IN (?,?,...)
     * Remove when all tenants are migrated off the bucket schema.
     */
    private Map<String, DynamicVertex> fetchSingleBatchWithBucket(List<String> vertexIds) {
        Map<String, DynamicVertex> results = new HashMap<>();

        Set<Integer> uniqueBucketIds = new HashSet<>();
        for (String vertexId : vertexIds) {
            uniqueBucketIds.add(calculateBucket(vertexId));
        }

        StringBuilder queryBuilder = new StringBuilder("SELECT id, json_data FROM ")
                .append(keyspace).append(".").append(tableName)
                .append(" WHERE bucket IN (");
        for (int i = 0; i < uniqueBucketIds.size(); i++) {
            queryBuilder.append(i == 0 ? "?" : ", ?");
        }
        queryBuilder.append(") AND id IN (");
        for (int i = 0; i < vertexIds.size(); i++) {
            queryBuilder.append(i == 0 ? "?" : ", ?");
        }
        queryBuilder.append(")");

        PreparedStatement preparedStatement = session.prepare(queryBuilder.toString());
        BoundStatement boundStatement = preparedStatement.bind();

        int bindIndex = 0;
        for (Integer bucketId : uniqueBucketIds) {
            boundStatement = boundStatement.setInt(bindIndex++, bucketId);
        }
        for (String vertexId : vertexIds) {
            boundStatement = boundStatement.setString(bindIndex++, vertexId);
        }

        boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = session.execute(boundStatement);

        processResultRows(resultSet, results);
        return results;
    }

    /**
     * Processes result rows from Cassandra into DynamicVertex objects.
     */
    private void processResultRows(ResultSet resultSet, Map<String, DynamicVertex> results) {
        for (Row row : resultSet) {
            String id = row.getString("id");
            String jsonData = row.getString("json_data");
            try {
                AtlasPerfMetrics.MetricRecorder deserializeRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly_DeserializeJson");
                Map<String, Object> props = objectMapper.readValue(jsonData, Map.class);
                RequestContext.get().endMetricRecord(deserializeRecorder);

                DynamicVertex vertex = new DynamicVertex(props);
                if (!vertex.hasProperty("id")) {
                    vertex.setProperty("id", id);
                }
                results.put(id, vertex);
            } catch (JsonProcessingException e) {
                LOG.warn("Failed to parse JSON for DynamicVertex ID {}: {}", id, e.getMessage());
            } catch (Exception e) {
                LOG.warn("Failed to convert or process data for DynamicVertex ID {}: {}", id, e.getMessage());
            }
        }
    }
}
