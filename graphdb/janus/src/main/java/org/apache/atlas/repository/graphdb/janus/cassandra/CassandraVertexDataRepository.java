package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set; // TODO replace line 23 to this with single import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized Cassandra implementation for vertex data repository.
 * Removed all bucket-related logic as distribution is now handled by the DistributedIdGenerator.
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

    // Updated Queries: Removed 'bucket' column
    private static final String INSERT_VERTEX = "INSERT into %s.%s (id, json_data, updated_at) values ('%s', '%s', %s)";
    private static final String DROP_VERTEX = "DELETE from %s.%s where id = '%s'";

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
    }

    @Override
    public void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("insertVertices");

        try {
            StringBuilder batchQuery = new StringBuilder();
            batchQuery.append("BEGIN BATCH ");

            for (String vertexId : serialisedVertices.keySet()) {
                // Removed calculateBucket call
                String insert = String.format(INSERT_VERTEX,
                        keyspace,
                        tableName,
                        vertexId,
                        serialisedVertices.get(vertexId),
                        RequestContext.get().getRequestTime());
                batchQuery.append(insert).append(";");
            }

            batchQuery.append("APPLY BATCH;");
            session.execute(batchQuery.toString());
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
                // Removed calculateBucket call and renamed the variable name to delete
                String delete = String.format(DROP_VERTEX,
                        keyspace,
                        tableName,
                        vertexId);
                batchQuery.append(delete).append(";");
            }
            batchQuery.append("APPLY BATCH;");
            session.execute(batchQuery.toString());
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    //  /**
    //  * Gets a prepared statement for a specific batch size, creating it if needed.
    //   This function is not used anywhwere so commented. Check and remove
    // */ 
    // private PreparedStatement getPreparedStatementForBatchSize(int batchSize) {
    //     return batchSizeToStatement.computeIfAbsent(batchSize, this::prepareStatementForBatchSize);
    // }

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
        // Removed ALLOW FILTERING as 'id' is now the primary partition key

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
     * Fetches a single batch of vertices directly as DynamicVertex objects using a single Cassandra call,
     * Refactored fetch: Removed all bucket lookup logic.
     * Uses a direct "WHERE id IN (...)" query.
     */
    private Map<String, DynamicVertex> fetchSingleBatchDirectly(List<String> vertexIdsInBatch) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder mainRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly");
        Map<String, DynamicVertex> results = new HashMap<>();

        if (vertexIdsInBatch == null || vertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        List<String> uniqueIds = new ArrayList<>(new HashSet<>(vertexIdsInBatch));

        try {
            // 1. Construct the dynamic query string
            StringBuilder queryBuilder = new StringBuilder("SELECT id, json_data FROM ")
                    .append(keyspace).append(".").append(tableName)
                    .append(" WHERE id IN (");
            for (int i = 0; i < uniqueIds.size(); i++) {
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

            for (int i = 0; i < uniqueIds.size(); i++) {
                boundStatement = boundStatement.setString(i, uniqueIds.get(i));
            }

            boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            ResultSet resultSet = session.execute(boundStatement);

            // 2. Process results.
            for (Row row : resultSet) {
                String id = row.getString("id");
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
                } catch (JsonProcessingException e) { //TODO : @NNikhil, let's remove this specific catch block
                        LOG.warn("Failed to parse JSON for DynamicVertex ID {}: {}", id, e.getMessage());
                } catch (Exception e) { // Catch broader exceptions during DynamicVertex creation
                    LOG.warn("Failed to convert or process data for DynamicVertex ID {}: {}", id, e.getMessage());
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