package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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
    //private static String INSERT_VERTEX = "INSERT into %s.%s (id, json_data, updated_at) values ('%s', '%s', %s)";

    /**
     * Creates a new enhanced Cassandra repository.
     *
     * @param session   The Cassandra session
     * @param keyspace  The keyspace containing vertex data
     * @param tableName The table name for vertex data
     */
    @Inject
    public CassandraVertexDataRepository(CqlSession session, ObjectMapper objectMapper, String keyspace, String tableName) {
        this.session = session;
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException {
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
        session.execute(batchQuery.toString());
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

    private int calculateBucket(String vertexId) {
        int numBuckets = 2 << 5; // 2^5=32
        return (int) (Long.parseLong(vertexId) % numBuckets);
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

        return results;
    }

    /**
     * Fetches a single batch of vertices directly as DynamicVertex objects.
     * Avoids the JSON string parsing overhead.
     */
    private Map<String, DynamicVertex> fetchSingleBatchDirectly(List<String> vertexIds) throws AtlasBaseException {
        try {
            // Get or prepare the statement for this batch size
            PreparedStatement statement = getPreparedStatementForBatchSize(vertexIds.size());

            // Bind values
            BoundStatement boundStatement = statement.bind();
            for (int i = 0; i < vertexIds.size(); i++) {
                boundStatement = boundStatement.setString(i, vertexIds.get(i));
            }

            // Set query timeout and other options
            boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            // Execute the query
            ResultSet resultSet = session.execute(boundStatement);

            // Process the results directly into DynamicVertex objects
            Map<String, DynamicVertex> results = new HashMap<>(vertexIds.size());

            for (Row row : resultSet) {
                String id = row.getString("id");
                String jsonData = row.getString("json_data");

                try {
                    // Parse the JSON directly to a Map
                    Map<String, Object> props = objectMapper.readValue(jsonData, Map.class);

                    // Create a DynamicVertex with all properties from the Map
                    DynamicVertex vertex = new DynamicVertex(props);

                    // Ensure ID is set in the vertex properties
                    if (!vertex.hasProperty("id")) {
                        vertex.setProperty("id", id);
                    }

                    results.put(id, vertex);
                } catch (Exception e) {
                    LOG.warn("Failed to convert JSON to DynamicVertex for ID {}: {}", id, e.getMessage());
                    // Skip this vertex but continue processing others
                }
            }

            return results;

        } catch (QueryValidationException e) {
            // These are non-recoverable errors with the query itself
            LOG.error("Invalid query error fetching vertex data directly: {}", e.getMessage());
            throw new AtlasBaseException("Invalid query: " + e.getMessage(), e);
        } catch (Exception e) {
            // For unexpected errors
            LOG.error("Unexpected error fetching vertex data directly", e);
            throw new AtlasBaseException("Failed to fetch vertex data directly: " + e.getMessage(), e);
        }
    }

}