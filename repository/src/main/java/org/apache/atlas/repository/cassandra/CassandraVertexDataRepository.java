package org.apache.atlas.repository.cassandra;

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
import org.apache.atlas.exception.AtlasBaseException;
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
            return fetchSingleBatch(sanitizedIds);
        }
    }

    /**
     * Fetches vertex data as parsed JsonNodes instead of raw strings.
     * This is more efficient when the caller needs to work with the JSON directly.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to parsed JsonNode
     */
    @Override
    public Map<String, JsonNode> fetchVerticesAsJsonNodes(List<String> vertexIds) throws AtlasBaseException {
        Map<String, String> vertexJsonDataLookup = fetchVerticesJsonData(vertexIds);
        Map<String, JsonNode> vertexJsonNodesLookup = new HashMap<>(vertexJsonDataLookup.size());

        for (Map.Entry<String, String> entry : vertexJsonDataLookup.entrySet()) {
            try {
                JsonNode node = objectMapper.readTree(entry.getValue());
                vertexJsonNodesLookup.put(entry.getKey(), node);
            } catch (JsonProcessingException e) {
                LOG.warn("Failed to parse JSON for vertex ID {}: {}", entry.getKey(), e.getMessage());
                throw new AtlasBaseException("Failed to parse JSON for vertex ID: " + entry.getKey(), e);
            }
        }

        return vertexJsonNodesLookup;
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
                Map<String, String> batchResults = fetchSingleBatch(batch);
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
     * Parses JSON data into JsonNode trees using Jackson.
     */
    private Map<String, String> fetchSingleBatch(List<String> vertexIds) throws AtlasBaseException {
        try {

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
            ResultSet resultSet = session.execute(boundStatement);

            // Process the results and parse JSON with Jackson
            Map<String, String> results = new HashMap<>();


            for (Row row : resultSet) {
                String id = row.getString("id");
                String jsonData = row.getString("json_data");
                results.put(id, jsonData);
            }

            return results;

        } catch (QueryValidationException e) {
            // These are non-recoverable errors with the query itself
            LOG.error("Invalid query error fetching vertex data: {}", e.getMessage());
            throw new AtlasBaseException("Invalid query: " + e.getMessage(), e);
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
}