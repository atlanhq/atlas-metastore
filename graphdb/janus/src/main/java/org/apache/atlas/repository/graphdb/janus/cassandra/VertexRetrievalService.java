package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.elasticsearch.common.Strings;
import org.janusgraph.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;

/**
 * Main entry point for the batch vertex retrieval system.
 * Coordinates the retrieval process and delegates to specialized components.
 */

public class VertexRetrievalService {
    private static final Logger LOG = LoggerFactory.getLogger(VertexRetrievalService.class);

    private final VertexDataRepository repository;
    private final JacksonVertexSerializer serializer;
    private final int defaultBatchSize;

    /**
     * Creates a new BatchVertexRetrievalService with custom configuration.
     *
     * @param session The Cassandra session
     */
    public VertexRetrievalService(CqlSession session, ObjectMapper objectMapper) {
        this.repository = new CassandraVertexDataRepository(session,  objectMapper,
                AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString(),
                AtlasConfiguration.ATLAS_CASSANDRA_VERTEX_TABLE.getString());
        this.serializer = new JacksonVertexSerializer(objectMapper);
        //AtlasConfiguration.ATLAS_CASSANDRA_BATCH_SIZE.getInt();
        this.defaultBatchSize = 10;
    }

    /**
     * Retrieves vertex by its ID.
     *
     * @param vertexId The ID of vertex to retrieve
     * @return A DynamicVertex
     */
    public DynamicVertex retrieveVertex(String vertexId) throws AtlasBaseException {
        Map<String, DynamicVertex> ret = retrieveVertices(Collections.singletonList(vertexId));
        return ret.get(vertexId);
    }

    public void insertVertices(List<AtlasVertex> vertices) throws AtlasBaseException {
        Map<String, String> toInsert = new HashMap<>(vertices.size());
        vertices.stream()
                .filter(x -> ((AtlasJanusVertex) x).getDynamicVertex().hasProperties())
                .forEach(x -> toInsert.put(x.getIdForDisplay(), serializer.serialize(((AtlasJanusVertex) x).getDynamicVertex())));

        repository.insertVertices(toInsert);
    }

    /**
     * Retrieves multiple vertices by their IDs.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @return A map of vertex ID to dynamic vertex data
     */
    public Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds) throws AtlasBaseException {
        return retrieveVertices(vertexIds, defaultBatchSize);
    }

    /**
     * Retrieves multiple vertices by their IDs with custom batch size.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @param batchSize The batch size to use
     * @return A map of vertex ID to dynamic vertex data
     */
    private Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds, int batchSize) throws AtlasBaseException {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, DynamicVertex> results = new HashMap<>();

        for (int i = 0; i < vertexIds.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, vertexIds.size());
            List<String> batch = vertexIds.subList(i, endIndex);

            // Use the JsonNode-based method for more efficient processing
            Map<String, JsonNode> jsonNodeMap = repository.fetchVerticesAsJsonNodes(batch);
            Map<String, DynamicVertex> batchResults = convertJsonNodesToVertices(jsonNodeMap);

            results.putAll(batchResults);
        }

        return results;
    }

    /**
     * Converts pre-parsed JsonNodes to DynamicVertex objects.
     * This is more efficient as it avoids parsing the JSON string again.
     */
    private Map<String, DynamicVertex> convertJsonNodesToVertices(Map<String, JsonNode> jsonNodeMap) {
        Map<String, DynamicVertex> vertexMap = new HashMap<>();

        for (Map.Entry<String, JsonNode> entry : jsonNodeMap.entrySet()) {
            String id = entry.getKey();
            JsonNode jsonNode = entry.getValue();

            try {
                // Use the direct JsonNode deserialization method
                DynamicVertex vertex = serializer.deserializeFromNode(jsonNode);
                vertexMap.put(id, vertex);
            } catch (Exception e) {
                LOG.error("Error converting JsonNode to DynamicVertex for ID: {}", id, e);
            }
        }

        return vertexMap;
    }
}