package org.apache.atlas.repository.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.exception.AtlasBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry point for the batch vertex retrieval system.
 * Coordinates the retrieval process and delegates to specialized components.
 */

@Service
public class BatchVertexRetrievalService {
    private static final Logger LOG = LoggerFactory.getLogger(BatchVertexRetrievalService.class);

    private final VertexDataRepository repository;
    private final JacksonVertexSerializer serializer;
    private final int defaultBatchSize;
    private final int maxParallelBatches;
    private final ExecutorService executorService;

    /**
     * Creates a new BatchVertexRetrievalService with custom configuration.
     *
     * @param session The Cassandra session
     */
    @Inject
    public BatchVertexRetrievalService(CqlSession session) {
        this.repository = new EnhancedCassandraRepository(session, "atlan_new_keyspace_2_1", "vertices");
        this.serializer = new JacksonVertexSerializer();
        this.defaultBatchSize = 10;
        //AtlasConfiguration.ATLAS_CASSANDRA_BATCH_SIZE.getInt();
        this.maxParallelBatches = 2;
        this.executorService = Executors.newFixedThreadPool(maxParallelBatches);
    }

    /**
     * Retrieves multiple vertices by their IDs.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @return A map of vertex ID to dynamic vertex data
     */
    public Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds) throws AtlasBaseException {
        return retrieveVertices(vertexIds, defaultBatchSize, false);
    }

    /**
     * Retrieves multiple vertices by their IDs with custom batch size.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @param batchSize The batch size to use
     * @param parallel Whether to use parallel processing
     * @return A map of vertex ID to dynamic vertex data
     */
    public Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds, int batchSize, boolean parallel) throws AtlasBaseException {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Use immutable copy to prevent concurrent modification issues
        final List<String> immutableVertexIds = new ArrayList<>(vertexIds);
        return retrieveVerticesSequential(immutableVertexIds, batchSize);
    }

    /**
     * Retrieves vertices sequentially in batches.
     */
    private Map<String, DynamicVertex> retrieveVerticesSequential(List<String> vertexIds, int batchSize) throws AtlasBaseException {
        Map<String, DynamicVertex> results = new HashMap<>();

        for (int i = 0; i < vertexIds.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, vertexIds.size());
            List<String> batch = vertexIds.subList(i, endIndex);

            // Use the JsonNode-based method for more efficient processing
            Map<String, JsonNode> jsonNodeMap = repository.fetchVerticesAsJsonElements(batch);
            Map<String, DynamicVertex> batchResults = convertJsonNodesToVertices(jsonNodeMap);

            results.putAll(batchResults);
        }

        return results;
    }

    /**
     * Converts JSON data strings to DynamicVertex objects.
     */
    private Map<String, DynamicVertex> convertJsonToVertices(Map<String, String> jsonDataMap) {
        Map<String, DynamicVertex> vertexMap = new HashMap<>();

        for (Map.Entry<String, String> entry : jsonDataMap.entrySet()) {
            String id = entry.getKey();
            String jsonData = entry.getValue();

            try {
                DynamicVertex vertex = serializer.deserialize(jsonData);
                vertexMap.put(id, vertex);
            } catch (Exception e) {
                LOG.error("Error deserializing JSON for ID: {}", id, e);
            }
        }

        return vertexMap;
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

    /**
     * Shuts down any resources used by this service.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted during shutdown", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}