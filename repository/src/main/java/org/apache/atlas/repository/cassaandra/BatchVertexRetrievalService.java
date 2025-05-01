package org.apache.atlas.repository.cassaandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.gson.*;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Main entry point for the batch vertex retrieval system.
 * Coordinates the retrieval process and delegates to specialized components.
 */

@Service
public class BatchVertexRetrievalService {
    private static final Logger LOG = LoggerFactory.getLogger(BatchVertexRetrievalService.class);

    private final VertexDataRepository repository;
    private final VertexDataSerializer serializer;
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

        this.repository = new EnhancedCassandraRepository(session, AtlasConfiguration.ATLAS_CASSANDRA_PROPERTIES_KEYSPACE.getString(), AtlasConfiguration.ATLAS_CASSANDRA_VERTICES_TABLE.getString());
        this.serializer = new GsonVertexSerializer();
        this.defaultBatchSize = AtlasConfiguration.ATLAS_CASSANDRA_BATCH_SIZE.getInt();
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

        if (parallel) {
            return retrieveVerticesInParallel(immutableVertexIds, batchSize);
        } else {
            return retrieveVerticesSequential(immutableVertexIds, batchSize);
        }
    }

    /**
     * Retrieves vertices sequentially in batches.
     */
    private Map<String, DynamicVertex> retrieveVerticesSequential(List<String> vertexIds, int batchSize) throws AtlasBaseException {
        Map<String, DynamicVertex> results = new HashMap<>();

        for (int i = 0; i < vertexIds.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, vertexIds.size());
            List<String> batch = vertexIds.subList(i, endIndex);

            // Use the JsonElement-based method for more efficient processing
            Map<String, JsonElement> jsonElementMap = repository.fetchVerticesAsJsonElements(batch);
            Map<String, DynamicVertex> batchResults = convertJsonElementsToVertices(jsonElementMap);

            results.putAll(batchResults);
        }

        return results;
    }

    /**
     * Retrieves vertices in parallel batches for improved performance.
     */
    private Map<String, DynamicVertex> retrieveVerticesInParallel(List<String> vertexIds, int batchSize) throws AtlasBaseException{
        final Map<String, DynamicVertex> results = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < vertexIds.size(); i += batchSize) {
            final int startIndex = i;
            final int endIndex = Math.min(i + batchSize, vertexIds.size());

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                List<String> batch = vertexIds.subList(startIndex, endIndex);
                // Use JsonElement-based method for more efficient processing
                Map<String, JsonElement> jsonElementMap = null;
                try {
                    jsonElementMap = repository.fetchVerticesAsJsonElements(batch);
                } catch (AtlasBaseException e) {
                    LOG.error("Error fetching vertices in parallel for batch:{}", e);
                    throw new RuntimeException(e);
                }
                Map<String, DynamicVertex> batchResults = convertJsonElementsToVertices(jsonElementMap);
                results.putAll(batchResults);
            }, executorService);

            futures.add(future);
        }

        // Wait for all parallel tasks to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error during parallel vertex retrieval", e);
            Thread.currentThread().interrupt();
            throw new AtlasBaseException("Failed to retrieve vertices in parallel", e);
        }

        return results;
    }

    /**
     * Converts JSON data strings to DynamicVertex objects.
     */
    private Map<String, DynamicVertex> convertJsonToVertices(Map<String, String> jsonDataMap) {
        return jsonDataMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> serializer.deserialize(entry.getValue())
                ));
    }

    /**
     * Converts pre-parsed JsonElements to DynamicVertex objects.
     * This is more efficient as it avoids parsing the JSON string again.
     */
    private Map<String, DynamicVertex> convertJsonElementsToVertices(Map<String, JsonElement> jsonElementMap) {
        Map<String, DynamicVertex> vertexMap = new HashMap<>();

        // Create a custom JsonDeserializationContext that can be reused
        JsonDeserializationContext context = new JsonDeserializationContext() {
            @Override
            public <T> T deserialize(JsonElement json, Type typeOfT) throws JsonParseException {
                if (typeOfT.equals(DynamicVertex.class)) {
                    return (T) ((GsonVertexSerializer)serializer).deserializeFromElement(json);
                }
                throw new JsonParseException("Unsupported type: " + typeOfT);
            }
        };

        for (Map.Entry<String, JsonElement> entry : jsonElementMap.entrySet()) {
            String id = entry.getKey();
            JsonElement jsonElement = entry.getValue();

            try {
                // Cast to our implementation to use the direct JsonElement method
                DynamicVertex vertex = ((GsonVertexSerializer)serializer).deserializeFromElement(jsonElement);
                vertexMap.put(id, vertex);
            } catch (Exception e) {
                LOG.error("Error converting JsonElement to DynamicVertex for ID: {}", id, e);
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
