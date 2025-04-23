package org.apache.atlas.repository.cassandra;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.exception.AtlasBaseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Repository interface for vertex data access.
 */

interface VertexDataRepository  {
    /**
     * Fetches JSON data for multiple vertices by their IDs.
     *
     * @param vertexIds The list of vertex IDs to fetch
     * @return A map of vertex ID to JSON data string
     */
    Map<String, String> fetchVerticesJsonData(List<String> vertexIds) throws AtlasBaseException;

    /**
     * Fetches vertex data as parsed JsonElements instead of raw strings.
     * This is more efficient when the caller needs to work with the JSON directly.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to parsed JsonElement
     */
    default Map<String, JsonNode> fetchVerticesAsJsonElements(List<String> vertexIds) throws AtlasBaseException {
        // Default implementation that delegates to fetchVerticesJsonData
        Map<String, String> jsonStrings = fetchVerticesJsonData(vertexIds);
        return parseJson(jsonStrings, new ObjectMapper());
    }

    private Map<String, JsonNode> parseJson(Map<String, String> jsonStrings, ObjectMapper mapper) {
        // Pre-size the HashMap with appropriate capacity and load factor for better performance
        Map<String, JsonNode> jsonElements = new HashMap<>(Math.round(jsonStrings.size() / 0.75f) + 1);

        for (Map.Entry<String, String> entry : jsonStrings.entrySet()) {
            String jsonString = entry.getValue();
            // Check for null or empty strings before parsing to avoid unnecessary exceptions
            if (jsonString != null && !jsonString.isEmpty()) {
                try {
                    // Use Jackson's streaming parser for better performance
                    JsonNode element = mapper.readTree(jsonString);
                    jsonElements.put(entry.getKey(), element);
                } catch (JsonProcessingException e) {
                    // Skip invalid JSON entries but log them for troubleshooting
                    //LOG.debug("Invalid JSON for vertex ID {}: {}", entry.getKey(), e.getMessage());
                }
            }
        }

        return jsonElements;

    }

}
