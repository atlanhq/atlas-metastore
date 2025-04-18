package org.apache.atlas.repository.cassaandra;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.atlas.exception.AtlasBaseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Repository interface for vertex data access.
 */
interface VertexDataRepository {
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
    default Map<String, JsonElement> fetchVerticesAsJsonElements(List<String> vertexIds) throws AtlasBaseException {
        // Default implementation that delegates to fetchVerticesJsonData
        Map<String, String> jsonStrings = fetchVerticesJsonData(vertexIds);
        Map<String, JsonElement> jsonElements = new HashMap<>(jsonStrings.size());

        JsonParser parser = new JsonParser();
        for (Map.Entry<String, String> entry : jsonStrings.entrySet()) {
            try {
                JsonElement element = parser.parse(entry.getValue());
                jsonElements.put(entry.getKey(), element);
            } catch (JsonSyntaxException e) {
                // Skip invalid JSON entries
            }
        }

        return jsonElements;
    }
}
