package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.exception.AtlasBaseException;

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
    Map<String, JsonNode> fetchVerticesAsJsonNodes(List<String> vertexIds) throws AtlasBaseException;

    void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException;
}