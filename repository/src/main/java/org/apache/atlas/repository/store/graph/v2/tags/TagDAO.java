package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.model.instance.AtlasClassification;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data Access Object interface combining operations for Tags,
 * associated Vertex properties, and related Cassandra interactions.
 */
public interface TagDAO {

    // --- Methods from original TagDAO ---

    /**
     * Retrieves all AtlasClassification tags directly associated with a given vertex ID.
     *
     * @param vertexId The unique identifier of the vertex.
     * @return A list of AtlasClassification objects, or an empty list if none are found.
     */
    List<AtlasClassification> getTagsForVertex(String vertexId);

    // --- Methods derived from CassandraConnector interface ---

    /**
     * Retrieves the properties of a specific direct tag associated with an asset vertex.
     * This might return raw properties rather than a full AtlasClassification object.
     *
     * @param assetVertexId The vertex ID of the asset.
     * @param tagTypeName   The type name of the tag to retrieve.
     * @return A map containing the tag's properties, or null if the tag is not found for the asset.
     */
    static Map<String, Object> getTag(String assetVertexId, String tagTypeName)
    {
        return null;
    }

    /**
     * Records which assets have received a propagated tag from a source asset.
     *
     * @param sourceAssetId          The vertex ID of the asset where the tag originated.
     * @param tagTypeName            The type name of the propagated tag.
     * @param propagatedAssetVertexIds A set of vertex IDs for assets that received the propagated tag.
     */
    void putPropagatedTags(String sourceAssetId, String tagTypeName, Set<String> propagatedAssetVertexIds); // Note: Not static


    /**
     * Calculates a bucket number based on the input value (e.g., vertex ID).
     * (Often used internally before accessing storage).
     *
     * @param value The string value to calculate the bucket for.
     * @return An integer representing the calculated bucket number.
     */
    static int calculateBucket(String value) // Note: Not static
    {
        return 0;
    }
}