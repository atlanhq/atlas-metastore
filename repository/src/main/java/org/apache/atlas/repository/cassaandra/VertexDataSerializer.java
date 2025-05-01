package org.apache.atlas.repository.cassaandra;


/**
 * Interface for serializing and deserializing vertex data.
 */
interface VertexDataSerializer {
    /**
     * Deserializes a JSON string into a DynamicVertex.
     *
     * @param jsonData The JSON data string
     * @return A DynamicVertex object
     */
    DynamicVertex deserialize(String jsonData);

    /**
     * Serializes a DynamicVertex into a JSON string.
     *
     * @param vertex The DynamicVertex to serialize
     * @return A JSON string representation
     */
    String serialize(DynamicVertex vertex);
}
