package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Jackson-based implementation of the VertexDataSerializer.
 */
class JacksonVertexSerializer implements VertexSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonVertexSerializer.class);
    private final ObjectMapper objectMapper;
    private final DynamicVertexDeserializer deserializer;


    public JacksonVertexSerializer(final  ObjectMapper objectMapper) {
        this.deserializer = new DynamicVertexDeserializer();
        this.objectMapper = objectMapper;

        // Configure Jackson to handle nulls similar to GSON
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);

        // Register custom deserializer
        SimpleModule module = new SimpleModule();
        module.addDeserializer(DynamicVertex.class, deserializer);
        this.objectMapper.registerModule(module);
    }

    @Override
    public DynamicVertex deserialize(String jsonData) {
        try {
            return objectMapper.readValue(jsonData, DynamicVertex.class);
        } catch (JsonProcessingException e) {
            // Handle exception appropriately - might want to wrap this
            throw new RuntimeException("Failed to deserialize vertex data", e);
        }
    }

    @Override
    public String serialize(DynamicVertex vertex) {
        try {
            return objectMapper.writeValueAsString(vertex.getAllProperties());
        } catch (JsonProcessingException e) {
            // Handle exception appropriately
            throw new RuntimeException("Failed to serialize vertex data", e);
        }
    }

    /**
     * Deserializes a JsonNode directly into a DynamicVertex without going through string conversion.
     * This is more efficient when the JsonNode is already available.
     *
     * @param jsonNode The JsonNode to deserialize
     * @return A DynamicVertex object
     */
    public DynamicVertex deserializeFromNode(JsonNode jsonNode) {
        return deserializer.deserializeFromNode(jsonNode);
    }

    /**
     * Custom deserializer for DynamicVertex to handle any property structure.
     */
    private static class DynamicVertexDeserializer extends StdDeserializer<DynamicVertex> {

        public DynamicVertexDeserializer() {
            super(DynamicVertex.class);
        }

        @Override
        public DynamicVertex deserialize(com.fasterxml.jackson.core.JsonParser jp,
                                         DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            return deserializeFromNode(node);
        }

        public DynamicVertex deserializeFromNode(JsonNode jsonNode) {
            DynamicVertex vertex = new DynamicVertex();

            if (jsonNode.isObject()) {
                ObjectNode objectNode = (ObjectNode) jsonNode;
                objectNode.fields().forEachRemaining(entry -> {
                    String key = entry.getKey();
                    JsonNode value = entry.getValue();

                    Object javaValue = convertJsonNodeToJava(value);
                    vertex.setProperty(key, javaValue);
                });
            }

            return vertex;
        }

        /**
         * Converts a JsonNode to an appropriate Java object.
         */
        private Object convertJsonNodeToJava(JsonNode node) {
            if (node.isNull()) {
                return null;
            } else if (node.isTextual()) {
                return node.asText();
            }  else if (node.isBoolean()) {
                return node.asBoolean();
            } else if (node.isArray()) {
                List<Object> list = new ArrayList<>();
                ArrayNode arrayNode = (ArrayNode) node;
                for (JsonNode element : arrayNode) {
                    list.add(convertJsonNodeToJava(element));
                }
                return list;
            }

            /*else if (node.isNumber()) {
                // Check if it's an integer or a floating-point number
                if (node.isIntegralNumber()) {
                    String textValue = node.asText(); // Get the original string representation
                    try {
                        return Integer.parseInt(textValue);
                        //return intValue;
                    } catch (NumberFormatException e) {
                        try {
                            long longValue = Long.parseLong(textValue);
                            return longValue;
                        } catch (NumberFormatException ex) {
                            // For really big integers that don't fit in Long
                            return node.bigIntegerValue();
                        }
                    }
                } else {
                    if (node.isFloatingPointNumber()) {
                        String textValue = node.asText(); // Get the original string representation
                        try {
                            float floatValue = Float.parseFloat(textValue);
                            return floatValue;
                        } catch (NumberFormatException e) {
                            return node.asDouble();
                        }
                    } else {
                        return node.asDouble();
                    }
                }
            }*/

            /* else if (node.isArray()) {
                List<Object> list = new ArrayList<>();
                ArrayNode arrayNode = (ArrayNode) node;
                for (JsonNode element : arrayNode) {
                    list.add(convertJsonNodeToJava(element));
                }
                return list;
            } else if (node.isObject()) {
                Map<String, Object> map = new HashMap<>();
                ObjectNode objectNode = (ObjectNode) node;
                objectNode.fields().forEachRemaining(entry -> {
                    map.put(entry.getKey(), convertJsonNodeToJava(entry.getValue()));
                });
                return map;
            }*/

            // Default case
            return node.toString();
        }
    }
}