package org.apache.atlas.repository.cassandra;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GSON-based implementation of the VertexDataSerializer.
 */
class GsonVertexSerializer implements VertexDataSerializer {
    private final Gson gson;
    private final DynamicVertexDeserializer deserializer;

    public GsonVertexSerializer() {
        this.deserializer = new DynamicVertexDeserializer();
        this.gson = new GsonBuilder()
                .registerTypeAdapter(DynamicVertex.class, deserializer)
                .serializeNulls()
                .create();
    }

    @Override
    public DynamicVertex deserialize(String jsonData) {
        return gson.fromJson(jsonData, DynamicVertex.class);
    }

    @Override
    public String serialize(DynamicVertex vertex) {
        return gson.toJson(vertex);
    }

    /**
     * Deserializes a JsonElement directly into a DynamicVertex without going through string conversion.
     * This is more efficient when the JsonElement is already available.
     *
     * @param jsonElement The JsonElement to deserialize
     * @return A DynamicVertex object
     */
    public DynamicVertex deserializeFromElement(JsonElement jsonElement) {
        return deserializer.deserialize(jsonElement, DynamicVertex.class, null);
    }

    /**
     * Custom deserializer for DynamicVertex to handle any property structure.
     */
    private static class DynamicVertexDeserializer implements JsonDeserializer<DynamicVertex> {
        @Override
        public DynamicVertex deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            DynamicVertex vertex = new DynamicVertex();
            JsonObject jsonObject = json.getAsJsonObject();

            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                String key = entry.getKey();
                JsonElement value = entry.getValue();

                Object javaValue = convertJsonElementToJava(value);
                vertex.setProperty(key, javaValue);
            }

            return vertex;
        }

        /**
         * Converts a JsonElement to an appropriate Java object.
         */
        private Object convertJsonElementToJava(JsonElement element) {
            if (element.isJsonNull()) {
                return null;
            } else if (element.isJsonPrimitive()) {
                JsonPrimitive primitive = element.getAsJsonPrimitive();
                if (primitive.isString()) {
                    return primitive.getAsString();
                } else if (primitive.isNumber()) {
                    Number number = primitive.getAsNumber();
                    // Check if it's an integer or a floating-point number
                    double doubleValue = number.doubleValue();
                    if (doubleValue == Math.floor(doubleValue)) {
                        if (doubleValue <= Long.MAX_VALUE && doubleValue >= Long.MIN_VALUE) {
                            return number.longValue();
                        }
                    }
                    return number.doubleValue();
                } else if (primitive.isBoolean()) {
                    return primitive.getAsBoolean();
                }
            } else if (element.isJsonArray()) {
                List<Object> list = new ArrayList<>();
                JsonArray array = element.getAsJsonArray();
                for (JsonElement arrayElement : array) {
                    list.add(convertJsonElementToJava(arrayElement));
                }
                return list;
            } else if (element.isJsonObject()) {
                Map<String, Object> map = new HashMap<>();
                JsonObject object = element.getAsJsonObject();
                for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                    map.put(entry.getKey(), convertJsonElementToJava(entry.getValue()));
                }
                return map;
            }

            // Default case
            return element.toString();
        }
    }
}