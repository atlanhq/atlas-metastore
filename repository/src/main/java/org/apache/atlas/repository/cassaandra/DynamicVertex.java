package org.apache.atlas.repository.cassaandra;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents a dynamic vertex with arbitrary properties.
 */
public class DynamicVertex {

    private final Map<String, Object> properties = new HashMap<>();

    /**
     * Gets a property value by its key.
     *
     * @param key The property key
     * @return The property value, or null if not found
     */
    public Object getProperty(String key) {
        return properties.get(key);
    }

    /**
     * Gets a property value as a specific type.
     *
     * @param key The property key
     * @param clazz The expected class type
     * @return The property value cast to the specified type, or null if not found or not compatible
     */
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key, Class<T> clazz) {
        Object value = properties.get(key);
        if (value != null && clazz.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    /**
     * Sets a property value.
     *
     * @param key The property key
     * @param value The property value
     * @return This DynamicVertex instance for method chaining
     */
    public DynamicVertex setProperty(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    /**
     * Gets all property keys.
     *
     * @return A set of all property keys
     */
    public Set<String> getPropertyKeys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    /**
     * Gets all properties as a map.
     *
     * @return An unmodifiable map of all properties
     */
    public Map<String, Object> getAllProperties() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Checks if a property exists.
     *
     * @param key The property key
     * @return true if the property exists, false otherwise
     */
    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    /**
     * Removes a property.
     *
     * @param key The property key
     * @return The previous value, or null if no mapping existed
     */
    public Object removeProperty(String key) {
        return properties.remove(key);
    }

    @Override
    public String toString() {
        return "DynamicVertex{properties=" + properties + '}';
    }
}