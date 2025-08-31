package org.apache.atlas.repository.store.graph.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PreProcessorAttributeTracker {
    private static final Logger LOG = LoggerFactory.getLogger(PreProcessorAttributeTracker.class);

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public void trackAttribute(String preprocessorName, String attributeName, Object value) {
        if (preprocessorName == null || attributeName == null) {
            LOG.warn("Attempt to track attribute with null preprocessor name or attribute name");
            return;
        }

        try {
            // Store with a simple key, no nesting
            attributes.put(attributeName, value);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Tracked attribute: attribute={}, value={}", attributeName, value);
            }
        } catch (Exception e) {
            LOG.error("Failed to track attribute: attribute={}", attributeName, e);
        }
    }

    public Map<String, Object> getTrackedAttributes() {
        // Return an unmodifiable copy to prevent external modifications
        return Collections.unmodifiableMap(new HashMap<>(attributes));
    }

    public void clear() {
        attributes.clear();
    }
} 