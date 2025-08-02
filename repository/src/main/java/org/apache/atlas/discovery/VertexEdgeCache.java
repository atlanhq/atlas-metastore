package org.apache.atlas.discovery;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

@Component
public class VertexEdgeCache {

    // MEMORY LEAK FIX: Add size limit to prevent unlimited ThreadLocal growth
    private static final int MAX_CACHE_SIZE = 500;
    private final ThreadLocal<Map<CachedVertexEdgesKey, List<AtlasEdge>>> edgeCache = ThreadLocal.withInitial(HashMap::new);

    public List<AtlasEdge> getEdges(AtlasVertex vertex, AtlasEdgeDirection direction, String edgeLabel) {
        CachedVertexEdgesKey key = new CachedVertexEdgesKey(vertex.getId(), direction, edgeLabel);
        Map<CachedVertexEdgesKey, List<AtlasEdge>> cache = edgeCache.get();
        
        // MEMORY LEAK FIX: Clear cache if it grows too large to prevent memory accumulation
        if (cache.size() > MAX_CACHE_SIZE) {
            cache.clear();
        }
        
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else {
            List<AtlasEdge> edges = newArrayList(vertex.getEdges(direction, edgeLabel));
            cache.put(key, edges);
            return edges;
        }
    }
    
    /**
     * MEMORY LEAK FIX: Clear ThreadLocal cache to prevent memory accumulation
     * This should be called after request processing completes
     */
    public void clearCache() {
        try {
            Map<CachedVertexEdgesKey, List<AtlasEdge>> cache = edgeCache.get();
            if (cache != null) {
                cache.clear();
            }
            edgeCache.remove();
        } catch (Exception e) {
            // Non-critical error, just log and continue
        }
    }
    
    /**
     * Get current cache size for monitoring purposes
     */
    public int getCacheSize() {
        try {
            Map<CachedVertexEdgesKey, List<AtlasEdge>> cache = edgeCache.get();
            return cache != null ? cache.size() : 0;
        } catch (Exception e) {
            return 0;
        }
    }
}
