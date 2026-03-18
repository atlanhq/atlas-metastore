package org.apache.atlas.repository.graphdb.migrator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Results from super vertex detection.
 * A super vertex has more edges than the configured threshold (default 100,000).
 */
public class SuperVertexReport {

    private int    totalSuperVertexCount;
    private long   maxEdgeCount;
    private long   totalVerticesScanned;
    private long   scanDurationMs;

    // Distribution buckets
    private long verticesOver1kEdges;
    private long verticesOver10kEdges;
    private long verticesOver100kEdges;
    private long verticesOver1mEdges;

    // Top-N super vertices sorted by edge count descending
    private List<SuperVertexEntry> topSuperVertices;

    public SuperVertexReport() {
        this.topSuperVertices = new ArrayList<>();
    }

    // --- Getters & Setters ---

    public int getTotalSuperVertexCount()   { return totalSuperVertexCount; }
    public void setTotalSuperVertexCount(int c) { this.totalSuperVertexCount = c; }

    public long getMaxEdgeCount()           { return maxEdgeCount; }
    public void setMaxEdgeCount(long c)     { this.maxEdgeCount = c; }

    public long getTotalVerticesScanned()   { return totalVerticesScanned; }
    public void setTotalVerticesScanned(long c) { this.totalVerticesScanned = c; }

    public long getScanDurationMs()         { return scanDurationMs; }
    public void setScanDurationMs(long ms)  { this.scanDurationMs = ms; }

    public long getVerticesOver1kEdges()    { return verticesOver1kEdges; }
    public void setVerticesOver1kEdges(long c)  { this.verticesOver1kEdges = c; }

    public long getVerticesOver10kEdges()   { return verticesOver10kEdges; }
    public void setVerticesOver10kEdges(long c) { this.verticesOver10kEdges = c; }

    public long getVerticesOver100kEdges()  { return verticesOver100kEdges; }
    public void setVerticesOver100kEdges(long c) { this.verticesOver100kEdges = c; }

    public long getVerticesOver1mEdges()    { return verticesOver1mEdges; }
    public void setVerticesOver1mEdges(long c)  { this.verticesOver1mEdges = c; }

    public List<SuperVertexEntry> getTopSuperVertices() { return topSuperVertices; }
    public void setTopSuperVertices(List<SuperVertexEntry> list) { this.topSuperVertices = list; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total_super_vertex_count", totalSuperVertexCount);
        m.put("max_edge_count", maxEdgeCount);
        m.put("total_vertices_scanned", totalVerticesScanned);
        m.put("scan_duration_ms", scanDurationMs);
        m.put("vertices_over_1k_edges", verticesOver1kEdges);
        m.put("vertices_over_10k_edges", verticesOver10kEdges);
        m.put("vertices_over_100k_edges", verticesOver100kEdges);
        m.put("vertices_over_1m_edges", verticesOver1mEdges);

        List<Map<String, Object>> top = new ArrayList<>();
        for (SuperVertexEntry e : topSuperVertices) {
            top.add(e.toMap());
        }
        m.put("top_super_vertices", top);
        return m;
    }

    /**
     * Single super vertex entry.
     */
    public static class SuperVertexEntry implements Comparable<SuperVertexEntry> {
        private final String vertexId;
        private final String typeName;
        private final long   edgeCount;
        private final Map<String, Long> edgeLabelCounts;

        public SuperVertexEntry(String vertexId, String typeName, long edgeCount,
                                Map<String, Long> edgeLabelCounts) {
            this.vertexId       = vertexId;
            this.typeName       = typeName;
            this.edgeCount      = edgeCount;
            this.edgeLabelCounts = edgeLabelCounts != null ? edgeLabelCounts : new LinkedHashMap<>();
        }

        public String getVertexId()      { return vertexId; }
        public String getTypeName()      { return typeName; }
        public long   getEdgeCount()     { return edgeCount; }
        public Map<String, Long> getEdgeLabelCounts() { return edgeLabelCounts; }

        @Override
        public int compareTo(SuperVertexEntry other) {
            return Long.compare(this.edgeCount, other.edgeCount); // ascending for min-heap
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("vertex_id", vertexId);
            m.put("type_name", typeName);
            m.put("edge_count", edgeCount);
            m.put("edge_label_counts", edgeLabelCounts);
            return m;
        }
    }
}
