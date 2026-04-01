package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Structured analysis report for super vertex detection and graph metrics.
 * Designed for:
 *   - Human-readable console output ({@link #toPrettyString()})
 *   - Flat key-value map for Mixpanel ingestion ({@link #toFlatMap()})
 *   - JSON for logging ({@link #toJson()})
 */
public class AnalysisReport {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    private String  domainName;
    private String  keyspace;
    private String  analyzeMode;  // "cassandra" or "janus"
    private Instant analysisTimestamp;

    // Graph size
    private long   totalVertices;
    private long   totalEdgesOut;
    private long   totalEdgesIn;
    private long   totalEdgesById;
    private double avgEdgesPerVertex;

    // Super vertex summary
    private int  superVertexThreshold;
    private int  totalSuperVertexCount;
    private long maxEdgeCount;

    // Distribution buckets
    private long verticesOver1kEdges;
    private long verticesOver10kEdges;
    private long verticesOver100kEdges;
    private long verticesOver1mEdges;

    // Top N super vertices
    private List<SuperVertexReport.SuperVertexEntry> topSuperVertices = new ArrayList<>();

    // Vertex type distribution
    private Map<String, Long> vertexCountByType = new LinkedHashMap<>();

    private long scanDurationMs;

    // JanusGraph-specific diagnostics
    private long edgestoreRowCount;       // total edgestore rows (edges + properties + system)
    private long edgestorePropertyCount;  // property rows decoded from edgestore
    private long edgestoreSystemCount;    // system relation rows decoded from edgestore
    private long decodedOutEdges;         // OUT-edges decoded from edgestore
    private long decodedInEdges;          // IN-edges decoded from edgestore
    private long esVertexCount;            // vertex count from ES _count (may differ from edgestore)
    private long esEdgeCount;              // edge count from ES _count (may differ from decoded)

    /**
     * Populate this report from a SuperVertexReport and vertex type counts.
     */
    /**
     * Populate from CassandraGraph (post-migration) analysis.
     */
    public void populate(String domainName, MigratorConfig config,
                         SuperVertexReport svReport, Map<String, Long> typeCounts) {
        this.domainName          = domainName;
        this.keyspace            = config.getTargetCassandraKeyspace();
        this.analyzeMode         = "cassandra";
        this.analysisTimestamp   = Instant.now();
        this.superVertexThreshold = config.getSuperVertexThreshold();

        // From SuperVertexReport
        this.totalEdgesOut          = svReport.getEdgesOutRowCount();
        this.totalEdgesIn           = svReport.getEdgesInRowCount();
        this.totalEdgesById         = svReport.getEdgesByIdRowCount();
        this.maxEdgeCount           = svReport.getMaxEdgeCount();
        this.totalSuperVertexCount  = svReport.getTotalSuperVertexCount();
        this.verticesOver1kEdges    = svReport.getVerticesOver1kEdges();
        this.verticesOver10kEdges   = svReport.getVerticesOver10kEdges();
        this.verticesOver100kEdges  = svReport.getVerticesOver100kEdges();
        this.verticesOver1mEdges    = svReport.getVerticesOver1mEdges();
        this.scanDurationMs         = svReport.getScanDurationMs();
        this.topSuperVertices       = svReport.getTopSuperVertices() != null
                                        ? svReport.getTopSuperVertices() : new ArrayList<>();

        // From vertex type scan
        this.totalVertices = typeCounts.values().stream().mapToLong(Long::longValue).sum();
        this.avgEdgesPerVertex = totalVertices > 0
            ? (double) totalEdgesOut / totalVertices : 0.0;

        // Sort types by count descending
        this.vertexCountByType = typeCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * Populate from JanusGraph (pre-migration) analysis.
     * Vertex counts come from ES, super vertex data from edgestore scan.
     */
    public void populateFromJanusGraph(String domainName, MigratorConfig config,
                                        SuperVertexReport svReport, Map<String, Long> typeCounts,
                                        long totalVerticesFromEs, long totalEdgesFromEs) {
        this.domainName           = domainName;
        this.keyspace             = config.getSourceCassandraKeyspace();
        this.analyzeMode          = "janus";
        this.analysisTimestamp    = Instant.now();
        this.superVertexThreshold = config.getSuperVertexThreshold();

        // From edgestore scan with EdgeSerializer decode (svReport)
        // svReport fields are reused: edgesOutRowCount=totalEdgestoreRows,
        // edgesInRowCount=propertyCount, edgesByIdRowCount=systemCount
        this.edgestoreRowCount      = svReport.getEdgesOutRowCount();
        this.edgestorePropertyCount = svReport.getEdgesInRowCount();
        this.edgestoreSystemCount   = svReport.getEdgesByIdRowCount();
        this.decodedOutEdges        = svReport.getDecodedOutEdges();
        this.decodedInEdges         = svReport.getDecodedInEdges();
        this.maxEdgeCount           = svReport.getMaxEdgeCount();
        this.totalSuperVertexCount  = svReport.getTotalSuperVertexCount();
        this.verticesOver1kEdges    = svReport.getVerticesOver1kEdges();
        this.verticesOver10kEdges   = svReport.getVerticesOver10kEdges();
        this.verticesOver100kEdges  = svReport.getVerticesOver100kEdges();
        this.verticesOver1mEdges    = svReport.getVerticesOver1mEdges();
        this.scanDurationMs         = svReport.getScanDurationMs();
        this.topSuperVertices       = svReport.getTopSuperVertices() != null
                                        ? svReport.getTopSuperVertices() : new ArrayList<>();

        // Cassandra is source of truth: vertex count from edgestore distinct keys, edges from decoded scan
        this.totalVertices = svReport.getTotalVerticesScanned();
        this.esVertexCount = totalVerticesFromEs;
        this.esEdgeCount = totalEdgesFromEs > 0 ? totalEdgesFromEs : 0;
        this.totalEdgesOut  = decodedOutEdges > 0 ? decodedOutEdges : this.esEdgeCount;
        this.totalEdgesIn   = decodedInEdges;
        this.totalEdgesById = decodedOutEdges;  // JanusGraph: each edge has one ID, same as OUT count
        this.avgEdgesPerVertex = totalVertices > 0
            ? (double) totalEdgesOut / totalVertices : 0.0;

        // Sort types by count descending
        this.vertexCountByType = typeCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * Flat key-value map suitable for Mixpanel /engage $set or /import properties.
     * No nested objects — all values are scalar (String, Long, Double).
     */
    public Map<String, Object> toFlatMap() {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("domain", domainName);
        m.put("keyspace", keyspace);
        m.put("analysis_timestamp", analysisTimestamp != null ? analysisTimestamp.toString() : null);

        // Graph size
        m.put("total_vertices", totalVertices);
        m.put("total_edges_out", totalEdgesOut);
        m.put("total_edges_in", totalEdgesIn);
        m.put("total_edges_by_id", totalEdgesById);
        m.put("avg_edges_per_vertex", Math.round(avgEdgesPerVertex * 100.0) / 100.0);

        // Super vertex summary
        m.put("max_edge_count", maxEdgeCount);
        m.put("super_vertex_threshold", superVertexThreshold);
        m.put("total_super_vertex_count", totalSuperVertexCount);

        // Distribution buckets
        m.put("vertices_over_1k_edges", verticesOver1kEdges);
        m.put("vertices_over_10k_edges", verticesOver10kEdges);
        m.put("vertices_over_100k_edges", verticesOver100kEdges);
        m.put("vertices_over_1m_edges", verticesOver1mEdges);

        m.put("scan_duration_ms", scanDurationMs);
        if ("janus".equals(analyzeMode)) {
            m.put("es_vertex_count", esVertexCount);
            m.put("es_edge_count", esEdgeCount);
            m.put("edgestore_row_count", edgestoreRowCount);
            m.put("decoded_out_edges", decodedOutEdges);
            m.put("decoded_in_edges", decodedInEdges);
            m.put("edgestore_property_count", edgestorePropertyCount);
            m.put("edgestore_system_count", edgestoreSystemCount);
        }

        // Top N super vertices — flattened to top_1_*, top_2_*, ...
        int rank = 1;
        for (SuperVertexReport.SuperVertexEntry entry : topSuperVertices) {
            if (rank > 100) break;
            String prefix = "top_" + rank;
            m.put(prefix + "_vertex_id", entry.getVertexId());
            m.put(prefix + "_type_name", entry.getTypeName());
            m.put(prefix + "_edge_count", entry.getEdgeCount());
            // Edge labels as compact string: "label1:count1,label2:count2"
            if (entry.getEdgeLabelCounts() != null && !entry.getEdgeLabelCounts().isEmpty()) {
                String labels = entry.getEdgeLabelCounts().entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(5) // top 5 labels per vertex
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(","));
                m.put(prefix + "_edge_labels", labels);
            }
            rank++;
        }

        // Top 10 vertex types — flattened to top_type_1_*, top_type_2_*, ...
        int typeRank = 1;
        for (Map.Entry<String, Long> entry : vertexCountByType.entrySet()) {
            if (typeRank > 10) break;
            String prefix = "top_type_" + typeRank;
            m.put(prefix + "_name", entry.getKey());
            m.put(prefix + "_count", entry.getValue());
            if (totalVertices > 0) {
                double pct = (double) entry.getValue() / totalVertices * 100.0;
                m.put(prefix + "_pct", Math.round(pct * 10.0) / 10.0);
            }
            typeRank++;
        }

        return m;
    }

    /**
     * Pretty-printed JSON for logging.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(toFlatMap());
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Human-readable console report.
     */
    public String toPrettyString() {
        StringBuilder sb = new StringBuilder();
        String LINE = "================================================================================";
        String THIN = "--------------------------------------------------------------------------------";

        boolean isJanus = "janus".equals(analyzeMode);
        String modeLabel = isJanus ? "JanusGraph (pre-migration)" : "CassandraGraph (post-migration)";

        sb.append("\n").append(LINE).append("\n");
        sb.append("  GRAPH ANALYSIS REPORT\n");
        sb.append(LINE).append("\n\n");

        sb.append("  Domain:         ").append(domainName != null ? domainName : "unknown").append("\n");
        sb.append("  Keyspace:       ").append(keyspace).append("\n");
        sb.append("  Mode:           ").append(modeLabel).append("\n");
        sb.append("  Analyzed:       ").append(analysisTimestamp).append("\n");
        sb.append("  Scan duration:  ").append(formatDuration(scanDurationMs)).append("\n");
        sb.append("\n");

        // Graph size
        sb.append(THIN).append("\n");
        sb.append("  GRAPH SIZE\n");
        sb.append(THIN).append("\n\n");
        sb.append(String.format("  %-36s %,12d\n", "Total Vertices", totalVertices));
        if (isJanus) {
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (OUT)", totalEdgesOut));
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (IN)", totalEdgesIn));
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (BY_ID)", totalEdgesById));
            sb.append(String.format("  %-36s %12.1f\n", "Avg Edges Per Vertex", avgEdgesPerVertex));
            sb.append("\n");
            sb.append(String.format("  %-36s %,12d\n", "ES Vertex Count (for comparison)", esVertexCount));
            sb.append(String.format("  %-36s %,12d\n", "ES Edge Count (for comparison)", esEdgeCount));
            sb.append(String.format("  %-36s %,12d\n", "Edgestore Rows (total)", edgestoreRowCount));
            sb.append(String.format("  %-36s %,12d\n", "  - OUT-edge rows (decoded)", decodedOutEdges));
            sb.append(String.format("  %-36s %,12d\n", "  - IN-edge rows (decoded)", decodedInEdges));
            sb.append(String.format("  %-36s %,12d\n", "  - Property rows", edgestorePropertyCount));
            sb.append(String.format("  %-36s %,12d\n", "  - System rows", edgestoreSystemCount));
        } else {
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (OUT)", totalEdgesOut));
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (IN)", totalEdgesIn));
            sb.append(String.format("  %-36s %,12d\n", "Total Edges (BY_ID)", totalEdgesById));
            sb.append(String.format("  %-36s %12.1f\n", "Avg Edges Per Vertex", avgEdgesPerVertex));
        }
        sb.append("\n");

        // Edge distribution
        sb.append(THIN).append("\n");
        sb.append(isJanus ? "  EDGE DISTRIBUTION (decoded edges per vertex, OUT+IN)\n"
                          : "  EDGE DISTRIBUTION\n");
        sb.append(THIN).append("\n\n");
        sb.append(String.format("  %-36s %,12d\n", "Vertices > 1K edges", verticesOver1kEdges));
        sb.append(String.format("  %-36s %,12d\n", "Vertices > 10K edges", verticesOver10kEdges));
        sb.append(String.format("  %-36s %,12d\n", "Vertices > 100K edges", verticesOver100kEdges));
        sb.append(String.format("  %-36s %,12d\n", "Vertices > 1M edges", verticesOver1mEdges));
        sb.append("\n");

        // Super vertices
        sb.append(THIN).append("\n");
        sb.append(String.format("  SUPER VERTICES (threshold: %,d edges)\n", superVertexThreshold));
        sb.append(THIN).append("\n\n");
        sb.append(String.format("  Total super vertices: %d\n", totalSuperVertexCount));
        sb.append(String.format("  Max edge count: %,d\n\n", maxEdgeCount));

        if (!topSuperVertices.isEmpty()) {
            String countHeader = "Edge Count";
            sb.append(String.format("  %-4s %-40s %-20s %12s   %s\n",
                "#", "Vertex ID", "Type", countHeader, "Top Labels"));
            sb.append(String.format("  %-4s %-40s %-20s %12s   %s\n",
                "--", "---------", "----", "----------", "----------"));

            int rank = 1;
            for (SuperVertexReport.SuperVertexEntry entry : topSuperVertices) {
                if (rank > 20) {
                    sb.append(String.format("  ... and %d more super vertices\n",
                        topSuperVertices.size() - 20));
                    break;
                }
                String labels = "";
                if (entry.getEdgeLabelCounts() != null && !entry.getEdgeLabelCounts().isEmpty()) {
                    labels = entry.getEdgeLabelCounts().entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .limit(3)
                        .map(e -> e.getKey() + ":" + formatCount(e.getValue()))
                        .collect(Collectors.joining(", "));
                }
                sb.append(String.format("  %-4d %-40s %-20s %,12d   %s\n",
                    rank,
                    truncate(entry.getVertexId(), 40),
                    truncate(entry.getTypeName() != null ? entry.getTypeName() : "unknown", 20),
                    entry.getEdgeCount(),
                    labels));
                rank++;
            }
            sb.append("\n");
        }

        // Vertex types
        if (!vertexCountByType.isEmpty()) {
            sb.append(THIN).append("\n");
            sb.append("  VERTEX TYPES (Top 10)\n");
            sb.append(THIN).append("\n\n");
            sb.append(String.format("  %-35s %12s %8s\n", "Type", "Count", "%"));
            sb.append(String.format("  %-35s %12s %8s\n", "----", "-----", "-"));

            int count = 0;
            for (Map.Entry<String, Long> entry : vertexCountByType.entrySet()) {
                if (count >= 10) break;
                double pct = totalVertices > 0
                    ? (double) entry.getValue() / totalVertices * 100.0 : 0.0;
                sb.append(String.format("  %-35s %,12d %7.1f%%\n",
                    truncate(entry.getKey(), 35), entry.getValue(), pct));
                count++;
            }

            int remaining = vertexCountByType.size() - 10;
            if (remaining > 0) {
                sb.append(String.format("  ... and %d more types\n", remaining));
            }
            sb.append("\n");
        }

        sb.append(LINE).append("\n");
        return sb.toString();
    }

    // --- Helpers ---

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }

    private static String formatDuration(long ms) {
        long secs = ms / 1000;
        if (secs < 60) return secs + "s";
        if (secs < 3600) return String.format("%dm %ds", secs / 60, secs % 60);
        return String.format("%dh %dm %ds", secs / 3600, (secs % 3600) / 60, secs % 60);
    }

    private static String formatCount(long count) {
        if (count >= 1_000_000) return String.format("%.1fM", count / 1_000_000.0);
        if (count >= 1_000) return String.format("%.1fK", count / 1_000.0);
        return String.valueOf(count);
    }

    // --- Getters (for testing) ---

    public String getDomainName()          { return domainName; }
    public String getKeyspace()            { return keyspace; }
    public long   getTotalVertices()       { return totalVertices; }
    public long   getTotalEdgesOut()       { return totalEdgesOut; }
    public long   getTotalEdgesIn()        { return totalEdgesIn; }
    public long   getTotalEdgesById()      { return totalEdgesById; }
    public int    getTotalSuperVertexCount() { return totalSuperVertexCount; }
    public long   getMaxEdgeCount()        { return maxEdgeCount; }
    public long   getScanDurationMs()      { return scanDurationMs; }
}
