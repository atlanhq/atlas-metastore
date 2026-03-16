package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structured validation report. Collects individual check results
 * and can serialize to JSON for logging or future Mixpanel push.
 */
public class ValidationReport {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    private final String  tenantId;
    private final Instant startTime;
    private Instant endTime;
    private boolean overallPassed = true;

    // Ordered list of check results
    private final List<ValidationCheckResult> checks = new ArrayList<>();

    // Super vertex report (from target scan or baseline)
    private SuperVertexReport superVertexReport;

    // Source baseline (from Phase 1 scan, null if --validate-only without prior run)
    private SourceBaselineCollector.BaselineSnapshot sourceBaseline;

    // Per-type statistics from deep vertex checks
    private final Map<String, TypeStats> typeStatsMap = new ConcurrentHashMap<>();

    // Full groupBy type_name counts from target vertices table
    private Map<String, Long> vertexCountByType = new LinkedHashMap<>();

    // Summary counters (target)
    private long vertexCount;
    private long edgeOutCount;
    private long edgeInCount;
    private long edgeByIdCount;
    private long esDocCount;
    private long typeDefCount;
    private long typeDefByCategoryCount;

    public ValidationReport(String tenantId) {
        this.tenantId  = tenantId;
        this.startTime = Instant.now();
    }

    public void addCheck(ValidationCheckResult result) {
        checks.add(result);
        if (!result.isPassed()) {
            overallPassed = false;
        }
    }

    public void complete() {
        this.endTime = Instant.now();
    }

    // --- Getters & Setters ---

    public boolean isOverallPassed()   { return overallPassed; }
    public String getTenantId()        { return tenantId; }
    public Instant getStartTime()      { return startTime; }
    public Instant getEndTime()        { return endTime; }
    public List<ValidationCheckResult> getChecks() { return checks; }

    public long getVertexCount()          { return vertexCount; }
    public void setVertexCount(long c)    { this.vertexCount = c; }

    public long getEdgeOutCount()         { return edgeOutCount; }
    public void setEdgeOutCount(long c)   { this.edgeOutCount = c; }

    public long getEdgeInCount()          { return edgeInCount; }
    public void setEdgeInCount(long c)    { this.edgeInCount = c; }

    public long getEdgeByIdCount()        { return edgeByIdCount; }
    public void setEdgeByIdCount(long c)  { this.edgeByIdCount = c; }

    public long getEsDocCount()           { return esDocCount; }
    public void setEsDocCount(long c)     { this.esDocCount = c; }

    public long getTypeDefCount()         { return typeDefCount; }
    public void setTypeDefCount(long c)   { this.typeDefCount = c; }

    public long getTypeDefByCategoryCount()        { return typeDefByCategoryCount; }
    public void setTypeDefByCategoryCount(long c)  { this.typeDefByCategoryCount = c; }

    public SuperVertexReport getSuperVertexReport()             { return superVertexReport; }
    public void setSuperVertexReport(SuperVertexReport r)       { this.superVertexReport = r; }

    public SourceBaselineCollector.BaselineSnapshot getSourceBaseline()  { return sourceBaseline; }
    public void setSourceBaseline(SourceBaselineCollector.BaselineSnapshot b) { this.sourceBaseline = b; }

    public Map<String, TypeStats> getTypeStatsMap() { return typeStatsMap; }

    public Map<String, Long> getVertexCountByType()            { return vertexCountByType; }
    public void setVertexCountByType(Map<String, Long> counts) { this.vertexCountByType = counts; }

    /**
     * Serialize to a Map suitable for structured logging or future Mixpanel push.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tenant_id", tenantId);
        m.put("start_time", startTime.toString());
        m.put("end_time", endTime != null ? endTime.toString() : null);
        m.put("overall_passed", overallPassed);
        m.put("vertex_count", vertexCount);
        m.put("edge_out_count", edgeOutCount);
        m.put("edge_in_count", edgeInCount);
        m.put("edge_by_id_count", edgeByIdCount);
        m.put("es_doc_count", esDocCount);
        m.put("typedef_count", typeDefCount);

        // Source baseline comparison
        if (sourceBaseline != null) {
            Map<String, Object> baseline = new LinkedHashMap<>();
            baseline.put("source_vertex_count", sourceBaseline.totalVertices);
            baseline.put("source_edge_count", sourceBaseline.totalEdges);
            m.put("source_baseline", baseline);
        }

        // Check summaries
        List<Map<String, Object>> checkSummaries = new ArrayList<>();
        for (ValidationCheckResult c : checks) {
            checkSummaries.add(c.toMap());
        }
        m.put("checks", checkSummaries);

        // Super vertex summary
        if (superVertexReport != null) {
            m.put("super_vertices", superVertexReport.toMap());
        }

        // Per-type stats (from deep vertex sampling)
        if (!typeStatsMap.isEmpty()) {
            Map<String, Object> stats = new LinkedHashMap<>();
            for (Map.Entry<String, TypeStats> e : typeStatsMap.entrySet()) {
                stats.put(e.getKey(), e.getValue().toMap());
            }
            m.put("type_stats_sampled", stats);
        }

        // Full groupBy type_name counts (target)
        if (!vertexCountByType.isEmpty()) {
            // Sort by count descending for readability
            Map<String, Long> sorted = new LinkedHashMap<>();
            vertexCountByType.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(e -> sorted.put(e.getKey(), e.getValue()));
            m.put("vertex_count_by_type", sorted);

            // Source vs target comparison per type (if baseline available)
            if (sourceBaseline != null && sourceBaseline.typeVertexCounts != null) {
                Map<String, Object> comparison = new LinkedHashMap<>();
                // Merge all type names from both source and target
                Set<String> allTypes = new java.util.TreeSet<>();
                allTypes.addAll(sorted.keySet());
                allTypes.addAll(sourceBaseline.typeVertexCounts.keySet());

                for (String type : allTypes) {
                    long src = sourceBaseline.typeVertexCounts.getOrDefault(type, 0L);
                    long tgt = sorted.getOrDefault(type, 0L);
                    long diff = tgt - src;
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("source", src);
                    entry.put("target", tgt);
                    entry.put("diff", diff);
                    if (src > 0 && diff < 0) {
                        entry.put("status", "MISSING");
                    } else if (diff == 0) {
                        entry.put("status", "OK");
                    } else {
                        entry.put("status", "OK");
                    }
                    comparison.put(type, entry);
                }
                m.put("type_count_comparison", comparison);
            }
        }

        return m;
    }

    /**
     * Pretty-printed JSON for logging.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(toMap());
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Per-type statistics collected during deep vertex checks.
     */
    public static class TypeStats {
        private int vertexCount;
        private int validCount;
        private int errorCount;
        private int missingGuid;
        private int missingQualifiedName;
        private long totalEdgeCount;

        public synchronized void incrVertexCount()        { vertexCount++; }
        public synchronized void incrValidCount()         { validCount++; }
        public synchronized void incrErrorCount()         { errorCount++; }
        public synchronized void incrMissingGuid()        { missingGuid++; }
        public synchronized void incrMissingQualifiedName() { missingQualifiedName++; }
        public synchronized void addEdgeCount(long c)     { totalEdgeCount += c; }

        public int getVertexCount()        { return vertexCount; }
        public int getValidCount()         { return validCount; }
        public int getErrorCount()         { return errorCount; }
        public int getMissingGuid()        { return missingGuid; }
        public int getMissingQualifiedName() { return missingQualifiedName; }
        public long getTotalEdgeCount()    { return totalEdgeCount; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("vertex_count", vertexCount);
            m.put("valid_count", validCount);
            m.put("error_count", errorCount);
            m.put("missing_guid", missingGuid);
            m.put("missing_qualified_name", missingQualifiedName);
            m.put("total_edge_count", totalEdgeCount);
            return m;
        }
    }
}
