package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Duration;
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

    // Source/target metadata for display
    private String sourceKeyspace;
    private String sourceEsIndex;
    private String targetKeyspace;
    private String targetEsIndex;

    // Source-side counters (direct query, independent of baseline)
    private long sourceEdgestoreCount = -1;
    private long sourceEsDocCount = -1;

    // Summary counters (target)
    private long vertexCount;
    private long edgeOutCount;
    private long edgeInCount;
    private long edgeByIdCount;
    private long esDocCount;
    private long typeDefCount;
    private long typeDefByCategoryCount;
    private long edgeIndexCount;
    private long configStoreEntryCount;
    private long tagsCount;
    private long propagatedTagsCount;

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

    public long getEdgeIndexCount()                { return edgeIndexCount; }
    public void setEdgeIndexCount(long c)          { this.edgeIndexCount = c; }

    public long getConfigStoreEntryCount()         { return configStoreEntryCount; }
    public void setConfigStoreEntryCount(long c)   { this.configStoreEntryCount = c; }

    public long getTagsCount()                     { return tagsCount; }
    public void setTagsCount(long c)               { this.tagsCount = c; }

    public long getPropagatedTagsCount()           { return propagatedTagsCount; }
    public void setPropagatedTagsCount(long c)     { this.propagatedTagsCount = c; }

    public SuperVertexReport getSuperVertexReport()             { return superVertexReport; }
    public void setSuperVertexReport(SuperVertexReport r)       { this.superVertexReport = r; }

    public SourceBaselineCollector.BaselineSnapshot getSourceBaseline()  { return sourceBaseline; }
    public void setSourceBaseline(SourceBaselineCollector.BaselineSnapshot b) { this.sourceBaseline = b; }

    public Map<String, TypeStats> getTypeStatsMap() { return typeStatsMap; }

    public Map<String, Long> getVertexCountByType()            { return vertexCountByType; }
    public void setVertexCountByType(Map<String, Long> counts) { this.vertexCountByType = counts; }

    public long getSourceEdgestoreCount()           { return sourceEdgestoreCount; }
    public void setSourceEdgestoreCount(long c)     { this.sourceEdgestoreCount = c; }

    public long getSourceEsDocCount()               { return sourceEsDocCount; }
    public void setSourceEsDocCount(long c)         { this.sourceEsDocCount = c; }

    public void setSourceKeyspace(String s)  { this.sourceKeyspace = s; }
    public void setSourceEsIndex(String s)   { this.sourceEsIndex = s; }
    public void setTargetKeyspace(String s)  { this.targetKeyspace = s; }
    public void setTargetEsIndex(String s)   { this.targetEsIndex = s; }

    /**
     * Beautified console output for human readability.
     */
    public String toPrettyString() {
        StringBuilder sb = new StringBuilder();
        String LINE = "════════════════════════════════════════════════════════════════════════════════";
        String THIN = "────────────────────────────────────────────────────────────────────────────────";

        // Header
        sb.append("\n").append(LINE).append("\n");
        sb.append("  MIGRATION VALIDATION REPORT\n");
        sb.append(LINE).append("\n\n");

        // Metadata
        sb.append("  Tenant:       ").append(tenantId).append("\n");
        if (sourceKeyspace != null) {
            sb.append("  Source:       ").append(sourceKeyspace).append(" (Cassandra)");
            if (sourceEsIndex != null) sb.append("  |  ").append(sourceEsIndex).append(" (ES)");
            sb.append("\n");
        }
        if (targetKeyspace != null) {
            sb.append("  Target:       ").append(targetKeyspace).append(" (Cassandra)");
            if (targetEsIndex != null) sb.append("  |  ").append(targetEsIndex).append(" (ES)");
            sb.append("\n");
        }
        sb.append("  Started:      ").append(startTime).append("\n");
        if (endTime != null) {
            Duration d = Duration.between(startTime, endTime);
            sb.append("  Duration:     ").append(formatDuration(d)).append("\n");
        }
        String resultStr = overallPassed ? "PASSED" : "FAILED";
        sb.append("  Result:       ").append(resultStr).append("\n");
        sb.append("\n");

        // Source section (direct query against old data)
        if (sourceEdgestoreCount >= 0 || sourceEsDocCount >= 0) {
            sb.append(THIN).append("\n");
            sb.append("  SOURCE (Old Data)\n");
            sb.append(THIN).append("\n\n");
            if (sourceEdgestoreCount >= 0) {
                sb.append(String.format("  %-36s %,12d\n",
                    "Edgestore Rows (" + (sourceKeyspace != null ? sourceKeyspace : "JanusGraph") + ")",
                    sourceEdgestoreCount));
            }
            if (sourceEsDocCount >= 0) {
                sb.append(String.format("  %-36s %,12d\n",
                    "ES Docs (" + (sourceEsIndex != null ? sourceEsIndex : "source") + ")",
                    sourceEsDocCount));
            }
            if (sourceBaseline != null) {
                sb.append(String.format("  %-36s %,12d\n", "Baseline: Vertices (from scan)", sourceBaseline.totalVertices));
                sb.append(String.format("  %-36s %,12d\n", "Baseline: Edges (from scan)", sourceBaseline.totalEdges));
            }
            sb.append("\n");
        } else if (sourceBaseline != null) {
            sb.append(THIN).append("\n");
            sb.append("  SOURCE (Baseline from prior scan)\n");
            sb.append(THIN).append("\n\n");
            sb.append(String.format("  %-36s %,12d\n", "Baseline: Vertices", sourceBaseline.totalVertices));
            sb.append(String.format("  %-36s %,12d\n", "Baseline: Edges", sourceBaseline.totalEdges));
            sb.append("\n");
        }

        // Target counts summary
        sb.append(THIN).append("\n");
        sb.append("  TARGET (New Data)\n");
        sb.append(THIN).append("\n\n");
        sb.append(String.format("  %-36s %,12d\n", "Vertices", vertexCount));
        sb.append(String.format("  %-36s %,12d\n", "Edges OUT", edgeOutCount));
        sb.append(String.format("  %-36s %,12d\n", "Edges IN", edgeInCount));
        sb.append(String.format("  %-36s %,12d\n", "Edges BY_ID", edgeByIdCount));
        sb.append(String.format("  %-36s %,12d\n", "ES Docs (target index)", esDocCount));
        sb.append(String.format("  %-36s %,12d\n", "TypeDefs", typeDefCount));
        sb.append(String.format("  %-36s %,12d\n", "TypeDefs (by category)", typeDefByCategoryCount));
        sb.append(String.format("  %-36s %,12d\n", "Edge Index", edgeIndexCount));
        if (configStoreEntryCount >= 0) {
            sb.append(String.format("  %-36s %,12d\n", "config_store entries", configStoreEntryCount));
        }
        if (tagsCount >= 0) {
            sb.append(String.format("  %-36s %,12d\n", "tags_by_id rows", tagsCount));
        }
        if (propagatedTagsCount >= 0) {
            sb.append(String.format("  %-36s %,12d\n", "propagated_tags rows", propagatedTagsCount));
        }
        sb.append("\n");

        // Validation checks table
        sb.append(THIN).append("\n");
        sb.append("  VALIDATION CHECKS (").append(checks.size()).append(")\n");
        sb.append(THIN).append("\n\n");
        sb.append(String.format("  %-4s %-8s %-30s %s\n", "#", "Status", "Check", "Message"));
        sb.append(String.format("  %-4s %-8s %-30s %s\n", "──", "──────", "─────", "───────"));

        int idx = 1;
        for (ValidationCheckResult c : checks) {
            String icon;
            switch (c.getSeverity()) {
                case PASS: icon = "PASS"; break;
                case WARN: icon = "WARN"; break;
                case FAIL: icon = "FAIL"; break;
                default:   icon = "????"; break;
            }
            // Truncate message for display (keep first 80 chars)
            String msg = c.getMessage();
            if (msg != null && msg.length() > 80) {
                msg = msg.substring(0, 77) + "...";
            }
            sb.append(String.format("  %-4d %-8s %-30s %s\n", idx, icon, c.getCheckName(), msg));

            // Show sample failures if any
            if (!c.getSampleFailures().isEmpty()) {
                for (String f : c.getSampleFailures()) {
                    String fTrunc = f.length() > 90 ? f.substring(0, 87) + "..." : f;
                    sb.append(String.format("  %4s %8s %-30s   -> %s\n", "", "", "", fTrunc));
                }
            }
            idx++;
        }
        sb.append("\n");

        // Vertex count by type (top 20)
        if (!vertexCountByType.isEmpty()) {
            sb.append(THIN).append("\n");
            sb.append("  VERTEX COUNT BY TYPE (Top 20)\n");
            sb.append(THIN).append("\n\n");

            boolean hasBaseline = sourceBaseline != null && sourceBaseline.typeVertexCounts != null;

            if (hasBaseline) {
                sb.append(String.format("  %-35s %12s %12s %10s %8s\n",
                    "Type", "Target", "Source", "Diff", "Status"));
                sb.append(String.format("  %-35s %12s %12s %10s %8s\n",
                    "─────", "──────", "──────", "────", "──────"));
            } else {
                sb.append(String.format("  %-35s %12s\n", "Type", "Count"));
                sb.append(String.format("  %-35s %12s\n", "─────", "─────"));
            }

            int count = 0;
            List<Map.Entry<String, Long>> sorted = new ArrayList<>(vertexCountByType.entrySet());
            sorted.sort(Map.Entry.<String, Long>comparingByValue().reversed());

            for (Map.Entry<String, Long> e : sorted) {
                if (count >= 20) break;
                String type = e.getKey();
                long tgt = e.getValue();

                if (hasBaseline) {
                    long src = sourceBaseline.typeVertexCounts.getOrDefault(type, 0L);
                    long diff = tgt - src;
                    String status = (src > 0 && diff < 0) ? "MISSING" : "OK";
                    sb.append(String.format("  %-35s %,12d %,12d %,10d %8s\n",
                        truncate(type, 35), tgt, src, diff, status));
                } else {
                    sb.append(String.format("  %-35s %,12d\n", truncate(type, 35), tgt));
                }
                count++;
            }

            int remaining = sorted.size() - 20;
            if (remaining > 0) {
                sb.append(String.format("  ... and %d more types\n", remaining));
            }
            sb.append("\n");
        }

        // Super vertex summary
        if (superVertexReport != null && superVertexReport.getTotalSuperVertexCount() > 0) {
            sb.append(THIN).append("\n");
            sb.append("  SUPER VERTICES (").append(superVertexReport.getTotalSuperVertexCount()).append(" found)\n");
            sb.append(THIN).append("\n\n");
            sb.append(String.format("  %-40s %-20s %12s\n", "Vertex ID", "Type", "Edge Count"));
            sb.append(String.format("  %-40s %-20s %12s\n", "─────────", "────", "──────────"));
            for (SuperVertexReport.SuperVertexEntry entry : superVertexReport.getTopSuperVertices()) {
                sb.append(String.format("  %-40s %-20s %,12d\n",
                    truncate(entry.getVertexId(), 40),
                    truncate(entry.getTypeName() != null ? entry.getTypeName() : "unknown", 20),
                    entry.getEdgeCount()));
            }
            sb.append("\n");
            sb.append(String.format("  Buckets:  >1K edges: %d  |  >10K: %d  |  >100K: %d  |  >1M: %d\n",
                superVertexReport.getVerticesOver1kEdges(),
                superVertexReport.getVerticesOver10kEdges(),
                superVertexReport.getVerticesOver100kEdges(),
                superVertexReport.getVerticesOver1mEdges()));
            sb.append("\n");
        }

        // Final verdict
        sb.append(LINE).append("\n");
        if (overallPassed) {
            long passCount = checks.stream().filter(ValidationCheckResult::isPassed).count();
            sb.append("  RESULT: VALIDATION PASSED\n");
            sb.append("  All ").append(passCount).append("/").append(checks.size())
              .append(" checks passed. Migration is safe for cutover.\n");
        } else {
            long failCount = checks.stream().filter(c -> !c.isPassed()).count();
            sb.append("  RESULT: VALIDATION FAILED\n");
            sb.append("  ").append(failCount).append("/").append(checks.size())
              .append(" checks failed. Migration is NOT safe for cutover.\n");
            sb.append("  Failed checks:\n");
            for (ValidationCheckResult c : checks) {
                if (!c.isPassed()) {
                    sb.append("    - ").append(c.getCheckName()).append(": ").append(c.getMessage()).append("\n");
                }
            }
        }
        sb.append(LINE).append("\n");

        return sb.toString();
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }

    private static String formatDuration(Duration d) {
        long secs = d.getSeconds();
        if (secs < 60) return secs + "s";
        if (secs < 3600) return String.format("%dm %ds", secs / 60, secs % 60);
        return String.format("%dh %dm %ds", secs / 3600, (secs % 3600) / 60, secs % 60);
    }

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
        m.put("edge_index_count", edgeIndexCount);
        if (configStoreEntryCount >= 0) m.put("config_store_entry_count", configStoreEntryCount);
        if (tagsCount >= 0) m.put("tags_count", tagsCount);
        if (propagatedTagsCount >= 0) m.put("propagated_tags_count", propagatedTagsCount);

        // Source-side direct counts
        if (sourceEdgestoreCount >= 0) {
            m.put("source_edgestore_count", sourceEdgestoreCount);
        }
        if (sourceEsDocCount >= 0) {
            m.put("source_es_doc_count", sourceEsDocCount);
        }

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
                    } else if (src > 0 && diff > 0) {
                        entry.put("status", "EXTRA");
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
