package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe collector for source-side baseline statistics.
 * Called during Phase 1 (JanusGraph scan) for each decoded vertex.
 * Captures vertex/edge counts, per-type distribution, and top-N super vertices.
 *
 * Persists baseline to the migration_state table so Phase 3 validation
 * can compare source vs target counts.
 */
public class SourceBaselineCollector {

    private static final Logger LOG = LoggerFactory.getLogger(SourceBaselineCollector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BASELINE_PHASE = "source_baseline";

    private final int topN;
    private final int superVertexThreshold;

    // Thread-safe counters
    private final AtomicLong totalVertices = new AtomicLong(0);
    private final AtomicLong totalEdges    = new AtomicLong(0);

    // Per-type vertex counts
    private final ConcurrentHashMap<String, AtomicLong> typeVertexCounts = new ConcurrentHashMap<>();

    // Top-N super vertices by edge count (synchronized access)
    private final PriorityQueue<SuperVertexReport.SuperVertexEntry> topSuperVertices;

    // Actual count of super vertices (not capped by topN heap)
    private final AtomicLong superVertexCount = new AtomicLong(0);

    // Distribution counters
    private final AtomicLong verticesOver1k   = new AtomicLong(0);
    private final AtomicLong verticesOver10k  = new AtomicLong(0);
    private final AtomicLong verticesOver100k = new AtomicLong(0);
    private final AtomicLong verticesOver1m   = new AtomicLong(0);
    private final AtomicLong maxEdgeCount     = new AtomicLong(0);

    public SourceBaselineCollector(int superVertexThreshold, int topN) {
        this.superVertexThreshold = superVertexThreshold;
        this.topN = topN;
        this.topSuperVertices = new PriorityQueue<>(topN + 1);
    }

    /**
     * Record a decoded vertex from the source scan.
     * Called from scanner threads — must be thread-safe.
     */
    public void recordVertex(DecodedVertex vertex) {
        totalVertices.incrementAndGet();

        int edgeCount = vertex.getOutEdges().size();
        totalEdges.addAndGet(edgeCount);

        // Per-type count
        String typeName = vertex.getTypeName();
        if (typeName != null) {
            typeVertexCounts.computeIfAbsent(typeName, k -> new AtomicLong(0))
                            .incrementAndGet();
        }

        // Distribution buckets
        if (edgeCount > 1000)    verticesOver1k.incrementAndGet();
        if (edgeCount > 10_000)  verticesOver10k.incrementAndGet();
        if (edgeCount > 100_000) verticesOver100k.incrementAndGet();
        if (edgeCount > 1_000_000) verticesOver1m.incrementAndGet();

        // Update max
        long currentMax;
        do {
            currentMax = maxEdgeCount.get();
            if (edgeCount <= currentMax) break;
        } while (!maxEdgeCount.compareAndSet(currentMax, edgeCount));

        // Track top-N super vertices
        if (edgeCount >= superVertexThreshold) {
            superVertexCount.incrementAndGet();
            Map<String, Long> labelCounts = new LinkedHashMap<>();
            for (DecodedEdge edge : vertex.getOutEdges()) {
                labelCounts.merge(edge.getLabel(), 1L, Long::sum);
            }

            SuperVertexReport.SuperVertexEntry entry = new SuperVertexReport.SuperVertexEntry(
                vertex.getVertexId(), typeName, edgeCount, labelCounts);

            synchronized (topSuperVertices) {
                topSuperVertices.add(entry);
                if (topSuperVertices.size() > topN) {
                    topSuperVertices.poll(); // remove smallest
                }
            }
        }
    }

    /**
     * Build a SuperVertexReport from the collected source data.
     */
    public SuperVertexReport buildSuperVertexReport(long scanDurationMs) {
        SuperVertexReport report = new SuperVertexReport();
        report.setTotalVerticesScanned(totalVertices.get());
        report.setMaxEdgeCount(maxEdgeCount.get());
        report.setScanDurationMs(scanDurationMs);
        report.setVerticesOver1kEdges(verticesOver1k.get());
        report.setVerticesOver10kEdges(verticesOver10k.get());
        report.setVerticesOver100kEdges(verticesOver100k.get());
        report.setVerticesOver1mEdges(verticesOver1m.get());

        // Use actual counter (not heap size which is capped at topN)
        report.setTotalSuperVertexCount((int) superVertexCount.get());
        List<SuperVertexReport.SuperVertexEntry> topList;
        synchronized (topSuperVertices) {
            topList = new ArrayList<>(topSuperVertices);
        }
        topList.sort(Comparator.comparingLong(
            SuperVertexReport.SuperVertexEntry::getEdgeCount).reversed());
        report.setTopSuperVertices(topList);

        return report;
    }

    /**
     * Build a snapshot of the baseline for persistence and comparison.
     */
    public BaselineSnapshot buildSnapshot() {
        BaselineSnapshot snap = new BaselineSnapshot();
        snap.totalVertices = totalVertices.get();
        snap.totalEdges    = totalEdges.get();
        snap.typeVertexCounts = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> e : typeVertexCounts.entrySet()) {
            snap.typeVertexCounts.put(e.getKey(), e.getValue().get());
        }
        snap.maxEdgeCount      = maxEdgeCount.get();
        snap.verticesOver1k    = verticesOver1k.get();
        snap.verticesOver10k   = verticesOver10k.get();
        snap.verticesOver100k  = verticesOver100k.get();
        snap.verticesOver1m    = verticesOver1m.get();
        return snap;
    }

    /**
     * Save baseline to migration_state table as a JSON blob.
     */
    public void saveBaseline(MigrationStateStore stateStore) {
        try {
            BaselineSnapshot snap = buildSnapshot();
            String json = MAPPER.writeValueAsString(snap.toMap());
            // Persist totals in migration_state for backward compatibility
            stateStore.markRangeCompleted(BASELINE_PHASE, 0L, 0L, snap.totalVertices, snap.totalEdges);
            // Persist full JSON (including typeVertexCounts) in migration_metadata
            stateStore.saveMetadata(BASELINE_PHASE, json);

            LOG.info("Source baseline saved: vertices={}, edges={}, types={}, maxEdgeCount={}",
                     snap.totalVertices, snap.totalEdges, snap.typeVertexCounts.size(), snap.maxEdgeCount);
        } catch (Exception e) {
            LOG.warn("Failed to save source baseline: {}", e.getMessage());
        }
    }

    /**
     * Load baseline from migration_state table (from a previous run).
     * Returns null if no baseline exists.
     */
    @SuppressWarnings("unchecked")
    public static BaselineSnapshot loadBaseline(MigrationStateStore stateStore) {
        try {
            // Try loading full JSON first (includes typeVertexCounts)
            String json = stateStore.loadMetadata(BASELINE_PHASE);
            if (json != null) {
                Map<String, Object> map = MAPPER.readValue(json, Map.class);
                BaselineSnapshot snap = new BaselineSnapshot();
                snap.totalVertices  = ((Number) map.getOrDefault("total_vertices", 0)).longValue();
                snap.totalEdges     = ((Number) map.getOrDefault("total_edges", 0)).longValue();
                snap.maxEdgeCount   = ((Number) map.getOrDefault("max_edge_count", 0)).longValue();
                snap.verticesOver1k   = ((Number) map.getOrDefault("vertices_over_1k", 0)).longValue();
                snap.verticesOver10k  = ((Number) map.getOrDefault("vertices_over_10k", 0)).longValue();
                snap.verticesOver100k = ((Number) map.getOrDefault("vertices_over_100k", 0)).longValue();
                snap.verticesOver1m   = ((Number) map.getOrDefault("vertices_over_1m", 0)).longValue();

                Object typeCounts = map.get("type_vertex_counts");
                if (typeCounts instanceof Map) {
                    snap.typeVertexCounts = new LinkedHashMap<>();
                    for (Map.Entry<String, Object> e : ((Map<String, Object>) typeCounts).entrySet()) {
                        snap.typeVertexCounts.put(e.getKey(), ((Number) e.getValue()).longValue());
                    }
                }

                LOG.info("Loaded source baseline (full): vertices={}, edges={}, types={}",
                         snap.totalVertices, snap.totalEdges,
                         snap.typeVertexCounts != null ? snap.typeVertexCounts.size() : 0);
                return snap;
            }

            // Fall back to legacy format (totals only)
            long[] summary = stateStore.getPhaseSummary(BASELINE_PHASE);
            if (summary[0] == 0) {
                LOG.info("No source baseline found from previous run");
                return null;
            }
            BaselineSnapshot snap = new BaselineSnapshot();
            snap.totalVertices = summary[1];
            snap.totalEdges    = summary[2];
            LOG.info("Loaded source baseline (legacy): vertices={}, edges={}", snap.totalVertices, snap.totalEdges);
            return snap;
        } catch (Exception e) {
            LOG.warn("Failed to load source baseline: {}", e.getMessage());
            return null;
        }
    }

    public long getTotalVertices() { return totalVertices.get(); }
    public long getTotalEdges()    { return totalEdges.get(); }

    /**
     * Serializable snapshot of the source baseline.
     */
    public static class BaselineSnapshot {
        public long totalVertices;
        public long totalEdges;
        public Map<String, Long> typeVertexCounts;
        public long maxEdgeCount;
        public long verticesOver1k;
        public long verticesOver10k;
        public long verticesOver100k;
        public long verticesOver1m;

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("total_vertices", totalVertices);
            m.put("total_edges", totalEdges);
            m.put("max_edge_count", maxEdgeCount);
            m.put("vertices_over_1k", verticesOver1k);
            m.put("vertices_over_10k", verticesOver10k);
            m.put("vertices_over_100k", verticesOver100k);
            m.put("vertices_over_1m", verticesOver1m);
            if (typeVertexCounts != null) {
                m.put("type_vertex_counts", typeVertexCounts);
            }
            return m;
        }
    }
}
