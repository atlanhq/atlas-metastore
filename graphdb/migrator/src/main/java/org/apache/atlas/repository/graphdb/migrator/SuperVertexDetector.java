package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Detects super vertices by scanning target edge tables.
 * Used in --validate-only mode and as a cross-check against Phase 1 baseline.
 *
 * Strategy:
 *   edges_out is partitioned by out_vertex_id. A full scan groups rows naturally by partition.
 *   edges_in is partitioned by in_vertex_id. Same approach.
 *   We merge out+in counts per vertex to get total edge degree.
 *
 *   Only partition key + edge_label columns are selected to minimize I/O.
 *   Top-N tracking uses a bounded min-heap (PriorityQueue) — O(1) memory.
 */
public class SuperVertexDetector {

    private static final Logger LOG = LoggerFactory.getLogger(SuperVertexDetector.class);

    private final CqlSession session;
    private final String keyspace;
    private final int superVertexThreshold;
    private final int topN;

    public SuperVertexDetector(CqlSession session, String keyspace,
                               int superVertexThreshold, int topN) {
        this.session             = session;
        this.keyspace            = keyspace;
        this.superVertexThreshold = superVertexThreshold;
        this.topN                = topN;
    }

    /**
     * Scan edge tables and detect super vertices.
     * Uses a two-pass approach to limit memory:
     *   Pass 1: Count edges per vertex (only vertex IDs + counts in memory)
     *   Pass 2: Fetch label breakdown only for identified super vertices
     */
    public SuperVertexReport detect() {
        long startMs = System.currentTimeMillis();

        // Pass 1: Count edges per vertex (no label tracking — saves memory)
        LOG.info("Super vertex detection: pass 1 — counting edges...");

        LOG.info("  Scanning edges_out...");
        Map<String, Long> outEdgeCounts = new HashMap<>();
        long edgesOutRowCount = scanEdgeCountsOnly(keyspace + ".edges_out", "out_vertex_id", outEdgeCounts);

        LOG.info("  Scanning edges_in...");
        Map<String, Long> inEdgeCounts = new HashMap<>();
        long edgesInRowCount = scanEdgeCountsOnly(keyspace + ".edges_in", "in_vertex_id", inEdgeCounts);

        LOG.info("  Scanning edges_by_id...");
        long edgesByIdRowCount = scanRowCount(keyspace + ".edges_by_id", "edge_id");

        // Merge and identify super vertices + compute stats
        Set<String> allVertexIds = new HashSet<>(outEdgeCounts.keySet());
        allVertexIds.addAll(inEdgeCounts.keySet());

        long maxEdgeCount = 0;
        int superVertexCount = 0;
        long over1k = 0, over10k = 0, over100k = 0, over1m = 0;
        Set<String> superVertexIds = new HashSet<>();

        for (String vertexId : allVertexIds) {
            long outCount = outEdgeCounts.getOrDefault(vertexId, 0L);
            long inCount  = inEdgeCounts.getOrDefault(vertexId, 0L);
            long totalEdges = outCount + inCount;

            if (totalEdges > maxEdgeCount) maxEdgeCount = totalEdges;
            if (totalEdges > 1000)      over1k++;
            if (totalEdges > 10_000)    over10k++;
            if (totalEdges > 100_000)   over100k++;
            if (totalEdges > 1_000_000) over1m++;

            if (totalEdges >= superVertexThreshold) {
                superVertexCount++;
                superVertexIds.add(vertexId);
            }
        }

        // Free count maps — no longer needed for non-super vertices
        long distinctVertices = allVertexIds.size();
        allVertexIds = null; // allow GC

        // Pass 2: Fetch label breakdown only for super vertices
        LOG.info("Super vertex detection: pass 2 — fetching labels for {} super vertices...", superVertexIds.size());

        PriorityQueue<SuperVertexReport.SuperVertexEntry> topQueue =
            new PriorityQueue<>(topN + 1);

        for (String vertexId : superVertexIds) {
            long outCount = outEdgeCounts.getOrDefault(vertexId, 0L);
            long inCount  = inEdgeCounts.getOrDefault(vertexId, 0L);
            long totalEdges = outCount + inCount;

            Map<String, Long> mergedLabels = new LinkedHashMap<>();
            fetchLabelCounts(keyspace + ".edges_out", "out_vertex_id", vertexId, mergedLabels);
            fetchLabelCounts(keyspace + ".edges_in", "in_vertex_id", vertexId, mergedLabels);

            String typeName = lookupTypeName(vertexId);

            SuperVertexReport.SuperVertexEntry entry =
                new SuperVertexReport.SuperVertexEntry(vertexId, typeName, totalEdges, mergedLabels);

            topQueue.add(entry);
            if (topQueue.size() > topN) {
                topQueue.poll();
            }
        }

        // Build report
        SuperVertexReport report = new SuperVertexReport();
        report.setTotalSuperVertexCount(superVertexCount);
        report.setMaxEdgeCount(maxEdgeCount);
        report.setTotalVerticesScanned(distinctVertices);
        report.setScanDurationMs(System.currentTimeMillis() - startMs);
        report.setVerticesOver1kEdges(over1k);
        report.setVerticesOver10kEdges(over10k);
        report.setVerticesOver100kEdges(over100k);
        report.setVerticesOver1mEdges(over1m);
        report.setEdgesOutRowCount(edgesOutRowCount);
        report.setEdgesInRowCount(edgesInRowCount);
        report.setEdgesByIdRowCount(edgesByIdRowCount);

        // Top-N in descending order
        List<SuperVertexReport.SuperVertexEntry> topList = new ArrayList<>(topQueue);
        topList.sort(Comparator.comparingLong(
            SuperVertexReport.SuperVertexEntry::getEdgeCount).reversed());
        report.setTopSuperVertices(topList);

        LOG.info("Super vertex detection complete in {}ms: {} super vertices " +
                 "(threshold={}), max edges={}, distinct vertices={}",
                 report.getScanDurationMs(), superVertexCount,
                 superVertexThreshold, maxEdgeCount, distinctVertices);

        return report;
    }

    /**
     * Pass 1: Scan an edge table counting edges per vertex. Only selects
     * the partition key column — no label tracking to save memory.
     */
    private long scanEdgeCountsOnly(String table, String partitionKeyColumn,
                                     Map<String, Long> edgeCounts) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT " + partitionKeyColumn + " FROM " + table)
            .setPageSize(5000)
            .build();

        ResultSet rs = session.execute(stmt);
        long rowsRead = 0;

        for (Row row : rs) {
            String vertexId = row.getString(partitionKeyColumn);
            rowsRead++;
            edgeCounts.merge(vertexId, 1L, Long::sum);

            if (rowsRead % 1_000_000 == 0) {
                LOG.info("  ... scanned {} rows from {}, {} distinct vertices",
                         String.format("%,d", rowsRead), table, edgeCounts.size());
            }
        }

        LOG.info("Scan complete for {}: {} rows, {} distinct vertices",
                 table, String.format("%,d", rowsRead), edgeCounts.size());
        return rowsRead;
    }

    /**
     * Lightweight row count scan — only counts rows, no grouping.
     * Used for edges_by_id where we just need the total count.
     */
    private long scanRowCount(String table, String partitionKeyColumn) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT " + partitionKeyColumn + " FROM " + table)
            .setPageSize(5000)
            .build();

        ResultSet rs = session.execute(stmt);
        long rowsRead = 0;

        for (Row row : rs) {
            rowsRead++;
            if (rowsRead % 1_000_000 == 0) {
                LOG.info("  ... scanned {} rows from {}",
                         String.format("%,d", rowsRead), table);
            }
        }

        LOG.info("Scan complete for {}: {} rows", table, String.format("%,d", rowsRead));
        return rowsRead;
    }

    /**
     * Pass 2: Fetch edge labels for a single super vertex and merge into label counts.
     */
    private void fetchLabelCounts(String table, String partitionKeyColumn,
                                   String vertexId, Map<String, Long> labelCounts) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT edge_label FROM " + table + " WHERE " + partitionKeyColumn + " = ?")
            .addPositionalValue(vertexId)
            .setPageSize(5000)
            .build();

        ResultSet rs = session.execute(stmt);
        for (Row row : rs) {
            String label = row.getString("edge_label");
            if (label != null) {
                labelCounts.merge(label, 1L, Long::sum);
            }
        }
    }

    private String lookupTypeName(String vertexId) {
        try {
            ResultSet rs = session.execute(
                SimpleStatement.newInstance(
                    "SELECT type_name FROM " + keyspace + ".vertices WHERE vertex_id = ?",
                    vertexId));
            Row row = rs.one();
            return row != null ? row.getString("type_name") : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
