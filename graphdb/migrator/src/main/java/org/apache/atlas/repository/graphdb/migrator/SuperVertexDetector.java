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
     */
    public SuperVertexReport detect() {
        long startMs = System.currentTimeMillis();

        // Phase 1: Count outgoing edges per vertex
        LOG.info("Super vertex detection: scanning edges_out...");
        Map<String, Long> outEdgeCounts = new HashMap<>();
        Map<String, Map<String, Long>> outLabelCounts = new HashMap<>();
        scanEdgeTable(keyspace + ".edges_out", "out_vertex_id", outEdgeCounts, outLabelCounts);

        // Phase 2: Count incoming edges per vertex
        LOG.info("Super vertex detection: scanning edges_in...");
        Map<String, Long> inEdgeCounts = new HashMap<>();
        Map<String, Map<String, Long>> inLabelCounts = new HashMap<>();
        scanEdgeTable(keyspace + ".edges_in", "in_vertex_id", inEdgeCounts, inLabelCounts);

        // Phase 3: Merge and identify super vertices
        Set<String> allVertexIds = new HashSet<>(outEdgeCounts.keySet());
        allVertexIds.addAll(inEdgeCounts.keySet());

        long maxEdgeCount = 0;
        int superVertexCount = 0;
        long over1k = 0, over10k = 0, over100k = 0, over1m = 0;

        // Min-heap for top-N tracking
        PriorityQueue<SuperVertexReport.SuperVertexEntry> topQueue =
            new PriorityQueue<>(topN + 1);

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

                // Merge label counts
                Map<String, Long> mergedLabels = new LinkedHashMap<>();
                Map<String, Long> outLabels = outLabelCounts.get(vertexId);
                Map<String, Long> inLabels  = inLabelCounts.get(vertexId);
                if (outLabels != null) mergedLabels.putAll(outLabels);
                if (inLabels != null) {
                    for (Map.Entry<String, Long> e : inLabels.entrySet()) {
                        mergedLabels.merge(e.getKey(), e.getValue(), Long::sum);
                    }
                }

                String typeName = lookupTypeName(vertexId);

                SuperVertexReport.SuperVertexEntry entry =
                    new SuperVertexReport.SuperVertexEntry(vertexId, typeName, totalEdges, mergedLabels);

                topQueue.add(entry);
                if (topQueue.size() > topN) {
                    topQueue.poll();
                }
            }
        }

        // Build report
        SuperVertexReport report = new SuperVertexReport();
        report.setTotalSuperVertexCount(superVertexCount);
        report.setMaxEdgeCount(maxEdgeCount);
        report.setTotalVerticesScanned(allVertexIds.size());
        report.setScanDurationMs(System.currentTimeMillis() - startMs);
        report.setVerticesOver1kEdges(over1k);
        report.setVerticesOver10kEdges(over10k);
        report.setVerticesOver100kEdges(over100k);
        report.setVerticesOver1mEdges(over1m);

        // Top-N in descending order
        List<SuperVertexReport.SuperVertexEntry> topList = new ArrayList<>(topQueue);
        topList.sort(Comparator.comparingLong(
            SuperVertexReport.SuperVertexEntry::getEdgeCount).reversed());
        report.setTopSuperVertices(topList);

        LOG.info("Super vertex detection complete in {}ms: {} super vertices " +
                 "(threshold={}), max edges={}, distinct vertices={}",
                 report.getScanDurationMs(), superVertexCount,
                 superVertexThreshold, maxEdgeCount, allVertexIds.size());

        return report;
    }

    /**
     * Scan an edge table counting edges per partition key vertex.
     * Only selects partition key + edge_label to minimize data transfer.
     */
    private void scanEdgeTable(String table, String partitionKeyColumn,
                               Map<String, Long> edgeCounts,
                               Map<String, Map<String, Long>> labelCounts) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT " + partitionKeyColumn + ", edge_label FROM " + table)
            .setPageSize(5000)
            .build();

        ResultSet rs = session.execute(stmt);
        long rowsRead = 0;

        for (Row row : rs) {
            String vertexId = row.getString(partitionKeyColumn);
            String edgeLabel = row.getString("edge_label");
            rowsRead++;

            edgeCounts.merge(vertexId, 1L, Long::sum);

            if (edgeLabel != null) {
                labelCounts
                    .computeIfAbsent(vertexId, k -> new HashMap<>())
                    .merge(edgeLabel, 1L, Long::sum);
            }

            if (rowsRead % 1_000_000 == 0) {
                LOG.info("  ... scanned {} rows from {}, {} distinct vertices",
                         String.format("%,d", rowsRead), table, edgeCounts.size());
            }
        }

        LOG.info("Scan complete for {}: {} rows, {} distinct vertices",
                 table, String.format("%,d", rowsRead), edgeCounts.size());
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
