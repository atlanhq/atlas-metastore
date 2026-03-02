package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Background reconciliation job that audits Cassandra↔ES consistency via random sampling.
 *
 * The ES outbox provides deterministic coverage of recent failures. This job catches edge
 * cases where the outbox entry itself was lost (e.g., Cassandra write of outbox failed).
 * A random sample of ~1000 GUIDs is checked each cycle — no full scan needed.
 *
 * Runs every 6 hours, lease-guarded so only one pod executes at a time.
 */
public class ESReconciliationJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ESReconciliationJob.class);

    private static final String LEASE_NAME = "es-reconciliation";
    private static final int LEASE_TTL_SECONDS = 2 * 3600; // 2 hours

    private static final int SAMPLE_PROBES = 5;       // number of random token probes
    private static final int ROWS_PER_PROBE = 200;    // rows per probe → ~1000 total
    private static final int MGET_BATCH_SIZE = 100;
    private static final int REINDEX_BATCH_SIZE = 50;
    private static final double CRITICAL_MISS_RATE = 0.05; // 5%

    private final CqlSession session;
    private final CassandraGraph graph;
    private final JobLeaseManager leaseManager;

    public ESReconciliationJob(CqlSession session, CassandraGraph graph, JobLeaseManager leaseManager) {
        this.session = session;
        this.graph = graph;
        this.leaseManager = leaseManager;
    }

    @Override
    public void run() {
        if (!leaseManager.tryAcquire(LEASE_NAME, LEASE_TTL_SECONDS)) {
            LOG.info("ESReconciliationJob: another pod holds the lease, skipping");
            return;
        }

        try {
            runAudit();
        } finally {
            leaseManager.release(LEASE_NAME);
        }
    }

    private void runAudit() {
        LOG.info("ESReconciliationJob: starting sample-based ES audit ({} probes x {} rows)",
                SAMPLE_PROBES, ROWS_PER_PROBE);
        long startTime = System.currentTimeMillis();

        try {
            RestClient esClient = AtlasElasticsearchDatabase.getLowLevelClient();
            if (esClient == null) {
                LOG.warn("ESReconciliationJob: ES client not available, skipping");
                return;
            }

            int totalSampled = 0;
            int totalMissing = 0;
            int totalReindexed = 0;

            // Sample vertices by picking random token positions across the Cassandra ring.
            // Uses the vertices table (simple PK) instead of vertex_index (composite PK)
            // to avoid ALLOW FILTERING restrictions on token-range scans.
            for (int probe = 0; probe < SAMPLE_PROBES; probe++) {
                long randomToken = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, Long.MAX_VALUE);

                SimpleStatement stmt = SimpleStatement.builder(
                        "SELECT vertex_id, properties FROM vertices " +
                        "WHERE token(vertex_id) >= ? LIMIT ?")
                    .addPositionalValues(randomToken, ROWS_PER_PROBE)
                    .build();

                ResultSet rs = session.execute(stmt);

                List<String> guidBatch = new ArrayList<>(MGET_BATCH_SIZE);
                Map<String, String> guidToVertexId = new LinkedHashMap<>();

                for (Row row : rs) {
                    String vertexId = row.getString("vertex_id");
                    String propsJson = row.getString("properties");
                    if (vertexId == null || propsJson == null) continue;

                    // Extract __guid from properties, skip soft-deleted vertices
                    String guid = extractGuid(propsJson);
                    if (guid == null) continue;
                    if (isDeleted(propsJson)) continue;

                    guidBatch.add(guid);
                    guidToVertexId.put(guid, vertexId);
                    totalSampled++;

                    if (guidBatch.size() >= MGET_BATCH_SIZE) {
                        Set<String> missing = findMissingInES(esClient, guidBatch, guidToVertexId);
                        totalMissing += missing.size();
                        if (!missing.isEmpty()) {
                            totalReindexed += reindexMissing(missing);
                        }
                        guidBatch.clear();
                        guidToVertexId.clear();
                    }
                }

                // Process remaining batch for this probe
                if (!guidBatch.isEmpty()) {
                    Set<String> missing = findMissingInES(esClient, guidBatch, guidToVertexId);
                    totalMissing += missing.size();
                    if (!missing.isEmpty()) {
                        totalReindexed += reindexMissing(missing);
                    }
                }
            }

            double missRate = totalSampled > 0 ? (double) totalMissing / totalSampled : 0;
            long elapsed = System.currentTimeMillis() - startTime;

            if (missRate > CRITICAL_MISS_RATE) {
                LOG.error("ESReconciliationJob: CRITICAL — miss rate {}/{} ({}) exceeds {}% threshold. " +
                          "This indicates a systemic ES sync problem.",
                        totalMissing, totalSampled, String.format("%.1f%%", missRate * 100),
                        (int)(CRITICAL_MISS_RATE * 100));
            }

            LOG.info("ESReconciliationJob: completed in {}ms — sampled {} GUIDs, {} missing from ES, {} reindexed (miss rate: {})",
                    elapsed, totalSampled, totalMissing, totalReindexed, String.format("%.2f%%", missRate * 100));

        } catch (Exception e) {
            LOG.error("ESReconciliationJob: failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private String extractGuid(String propsJson) {
        try {
            Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
            if (props == null) return null;
            Object guid = props.get("__guid");
            return guid != null ? String.valueOf(guid) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isDeleted(String propsJson) {
        try {
            // Fast string check before full parse
            if (!propsJson.contains("DELETED")) return false;
            @SuppressWarnings("unchecked")
            Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
            if (props == null) return false;
            Object state = props.get("__state");
            return state != null && "DELETED".equals(String.valueOf(state));
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> findMissingInES(RestClient esClient, List<String> guids,
                                         Map<String, String> guidToVertexId) {
        Set<String> missing = new LinkedHashSet<>();
        try {
            String indexName = Constants.VERTEX_INDEX_NAME;

            StringBuilder mgetBody = new StringBuilder("{\"docs\":[");
            boolean first = true;
            for (String guid : guids) {
                String vertexId = guidToVertexId.get(guid);
                if (vertexId == null) continue;
                if (!first) mgetBody.append(",");
                mgetBody.append("{\"_index\":\"").append(indexName)
                        .append("\",\"_id\":\"").append(vertexId)
                        .append("\",\"_source\":false}");
                first = false;
            }
            mgetBody.append("]}");

            Request mgetReq = new Request("POST", "/_mget");
            mgetReq.setJsonEntity(mgetBody.toString());
            Response resp = esClient.performRequest(mgetReq);
            String respBody = EntityUtils.toString(resp.getEntity());

            Map<String, Object> mgetResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> docs = (List<Map<String, Object>>) mgetResp.get("docs");
            if (docs == null) return missing;

            Map<String, String> vertexIdToGuid = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : guidToVertexId.entrySet()) {
                vertexIdToGuid.put(entry.getValue(), entry.getKey());
            }

            for (Map<String, Object> doc : docs) {
                Boolean found = (Boolean) doc.get("found");
                if (found == null || !found) {
                    String vertexId = String.valueOf(doc.get("_id"));
                    String guid = vertexIdToGuid.get(vertexId);
                    if (guid != null) {
                        missing.add(guid);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("ESReconciliationJob: _mget check failed for batch of {} GUIDs: {}",
                    guids.size(), e.getMessage());
        }
        return missing;
    }

    private int reindexMissing(Set<String> guids) {
        try {
            List<String> batch = new ArrayList<>(REINDEX_BATCH_SIZE);
            int totalReindexed = 0;

            for (String guid : guids) {
                batch.add(guid);
                if (batch.size() >= REINDEX_BATCH_SIZE) {
                    totalReindexed += graph.reindexVertices(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                totalReindexed += graph.reindexVertices(batch);
            }

            LOG.info("ESReconciliationJob: reindexed {} of {} missing GUIDs", totalReindexed, guids.size());
            return totalReindexed;
        } catch (Exception e) {
            LOG.error("ESReconciliationJob: reindex failed for {} GUIDs: {}", guids.size(), e.getMessage());
            return 0;
        }
    }
}
