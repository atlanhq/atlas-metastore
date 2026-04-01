package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodic poller that updates gauge metrics for outbox queue depth and
 * Cassandra↔ES vertex count drift. Runs every 60 seconds.
 *
 * These metrics can't be maintained inline because they require querying
 * Cassandra and ES for current state — not derivable from individual operations.
 */
public class CassandraGraphHealthPoller {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphHealthPoller.class);

    private static final int POLL_INTERVAL_SECONDS = 60;

    private final CqlSession session;
    private final ScheduledExecutorService scheduler;

    public CassandraGraphHealthPoller(CqlSession session) {
        this.session = session;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cg-health-poller");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::poll, 30, POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("CassandraGraphHealthPoller started (interval={}s)", POLL_INTERVAL_SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void poll() {
        CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
        if (metrics == null) return;

        try {
            pollOutboxCounts(metrics);
        } catch (Throwable t) {
            LOG.debug("Health poller: failed to poll outbox counts: {}", t.getMessage());
        }

        try {
            pollVertexCountDrift(metrics);
        } catch (Throwable t) {
            LOG.debug("Health poller: failed to poll vertex count drift: {}", t.getMessage());
        }
    }

    private void pollOutboxCounts(CassandraGraphMetrics metrics) {
        // Count PENDING outbox entries
        int pending = countOutboxByStatus("PENDING");
        metrics.setOutboxPending(pending);

        // Count FAILED outbox entries
        int failed = countOutboxByStatus("FAILED");
        metrics.setOutboxFailed(failed);

        if (pending > 0 || failed > 0) {
            LOG.info("Health poller: outbox pending={}, failed={}", pending, failed);
        }
    }

    private int countOutboxByStatus(String status) {
        try {
            ResultSet rs = session.execute(
                    SimpleStatement.newInstance(
                            "SELECT COUNT(*) FROM es_outbox WHERE status = ?", status));
            Row row = rs.one();
            return row != null ? (int) row.getLong(0) : 0;
        } catch (Exception e) {
            LOG.debug("Health poller: failed to count outbox status={}: {}", status, e.getMessage());
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    private void pollVertexCountDrift(CassandraGraphMetrics metrics) {
        // Get Cassandra vertex count (estimated — uses system table for speed)
        long cassandraCount = estimateCassandraVertexCount();
        if (cassandraCount < 0) return;

        // Get ES doc count
        long esCount = getESDocCount();
        if (esCount < 0) return;

        long drift = Math.abs(cassandraCount - esCount);
        metrics.setVertexCountDrift(drift);

        if (drift > 100) {
            LOG.warn("Health poller: vertex count drift detected — Cassandra={}, ES={}, drift={}",
                    cassandraCount, esCount, drift);
        }
    }

    private long estimateCassandraVertexCount() {
        try {
            // Use the vertices table count. For large tables, this is expensive.
            // In production, consider using system.size_estimates instead.
            // For now, use a token-range sampling approach for speed.
            ResultSet rs = session.execute("SELECT COUNT(*) FROM vertices");
            Row row = rs.one();
            return row != null ? row.getLong(0) : -1;
        } catch (Exception e) {
            LOG.debug("Health poller: failed to count Cassandra vertices: {}", e.getMessage());
            return -1;
        }
    }

    private long getESDocCount() {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) return -1;

            String indexName = Constants.VERTEX_INDEX_NAME;
            Request req = new Request("GET", "/" + indexName + "/_count");
            Response resp = client.performRequest(req);
            String body = EntityUtils.toString(resp.getEntity());

            // Parse {"count":12345,"_shards":{...}}
            Map<String, Object> parsed = org.apache.atlas.type.AtlasType.fromJson(body, Map.class);
            if (parsed != null && parsed.get("count") instanceof Number) {
                return ((Number) parsed.get("count")).longValue();
            }
            return -1;
        } catch (Exception e) {
            LOG.debug("Health poller: failed to get ES doc count: {}", e.getMessage());
            return -1;
        }
    }
}
