package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Sets vertex count drift as the sum of outbox PENDING + FAILED entries.
     * These represent entities written to Cassandra but not yet (or failed to be)
     * synced to ES. This is the actionable drift metric — if it's > 0, search
     * results may be stale.
     *
     * We don't compare total C* vertex count vs ES doc count because C* includes
     * TypeDef/system vertices (~thousands) that are never indexed to ES, causing
     * a permanent false-positive drift.
     */
    private void pollVertexCountDrift(CassandraGraphMetrics metrics) {
        int pending = countOutboxByStatus("PENDING");
        int failed = countOutboxByStatus("FAILED");
        long drift = pending + failed;
        metrics.setVertexCountDrift(drift);

        if (drift > 0) {
            LOG.info("Health poller: vertex sync drift={} (pending={}, failed={})", drift, pending, failed);
        }
    }

}
