package org.apache.atlas.repository.graphdb.cassandra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized Prometheus metrics for the CassandraGraph (ZeroGraph) data plane.
 *
 * All metrics use the {@code cg_} prefix (CassandraGraph). The {@code clusterName}
 * label is added automatically by the fleet-level Prometheus scraper (vmagent),
 * not by this class.
 *
 * Singleton — call {@link #getInstance(MeterRegistry)} once at graph creation,
 * then {@link #getInstance()} everywhere else.
 *
 * Gauge values (outbox pending/failed, drift, miss rate) are updated inline by
 * the code paths that change them — no background polling of Cassandra.
 */
public class CassandraGraphMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphMetrics.class);
    private static final String PREFIX = "cg_";

    private static volatile CassandraGraphMetrics INSTANCE;

    private final MeterRegistry registry;
    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();

    // ---- Gauges (mutable state, updated inline — no C* polling) ----
    private final AtomicInteger outboxPending = new AtomicInteger(0);
    private final AtomicInteger outboxFailed = new AtomicInteger(0);
    private final AtomicInteger outboxDrainMode = new AtomicInteger(0);
    private final AtomicLong reconciliationMissRateBps = new AtomicLong(0); // basis points (missRate * 10000)

    private CassandraGraphMetrics(MeterRegistry registry) {
        this.registry = registry;
        registerGauges();
    }

    public static CassandraGraphMetrics getInstance(MeterRegistry registry) {
        if (INSTANCE == null) {
            synchronized (CassandraGraphMetrics.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CassandraGraphMetrics(registry);
                    LOG.info("CassandraGraphMetrics initialized");
                }
            }
        }
        return INSTANCE;
    }

    public static CassandraGraphMetrics getInstance() {
        return INSTANCE;
    }

    // =========================================================================
    // Section 1: Write Path (commit)
    // =========================================================================

    public void recordCommit(boolean success, long durationMs,
                             int verticesCreated, int verticesUpdated, int verticesDeleted,
                             int edgesCreated, int edgesDeleted, int batchCount, int claimConflicts) {
        String status = success ? "success" : "failure";
        getCounter("commit_total", "status", status).increment();
        getTimer("commit_duration_seconds").record(Duration.ofMillis(durationMs));

        if (verticesCreated > 0) getCounter("vertices_written_total", "op", "create").increment(verticesCreated);
        if (verticesUpdated > 0) getCounter("vertices_written_total", "op", "update").increment(verticesUpdated);
        if (verticesDeleted > 0) getCounter("vertices_written_total", "op", "delete").increment(verticesDeleted);
        if (edgesCreated > 0)    getCounter("edges_written_total", "op", "create").increment(edgesCreated);
        if (edgesDeleted > 0)    getCounter("edges_written_total", "op", "delete").increment(edgesDeleted);
        if (batchCount > 0)      getCounter("cassandra_batch_count_total").increment(batchCount);
        if (claimConflicts > 0)  getCounter("claim_conflicts_total").increment(claimConflicts);
    }

    // =========================================================================
    // Section 2: ES Sync
    // =========================================================================

    public void recordESSync(String result, long durationMs, int indexOps, int deleteOps) {
        getCounter("es_sync_total", "result", result).increment();
        getTimer("es_sync_duration_seconds").record(Duration.ofMillis(durationMs));
        if (indexOps > 0)  getCounter("es_sync_ops_total", "op", "index").increment(indexOps);
        if (deleteOps > 0) getCounter("es_sync_ops_total", "op", "delete").increment(deleteOps);
    }

    public void recordESSyncRetry(int attempt) {
        getCounter("es_sync_retry_total", "attempt", String.valueOf(attempt)).increment();
    }

    public void recordESBulkItems(String status, int count) {
        if (count > 0) getCounter("es_bulk_items_total", "status", status).increment(count);
    }

    // =========================================================================
    // Section 3: Outbox
    // =========================================================================

    /**
     * Records outbox processing outcomes. Supports batch counts to avoid
     * per-item loops at call sites.
     */
    public void recordOutboxProcessed(String result, int count) {
        if (count > 0) getCounter("outbox_processed_total", "result", result).increment(count);
    }

    /** Sets outbox PENDING gauge from the batch size returned by getPendingEntries(). */
    public void setOutboxPending(int count) {
        outboxPending.set(count);
    }

    /** Increments outbox FAILED gauge when entries are marked FAILED. */
    public void incrementOutboxFailed(int count) {
        outboxFailed.addAndGet(count);
    }

    /** Decrements outbox FAILED gauge when reconciliation drains entries. */
    public void decrementOutboxFailed(int count) {
        outboxFailed.addAndGet(-count);
    }

    public void setOutboxDrainMode(boolean drain) {
        outboxDrainMode.set(drain ? 1 : 0);
    }

    public void recordOutboxPollDuration(long durationMs) {
        getTimer("outbox_poll_duration_seconds").record(Duration.ofMillis(durationMs));
    }

    // =========================================================================
    // Section 4: Reconciliation & Data Integrity
    // =========================================================================

    public void recordReconciliation(long durationMs, int failedDrained,
                                     int sampled, int missing, int reindexed, double missRate) {
        getTimer("reconciliation_duration_seconds").record(Duration.ofMillis(durationMs));
        if (failedDrained > 0) getCounter("reconciliation_failed_drained_total").increment(failedDrained);
        if (sampled > 0)       getCounter("reconciliation_sampled_total").increment(sampled);
        if (missing > 0)       getCounter("reconciliation_missing_total").increment(missing);
        if (reindexed > 0)     getCounter("reconciliation_reindexed_total").increment(reindexed);
        reconciliationMissRateBps.set((long) (missRate * 10000));
    }

    public void recordOrphanEdgeScan(long durationMs, int scanned, int found, int deleted) {
        getTimer("orphan_scan_duration_seconds").record(Duration.ofMillis(durationMs));
        if (scanned > 0) getCounter("orphan_scan_rows_total").increment(scanned);
        if (found > 0)   getCounter("orphan_edges_found_total").increment(found);
        if (deleted > 0)  getCounter("orphan_edges_deleted_total").increment(deleted);
    }

    // =========================================================================
    // Section 5: Background Jobs
    // =========================================================================

    public void recordJobRun(String jobName, String result, long durationMs) {
        getCounter("job_run_total", "job", jobName, "result", result).increment();
        if (!"skipped_lease".equals(result)) {
            getTimer("job_duration_seconds", "job", jobName).record(Duration.ofMillis(durationMs));
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private void registerGauges() {
        Gauge.builder(PREFIX + "outbox_pending", outboxPending, AtomicInteger::get)
                .description("Number of PENDING entries in the ES outbox")
                .register(registry);

        Gauge.builder(PREFIX + "outbox_failed", outboxFailed, AtomicInteger::get)
                .description("Number of FAILED entries in the ES outbox")
                .register(registry);

        Gauge.builder(PREFIX + "outbox_drain_mode", outboxDrainMode, AtomicInteger::get)
                .description("1 if outbox processor is in drain mode, 0 if idle")
                .register(registry);

        Gauge.builder(PREFIX + "reconciliation_miss_rate", reconciliationMissRateBps,
                        v -> v.get() / 10000.0)
                .description("ES reconciliation miss rate (fraction of sampled GUIDs missing from ES)")
                .register(registry);
    }

    private Counter getCounter(String name, String... tags) {
        String key = buildKey(name, tags);
        return counterCache.computeIfAbsent(key, k ->
                Counter.builder(PREFIX + name)
                        .description("CassandraGraph metric: " + name)
                        .tags(tags)
                        .register(registry));
    }

    private Timer getTimer(String name, String... tags) {
        String key = buildKey(name, tags);
        return timerCache.computeIfAbsent(key, k ->
                Timer.builder(PREFIX + name)
                        .description("CassandraGraph timer: " + name)
                        .tags(tags)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofMillis(1))
                        .maximumExpectedValue(Duration.ofMinutes(10))
                        .register(registry));
    }

    private static String buildKey(String name, String... tags) {
        if (tags.length == 0) return name;
        StringBuilder sb = new StringBuilder(name);
        for (String tag : tags) {
            sb.append(':').append(tag);
        }
        return sb.toString();
    }
}
