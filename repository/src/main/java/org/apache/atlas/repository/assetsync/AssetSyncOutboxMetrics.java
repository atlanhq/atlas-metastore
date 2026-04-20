package org.apache.atlas.repository.assetsync;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Layer 2 Prometheus surface for the asset-sync outbox (MS-1010).
 *
 * <p>Static-accessible facade — same pattern as
 * {@code ESConnectorMetrics} so the outbox internals (which live below the
 * Spring container during early bootstrap) can record metrics without
 * needing injection.</p>
 *
 * <p>Storage gauges (pending/processing/failed counts) are designed to be
 * updated by the relay leader on each poll cycle so the values reflect
 * the actual outbox state without per-pod read amplification on Cassandra.</p>
 */
public final class AssetSyncOutboxMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncOutboxMetrics.class);

    // ---- Layer 2a: writer ----
    private static final String PREFIX_WRITE = "atlas_es_outbox_";

    // ---- Layer 2b: storage (gauges) ----
    private static final AtomicInteger pendingCount               = new AtomicInteger(0);
    private static final AtomicInteger processingCount            = new AtomicInteger(0);
    private static final AtomicInteger failedCount                = new AtomicInteger(0);
    private static final AtomicLong    oldestPendingAgeSeconds    = new AtomicLong(0);

    // ---- Layer 2c: relay (counters/histograms) ----
    private static final String PREFIX_RELAY = "atlas_es_outbox_relay_";

    // ---- Layer 2d: leader election ----
    private static final AtomicInteger leaderGauge = new AtomicInteger(0);

    private static volatile boolean initialized = false;
    private static final Object initLock = new Object();

    private static Counter writeAttempts;
    private static Counter writeErrors;
    private static Timer   writeLatency;
    private static DistributionSummary payloadBytes;

    private static Counter relayPolls;
    private static Counter relayBatchesProcessed;
    private static Counter relayProcessed;
    private static Counter relayPermanentlyFailed;
    private static Counter relayReclaimedProcessing;
    private static Timer   relayLag;
    private static DistributionSummary relayBatchSize;

    private static Counter leaseHandovers;
    private static final ConcurrentMap<String, Counter> leaseAcquireAttemptsByResult = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Counter> writeErrorsByReason          = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Counter> relayFailuresByReason        = new ConcurrentHashMap<>();

    private AssetSyncOutboxMetrics() {}

    private static void ensureInit() {
        if (initialized) return;
        synchronized (initLock) {
            if (initialized) return;
            try {
                MeterRegistry r = getMeterRegistry();
                if (r == null) return;

                // Writer
                writeAttempts = Counter.builder(PREFIX_WRITE + "writes_total")
                        .description("Asset-sync outbox enqueue attempts (failure-only — should be rare)")
                        .register(r);
                writeErrors = Counter.builder(PREFIX_WRITE + "write_errors_total")
                        .description("Asset-sync outbox enqueue errors (Cassandra issue)")
                        .register(r);
                writeLatency = Timer.builder(PREFIX_WRITE + "write_latency_seconds")
                        .description("Latency of writing a failed entry to the outbox")
                        .register(r);
                payloadBytes = DistributionSummary.builder("atlas_es_outbox_payload_bytes")
                        .description("Serialized payload size per outbox entry")
                        .register(r);

                // Storage gauges (updated by relay leader on each poll cycle)
                Gauge.builder("atlas_es_outbox_pending_count", pendingCount, AtomicInteger::get)
                        .description("Number of PENDING entries in the asset-sync outbox")
                        .register(r);
                Gauge.builder("atlas_es_outbox_processing_count", processingCount, AtomicInteger::get)
                        .description("Number of currently-claimed entries (in flight)")
                        .register(r);
                Gauge.builder("atlas_es_outbox_failed_count", failedCount, AtomicInteger::get)
                        .description("Number of FAILED entries (max retries exhausted) awaiting reconciliation")
                        .register(r);
                Gauge.builder("atlas_es_outbox_oldest_pending_age_seconds", oldestPendingAgeSeconds, AtomicLong::get)
                        .description("Age of the oldest PENDING entry — early indicator of a stuck relay")
                        .register(r);

                // Relay
                relayPolls = Counter.builder(PREFIX_RELAY + "polls_total")
                        .description("Number of relay poll cycles executed")
                        .register(r);
                relayBatchesProcessed = Counter.builder(PREFIX_RELAY + "batches_processed_total")
                        .description("Number of batches the relay processed (claimed from outbox)")
                        .register(r);
                relayProcessed = Counter.builder(PREFIX_RELAY + "processed_total")
                        .description("Outbox entries successfully replayed to ES")
                        .register(r);
                relayPermanentlyFailed = Counter.builder(PREFIX_RELAY + "permanently_failed_total")
                        .description("Outbox entries moved to FAILED after exhausting retries")
                        .register(r);
                relayReclaimedProcessing = Counter.builder(PREFIX_RELAY + "reclaimed_processing_total")
                        .description("Stuck-PROCESSING rows reclaimed (indicates a leader crash)")
                        .register(r);
                relayLag = Timer.builder(PREFIX_RELAY + "lag_seconds")
                        .description("End-to-end lag from outbox enqueue to successful ES delivery")
                        .register(r);
                relayBatchSize = DistributionSummary.builder(PREFIX_RELAY + "batch_size")
                        .description("Number of entries claimed per relay batch")
                        .register(r);

                // Leader election
                Gauge.builder("atlas_es_outbox_relay_leader", leaderGauge, AtomicInteger::get)
                        .description("1 if this pod is the relay leader, 0 otherwise")
                        .register(r);
                leaseHandovers = Counter.builder("atlas_es_outbox_lease_handovers_total")
                        .description("Lease ownership transitions on this pod (acquired or lost)")
                        .register(r);

                initialized = true;
                LOG.info("AssetSyncOutboxMetrics: registered Layer 2 Prometheus metrics");
            } catch (Exception e) {
                LOG.warn("AssetSyncOutboxMetrics: failed to register metrics; metrics disabled this run", e);
            }
        }
    }

    // ---- Writer surface ----
    public static void recordWrite() {
        ensureInit();
        if (writeAttempts != null) writeAttempts.increment();
    }
    public static void recordWriteError(String reason) {
        ensureInit();
        if (!initialized) return;
        if (writeErrors != null) writeErrors.increment();
        try {
            writeErrorsByReason.computeIfAbsent(reason, r ->
                    Counter.builder(PREFIX_WRITE + "write_errors_by_reason_total")
                            .tag("reason", r).register(getMeterRegistry())).increment();
        } catch (Exception e) { /* metrics never break the write path */ }
    }
    public static Timer.Sample startWriteTimer() {
        ensureInit();
        return writeLatency != null ? Timer.start() : null;
    }
    public static void stopWriteTimer(Timer.Sample sample) {
        if (sample != null && writeLatency != null) sample.stop(writeLatency);
    }
    public static void recordPayloadBytes(int bytes) {
        ensureInit();
        if (payloadBytes != null && bytes > 0) payloadBytes.record(bytes);
    }

    // ---- Storage gauges ----
    public static void setPendingCount(int v)               { pendingCount.set(Math.max(0, v)); }
    public static void setProcessingCount(int v)            { processingCount.set(Math.max(0, v)); }
    public static void setFailedCount(int v)                { failedCount.set(Math.max(0, v)); }
    public static void setOldestPendingAgeSeconds(long v)   { oldestPendingAgeSeconds.set(Math.max(0, v)); }

    // ---- Relay surface ----
    public static void recordRelayPoll() {
        ensureInit();
        if (relayPolls != null) relayPolls.increment();
    }
    public static void recordRelayBatchProcessed() {
        ensureInit();
        if (relayBatchesProcessed != null) relayBatchesProcessed.increment();
    }
    public static void recordRelayProcessed(int count) {
        ensureInit();
        if (relayProcessed != null && count > 0) relayProcessed.increment(count);
    }
    public static void recordRelayFailure(String reason) {
        ensureInit();
        if (!initialized) return;
        try {
            relayFailuresByReason.computeIfAbsent(reason, r ->
                    Counter.builder(PREFIX_RELAY + "failures_total")
                            .tag("reason", r).register(getMeterRegistry())).increment();
        } catch (Exception e) { /* ignore */ }
    }
    public static void recordRelayPermanentlyFailed(int count) {
        ensureInit();
        if (relayPermanentlyFailed != null && count > 0) relayPermanentlyFailed.increment(count);
    }
    public static void recordRelayReclaimedProcessing(int count) {
        ensureInit();
        if (relayReclaimedProcessing != null && count > 0) relayReclaimedProcessing.increment(count);
    }
    public static void recordRelayLagMillis(long lagMs) {
        ensureInit();
        if (relayLag != null && lagMs >= 0) relayLag.record(java.time.Duration.ofMillis(lagMs));
    }
    public static void recordRelayBatchSize(int size) {
        ensureInit();
        if (relayBatchSize != null && size > 0) relayBatchSize.record(size);
    }

    // ---- Leader election surface ----
    public static void setLeader(boolean isLeader) {
        leaderGauge.set(isLeader ? 1 : 0);
    }
    public static void recordLeaseHandover() {
        ensureInit();
        if (leaseHandovers != null) leaseHandovers.increment();
    }
    public static void recordLeaseAcquireAttempt(String result) {
        ensureInit();
        if (!initialized) return;
        try {
            leaseAcquireAttemptsByResult.computeIfAbsent(result, k ->
                    Counter.builder("atlas_es_outbox_lease_acquire_attempts_total")
                            .tag("result", k).register(getMeterRegistry())).increment();
        } catch (Exception e) { /* ignore */ }
    }
}
