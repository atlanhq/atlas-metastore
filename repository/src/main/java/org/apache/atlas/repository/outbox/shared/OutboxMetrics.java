package org.apache.atlas.repository.outbox.shared;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Instance-based, prefix-parameterised Micrometer metrics for any outbox subsystem.
 *
 * <p>Replaces the static-class style of {@code AssetSyncOutboxMetrics} with a
 * constructor that accepts a {@link MeterRegistry} and a string prefix. All meters
 * are registered under that prefix, allowing multiple outbox subsystems (currently
 * tag-outbox; later possibly asset-sync after its own refactor) to own disjoint
 * metric namespaces without code duplication.</p>
 *
 * <p>Surface (categories parallel to PR #6568):</p>
 * <ul>
 *     <li><b>Writer:</b> writes_total, write_errors_total{reason}, write_latency_seconds,
 *         payload_bytes.</li>
 *     <li><b>Storage (gauges):</b> pending_count, processing_count, failed_count,
 *         oldest_pending_age_seconds.</li>
 *     <li><b>Relay (counters/histograms):</b> relay_polls_total,
 *         relay_batches_processed_total, relay_processed_total,
 *         relay_permanently_failed_total, relay_reclaimed_processing_total,
 *         relay_lag_seconds, relay_batch_size, relay_failures_total{reason}.</li>
 *     <li><b>Leader election:</b> relay_leader (gauge), lease_handovers_total,
 *         lease_acquire_attempts_total{result}.</li>
 *     <li><b>Reconciler:</b> reconciler_runs_total, reconciler_already_in_es_total,
 *         reconciler_reindexed_total, reconciler_still_missing_total,
 *         reconciler_tick_errors_total, reconciler_scanned_total{status},
 *         reconciler_last_run_timestamp_seconds (gauge),
 *         reconciler_healthy (gauge).</li>
 * </ul>
 *
 * <p>All {@code record*} methods are null-safe and fire-and-forget: if the registry
 * was not available at construction time or an individual meter failed to register,
 * the call is a no-op. The caller's write path is never affected by metrics
 * misbehaviour — exceptions from tagged-counter lookups are caught internally.</p>
 */
public final class OutboxMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxMetrics.class);

    private final MeterRegistry registry;
    private final String        prefix;
    private final boolean       initialized;

    // Writer
    private final Counter             writeAttempts;
    private final Counter             writeErrors;
    private final Timer               writeLatency;
    private final DistributionSummary payloadBytes;

    // Storage gauges — each instance owns its own atomics so gauge readings are
    // scoped to this subsystem and don't collide across parallel outboxes.
    private final AtomicInteger pendingCount            = new AtomicInteger(0);
    private final AtomicInteger processingCount         = new AtomicInteger(0);
    private final AtomicInteger failedCount             = new AtomicInteger(0);
    private final AtomicLong    oldestPendingAgeSeconds = new AtomicLong(0L);

    // Relay
    private final Counter             relayPolls;
    private final Counter             relayBatchesProcessed;
    private final Counter             relayProcessed;
    private final Counter             relayPermanentlyFailed;
    private final Counter             relayReclaimedProcessing;
    private final Timer               relayLag;
    private final DistributionSummary relayBatchSize;

    // Leader election
    private final AtomicInteger leaderGauge = new AtomicInteger(0);
    private final Counter       leaseHandovers;

    // Reconciler
    private final Counter       reconcilerRuns;
    private final Counter       reconcilerAlreadyInEs;
    private final Counter       reconcilerReindexed;
    private final Counter       reconcilerStillMissing;
    private final Counter       reconcilerTickErrors;
    private final AtomicLong    reconcilerLastRunEpochSeconds = new AtomicLong(0L);
    private final AtomicInteger reconcilerHealthy             = new AtomicInteger(0);

    // Tagged-counter caches (per-reason/per-result/per-status counters resolved lazily)
    private final ConcurrentMap<String, Counter> writeErrorsByReason          = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> relayFailuresByReason        = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> leaseAcquireAttemptsByResult = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> reconcilerScannedByStatus    = new ConcurrentHashMap<>();

    /**
     * Register every meter under the given prefix against the given registry.
     * Registration failures are logged but do not throw — the constructor
     * leaves {@code initialized=false} and subsequent calls degrade to no-ops.
     *
     * @param registry a Micrometer registry (typically {@code MetricUtils.getMeterRegistry()}).
     *                 Pass {@code null} to disable metrics entirely (useful in tests).
     * @param prefix   metric-name prefix. Must end with {@code "_"} by convention
     *                 (e.g. {@code "atlas_tag_outbox_"}). Not validated — caller's
     *                 responsibility.
     */
    public OutboxMetrics(MeterRegistry registry, String prefix) {
        this.registry = registry;
        this.prefix   = Objects.requireNonNull(prefix, "prefix");

        Counter             _writeAttempts             = null;
        Counter             _writeErrors               = null;
        Timer               _writeLatency              = null;
        DistributionSummary _payloadBytes              = null;
        Counter             _relayPolls                = null;
        Counter             _relayBatchesProcessed     = null;
        Counter             _relayProcessed            = null;
        Counter             _relayPermanentlyFailed    = null;
        Counter             _relayReclaimedProcessing  = null;
        Timer               _relayLag                  = null;
        DistributionSummary _relayBatchSize            = null;
        Counter             _leaseHandovers            = null;
        Counter             _reconcilerRuns            = null;
        Counter             _reconcilerAlreadyInEs     = null;
        Counter             _reconcilerReindexed       = null;
        Counter             _reconcilerStillMissing    = null;
        Counter             _reconcilerTickErrors      = null;
        boolean             _initialized               = false;

        if (registry != null) {
            try {
                // Writer
                _writeAttempts = Counter.builder(prefix + "writes_total")
                        .description("Outbox enqueue attempts (failure-only — should be rare)")
                        .register(registry);
                _writeErrors = Counter.builder(prefix + "write_errors_total")
                        .description("Outbox enqueue errors (Cassandra issue)")
                        .register(registry);
                _writeLatency = Timer.builder(prefix + "write_latency_seconds")
                        .description("Latency of writing a failed entry to the outbox")
                        .register(registry);
                _payloadBytes = DistributionSummary.builder(prefix + "payload_bytes")
                        .description("Serialized payload size per outbox entry")
                        .register(registry);

                // Storage gauges — each field-atomic is the gauge's value source.
                Gauge.builder(prefix + "pending_count", pendingCount, AtomicInteger::get)
                        .description("Number of PENDING entries in the outbox")
                        .register(registry);
                Gauge.builder(prefix + "processing_count", processingCount, AtomicInteger::get)
                        .description("Number of currently-claimed entries (in flight)")
                        .register(registry);
                Gauge.builder(prefix + "failed_count", failedCount, AtomicInteger::get)
                        .description("Number of FAILED entries (max retries exhausted) awaiting reconciliation")
                        .register(registry);
                Gauge.builder(prefix + "oldest_pending_age_seconds", oldestPendingAgeSeconds, AtomicLong::get)
                        .description("Age of the oldest PENDING entry — early indicator of a stuck relay")
                        .register(registry);

                // Relay
                _relayPolls = Counter.builder(prefix + "relay_polls_total")
                        .description("Number of relay poll cycles executed")
                        .register(registry);
                _relayBatchesProcessed = Counter.builder(prefix + "relay_batches_processed_total")
                        .description("Number of batches the relay processed (claimed from outbox)")
                        .register(registry);
                _relayProcessed = Counter.builder(prefix + "relay_processed_total")
                        .description("Outbox entries successfully replayed to ES")
                        .register(registry);
                _relayPermanentlyFailed = Counter.builder(prefix + "relay_permanently_failed_total")
                        .description("Outbox entries moved to FAILED after exhausting retries")
                        .register(registry);
                _relayReclaimedProcessing = Counter.builder(prefix + "relay_reclaimed_processing_total")
                        .description("Stuck-PROCESSING rows reclaimed (indicates a leader crash)")
                        .register(registry);
                _relayLag = Timer.builder(prefix + "relay_lag_seconds")
                        .description("End-to-end lag from outbox enqueue to successful ES delivery")
                        .register(registry);
                _relayBatchSize = DistributionSummary.builder(prefix + "relay_batch_size")
                        .description("Number of entries claimed per relay batch")
                        .register(registry);

                // Leader election
                Gauge.builder(prefix + "relay_leader", leaderGauge, AtomicInteger::get)
                        .description("1 if this pod is the relay leader, 0 otherwise")
                        .register(registry);
                _leaseHandovers = Counter.builder(prefix + "lease_handovers_total")
                        .description("Lease ownership transitions on this pod (acquired or lost)")
                        .register(registry);

                // Reconciler
                _reconcilerRuns = Counter.builder(prefix + "reconciler_runs_total")
                        .description("Reconciler tick count on this pod (only increments when leader)")
                        .register(registry);
                _reconcilerAlreadyInEs = Counter.builder(prefix + "reconciler_already_in_es_total")
                        .description("Outbox rows dropped because ES already had the entity (false FAILED cleared)")
                        .register(registry);
                _reconcilerReindexed = Counter.builder(prefix + "reconciler_reindexed_total")
                        .description("Outbox rows cleared after successful replay")
                        .register(registry);
                _reconcilerStillMissing = Counter.builder(prefix + "reconciler_still_missing_total")
                        .description("Outbox rows left in place because replay also failed (will retry next run)")
                        .register(registry);
                _reconcilerTickErrors = Counter.builder(prefix + "reconciler_tick_errors_total")
                        .description("Reconciler ticks that failed entirely (exception escaped runOnce)")
                        .register(registry);
                Gauge.builder(prefix + "reconciler_last_run_timestamp_seconds",
                                reconcilerLastRunEpochSeconds, AtomicLong::get)
                        .description("Unix timestamp of the last reconciler tick — alert if stale")
                        .register(registry);
                Gauge.builder(prefix + "reconciler_healthy", reconcilerHealthy, AtomicInteger::get)
                        .description("1 if the reconciler scheduler is healthy on this pod; 0 otherwise")
                        .register(registry);

                _initialized = true;
                LOG.info("OutboxMetrics: registered with prefix='{}'", prefix);
            } catch (Exception e) {
                LOG.warn("OutboxMetrics: registration failed for prefix='{}' — metrics disabled for this instance", prefix, e);
            }
        } else {
            LOG.info("OutboxMetrics: no registry provided — metrics disabled for prefix='{}'", prefix);
        }

        this.writeAttempts            = _writeAttempts;
        this.writeErrors              = _writeErrors;
        this.writeLatency             = _writeLatency;
        this.payloadBytes             = _payloadBytes;
        this.relayPolls               = _relayPolls;
        this.relayBatchesProcessed    = _relayBatchesProcessed;
        this.relayProcessed           = _relayProcessed;
        this.relayPermanentlyFailed   = _relayPermanentlyFailed;
        this.relayReclaimedProcessing = _relayReclaimedProcessing;
        this.relayLag                 = _relayLag;
        this.relayBatchSize           = _relayBatchSize;
        this.leaseHandovers           = _leaseHandovers;
        this.reconcilerRuns           = _reconcilerRuns;
        this.reconcilerAlreadyInEs    = _reconcilerAlreadyInEs;
        this.reconcilerReindexed      = _reconcilerReindexed;
        this.reconcilerStillMissing   = _reconcilerStillMissing;
        this.reconcilerTickErrors     = _reconcilerTickErrors;
        this.initialized              = _initialized;
    }

    // ---- Writer surface ----
    public void recordWrite() {
        if (writeAttempts != null) writeAttempts.increment();
    }
    public void recordWriteError(String reason) {
        if (!initialized) return;
        if (writeErrors != null) writeErrors.increment();
        incrementTagged(writeErrorsByReason, prefix + "write_errors_by_reason_total", "reason", reason, 1);
    }
    public Timer.Sample startWriteTimer() {
        return writeLatency != null ? Timer.start() : null;
    }
    public void stopWriteTimer(Timer.Sample sample) {
        if (sample != null && writeLatency != null) sample.stop(writeLatency);
    }
    public void recordPayloadBytes(int bytes) {
        if (payloadBytes != null && bytes > 0) payloadBytes.record(bytes);
    }

    // ---- Storage gauges ----
    public void setPendingCount(int v)             { pendingCount.set(Math.max(0, v)); }
    public void setProcessingCount(int v)          { processingCount.set(Math.max(0, v)); }
    public void setFailedCount(int v)              { failedCount.set(Math.max(0, v)); }
    public void setOldestPendingAgeSeconds(long v) { oldestPendingAgeSeconds.set(Math.max(0L, v)); }

    // ---- Relay surface ----
    public void recordRelayPoll() {
        if (relayPolls != null) relayPolls.increment();
    }
    public void recordRelayBatchProcessed() {
        if (relayBatchesProcessed != null) relayBatchesProcessed.increment();
    }
    public void recordRelayProcessed(int count) {
        if (relayProcessed != null && count > 0) relayProcessed.increment(count);
    }
    public void recordRelayFailure(String reason) {
        if (!initialized) return;
        incrementTagged(relayFailuresByReason, prefix + "relay_failures_total", "reason", reason, 1);
    }
    public void recordRelayPermanentlyFailed(int count) {
        if (relayPermanentlyFailed != null && count > 0) relayPermanentlyFailed.increment(count);
    }
    public void recordRelayReclaimedProcessing(int count) {
        if (relayReclaimedProcessing != null && count > 0) relayReclaimedProcessing.increment(count);
    }
    public void recordRelayLagMillis(long lagMs) {
        if (relayLag != null && lagMs >= 0) relayLag.record(Duration.ofMillis(lagMs));
    }
    public void recordRelayBatchSize(int size) {
        if (relayBatchSize != null && size > 0) relayBatchSize.record(size);
    }

    // ---- Leader election surface ----
    public void setLeader(boolean isLeader)                    { leaderGauge.set(isLeader ? 1 : 0); }
    public void recordLeaseHandover() {
        if (leaseHandovers != null) leaseHandovers.increment();
    }
    public void recordLeaseAcquireAttempt(String result) {
        if (!initialized) return;
        incrementTagged(leaseAcquireAttemptsByResult,
                prefix + "lease_acquire_attempts_total", "result", result, 1);
    }

    // ---- Reconciler surface ----
    public void recordReconcilerRun() {
        if (reconcilerRuns != null) reconcilerRuns.increment();
        reconcilerLastRunEpochSeconds.set(Instant.now().getEpochSecond());
    }
    public void recordReconcilerScanned(String status, int count) {
        if (!initialized || count <= 0) return;
        incrementTagged(reconcilerScannedByStatus,
                prefix + "reconciler_scanned_total", "status", status, count);
    }
    public void recordReconcilerAlreadyInEs(int count) {
        if (reconcilerAlreadyInEs != null && count > 0) reconcilerAlreadyInEs.increment(count);
    }
    public void recordReconcilerReindexed(int count) {
        if (reconcilerReindexed != null && count > 0) reconcilerReindexed.increment(count);
    }
    public void recordReconcilerStillMissing(int count) {
        if (reconcilerStillMissing != null && count > 0) reconcilerStillMissing.increment(count);
    }
    public void recordReconcilerTickError() {
        if (reconcilerTickErrors != null) reconcilerTickErrors.increment();
    }
    public void setReconcilerHealthy(boolean healthy) { reconcilerHealthy.set(healthy ? 1 : 0); }

    /**
     * Centralised lazy registration for tagged counters. Catches exceptions so
     * an observability problem can never break the caller's primary code path.
     */
    private void incrementTagged(ConcurrentMap<String, Counter> cache,
                                 String meterName,
                                 String tagKey,
                                 String tagValue,
                                 long amount) {
        if (registry == null || tagValue == null) return;
        try {
            cache.computeIfAbsent(tagValue, v ->
                    Counter.builder(meterName).tag(tagKey, v).register(registry)
            ).increment(amount);
        } catch (Exception e) {
            // metrics never break the caller
        }
    }
}
