package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.repository.assetsync.ConsumeResult;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.LeaseManager;
import org.apache.atlas.repository.assetsync.Outbox;
import org.apache.atlas.repository.assetsync.OutboxConsumer;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.outbox.shared.OutboxMetrics;
import org.apache.atlas.repository.outbox.shared.OutboxPodId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background relay for the tag-outbox. Polls the PENDING partition on a single
 * daemon thread, claims a batch, hands it to {@link TagOutboxConsumer}, and
 * resolves each entry's outcome back onto the outbox.
 *
 * <p>Adaptive polling: <b>idle mode</b> polls every
 * {@code config.idlePollSeconds()} with {@code config.idleBatchSize()}
 * entries; switches to <b>drain mode</b> (poll every
 * {@code config.drainPollSeconds()} with {@code config.drainBatchSize()}
 * entries) the moment any PENDING entries are seen, and reverts to idle after
 * {@value #EMPTY_POLLS_BEFORE_IDLE} consecutive empty polls.</p>
 *
 * <p>Leader election: every pod runs a processor instance, but only the pod
 * that holds {@code config.leaseName()} actually polls the outbox. Heartbeats
 * run on a separate scheduled task at {@code config.leaseHeartbeatSeconds()}
 * intervals so the lease can't expire between idle polls.</p>
 *
 * <p>Lifecycle: call {@link #start()} on bootstrap, {@link #stop()} on shutdown.
 * Stop releases the lease immediately so another pod can pick up relay work
 * without waiting for the TTL to expire.</p>
 *
 * <p>All tunables are sourced from the {@link TagOutboxConfig} passed at
 * construction — no direct {@code AtlasConfiguration} reads inside this class.</p>
 */
public final class TagOutboxProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxProcessor.class);

    /** Consecutive empty polls required before drain-mode reverts to idle. */
    private static final int EMPTY_POLLS_BEFORE_IDLE = 3;

    /** How often the relay leader refreshes storage gauges (pending/failed counts). */
    private static final long STORAGE_GAUGE_REFRESH_INTERVAL_MS = 30_000L;

    private final Outbox<EntityGuidRef>         outbox;
    private final OutboxConsumer<EntityGuidRef> consumer;
    private final LeaseManager                  leaseManager;
    private final OutboxMetrics                 metrics;
    private final TagOutboxConfig               config;

    private final ScheduledExecutorService      scheduler;
    private final AtomicBoolean                 running = new AtomicBoolean(false);

    // Adaptive state — single-threaded scheduler so plain fields are safe.
    private boolean drainMode             = false;
    private int     consecutiveEmptyPolls = 0;
    private boolean wasLeader             = false;
    private long    lastStorageGaugeRefreshMs = 0L;
    private volatile ScheduledFuture<?> currentTask;
    private volatile ScheduledFuture<?> heartbeatTask;

    public TagOutboxProcessor(Outbox<EntityGuidRef> outbox,
                              OutboxConsumer<EntityGuidRef> consumer,
                              LeaseManager leaseManager,
                              OutboxMetrics metrics,
                              TagOutboxConfig config) {
        this.outbox       = Objects.requireNonNull(outbox, "outbox");
        this.consumer     = Objects.requireNonNull(consumer, "consumer");
        this.leaseManager = Objects.requireNonNull(leaseManager, "leaseManager");
        this.metrics      = Objects.requireNonNull(metrics, "metrics");
        this.config       = Objects.requireNonNull(config, "config");

        final String threadName = config.relayThreadName();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        scheduleNext(config.idlePollSeconds());
        // Heartbeat on its own scheduled task — decoupled from poll cadence so
        // the lease can't silently expire during an extended idle poll.
        heartbeatTask = scheduler.scheduleWithFixedDelay(
                this::heartbeatTick,
                config.leaseHeartbeatSeconds(), config.leaseHeartbeatSeconds(),
                TimeUnit.SECONDS);
        LOG.info("TagOutboxProcessor started (pod='{}', lease='{}', idle={}s, drain={}s, idle_batch={}, drain_batch={}, lease_ttl={}s, heartbeat={}s)",
                OutboxPodId.get(), config.leaseName(),
                config.idlePollSeconds(), config.drainPollSeconds(),
                config.idleBatchSize(), config.drainBatchSize(),
                config.leaseTtlSeconds(), config.leaseHeartbeatSeconds());
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        ScheduledFuture<?> task = currentTask;
        if (task != null) task.cancel(false);
        ScheduledFuture<?> hb = heartbeatTask;
        if (hb != null) hb.cancel(false);

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Release lease so failover is immediate, not TTL-delayed.
        if (wasLeader) {
            leaseManager.release(config.leaseName());
            metrics.setLeader(false);
            metrics.recordLeaseHandover();
            clearStorageGauges();
        }
        LOG.info("TagOutboxProcessor stopped");
    }

    private void scheduleNext(int delaySeconds) {
        if (running.get()) {
            currentTask = scheduler.schedule(this::pollCycle, delaySeconds, TimeUnit.SECONDS);
        }
    }

    private void pollCycle() {
        if (!running.get()) return;
        metrics.recordRelayPoll();
        try {
            runOnce();
        } catch (Throwable t) {
            // Single-thread scheduler — an uncaught throw here would silently
            // suppress all future executions. Log and continue.
            LOG.error("TagOutboxProcessor: unexpected error in poll cycle", t);
        } finally {
            if (running.get()) {
                scheduleNext(drainMode ? config.drainPollSeconds() : config.idlePollSeconds());
            }
        }
    }

    private void runOnce() {
        boolean haveLease = ensureLease();
        metrics.setLeader(haveLease);
        if (!haveLease) {
            if (wasLeader) {
                wasLeader = false;
                metrics.recordLeaseHandover();
                clearStorageGauges();
                LOG.info("TagOutboxProcessor: lost lease '{}' — entering standby", config.leaseName());
            }
            return;
        }
        if (!wasLeader) {
            wasLeader = true;
            metrics.recordLeaseHandover();
            LOG.info("TagOutboxProcessor: this pod is now the relay leader (lease='{}')", config.leaseName());
        }

        refreshStorageGaugesIfDue();

        int batchSize = drainMode ? config.drainBatchSize() : config.idleBatchSize();
        List<OutboxEntry<EntityGuidRef>> batch = outbox.claim(batchSize);

        if (batch.isEmpty()) {
            consecutiveEmptyPolls++;
            if (drainMode && consecutiveEmptyPolls >= EMPTY_POLLS_BEFORE_IDLE) {
                drainMode = false;
                LOG.info("TagOutboxProcessor: PENDING drained — back to idle mode (poll every {}s)",
                        config.idlePollSeconds());
            }
            return;
        }

        consecutiveEmptyPolls = 0;
        if (!drainMode) {
            drainMode = true;
            LOG.info("TagOutboxProcessor: PENDING entries detected — entering drain mode " +
                    "(poll every {}s, batch size {})", config.drainPollSeconds(), config.drainBatchSize());
        }

        metrics.recordRelayBatchProcessed();
        metrics.recordRelayBatchSize(batch.size());

        ConsumeResult result = consumer.consume(batch);

        // Resolve per-entry outcomes
        for (OutboxEntryId id : result.getSucceeded()) {
            outbox.markDone(id);
            recordLagFor(id, batch);
        }
        metrics.recordRelayProcessed(result.getSucceeded().size());

        for (OutboxEntryId id : result.getRetryable()) {
            int newAttemptCount = attemptCountFor(id, batch) + 1;
            if (newAttemptCount >= config.maxAttempts()) {
                outbox.markFailed(id, newAttemptCount,
                        new RuntimeException("max attempts reached"));
                metrics.recordRelayPermanentlyFailed(1);
                metrics.recordRelayFailure("max_attempts");
            } else {
                outbox.releaseForRetry(id, newAttemptCount);
                metrics.recordRelayFailure("retryable");
            }
        }

        for (Map.Entry<OutboxEntryId, Throwable> e : result.getPermanentlyFailed().entrySet()) {
            int finalAttemptCount = attemptCountFor(e.getKey(), batch) + 1;
            outbox.markFailed(e.getKey(), finalAttemptCount, e.getValue());
            metrics.recordRelayPermanentlyFailed(1);
            metrics.recordRelayFailure("permanent");
        }

        LOG.info("TagOutboxProcessor: batch={} succeeded={} retryable={} permanent={}",
                batch.size(), result.getSucceeded().size(),
                result.getRetryable().size(), result.getPermanentlyFailed().size());
    }

    /**
     * Return whether this pod currently holds the lease. Leaders are renewed
     * by {@link #heartbeatTick()} on a dedicated cadence; followers attempt a
     * fresh acquisition here once per poll.
     */
    private boolean ensureLease() {
        if (wasLeader) return true;
        boolean ok = leaseManager.tryAcquire(config.leaseName(), config.leaseTtlSeconds());
        metrics.recordLeaseAcquireAttempt(ok ? "acquired" : "held_by_other");
        return ok;
    }

    /**
     * Dedicated heartbeat tick — runs on the same single-threaded scheduler as
     * {@link #pollCycle()}, so shared-state access is safe without synchronization.
     * Decoupling renewal from polling prevents silent lease expiry during an
     * idle poll that happens to exceed TTL.
     */
    private void heartbeatTick() {
        if (!running.get() || !wasLeader) return;
        try {
            boolean ok = leaseManager.heartbeat(config.leaseName(), config.leaseTtlSeconds());
            if (!ok) {
                metrics.recordLeaseAcquireAttempt("heartbeat_lost");
                ok = leaseManager.tryAcquire(config.leaseName(), config.leaseTtlSeconds());
                metrics.recordLeaseAcquireAttempt(ok ? "reacquired" : "held_by_other");
            }
            if (!ok) {
                wasLeader = false;
                metrics.setLeader(false);
                metrics.recordLeaseHandover();
                clearStorageGauges();
                LOG.info("TagOutboxProcessor: lost lease '{}' during heartbeat — entering standby",
                        config.leaseName());
            }
        } catch (Throwable t) {
            LOG.warn("TagOutboxProcessor: heartbeat tick failed", t);
        }
    }

    private int attemptCountFor(OutboxEntryId id, List<OutboxEntry<EntityGuidRef>> batch) {
        for (OutboxEntry<EntityGuidRef> e : batch) {
            if (e.getId().equals(id)) return e.getAttemptCount();
        }
        return 0;
    }

    private void recordLagFor(OutboxEntryId id, List<OutboxEntry<EntityGuidRef>> batch) {
        for (OutboxEntry<EntityGuidRef> e : batch) {
            if (e.getId().equals(id) && e.getCreatedAt() != null) {
                metrics.recordRelayLagMillis(
                        Duration.between(e.getCreatedAt(), Instant.now()).toMillis());
                return;
            }
        }
    }

    /**
     * Refresh outbox storage gauges on a throttled interval — only the relay
     * leader publishes them so the values reflect actual outbox depth without
     * per-pod Cassandra read amplification. instanceof-check keeps
     * {@link Outbox} interface-free of observability concerns.
     */
    private void refreshStorageGaugesIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastStorageGaugeRefreshMs < STORAGE_GAUGE_REFRESH_INTERVAL_MS) return;
        lastStorageGaugeRefreshMs = now;

        if (!(outbox instanceof TagOutbox)) return;
        try {
            TagOutbox.StorageStats stats = ((TagOutbox) outbox).computeStorageStats();
            metrics.setPendingCount(stats.pendingCount);
            metrics.setProcessingCount(stats.processingCount);
            metrics.setFailedCount(stats.failedCount);
            metrics.setOldestPendingAgeSeconds(stats.oldestPendingAgeSeconds);
        } catch (Exception e) {
            LOG.warn("TagOutboxProcessor: storage gauge refresh failed (non-fatal): {}", e.getMessage());
        }
    }

    /**
     * Zero storage gauges when this pod stops being the authoritative source
     * (lost lease or shutdown). Without this, ex-leaders publish stale values
     * forever and any max()/sum() aggregation on the dashboard reads a snapshot
     * from an hour ago.
     */
    private void clearStorageGauges() {
        metrics.setPendingCount(0);
        metrics.setProcessingCount(0);
        metrics.setFailedCount(0);
        metrics.setOldestPendingAgeSeconds(0L);
        lastStorageGaugeRefreshMs = 0L;
    }
}
