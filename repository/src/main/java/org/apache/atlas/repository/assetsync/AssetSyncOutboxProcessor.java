package org.apache.atlas.repository.assetsync;

import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background relay for the asset-sync outbox (MS-1010).
 *
 * <p>Single-threaded scheduler. Adaptive polling: idle mode (30s, 100/batch) when
 * the outbox is empty, drain mode (2s, 500/batch) when there's work to do.
 * Mode transitions are logged and observable via metrics.</p>
 *
 * <p>Leader election: every pod runs a processor instance, but only the
 * pod that holds the {@link AssetSyncLeaseManager} lease actually polls
 * the outbox. Heartbeats every {@code lease.heartbeat.seconds} keep the
 * lease alive; if the leader pauses or crashes, the lease expires and
 * another pod takes over within the TTL window.</p>
 *
 * <p>Lifecycle: call {@link #start()} on bootstrap, {@link #stop()} on shutdown.
 * Stop releases the lease + clears any in-flight claims so failover is fast.</p>
 */
public final class AssetSyncOutboxProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncOutboxProcessor.class);

    private static final String LEASE_NAME = "asset-sync-relay";
    private static final int    EMPTY_POLLS_BEFORE_IDLE = 3;

    private static final long STORAGE_GAUGE_REFRESH_INTERVAL_MS = 30_000L;

    private final Outbox<EntityGuidRef>          outbox;
    private final OutboxConsumer<EntityGuidRef>  consumer;
    private final LeaseManager                      leaseManager;
    private long lastStorageGaugeRefreshMs = 0L;
    private final ScheduledExecutorService          scheduler;
    private final AtomicBoolean                     running = new AtomicBoolean(false);

    private final int  idlePollSeconds;
    private final int  drainPollSeconds;
    private final int  idleBatchSize;
    private final int  drainBatchSize;
    private final int  leaseTtlSeconds;
    private final int  leaseHeartbeatSeconds;
    private final long heartbeatIntervalMs;

    // Adaptive state — single-threaded scheduler so plain fields are safe
    private boolean drainMode               = false;
    private int     consecutiveEmptyPolls   = 0;
    private long    lastHeartbeatMs         = 0L;
    private boolean wasLeader               = false;
    private volatile ScheduledFuture<?>     currentTask;

    public AssetSyncOutboxProcessor(Outbox<EntityGuidRef> outbox,
                                    OutboxConsumer<EntityGuidRef> consumer,
                                    LeaseManager leaseManager) {
        this.outbox       = outbox;
        this.consumer     = consumer;
        this.leaseManager = leaseManager;

        this.idlePollSeconds       = AtlasConfiguration.ASSET_SYNC_RELAY_IDLE_POLL_SECONDS.getInt();
        this.drainPollSeconds      = AtlasConfiguration.ASSET_SYNC_RELAY_DRAIN_POLL_SECONDS.getInt();
        this.idleBatchSize         = AtlasConfiguration.ASSET_SYNC_RELAY_IDLE_BATCH_SIZE.getInt();
        this.drainBatchSize        = AtlasConfiguration.ASSET_SYNC_RELAY_DRAIN_BATCH_SIZE.getInt();
        this.leaseTtlSeconds       = AtlasConfiguration.ASSET_SYNC_RELAY_LEASE_TTL_SECONDS.getInt();
        this.leaseHeartbeatSeconds = AtlasConfiguration.ASSET_SYNC_RELAY_LEASE_HEARTBEAT_SECONDS.getInt();
        this.heartbeatIntervalMs   = Duration.ofSeconds(leaseHeartbeatSeconds).toMillis();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "asset-sync-outbox-relay");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduleNext(idlePollSeconds);
            LOG.info("AssetSyncOutboxProcessor started (pod='{}', idle={}s, drain={}s, " +
                            "idle_batch={}, drain_batch={}, lease_ttl={}s, heartbeat={}s)",
                    AssetSyncPodId.get(), idlePollSeconds, drainPollSeconds,
                    idleBatchSize, drainBatchSize, leaseTtlSeconds, leaseHeartbeatSeconds);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            ScheduledFuture<?> task = currentTask;
            if (task != null) task.cancel(false);

            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Release lease so failover is immediate
            if (wasLeader) {
                leaseManager.release(LEASE_NAME);
                AssetSyncOutboxMetrics.setLeader(false);
                AssetSyncOutboxMetrics.recordLeaseHandover();
            }
            LOG.info("AssetSyncOutboxProcessor stopped");
        }
    }

    private void scheduleNext(int delaySeconds) {
        if (running.get()) {
            currentTask = scheduler.schedule(this::pollCycle, delaySeconds, TimeUnit.SECONDS);
        }
    }

    private void pollCycle() {
        if (!running.get()) return;
        AssetSyncOutboxMetrics.recordRelayPoll();
        try {
            runOnce();
        } catch (Throwable t) {
            // Defensive: never let an unexpected throw kill the scheduler
            LOG.error("AssetSyncOutboxProcessor: unexpected error in poll cycle", t);
        } finally {
            if (running.get()) {
                scheduleNext(drainMode ? drainPollSeconds : idlePollSeconds);
            }
        }
    }

    private void runOnce() {
        // Try to acquire (or heartbeat-renew) the lease
        boolean haveLease = ensureLease();
        AssetSyncOutboxMetrics.setLeader(haveLease);
        if (!haveLease) {
            if (wasLeader) {
                wasLeader = false;
                AssetSyncOutboxMetrics.recordLeaseHandover();
                LOG.info("AssetSyncOutboxProcessor: lost lease '{}' — entering standby", LEASE_NAME);
            }
            return;
        }
        if (!wasLeader) {
            wasLeader = true;
            AssetSyncOutboxMetrics.recordLeaseHandover();
            LOG.info("AssetSyncOutboxProcessor: this pod is now the relay leader");
        }

        // Storage gauges: leader refreshes them on a throttled interval so the
        // values reflect actual outbox depth without per-pod read amplification.
        refreshStorageGaugesIfDue();

        int batchSize = drainMode ? drainBatchSize : idleBatchSize;
        List<OutboxEntry<EntityGuidRef>> batch = outbox.claim(batchSize);

        if (batch.isEmpty()) {
            consecutiveEmptyPolls++;
            if (drainMode && consecutiveEmptyPolls >= EMPTY_POLLS_BEFORE_IDLE) {
                drainMode = false;
                LOG.info("AssetSyncOutboxProcessor: PENDING drained — back to idle mode (poll every {}s)",
                        idlePollSeconds);
            }
            return;
        }

        consecutiveEmptyPolls = 0;
        if (!drainMode) {
            drainMode = true;
            LOG.info("AssetSyncOutboxProcessor: PENDING entries detected — entering drain mode " +
                    "(poll every {}s, batch size {})", drainPollSeconds, drainBatchSize);
        }

        AssetSyncOutboxMetrics.recordRelayBatchProcessed();
        AssetSyncOutboxMetrics.recordRelayBatchSize(batch.size());

        ConsumeResult result = consumer.consume(batch);

        // Resolve per-entry outcomes
        for (OutboxEntryId id : result.getSucceeded()) {
            outbox.markDone(id);
            recordLagFor(id, batch);
        }
        AssetSyncOutboxMetrics.recordRelayProcessed(result.getSucceeded().size());

        for (OutboxEntryId id : result.getRetryable()) {
            int newAttemptCount = attemptCountFor(id, batch) + 1;
            if (newAttemptCount >= maxAttempts()) {
                outbox.markFailed(id, newAttemptCount,
                        new RuntimeException("max attempts reached"));
                AssetSyncOutboxMetrics.recordRelayPermanentlyFailed(1);
                AssetSyncOutboxMetrics.recordRelayFailure("max_attempts");
            } else {
                outbox.releaseForRetry(id, newAttemptCount);
                AssetSyncOutboxMetrics.recordRelayFailure("retryable");
            }
        }

        for (Map.Entry<OutboxEntryId, Throwable> e : result.getPermanentlyFailed().entrySet()) {
            int finalAttemptCount = attemptCountFor(e.getKey(), batch) + 1;
            outbox.markFailed(e.getKey(), finalAttemptCount, e.getValue());
            AssetSyncOutboxMetrics.recordRelayPermanentlyFailed(1);
            AssetSyncOutboxMetrics.recordRelayFailure("permanent");
        }

        LOG.info("AssetSyncOutboxProcessor: batch={} succeeded={} retryable={} permanent={}",
                batch.size(), result.getSucceeded().size(),
                result.getRetryable().size(), result.getPermanentlyFailed().size());
    }

    /**
     * Acquire the lease if not held; renew via heartbeat if held; return whether
     * we currently own it. Heartbeats are throttled to once per heartbeat interval.
     */
    private boolean ensureLease() {
        long now = System.currentTimeMillis();

        if (wasLeader && now - lastHeartbeatMs < heartbeatIntervalMs) {
            return true; // still inside heartbeat window — assume we hold it
        }

        boolean ok;
        if (wasLeader) {
            ok = leaseManager.heartbeat(LEASE_NAME, leaseTtlSeconds);
            if (!ok) {
                AssetSyncOutboxMetrics.recordLeaseAcquireAttempt("heartbeat_lost");
                // Heartbeat failed — attempt to re-acquire (someone may have stolen it during a pause)
                ok = leaseManager.tryAcquire(LEASE_NAME, leaseTtlSeconds);
                AssetSyncOutboxMetrics.recordLeaseAcquireAttempt(ok ? "reacquired" : "held_by_other");
            }
        } else {
            ok = leaseManager.tryAcquire(LEASE_NAME, leaseTtlSeconds);
            AssetSyncOutboxMetrics.recordLeaseAcquireAttempt(ok ? "acquired" : "held_by_other");
        }
        lastHeartbeatMs = now;
        return ok;
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
                AssetSyncOutboxMetrics.recordRelayLagMillis(
                        java.time.Duration.between(e.getCreatedAt(), Instant.now()).toMillis());
                return;
            }
        }
    }

    private int maxAttempts() {
        return AtlasConfiguration.ASSET_SYNC_OUTBOX_MAX_ATTEMPTS.getInt();
    }

    /**
     * Refresh outbox storage gauges if the throttle interval has elapsed.
     * Only the AssetSyncOutbox concrete type exposes computeStorageStats(),
     * so we instanceof-check before calling — keeps the {@link Outbox}
     * interface free of observability concerns.
     */
    private void refreshStorageGaugesIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastStorageGaugeRefreshMs < STORAGE_GAUGE_REFRESH_INTERVAL_MS) return;
        lastStorageGaugeRefreshMs = now;

        if (!(outbox instanceof AssetSyncOutbox)) return;
        try {
            AssetSyncOutbox.StorageStats stats = ((AssetSyncOutbox) outbox).computeStorageStats();
            AssetSyncOutboxMetrics.setPendingCount(stats.pendingCount);
            AssetSyncOutboxMetrics.setProcessingCount(stats.processingCount);
            AssetSyncOutboxMetrics.setFailedCount(stats.failedCount);
            AssetSyncOutboxMetrics.setOldestPendingAgeSeconds(stats.oldestPendingAgeSeconds);
        } catch (Exception e) {
            LOG.warn("AssetSyncOutboxProcessor: storage gauge refresh failed (non-fatal): {}", e.getMessage());
        }
    }
}
