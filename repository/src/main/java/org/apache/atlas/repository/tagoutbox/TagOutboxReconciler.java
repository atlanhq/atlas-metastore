package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.repository.assetsync.ConsumeResult;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.LeaseManager;
import org.apache.atlas.repository.assetsync.OutboxConsumer;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.outbox.shared.OutboxMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hourly sweeper for tag-outbox entries the relay can't self-heal. Shares the
 * relay's lease — only the pod holding {@code config.leaseName()} runs ticks.
 *
 * <p>Scans two groups:</p>
 * <ul>
 *     <li><b>FAILED</b> — entries that exhausted {@code config.maxAttempts()} relay retries.</li>
 *     <li><b>Orphaned PENDING</b> — entries whose last attempt is older than
 *         {@code config.reconcilerStuckPendingThresholdSeconds()} and not in a
 *         legitimate backoff window. Usually indicates a leader that died mid-batch
 *         or a missed lease handover.</li>
 * </ul>
 *
 * <p><b>Replay strategy — critical difference from {@code AssetSyncReconciler}:</b>
 * this reconciler does <b>not</b> perform an ES presence check before replaying.
 * For tag-denorm rows, a doc's presence in ES says nothing about whether the
 * 5 denorm fields are current. Dropping a row just because {@code _mget} found
 * the doc would silently leave denorm stale. Instead, the reconciler hands
 * every batch to the same {@link OutboxConsumer} the relay uses — which calls
 * {@code entityStore.repairClassificationMappingsV2(...)} and re-fires the
 * deferred ES ops.</p>
 *
 * <p>Outcome handling per entry:</p>
 * <ul>
 *     <li><b>Succeeded</b> — delete row from its current partition
 *         (FAILED rows go through {@code deleteFailed}, PENDING rows through
 *         {@code markDone}).</li>
 *     <li><b>Retryable / permanently failed</b> — leave the row in place for the
 *         next reconciler run. FAILED rows stay FAILED; PENDING rows remain for
 *         the relay to eventually drain (or for the next reconciler tick).</li>
 * </ul>
 *
 * <p>Boot-safe: {@link #start()} and every tick catch all exceptions so the
 * reconciler can never take down Atlas. Observability via
 * {@code reconciler_healthy} gauge and {@code reconciler_tick_errors_total}.</p>
 */
public final class TagOutboxReconciler {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxReconciler.class);

    private final TagOutbox                     outbox;
    private final OutboxConsumer<EntityGuidRef> consumer;
    private final LeaseManager                  leaseManager;
    private final OutboxMetrics                 metrics;
    private final TagOutboxConfig               config;
    private final ScheduledExecutorService      scheduler;
    private final AtomicBoolean                 running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?>         currentTask;

    public TagOutboxReconciler(TagOutbox outbox,
                               OutboxConsumer<EntityGuidRef> consumer,
                               LeaseManager leaseManager,
                               OutboxMetrics metrics,
                               TagOutboxConfig config) {
        this.outbox       = Objects.requireNonNull(outbox, "outbox");
        this.consumer     = Objects.requireNonNull(consumer, "consumer");
        this.leaseManager = Objects.requireNonNull(leaseManager, "leaseManager");
        this.metrics      = Objects.requireNonNull(metrics, "metrics");
        this.config       = Objects.requireNonNull(config, "config");
        this.scheduler    = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, config.reconcilerThreadName());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Schedule the recurring tick. Jittered initial delay spreads multi-tenant
     * load so reconciler instances across tenants don't all hit ES at the same
     * wall-clock moment.
     *
     * <p>Boot-safe: any exception here is caught and logged; the reconciler is
     * optional observability/self-healing infrastructure — its failure must not
     * break Atlas startup. On failure the {@code reconciler_healthy} gauge stays
     * at 0 and a follow-up restart can try again.</p>
     */
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        try {
            int intervalSec = config.reconcilerIntervalSeconds();
            int jitterSec   = Math.max(0, config.reconcilerJitterSeconds());
            long initialDelaySec = intervalSec + (jitterSec > 0
                    ? ThreadLocalRandom.current().nextLong(-jitterSec, jitterSec + 1L) : 0);
            if (initialDelaySec < 0) initialDelaySec = 0;

            currentTask = scheduler.scheduleWithFixedDelay(
                    this::safeTick,
                    initialDelaySec, intervalSec, TimeUnit.SECONDS);
            metrics.setReconcilerHealthy(true);
            LOG.info("TagOutboxReconciler started (interval={}s, initial_delay={}s, lease='{}')",
                    intervalSec, initialDelaySec, config.leaseName());
        } catch (Throwable t) {
            running.set(false);
            metrics.setReconcilerHealthy(false);
            LOG.error("TagOutboxReconciler: start() FAILED — reconciler not scheduled on this pod; " +
                    "relay + verify still active. FAILED rows will accumulate until another pod's " +
                    "reconciler runs or this one is restarted.", t);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        metrics.setReconcilerHealthy(false);
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
        LOG.info("TagOutboxReconciler stopped");
    }

    /**
     * Scheduler entry point. Catches Throwable so no tick can kill the recurring
     * task ({@code scheduleWithFixedDelay} silently suppresses all future runs on
     * an uncaught exception). Records a tick-error counter so ops can alert on
     * {@code reconciler_tick_errors_total > 0} even when the scheduler itself
     * looks healthy.
     */
    private void safeTick() {
        if (!running.get()) return;
        try {
            runOnce();
        } catch (Throwable t) {
            metrics.recordReconcilerTickError();
            LOG.error("TagOutboxReconciler: unexpected error in reconcile tick — will retry next interval", t);
        }
    }

    /** One reconciler tick. No-op if this pod doesn't hold the relay lease. */
    public void runOnce() {
        if (!leaseManager.isHeldByMe(config.leaseName())) return;

        metrics.recordReconcilerRun();

        int      batchSize = config.reconcilerBatchSize();
        Duration stuckFor  = Duration.ofSeconds(config.reconcilerStuckPendingThresholdSeconds());

        try {
            reconcile(outbox.scanFailed(batchSize), TagOutbox.STATUS_FAILED);
        } catch (Throwable t) {
            LOG.error("TagOutboxReconciler: FAILED sweep failed", t);
        }
        try {
            reconcile(outbox.scanStuckPending(stuckFor, batchSize), TagOutbox.STATUS_PENDING);
        } catch (Throwable t) {
            LOG.error("TagOutboxReconciler: stuck-PENDING sweep failed", t);
        }
    }

    /**
     * Hand the batch to the consumer (same replay path the relay uses), then
     * resolve outcomes: succeeded → delete row; retryable/permanent → leave in
     * place. No ES presence check — see class javadoc for rationale.
     */
    private void reconcile(List<OutboxEntry<EntityGuidRef>> batch, String partition) {
        if (batch.isEmpty()) return;

        metrics.recordReconcilerScanned(partition, batch.size());

        ConsumeResult result;
        try {
            result = consumer.consume(batch);
        } catch (Throwable t) {
            // Consumer is supposed to classify rather than throw — belt-and-braces.
            metrics.recordReconcilerStillMissing(batch.size());
            LOG.warn("TagOutboxReconciler: consumer threw for batch of {} in partition={} — left in place: {}",
                    batch.size(), partition, t.getMessage());
            return;
        }

        // Successful GUIDs: drop the row from its current partition.
        for (OutboxEntryId id : result.getSucceeded()) {
            deleteByPartition(partition, id);
        }

        // Retryable + permanentlyFailed: leave rows in place. Retryables will get
        // re-attempted by the relay (if PENDING) or the next reconciler tick (if
        // FAILED). Permanent failures need human intervention once the upstream
        // cause is addressed — leaving them FAILED is the correct observable state.
        int leftInPlace = result.getRetryable().size() + result.getPermanentlyFailed().size();
        if (leftInPlace > 0) {
            metrics.recordReconcilerStillMissing(leftInPlace);
        }
        if (!result.getSucceeded().isEmpty()) {
            metrics.recordReconcilerReindexed(result.getSucceeded().size());
        }

        LOG.info("TagOutboxReconciler: partition={} batch={} succeeded={} retryable={} permanent={}",
                partition, batch.size(),
                result.getSucceeded().size(),
                result.getRetryable().size(),
                result.getPermanentlyFailed().size());
    }

    private void deleteByPartition(String partition, OutboxEntryId id) {
        if (id == null) return;
        if (TagOutbox.STATUS_FAILED.equals(partition)) {
            outbox.deleteFailed(id);
        } else {
            outbox.markDone(id); // drops the PENDING row
        }
    }
}
