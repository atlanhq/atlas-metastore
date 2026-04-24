package org.apache.atlas.repository.assetsync;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.util.RepairIndex;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hourly sweeper for outbox entries the relay can't self-heal (MS-1010 reconciliation).
 *
 * <p>Scans the two partitions the relay would otherwise leave alone:</p>
 * <ul>
 *     <li><b>FAILED</b> — max_attempts exhausted, relay gave up.</li>
 *     <li><b>orphaned PENDING</b> — last attempt older than the stuck threshold and
 *         no legitimate backoff window, meaning a leader died mid-batch or the
 *         relay hasn't picked it up.</li>
 * </ul>
 *
 * <p>Per batch: one {@code terms} query against ES to find which GUIDs are already
 * present (the "false FAILED" case — entity made it to ES via a later mutation or
 * a JG internal retry). Rows for those are dropped. Remaining GUIDs are handed to
 * {@link RepairIndex#restoreByIds} in one shot — idempotent, lenient (silently
 * skips entities purged from JG). On success all rows are dropped. On failure,
 * rows are left in place and the next reconciler run retries.</p>
 *
 * <p>Lease-gated: only runs on the pod holding the {@code asset-sync-relay} lease,
 * so there's exactly one reconciler per tenant at a time. Shares the lease with
 * the relay but runs on its own {@link java.util.concurrent.ScheduledExecutorService}
 * so a slow sweep doesn't block relay ticks.</p>
 *
 * <p>Boot-safe: {@link #start()} and every tick catch all exceptions so the
 * reconciler can never take down Atlas. Observability via
 * {@code atlas_es_outbox_reconciler_healthy} (init state) and
 * {@code atlas_es_outbox_reconciler_tick_errors_total} (per-tick failures).</p>
 */
public final class AssetSyncReconciler {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncReconciler.class);

    private static final String GUID_FIELD = "__guid";

    private final AssetSyncOutbox outbox;
    private final RepairIndex     repairIndex;
    private final LeaseManager    leaseManager;
    private final String          leaseName;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean   running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> currentTask;

    public AssetSyncReconciler(AssetSyncOutbox outbox,
                                RepairIndex repairIndex,
                                LeaseManager leaseManager,
                                String leaseName) {
        this.outbox       = outbox;
        this.repairIndex  = repairIndex;
        this.leaseManager = leaseManager;
        this.leaseName    = leaseName;
        this.scheduler    = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "asset-sync-outbox-reconciler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start the hourly (configurable) scheduled sweep. Adds a jittered initial
     * delay so multiple tenants don't all hit ES at the same wall-clock moment.
     * Subsequent ticks are spaced at the full interval via scheduleWithFixedDelay.
     *
     * <p>Boot-safe: all exceptions are caught and logged so a reconciler init
     * failure cannot cascade into an Atlas startup failure. On failure the
     * reconciler_healthy gauge stays at 0 and the relay + post-commit verifier
     * continue normally — the reconciler is strictly advisory.</p>
     */
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        try {
            int intervalSec = AtlasConfiguration.ASSET_SYNC_RECONCILER_INTERVAL_SECONDS.getInt();
            int jitterSec   = Math.max(0, AtlasConfiguration.ASSET_SYNC_RECONCILER_JITTER_SECONDS.getInt());
            long initialDelaySec = intervalSec + (jitterSec > 0
                    ? ThreadLocalRandom.current().nextLong(-jitterSec, jitterSec + 1L) : 0);
            if (initialDelaySec < 0) initialDelaySec = 0;

            currentTask = scheduler.scheduleWithFixedDelay(
                    this::safeTick,
                    initialDelaySec, intervalSec, TimeUnit.SECONDS);
            AssetSyncOutboxMetrics.setReconcilerHealthy(true);
            LOG.info("AssetSyncReconciler started (interval={}s, initial_delay={}s, lease='{}')",
                    intervalSec, initialDelaySec, leaseName);
        } catch (Throwable t) {
            // Roll back running-state so stop() is a no-op and so any future
            // restart attempt can try again. Never rethrow — the reconciler
            // is optional and must not break Atlas boot.
            running.set(false);
            AssetSyncOutboxMetrics.setReconcilerHealthy(false);
            LOG.error("AssetSyncReconciler: start() FAILED — reconciler not scheduled on this pod; " +
                    "relay + post-commit verify still active. FAILED outbox rows will accumulate until " +
                    "another pod's reconciler runs or this one is restarted.", t);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        AssetSyncOutboxMetrics.setReconcilerHealthy(false);
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
        LOG.info("AssetSyncReconciler stopped");
    }

    /**
     * Scheduler entrypoint. Catches Throwable so no tick can kill the recurring
     * task (otherwise {@code scheduleWithFixedDelay} silently suppresses all
     * future executions). Records a tick-error metric on failure so ops can
     * alert on {@code atlas_es_outbox_reconciler_tick_errors_total} > 0 even
     * when the scheduler itself looks healthy.
     */
    private void safeTick() {
        if (!running.get()) return;
        try {
            runOnce();
        } catch (Throwable t) {
            AssetSyncOutboxMetrics.recordReconcilerTickError();
            LOG.error("AssetSyncReconciler: unexpected error in reconcile tick — will retry next interval", t);
        }
    }

    /**
     * One reconciler tick. No-op if this pod doesn't hold the relay lease.
     */
    public void runOnce() {
        if (!leaseManager.isHeldByMe(leaseName)) return;

        AssetSyncOutboxMetrics.recordReconcilerRun();

        int batchSize = AtlasConfiguration.ASSET_SYNC_RECONCILER_BATCH_SIZE.getInt();
        Duration stuckFor = Duration.ofSeconds(
                AtlasConfiguration.ASSET_SYNC_RECONCILER_STUCK_PENDING_THRESHOLD_SECONDS.getLong());

        try {
            reconcile(outbox.scanFailed(batchSize), AssetSyncOutbox.STATUS_FAILED);
        } catch (Throwable t) {
            LOG.error("AssetSyncReconciler: FAILED sweep failed", t);
        }
        try {
            reconcile(outbox.scanStuckPending(stuckFor, batchSize), AssetSyncOutbox.STATUS_PENDING);
        } catch (Throwable t) {
            LOG.error("AssetSyncReconciler: stuck-PENDING sweep failed", t);
        }
    }

    /**
     * Given a batch of outbox entries in a single status partition, verify ES
     * presence, drop rows whose entities are already in ES, and replay the rest
     * via RepairIndex. Rows whose replay throws are left in place for the next run.
     */
    private void reconcile(List<OutboxEntry<EntityGuidRef>> batch, String partition) {
        if (batch.isEmpty()) return;

        AssetSyncOutboxMetrics.recordReconcilerScanned(partition, batch.size());

        // Map guid → row id so we can drop the right partition after acting.
        Map<String, OutboxEntryId> guidToId = new LinkedHashMap<>(batch.size());
        for (OutboxEntry<EntityGuidRef> e : batch) {
            guidToId.put(e.getPayload().getEntityGuid(), e.getId());
        }

        Set<String> presentInEs;
        try {
            presentInEs = findPresentInEs(new ArrayList<>(guidToId.keySet()));
        } catch (Exception e) {
            // ES query itself failed — skip this batch; the relay + next reconciler
            // run will handle it. Do NOT mark entries still_missing for this, since
            // we didn't actually try the replay.
            LOG.warn("AssetSyncReconciler: ES presence check failed for {} guids, skipping batch: {}",
                    batch.size(), e.getMessage());
            return;
        }

        // Drop rows for entities already in ES — the "false FAILED" case.
        for (String guid : presentInEs) {
            deleteByPartition(partition, guidToId.get(guid));
        }
        if (!presentInEs.isEmpty()) {
            AssetSyncOutboxMetrics.recordReconcilerAlreadyInEs(presentInEs.size());
        }

        // Everything else needs a fresh replay.
        Set<String> needsReindex = new LinkedHashSet<>(guidToId.keySet());
        needsReindex.removeAll(presentInEs);
        if (needsReindex.isEmpty()) return;

        try {
            repairIndex.restoreByIds(needsReindex);
            // restoreByIds is idempotent and silently skips entities purged from JG,
            // so a clean return means "ES has been made consistent with JG for all of
            // these GUIDs" — dropping the rows is correct whether they were re-indexed
            // or were purged stragglers.
            for (String guid : needsReindex) {
                deleteByPartition(partition, guidToId.get(guid));
            }
            AssetSyncOutboxMetrics.recordReconcilerReindexed(needsReindex.size());
            LOG.info("AssetSyncReconciler: reconciled partition={} verified_in_es={} reindexed={}",
                    partition, presentInEs.size(), needsReindex.size());
        } catch (Exception e) {
            // Replay threw — leave the rows in place. Next reconciler run picks them up.
            AssetSyncOutboxMetrics.recordReconcilerStillMissing(needsReindex.size());
            LOG.warn("AssetSyncReconciler: RepairIndex.restoreByIds failed for {} guids in partition={} — left in place for next run: {}",
                    needsReindex.size(), partition, e.getMessage());
        }
    }

    private void deleteByPartition(String partition, OutboxEntryId id) {
        if (id == null) return;
        if (AssetSyncOutbox.STATUS_FAILED.equals(partition)) {
            outbox.deleteFailed(id);
        } else {
            outbox.markDone(id); // drops the PENDING row
        }
    }

    /**
     * Issues a terms query against the vertex index and returns the subset of
     * input GUIDs that ES has documents for. Mirrors the approach in
     * {@link PostCommitEsVerifier#findMissing} but returns the inverse set —
     * the reconciler's natural frame is "which ones have recovered" not "which
     * ones are still missing".
     */
    @SuppressWarnings("unchecked")
    private static Set<String> findPresentInEs(List<String> guids) throws Exception {
        if (guids.isEmpty()) return Collections.emptySet();

        RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
        if (client == null) throw new IllegalStateException("ES low-level client unavailable");

        Map<String, Object> termsClause = Collections.singletonMap(GUID_FIELD, guids);
        Map<String, Object> query       = Collections.singletonMap("terms", termsClause);
        Map<String, Object> body        = new LinkedHashMap<>();
        body.put("size", guids.size());
        body.put("_source", Collections.singletonList(GUID_FIELD));
        body.put("query", query);

        Request req = new Request("POST", "/" + Constants.VERTEX_INDEX_NAME + "/_search");
        req.setEntity(new StringEntity(AtlasType.toJson(body), ContentType.APPLICATION_JSON));

        Response resp = client.performRequest(req);
        int status = resp.getStatusLine().getStatusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException("ES search returned status " + status);
        }

        String respBody = EntityUtils.toString(resp.getEntity());
        Map<String, Object> parsed = AtlasType.fromJson(respBody, Map.class);
        Map<String, Object> hits = (Map<String, Object>) parsed.get("hits");
        List<Map<String, Object>> hitList = hits == null ? Collections.emptyList()
                : (List<Map<String, Object>>) hits.get("hits");

        Set<String> found = new HashSet<>();
        for (Map<String, Object> hit : hitList) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            if (source == null) continue;
            Object guidField = source.get(GUID_FIELD);
            if (guidField instanceof String) found.add((String) guidField);
        }
        return found;
    }
}
