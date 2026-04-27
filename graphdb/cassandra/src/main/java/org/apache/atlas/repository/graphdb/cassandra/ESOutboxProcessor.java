package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lease-guarded background processor that polls the es_outbox PENDING partition
 * and retries ES sync. Only one pod runs this at a time (via JobLeaseManager).
 *
 * Adaptive polling:
 * - Idle mode (PENDING empty):  polls every 30s — minimal Cassandra I/O
 * - Drain mode (PENDING has entries): polls every 2s with batch size 500 — fast recovery
 *
 * Exponential backoff per entry: entries are skipped if their backoff window hasn't
 * elapsed since the last attempt. Backoff doubles with each attempt (2s, 4s, 8s, 16s, 32s)
 * with ±25% jitter to prevent thundering herd. Total retry window across 5 attempts
 * is ~1 minute, covering transient ES failures. Prolonged outages are handled by the
 * 6-hour ESReconciliationJob which reads from Cassandra source of truth.
 *
 * Poison-pill safe: each entry's JSON is validated individually before being added
 * to the bulk request. Malformed entries are marked FAILED immediately.
 *
 * Lifecycle: call {@link #start()} once at application startup, {@link #stop()} at shutdown.
 */
public class ESOutboxProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ESOutboxProcessor.class);

    // Adaptive polling: idle vs drain mode
    private static final int IDLE_POLL_SECONDS = 30;
    private static final int DRAIN_POLL_SECONDS = 2;
    private static final int IDLE_BATCH_SIZE = 100;
    private static final int DRAIN_BATCH_SIZE = 500;

    // Lease TTL must exceed the longest possible poll+process cycle.
    // Drain mode: 2s poll + up to ~5s for a 500-entry bulk request = ~7s.
    // Use 60s for safety — if the holder crashes, another pod takes over within 60s.
    private static final int LEASE_TTL_SECONDS = 60;

    // Number of consecutive empty polls before switching from drain → idle
    private static final int EMPTY_POLLS_BEFORE_IDLE = 3;

    // Exponential backoff: base delay doubles per attempt (2s, 4s, 8s, 16s, 32s)
    // Total retry window across 5 attempts: ~1 minute. Covers transient ES failures.
    // Beyond that, entries move to FAILED for the 6-hour ESReconciliationJob to handle.
    private static final long BACKOFF_BASE_MS = 2000L;
    private static final long BACKOFF_MAX_MS = 32_000L; // cap at 32 seconds
    private static final double BACKOFF_JITTER_FACTOR = 0.25; // ±25% jitter

    private final ESOutboxRepository outboxRepository;
    private final JobLeaseManager leaseManager;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Adaptive state
    private volatile boolean drainMode = false;
    private volatile int consecutiveEmptyPolls = 0;
    private volatile ScheduledFuture<?> currentTask;

    public ESOutboxProcessor(ESOutboxRepository outboxRepository, JobLeaseManager leaseManager) {
        this.outboxRepository = outboxRepository;
        this.leaseManager = leaseManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "es-outbox-processor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduleNext(IDLE_POLL_SECONDS);
            LOG.info("ESOutboxProcessor started (idle={}s, drain={}s, idle_batch={}, drain_batch={})",
                    IDLE_POLL_SECONDS, DRAIN_POLL_SECONDS, IDLE_BATCH_SIZE, DRAIN_BATCH_SIZE);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            ScheduledFuture<?> task = currentTask;
            if (task != null) {
                task.cancel(false);
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("ESOutboxProcessor stopped");
        }
    }

    private void scheduleNext(int delaySeconds) {
        if (running.get()) {
            currentTask = scheduler.schedule(this::pollCycle, delaySeconds, TimeUnit.SECONDS);
        }
    }

    private void pollCycle() {
        if (!running.get()) return;

        try {
            processPendingEntries();
        } finally {
            // Schedule the next poll based on current mode
            if (running.get()) {
                scheduleNext(drainMode ? DRAIN_POLL_SECONDS : IDLE_POLL_SECONDS);
            }
        }
    }

    private void processPendingEntries() {
        if (!leaseManager.tryAcquire("es-outbox-processor", LEASE_TTL_SECONDS)) {
            return; // Another pod holds the lease
        }
        long pollStartMs = System.currentTimeMillis();
        try {
            int batchSize = drainMode ? DRAIN_BATCH_SIZE : IDLE_BATCH_SIZE;
            List<ESOutboxRepository.OutboxEntry> entries = outboxRepository.getPendingEntries(batchSize);
            updateOutboxPendingGauge(entries.size());

            if (entries.isEmpty()) {
                consecutiveEmptyPolls++;
                if (drainMode && consecutiveEmptyPolls >= EMPTY_POLLS_BEFORE_IDLE) {
                    drainMode = false;
                    updateDrainModeMetric(false);
                    LOG.info("ESOutboxProcessor: PENDING drained, switching to idle mode (poll every {}s)",
                            IDLE_POLL_SECONDS);
                }
                return;
            }

            // Entries found — switch to drain mode
            consecutiveEmptyPolls = 0;
            if (!drainMode) {
                drainMode = true;
                updateDrainModeMetric(true);
                LOG.info("ESOutboxProcessor: PENDING entries detected, switching to drain mode " +
                        "(poll every {}s, batch size {})", DRAIN_POLL_SECONDS, DRAIN_BATCH_SIZE);
            }

            LOG.info("ESOutboxProcessor: processing {} pending entries (drain mode)", entries.size());

            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ESOutboxProcessor: ES client not available, will retry next cycle");
                return;
            }

            String indexName = Constants.getVertexIndexName();

            // Phase 1: Triage — separate exhausted, backing-off, poison, and valid entries
            StringBuilder bulkBody = new StringBuilder();
            Map<String, ESOutboxRepository.OutboxEntry> entryMap = new LinkedHashMap<>();
            int skippedBackoff = 0;
            int exhaustedCount = 0;
            int poisonPillCount = 0;

            for (ESOutboxRepository.OutboxEntry entry : entries) {
                // Exhausted: hit max attempts → mark FAILED, remove from batch
                if (entry.attemptCount >= ESOutboxRepository.MAX_ATTEMPTS) {
                    outboxRepository.markFailed(entry.vertexId, entry.attemptCount, entry.action, entry.propertiesJson);
                    exhaustedCount++;
                    LOG.error("ESOutboxProcessor: marking vertex '{}' as FAILED after {} attempts (action={})",
                            entry.vertexId, entry.attemptCount, entry.action);
                    continue;
                }

                // Exponential backoff via time-based skipping (not Thread.sleep).
                if (entry.attemptCount > 0 && !isBackoffElapsed(entry)) {
                    skippedBackoff++;
                    continue;
                }

                // Validate entry before adding to bulk body — catch poison pills
                if (!appendToBulk(bulkBody, entry, indexName)) {
                    // Poison pill: malformed JSON or missing data — mark FAILED immediately
                    outboxRepository.markFailed(entry.vertexId, entry.attemptCount, entry.action, entry.propertiesJson);
                    poisonPillCount++;
                    LOG.error("ESOutboxProcessor: marking vertex '{}' as FAILED (poison pill: invalid entry data, action={})",
                            entry.vertexId, entry.action);
                    continue;
                }

                entryMap.put(entry.vertexId, entry);
            }

            // Record triage metrics in batch (no per-item loops)
            recordOutboxMetrics("exhausted", exhaustedCount);
            recordOutboxMetrics("backoff_skipped", skippedBackoff);
            recordOutboxMetrics("poison_pill", poisonPillCount);
            incrementOutboxFailedGauge(exhaustedCount + poisonPillCount);

            if (skippedBackoff > 0) {
                LOG.debug("ESOutboxProcessor: skipped {} entries still in backoff window", skippedBackoff);
            }

            if (entryMap.isEmpty()) {
                return;
            }

            // Phase 2: Send bulk request to ES
            try {
                Request bulkReq = new Request("POST", "/_bulk");
                bulkReq.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));
                Response resp = client.performRequest(bulkReq);
                int status = resp.getStatusLine().getStatusCode();
                String respBody = EntityUtils.toString(resp.getEntity());

                if (status >= 200 && status < 300) {
                    if (respBody != null && respBody.contains("\"errors\":true")) {
                        // Partial success — parse per-item results, separating retryable from permanent
                        Set<String> retryableIds = new LinkedHashSet<>();
                        Set<String> permanentIds = new LinkedHashSet<>();
                        parseFailedIds(respBody, retryableIds, permanentIds);

                        // Succeeded = everything not in either failure set
                        List<String> succeededIds = new ArrayList<>();
                        for (String id : entryMap.keySet()) {
                            if (!retryableIds.contains(id) && !permanentIds.contains(id)) {
                                succeededIds.add(id);
                            }
                        }
                        outboxRepository.batchMarkDone(succeededIds);
                        recordOutboxMetrics("success", succeededIds.size());

                        // Retryable failures — increment attempt, will retry on next cycle
                        for (String failedId : retryableIds) {
                            ESOutboxRepository.OutboxEntry entry = entryMap.get(failedId);
                            if (entry != null) {
                                outboxRepository.incrementAttempt(failedId, entry.attemptCount + 1);
                            }
                        }
                        recordOutboxMetrics("failed", retryableIds.size());

                        // Permanent failures — mark FAILED immediately, skip retries
                        for (String permId : permanentIds) {
                            ESOutboxRepository.OutboxEntry entry = entryMap.get(permId);
                            if (entry != null) {
                                outboxRepository.markFailed(permId, entry.attemptCount, entry.action, entry.propertiesJson);
                            }
                        }
                        recordOutboxMetrics("permanent", permanentIds.size());
                        incrementOutboxFailedGauge(permanentIds.size());

                        LOG.info("ESOutboxProcessor: {} succeeded, {} retryable, {} permanent failures",
                                succeededIds.size(), retryableIds.size(), permanentIds.size());
                    } else {
                        // All succeeded
                        outboxRepository.batchMarkDone(new ArrayList<>(entryMap.keySet()));
                        recordOutboxMetrics("success", entryMap.size());
                        LOG.info("ESOutboxProcessor: all {} entries synced to ES", entryMap.size());
                    }
                } else {
                    // Entire request failed — increment attempt count for all
                    for (ESOutboxRepository.OutboxEntry entry : entryMap.values()) {
                        outboxRepository.incrementAttempt(entry.vertexId, entry.attemptCount + 1);
                    }
                    LOG.warn("ESOutboxProcessor: bulk request failed with status {}, will retry {} entries",
                            status, entryMap.size());
                }
            } catch (Exception e) {
                // Connection/IO error — increment attempt count for all
                for (ESOutboxRepository.OutboxEntry entry : entryMap.values()) {
                    outboxRepository.incrementAttempt(entry.vertexId, entry.attemptCount + 1);
                }
                LOG.warn("ESOutboxProcessor: bulk request error, will retry {} entries: {}",
                        entryMap.size(), e.getMessage());
            }
        } catch (Exception e) {
            LOG.error("ESOutboxProcessor: unexpected error during processing", e);
        } finally {
            leaseManager.release("es-outbox-processor");
            recordOutboxPollDuration(pollStartMs);
        }
    }

    private void recordOutboxMetrics(String result, int count) {
        try {
            CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
            if (metrics != null && count > 0) {
                metrics.recordOutboxProcessed(result, count);
            }
        } catch (Throwable t) {
            // non-fatal
        }
    }

    private void recordOutboxPollDuration(long startMs) {
        try {
            CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
            if (metrics != null) {
                metrics.recordOutboxPollDuration(System.currentTimeMillis() - startMs);
            }
        } catch (Throwable t) {
            // non-fatal
        }
    }

    private void updateDrainModeMetric(boolean drain) {
        try {
            CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
            if (metrics != null) {
                metrics.setOutboxDrainMode(drain);
            }
        } catch (Throwable t) {
            // non-fatal
        }
    }

    private void updateOutboxPendingGauge(int count) {
        try {
            CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
            if (metrics != null) {
                metrics.setOutboxPending(count);
            }
        } catch (Throwable t) {
            // non-fatal
        }
    }

    private void incrementOutboxFailedGauge(int count) {
        try {
            CassandraGraphMetrics metrics = CassandraGraphMetrics.getInstance();
            if (metrics != null && count > 0) {
                metrics.incrementOutboxFailed(count);
            }
        } catch (Throwable t) {
            // non-fatal
        }
    }

    /**
     * Computes exponential backoff with jitter and checks if enough time has elapsed
     * since the last attempt.
     *
     * Backoff schedule (±25% jitter):
     *   attempt 1: ~2s, attempt 2: ~4s, attempt 3: ~8s, attempt 4: ~16s, attempt 5: ~32s (cap)
     *
     * Total retry window: ~1 minute across 5 attempts.
     */
    private boolean isBackoffElapsed(ESOutboxRepository.OutboxEntry entry) {
        long backoffMs = Math.min(BACKOFF_BASE_MS * (1L << (entry.attemptCount - 1)), BACKOFF_MAX_MS);

        // Add jitter (±25%) to prevent thundering herd when multiple entries fail together
        long jitterRange = (long) (backoffMs * BACKOFF_JITTER_FACTOR);
        long jitter = jitterRange > 0 ? ThreadLocalRandom.current().nextLong(-jitterRange, jitterRange) : 0;
        long effectiveBackoffMs = backoffMs + jitter;

        long elapsedMs = Instant.now().toEpochMilli() - entry.lastAttemptedAt.toEpochMilli();
        return elapsedMs >= effectiveBackoffMs;
    }

    /**
     * Validates and appends a single entry to the bulk NDJSON body.
     * Returns false if the entry is malformed (poison pill) — caller should mark it FAILED.
     */
    private boolean appendToBulk(StringBuilder bulkBody, ESOutboxRepository.OutboxEntry entry, String indexName) {
        try {
            if (entry.vertexId == null || entry.vertexId.isEmpty()) {
                return false;
            }

            if (ESOutboxRepository.ACTION_DELETE.equals(entry.action)) {
                bulkBody.append("{\"delete\":{\"_index\":\"").append(indexName)
                        .append("\",\"_id\":\"").append(entry.vertexId).append("\"}}\n");
                return true;
            }

            // Update action — the stored propertiesJson is a script-based update body:
            // {"script":{"source":"<UPSERT_SCRIPT>","lang":"painless","params":{"updates":{...}}},"upsert":{...}}
            // The script sets non-null fields and explicitly removes null fields from _source,
            // replacing the old doc/doc_as_upsert approach (which relied on the remove_null_fields
            // ingest pipeline — pipelines do not run on ES update operations).
            String json = entry.propertiesJson;
            if (json == null || json.isEmpty()) {
                return false;
            }

            // Verify it's parseable JSON (catches truncated/corrupt payloads)
            if (AtlasType.fromJson(json, Map.class) == null) {
                return false;
            }

            // Ensure no raw newlines that would break NDJSON format
            String safeJson = json.indexOf('\n') >= 0 ? json.replace('\n', ' ') : json;

            bulkBody.append("{\"update\":{\"_index\":\"").append(indexName)
                    .append("\",\"_id\":\"").append(entry.vertexId).append("\"}}\n");
            bulkBody.append(safeJson).append("\n");
            return true;
        } catch (Exception e) {
            LOG.warn("ESOutboxProcessor: invalid entry for vertex '{}': {}", entry.vertexId, e.getMessage());
            return false;
        }
    }

    /**
     * Parses an ES bulk response to separate retryable from permanent failures.
     * Retryable: 5xx server errors, 429 rate-limited — will be retried on next cycle.
     * Permanent: 4xx mapping errors, bad request — marked FAILED immediately to avoid
     * wasting ~1 minute of pointless retries per entry.
     */
    @SuppressWarnings("unchecked")
    private void parseFailedIds(String respBody, Set<String> retryableIds, Set<String> permanentIds) {
        try {
            Map<String, Object> bulkResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
            if (items == null) return;

            for (Map<String, Object> item : items) {
                Map<String, Object> action = (Map<String, Object>) item.values().iterator().next();
                if (action == null || !action.containsKey("error")) continue;

                String docId = String.valueOf(action.get("_id"));
                Object statusObj = action.get("status");
                int itemStatus = (statusObj instanceof Number) ? ((Number) statusObj).intValue() : 0;

                if (itemStatus >= 500 || itemStatus == 429) {
                    retryableIds.add(docId);
                } else {
                    permanentIds.add(docId);
                    LOG.error("ESOutboxProcessor: permanent bulk item failure _id='{}', status={}, error={}",
                            docId, itemStatus, AtlasType.toJson(action.get("error")));
                }
            }
        } catch (Exception e) {
            LOG.warn("ESOutboxProcessor: failed to parse bulk response: {}", e.getMessage());
        }
    }
}
