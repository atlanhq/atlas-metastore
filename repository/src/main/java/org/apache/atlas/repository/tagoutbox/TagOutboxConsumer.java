package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.repository.assetsync.ConsumeResult;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.OutboxConsumer;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityCreateOrUpdateMutationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Replays tag-outbox entries by recomputing the 5 denorm fields and partial-updating ES.
 *
 * <p>Mirrors {@code TagDenormDLQReplayService.replayTagDenormDLQEntry} exactly
 * so that the failure-handling semantics are identical between the (soon-to-be-retired)
 * Kafka DLQ consumer and this outbox consumer. Deliberately does <b>not</b> call
 * {@code RepairIndex.restoreByIds} — a full vertex reindex is wasteful and
 * potentially wrong for tag-denorm failures (rewrites non-denorm fields from
 * current vertex state, which may miss denorm values that live only in the
 * Cassandra tag store).</p>
 *
 * <p>Per batch, chunks are sized at {@code config.consumerRepairBatchSize()}
 * (default 300, matching the DLQ replay service). For each chunk:</p>
 * <pre>
 *     try {
 *         errors = entityStore.repairClassificationMappingsV2(chunk);
 *         postProcessor.executeESOperations(RequestContext.getESDeferredOperations());
 *     } finally {
 *         RequestContext.getESDeferredOperations().clear();
 *     }
 * </pre>
 *
 * <p>Classification rules:</p>
 * <ul>
 *     <li>GUIDs returned in the error map → permanent failure (the repair path
 *         itself declared them unrecoverable).</li>
 *     <li>If {@code repairClassificationMappingsV2} throws → all GUIDs in the
 *         chunk are retryable (transient failure surface).</li>
 *     <li>All other GUIDs in the chunk → succeeded.</li>
 * </ul>
 *
 * <p>The {@code RequestContext.getESDeferredOperations().clear()} in
 * {@code finally} is load-bearing: deferred ops from a failed chunk must not
 * leak into the next chunk's request context or into an unrelated thread's
 * follow-up work.</p>
 */
public final class TagOutboxConsumer implements OutboxConsumer<EntityGuidRef> {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxConsumer.class);

    private final AtlasEntityStore                       entityStore;
    private final EntityCreateOrUpdateMutationPostProcessor postProcessor;
    private final int                                    repairBatchSize;

    public TagOutboxConsumer(AtlasEntityStore entityStore,
                             EntityCreateOrUpdateMutationPostProcessor postProcessor,
                             TagOutboxConfig config) {
        this.entityStore     = Objects.requireNonNull(entityStore, "entityStore");
        this.postProcessor   = Objects.requireNonNull(postProcessor, "postProcessor");
        this.repairBatchSize = Math.max(1, config.consumerRepairBatchSize());
    }

    @Override
    public ConsumeResult consume(List<OutboxEntry<EntityGuidRef>> batch) {
        if (batch == null || batch.isEmpty()) return ConsumeResult.empty();

        // GUID → row id lookup. LinkedHashMap preserves the claim order so the
        // outcome lists match the caller's input ordering (makes logs readable).
        Map<String, OutboxEntryId> guidToId = new LinkedHashMap<>(batch.size());
        for (OutboxEntry<EntityGuidRef> entry : batch) {
            String guid = entry.getPayload() != null ? entry.getPayload().getEntityGuid() : null;
            if (guid != null && !guid.isEmpty()) {
                guidToId.putIfAbsent(guid, entry.getId());
            }
        }
        if (guidToId.isEmpty()) return ConsumeResult.empty();

        List<OutboxEntryId>            succeeded         = new ArrayList<>(guidToId.size());
        List<OutboxEntryId>            retryable         = new ArrayList<>();
        Map<OutboxEntryId, Throwable>  permanentlyFailed = new HashMap<>();

        List<String> allGuids = new ArrayList<>(guidToId.keySet());
        for (int i = 0; i < allGuids.size(); i += repairBatchSize) {
            int end = Math.min(i + repairBatchSize, allGuids.size());
            List<String> chunk = allGuids.subList(i, end);
            processChunk(chunk, guidToId, succeeded, retryable, permanentlyFailed);
        }

        LOG.info("TagOutboxConsumer: batch={} succeeded={} retryable={} permanent={}",
                batch.size(), succeeded.size(), retryable.size(), permanentlyFailed.size());
        return new ConsumeResult(succeeded, retryable, permanentlyFailed);
    }

    /**
     * Replay one chunk. Matches DLQReplayService's flow: call repair, flush deferred
     * ops outside the try-throw boundary so that partial failures are still flushed,
     * clear the context in finally so leak can't happen across chunks.
     */
    private void processChunk(List<String> chunk,
                              Map<String, OutboxEntryId> guidToId,
                              List<OutboxEntryId> succeeded,
                              List<OutboxEntryId> retryable,
                              Map<OutboxEntryId, Throwable> permanentlyFailed) {
        Map<String, String> errors = Collections.emptyMap();
        boolean chunkThrew = false;
        Throwable chunkCause = null;

        try {
            errors = entityStore.repairClassificationMappingsV2(chunk);
            // Flush deferred ES operations that repairClassificationMappingsV2 staged
            // on this thread's RequestContext. Same ordering as DLQReplayService:556.
            List<ESDeferredOperation> deferredOps = RequestContext.get().getESDeferredOperations();
            if (!deferredOps.isEmpty()) {
                postProcessor.executeESOperations(deferredOps);
            }
        } catch (AtlasBaseException | RuntimeException e) {
            chunkThrew = true;
            chunkCause = e;
            LOG.warn("TagOutboxConsumer: chunk of {} threw during repair — releasing for retry: {}",
                    chunk.size(), e.getMessage());
        } finally {
            // Load-bearing: the next chunk (or whatever work this thread does next)
            // must not inherit stale deferred ops. Mirrors DLQReplayService:562.
            RequestContext.get().getESDeferredOperations().clear();
        }

        // Classify outcomes
        if (chunkThrew) {
            for (String guid : chunk) {
                OutboxEntryId id = guidToId.get(guid);
                if (id != null) retryable.add(id);
            }
            return;
        }
        for (String guid : chunk) {
            OutboxEntryId id = guidToId.get(guid);
            if (id == null) continue;
            if (errors.containsKey(guid)) {
                permanentlyFailed.put(id, new RuntimeException(errors.get(guid)));
            } else {
                succeeded.add(id);
            }
        }
    }
}
