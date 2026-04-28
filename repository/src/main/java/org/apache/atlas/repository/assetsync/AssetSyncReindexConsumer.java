package org.apache.atlas.repository.assetsync;

import org.apache.atlas.util.RepairIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link OutboxConsumer} that replays asset-sync entries by re-deriving the ES
 * document from JG/Cassandra (the source of truth) and re-firing the index
 * write via {@link RepairIndex#restoreByIds(Set)} (MS-1010, Option B).
 *
 * <p>Each outbox entry is just a GUID. The relay batches entries, calls
 * {@code RepairIndex.restoreByIds(guids)} once per batch, and classifies the
 * outcome:
 * <ul>
 *     <li>RepairIndex returned cleanly → all entries marked succeeded</li>
 *     <li>RepairIndex threw → all entries marked retryable (transient failure)</li>
 * </ul>
 *
 * <p>Per-entry success/failure granularity is not exposed by RepairIndex today;
 * the all-or-none classification is acceptable because {@code restoreByIds} is
 * idempotent — already-indexed entities are simply re-written, and entities
 * deleted from JG between verify and replay are silently skipped.</p>
 */
public final class AssetSyncReindexConsumer implements OutboxConsumer<EntityGuidRef> {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncReindexConsumer.class);

    private final RepairIndex repairIndex;

    public AssetSyncReindexConsumer(RepairIndex repairIndex) {
        this.repairIndex = repairIndex;
    }

    @Override
    public ConsumeResult consume(List<OutboxEntry<EntityGuidRef>> batch) {
        if (batch == null || batch.isEmpty()) return ConsumeResult.empty();

        Set<String> guids = new HashSet<>(batch.size());
        List<OutboxEntryId> ids = new ArrayList<>(batch.size());
        for (OutboxEntry<EntityGuidRef> entry : batch) {
            guids.add(entry.getPayload().getEntityGuid());
            ids.add(entry.getId());
        }

        try {
            repairIndex.restoreByIds(guids);
            return new ConsumeResult(ids, java.util.Collections.emptyList(), java.util.Collections.emptyMap());
        } catch (Exception e) {
            LOG.warn("AssetSyncReindexConsumer: restoreByIds failed for {} guids — releasing for retry: {}",
                    guids.size(), e.getMessage());
            return new ConsumeResult(java.util.Collections.emptyList(), ids, java.util.Collections.emptyMap());
        }
    }
}
