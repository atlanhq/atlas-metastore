package org.apache.atlas.repository.assetsync;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;

/**
 * Writes verify-misses into the asset-sync outbox (MS-1010, Option B).
 *
 * <p>Called by {@link PostCommitEsVerifier} when an entity GUID committed via
 * JG is found to be missing from Elasticsearch. One outbox row per missing
 * GUID. Best-effort: enqueue failures are logged + counted, never rethrown.</p>
 */
public final class AssetSyncSink {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncSink.class);

    private final Outbox<EntityGuidRef> outbox;

    public AssetSyncSink(Outbox<EntityGuidRef> outbox) {
        this.outbox = outbox;
    }

    public void enqueueMissing(Set<String> missingGuids) {
        if (missingGuids == null || missingGuids.isEmpty()) return;
        Instant now = Instant.now();
        for (String guid : missingGuids) {
            try {
                OutboxEntry<EntityGuidRef> entry = new OutboxEntry<>(
                        new OutboxEntryId(guid, ""),
                        new EntityGuidRef(guid),
                        0,
                        now,
                        null
                );
                Timer.Sample t = AssetSyncOutboxMetrics.startWriteTimer();
                try {
                    outbox.enqueue(entry);
                    AssetSyncOutboxMetrics.recordWrite();
                } finally {
                    AssetSyncOutboxMetrics.stopWriteTimer(t);
                }
            } catch (Throwable t) {
                AssetSyncOutboxMetrics.recordWriteError(reasonOf(t));
                LOG.error("AssetSyncSink: enqueue failed for guid='{}': {}", guid, t.getMessage(), t);
            }
        }
    }

    private static String reasonOf(Throwable t) {
        if (t == null) return "unknown";
        String name = t.getClass().getSimpleName();
        return name.length() > 0 ? name : "unknown";
    }
}
