package org.apache.atlas.repository.assetsync;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Cassandra-backed implementation of {@link Outbox} for the asset-sync use case (MS-1010, Option B).
 *
 * <p>Slim, GUID-only schema: {@code asset_sync_outbox} with PK {@code ((status), entity_guid)}.
 * No payload — the relay re-derives the ES document from Cassandra at replay time via
 * {@code RepairIndex.restoreByIds}.</p>
 *
 * <p>Partitioning by status makes {@code WHERE status='PENDING'} a direct partition scan.
 * The PENDING partition stays small under normal operation — the post-commit verifier
 * only enqueues entities that ES is genuinely missing.</p>
 */
public final class AssetSyncOutbox implements Outbox<EntityGuidRef> {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncOutbox.class);

    static final String STATUS_PENDING = "PENDING";
    static final String STATUS_FAILED  = "FAILED";

    private final CqlSession        session;
    private final String            keyspace;
    private final int               maxAttempts;
    private final long              backoffBaseMs;
    private final long              backoffMaxMs;

    private final PreparedStatement insertPendingStmt;
    private final PreparedStatement deletePendingStmt;
    private final PreparedStatement deleteFailedStmt;
    private final PreparedStatement insertFailedStmt;
    private final PreparedStatement claimStmt;
    private final PreparedStatement releaseClaimStmt;
    private final PreparedStatement releaseForRetryStmt;
    private final PreparedStatement selectClaimableStmt;
    private final PreparedStatement selectByStatusStmt;
    private final PreparedStatement countByStatusStmt;

    public AssetSyncOutbox(CqlSession session) {
        this.session       = session;
        this.keyspace      = AtlasConfiguration.ASSET_SYNC_OUTBOX_KEYSPACE.getString();
        this.maxAttempts   = AtlasConfiguration.ASSET_SYNC_OUTBOX_MAX_ATTEMPTS.getInt();
        this.backoffBaseMs = AtlasConfiguration.ASSET_SYNC_RELAY_BACKOFF_BASE_MS.getLong();
        this.backoffMaxMs  = AtlasConfiguration.ASSET_SYNC_RELAY_BACKOFF_MAX_MS.getLong();

        this.insertPendingStmt = session.prepare(
                "INSERT INTO " + keyspace + ".asset_sync_outbox " +
                "(status, entity_guid, attempt_count, created_at, last_attempted_at, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
        );
        this.deletePendingStmt = session.prepare(
                "DELETE FROM " + keyspace + ".asset_sync_outbox " +
                "WHERE status = ? AND entity_guid = ?"
        );
        // Reconciler uses the same DELETE shape, but against the FAILED partition.
        this.deleteFailedStmt = session.prepare(
                "DELETE FROM " + keyspace + ".asset_sync_outbox " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.insertFailedStmt = session.prepare(
                "INSERT INTO " + keyspace + ".asset_sync_outbox " +
                "(status, entity_guid, attempt_count, created_at, last_attempted_at, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
        );
        this.claimStmt = session.prepare(
                "UPDATE " + keyspace + ".asset_sync_outbox " +
                "SET claimed_by = ?, claimed_until = ? " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.releaseClaimStmt = session.prepare(
                "UPDATE " + keyspace + ".asset_sync_outbox " +
                "SET claimed_by = null, claimed_until = null " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.releaseForRetryStmt = session.prepare(
                "UPDATE " + keyspace + ".asset_sync_outbox " +
                "SET attempt_count = ?, last_attempted_at = ?, next_attempt_at = ?, " +
                "    claimed_by = null, claimed_until = null " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.selectClaimableStmt = session.prepare(
                "SELECT entity_guid, attempt_count, " +
                "       created_at, last_attempted_at, next_attempt_at, claimed_by, claimed_until " +
                "FROM " + keyspace + ".asset_sync_outbox WHERE status = ?"
        );
        // Reconciler scan — reads the whole partition; client filters for stuck PENDING.
        this.selectByStatusStmt = session.prepare(
                "SELECT entity_guid, attempt_count, " +
                "       created_at, last_attempted_at, next_attempt_at " +
                "FROM " + keyspace + ".asset_sync_outbox WHERE status = ?"
        );
        this.countByStatusStmt = session.prepare(
                "SELECT entity_guid, claimed_until, created_at " +
                "FROM " + keyspace + ".asset_sync_outbox WHERE status = ?"
        );
    }

    @Override
    public void enqueue(OutboxEntry<EntityGuidRef> entry) {
        Instant now = Instant.now();
        EntityGuidRef ref = entry.getPayload();
        // Same entity_guid re-enqueues collapse on PK — last write wins. Safe because
        // every replay re-reads from Cassandra; collapsing duplicate verify-misses is correct.
        session.execute(insertPendingStmt.bind(
                STATUS_PENDING,
                ref.getEntityGuid(),
                entry.getAttemptCount(),
                entry.getCreatedAt() != null ? entry.getCreatedAt() : now,
                entry.getLastAttemptedAt(),
                now
        ));
    }

    @Override
    public List<OutboxEntry<EntityGuidRef>> claim(int batchSize) {
        Instant now = Instant.now();
        long claimTtlMs = Duration.ofSeconds(
                AtlasConfiguration.ASSET_SYNC_RELAY_CLAIM_TTL_SECONDS.getInt()).toMillis();
        Instant claimedUntil = now.plusMillis(claimTtlMs);
        String podId = AssetSyncPodId.get();

        ResultSet rs = session.execute(
                selectClaimableStmt.bind(STATUS_PENDING).setPageSize(batchSize * 2));

        List<OutboxEntry<EntityGuidRef>> claimed = new ArrayList<>(batchSize);
        for (Row row : rs) {
            if (claimed.size() >= batchSize) break;
            // Skip rows still being processed by someone else (within the claim TTL)
            Instant existingClaimedUntil = row.getInstant("claimed_until");
            if (existingClaimedUntil != null && existingClaimedUntil.isAfter(now)) continue;
            // Skip rows whose backoff window hasn't elapsed
            Instant nextAttemptAt = row.getInstant("next_attempt_at");
            if (nextAttemptAt != null && nextAttemptAt.isAfter(now)) continue;

            String entityGuid = row.getString("entity_guid");
            int attemptCount  = row.getInt("attempt_count");

            // Stamp claim atomically before handing to the consumer
            session.execute(claimStmt.bind(podId, claimedUntil, STATUS_PENDING, entityGuid));

            claimed.add(new OutboxEntry<>(
                    new OutboxEntryId(entityGuid, ""),
                    new EntityGuidRef(entityGuid),
                    attemptCount,
                    row.getInstant("created_at"),
                    row.getInstant("last_attempted_at")
            ));
        }
        return claimed;
    }

    @Override
    public void markDone(OutboxEntryId id) {
        session.execute(deletePendingStmt.bind(STATUS_PENDING, id.getPartA()));
    }

    @Override
    public void markFailed(OutboxEntryId id, int finalAttemptCount, Throwable cause) {
        Instant now = Instant.now();
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(deletePendingStmt.bind(STATUS_PENDING, id.getPartA()));
        batch.addStatement(insertFailedStmt.bind(
                STATUS_FAILED,
                id.getPartA(),
                finalAttemptCount,
                now,
                now,
                null
        ));
        session.execute(batch.build());
        LOG.warn("AssetSyncOutbox: entry '{}' moved to FAILED after {} attempts: {}",
                id, finalAttemptCount, cause != null ? cause.getMessage() : "n/a");
    }

    @Override
    public void releaseForRetry(OutboxEntryId id, int newAttemptCount) {
        Instant now = Instant.now();
        long backoff = computeBackoffMs(newAttemptCount);
        Instant nextAttemptAt = now.plusMillis(backoff);
        session.execute(releaseForRetryStmt.bind(
                newAttemptCount, now, nextAttemptAt,
                STATUS_PENDING, id.getPartA()));
    }

    @Override
    public void releaseClaim(OutboxEntryId id) {
        session.execute(releaseClaimStmt.bind(STATUS_PENDING, id.getPartA()));
    }

    public int getMaxAttempts() { return maxAttempts; }

    /**
     * Read FAILED entries for the reconciler. Single-partition scan, capped at {@code limit}.
     * Relay never touches FAILED rows so there's no contention with the reconciler here.
     */
    public List<OutboxEntry<EntityGuidRef>> scanFailed(int limit) {
        ResultSet rs = session.execute(selectByStatusStmt.bind(STATUS_FAILED).setPageSize(limit));
        List<OutboxEntry<EntityGuidRef>> out = new ArrayList<>(limit);
        for (Row row : rs) {
            if (out.size() >= limit) break;
            String guid = row.getString("entity_guid");
            out.add(new OutboxEntry<>(
                    new OutboxEntryId(guid, ""),
                    new EntityGuidRef(guid),
                    row.getInt("attempt_count"),
                    row.getInstant("created_at"),
                    row.getInstant("last_attempted_at")));
        }
        return out;
    }

    /**
     * Read PENDING entries that look orphaned: either never attempted and older than
     * {@code stuckFor}, or last attempted and not retried within {@code stuckFor}.
     * The relay normally drains PENDING in seconds; anything this old indicates a
     * missed leader election window or a relay that died mid-batch. Scans the
     * PENDING partition and filters client-side (Cassandra can't filter on non-PK
     * columns without ALLOW FILTERING).
     */
    public List<OutboxEntry<EntityGuidRef>> scanStuckPending(Duration stuckFor, int limit) {
        Instant cutoff = Instant.now().minus(stuckFor);
        ResultSet rs = session.execute(selectByStatusStmt.bind(STATUS_PENDING).setPageSize(limit * 2));
        List<OutboxEntry<EntityGuidRef>> out = new ArrayList<>(limit);
        for (Row row : rs) {
            if (out.size() >= limit) break;
            Instant lastAttemptedAt = row.getInstant("last_attempted_at");
            Instant createdAt       = row.getInstant("created_at");
            Instant nextAttemptAt   = row.getInstant("next_attempt_at");

            // Stuck = hasn't been attempted recently AND isn't sitting in a legitimate
            // backoff window. next_attempt_at in the future means relay is planning to
            // retry it; reconciler should not steal those.
            Instant reference = lastAttemptedAt != null ? lastAttemptedAt : createdAt;
            if (reference == null || reference.isAfter(cutoff)) continue;
            if (nextAttemptAt != null && nextAttemptAt.isAfter(Instant.now())) continue;

            String guid = row.getString("entity_guid");
            out.add(new OutboxEntry<>(
                    new OutboxEntryId(guid, ""),
                    new EntityGuidRef(guid),
                    row.getInt("attempt_count"),
                    createdAt,
                    lastAttemptedAt));
        }
        return out;
    }

    /**
     * Delete a row from the FAILED partition. Used by the reconciler after it
     * confirms the entity is now in ES (false FAILED) or re-indexes it successfully.
     * markDone / releaseForRetry only touch the PENDING partition.
     */
    public void deleteFailed(OutboxEntryId id) {
        session.execute(deleteFailedStmt.bind(STATUS_FAILED, id.getPartA()));
    }

    long computeBackoffMs(int attemptCount) {
        long base = backoffBaseMs * (long) Math.pow(2, Math.max(0, attemptCount - 1));
        return Math.min(Math.max(backoffBaseMs, base), backoffMaxMs);
    }

    public StorageStats computeStorageStats() {
        int pendingCount    = 0;
        int processingCount = 0;
        int failedCount     = 0;
        long oldestPendingAgeSeconds = 0;
        Instant now = Instant.now();

        for (Row row : session.execute(countByStatusStmt.bind(STATUS_PENDING))) {
            pendingCount++;
            Instant claimedUntil = row.getInstant("claimed_until");
            if (claimedUntil != null && claimedUntil.isAfter(now)) processingCount++;
            Instant createdAt = row.getInstant("created_at");
            if (createdAt != null) {
                long ageSeconds = Duration.between(createdAt, now).getSeconds();
                if (ageSeconds > oldestPendingAgeSeconds) oldestPendingAgeSeconds = ageSeconds;
            }
        }
        for (Row row : session.execute(countByStatusStmt.bind(STATUS_FAILED))) failedCount++;
        return new StorageStats(pendingCount, processingCount, failedCount, oldestPendingAgeSeconds);
    }

    public static final class StorageStats {
        public final int  pendingCount;
        public final int  processingCount;
        public final int  failedCount;
        public final long oldestPendingAgeSeconds;
        StorageStats(int pendingCount, int processingCount, int failedCount, long oldestPendingAgeSeconds) {
            this.pendingCount = pendingCount;
            this.processingCount = processingCount;
            this.failedCount = failedCount;
            this.oldestPendingAgeSeconds = oldestPendingAgeSeconds;
        }
    }
}
