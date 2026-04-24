package org.apache.atlas.repository.tagoutbox;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.Outbox;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.outbox.shared.OutboxPodId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Cassandra-backed {@link Outbox} for tag-denorm ES-write failures.
 *
 * <p>Slim, GUID-only row shape — identical to {@code asset_sync_outbox}. No payload
 * is stored; the relay re-derives the tag denorm fields by reading from Cassandra
 * at replay time via {@code repairClassificationMappingsV2}.</p>
 *
 * <p>Primary key {@code ((status), entity_guid)} keeps {@code WHERE status = 'PENDING'}
 * a single-partition scan. Multiple re-enqueues for the same GUID naturally dedupe
 * to one row because replay is idempotent (the repair path re-reads the current
 * tag state from Cassandra).</p>
 *
 * <p>Table and keyspace identifiers are carried on the {@link TagOutboxConfig}
 * record — this class never hard-codes them. An operator can rename either by
 * changing {@code atlas.tag.outbox.keyspace} / {@code atlas.tag.outbox.table.name}
 * properties without a code change.</p>
 */
public final class TagOutbox implements Outbox<EntityGuidRef> {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutbox.class);

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_FAILED  = "FAILED";

    private final CqlSession       session;
    private final TagOutboxConfig  config;

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

    public TagOutbox(CqlSession session, TagOutboxConfig config) {
        this.session = Objects.requireNonNull(session, "session");
        this.config  = Objects.requireNonNull(config, "config");

        final String qualified = config.keyspace() + "." + config.outboxTableName();

        this.insertPendingStmt = session.prepare(
                "INSERT INTO " + qualified + " " +
                "(status, entity_guid, attempt_count, created_at, last_attempted_at, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
        );
        this.deletePendingStmt = session.prepare(
                "DELETE FROM " + qualified + " WHERE status = ? AND entity_guid = ?"
        );
        this.deleteFailedStmt = session.prepare(
                "DELETE FROM " + qualified + " WHERE status = ? AND entity_guid = ?"
        );
        this.insertFailedStmt = session.prepare(
                "INSERT INTO " + qualified + " " +
                "(status, entity_guid, attempt_count, created_at, last_attempted_at, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
        );
        this.claimStmt = session.prepare(
                "UPDATE " + qualified + " " +
                "SET claimed_by = ?, claimed_until = ? " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.releaseClaimStmt = session.prepare(
                "UPDATE " + qualified + " " +
                "SET claimed_by = null, claimed_until = null " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.releaseForRetryStmt = session.prepare(
                "UPDATE " + qualified + " " +
                "SET attempt_count = ?, last_attempted_at = ?, next_attempt_at = ?, " +
                "    claimed_by = null, claimed_until = null " +
                "WHERE status = ? AND entity_guid = ?"
        );
        this.selectClaimableStmt = session.prepare(
                "SELECT entity_guid, attempt_count, " +
                "       created_at, last_attempted_at, next_attempt_at, claimed_by, claimed_until " +
                "FROM " + qualified + " WHERE status = ?"
        );
        this.selectByStatusStmt = session.prepare(
                "SELECT entity_guid, attempt_count, " +
                "       created_at, last_attempted_at, next_attempt_at " +
                "FROM " + qualified + " WHERE status = ?"
        );
        this.countByStatusStmt = session.prepare(
                "SELECT entity_guid, claimed_until, created_at " +
                "FROM " + qualified + " WHERE status = ?"
        );

        LOG.info("TagOutbox initialized for table='{}'", qualified);
    }

    @Override
    public void enqueue(OutboxEntry<EntityGuidRef> entry) {
        Instant now = Instant.now();
        EntityGuidRef ref = entry.getPayload();
        // Same entity_guid re-enqueues collapse on PK — last write wins. Safe because
        // replay re-reads from Cassandra; collapsing duplicate failure events is correct.
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
        long claimTtlMs = Duration.ofSeconds(config.claimTtlSeconds()).toMillis();
        Instant claimedUntil = now.plusMillis(claimTtlMs);
        String podId = OutboxPodId.get();

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
                STATUS_FAILED, id.getPartA(), finalAttemptCount, now, now, null));
        session.execute(batch.build());
        LOG.warn("TagOutbox: entry '{}' moved to FAILED after {} attempts: {}",
                id, finalAttemptCount, cause != null ? cause.getMessage() : "n/a");
    }

    @Override
    public void releaseForRetry(OutboxEntryId id, int newAttemptCount) {
        Instant now = Instant.now();
        long backoff = computeBackoffMs(newAttemptCount);
        Instant nextAttemptAt = now.plusMillis(backoff);
        session.execute(releaseForRetryStmt.bind(
                newAttemptCount, now, nextAttemptAt, STATUS_PENDING, id.getPartA()));
    }

    @Override
    public void releaseClaim(OutboxEntryId id) {
        session.execute(releaseClaimStmt.bind(STATUS_PENDING, id.getPartA()));
    }

    public int getMaxAttempts() { return config.maxAttempts(); }

    /**
     * Read a bounded slice of FAILED entries for the reconciler. Single-partition scan.
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
     * Read PENDING entries that look orphaned: last attempted older than {@code stuckFor}
     * and not in a legitimate backoff window. Anything this old indicates a missed leader
     * election or a relay that died mid-batch.
     */
    public List<OutboxEntry<EntityGuidRef>> scanStuckPending(Duration stuckFor, int limit) {
        Instant cutoff = Instant.now().minus(stuckFor);
        ResultSet rs = session.execute(
                selectByStatusStmt.bind(STATUS_PENDING).setPageSize(limit * 2));
        List<OutboxEntry<EntityGuidRef>> out = new ArrayList<>(limit);
        for (Row row : rs) {
            if (out.size() >= limit) break;
            Instant lastAttemptedAt = row.getInstant("last_attempted_at");
            Instant createdAt       = row.getInstant("created_at");
            Instant nextAttemptAt   = row.getInstant("next_attempt_at");

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
     * Delete a row from the FAILED partition (e.g., after the reconciler confirms
     * the entity has been repaired and the row is no longer useful).
     */
    public void deleteFailed(OutboxEntryId id) {
        session.execute(deleteFailedStmt.bind(STATUS_FAILED, id.getPartA()));
    }

    /**
     * Admin-invoked retry: promote a FAILED row back to PENDING with
     * {@code attempt_count=0} so the relay picks it up on the next poll.
     *
     * <p>Idempotent. If no FAILED row exists for the given id, a fresh PENDING
     * row is still created — the relay then drains it by reading Cassandra
     * truth, which is safe (replay is idempotent). Returns {@code true} if a
     * FAILED row existed and was removed; {@code false} otherwise.</p>
     */
    public boolean retryFailed(OutboxEntryId id) {
        String guid = id.getPartA();
        Instant now = Instant.now();

        // Create fresh PENDING row first so we never have a window where the
        // entity is missing from the outbox entirely.
        session.execute(insertPendingStmt.bind(
                STATUS_PENDING, guid, 0, now, null, now));

        // Attempt to clean up the FAILED row. Success/failure here is purely
        // cosmetic — the PENDING row is the one that matters for replay.
        try {
            session.execute(deleteFailedStmt.bind(STATUS_FAILED, guid));
            return true;
        } catch (Exception e) {
            LOG.warn("TagOutbox: retryFailed on '{}' — PENDING re-created, FAILED delete failed: {}",
                    guid, e.getMessage());
            return false;
        }
    }

    /** Exponential backoff with no jitter — caller's next_attempt_at is the result. */
    long computeBackoffMs(int attemptCount) {
        long base = config.backoffBaseMs() * (long) Math.pow(2, Math.max(0, attemptCount - 1));
        return Math.min(Math.max(config.backoffBaseMs(), base), config.backoffMaxMs());
    }

    /**
     * Snapshot of the current outbox storage, used by the relay leader to publish
     * {@code pending_count}, {@code processing_count}, {@code failed_count}, and
     * {@code oldest_pending_age_seconds} gauges without per-pod read amplification.
     */
    public StorageStats computeStorageStats() {
        int  pending    = 0;
        int  processing = 0;
        int  failed     = 0;
        long oldestAge  = 0L;
        Instant now = Instant.now();

        for (Row row : session.execute(countByStatusStmt.bind(STATUS_PENDING))) {
            pending++;
            Instant claimedUntil = row.getInstant("claimed_until");
            if (claimedUntil != null && claimedUntil.isAfter(now)) processing++;
            Instant createdAt = row.getInstant("created_at");
            if (createdAt != null) {
                long age = Duration.between(createdAt, now).getSeconds();
                if (age > oldestAge) oldestAge = age;
            }
        }
        for (Row ignored : session.execute(countByStatusStmt.bind(STATUS_FAILED))) failed++;
        return new StorageStats(pending, processing, failed, oldestAge);
    }

    public static final class StorageStats {
        public final int  pendingCount;
        public final int  processingCount;
        public final int  failedCount;
        public final long oldestPendingAgeSeconds;
        StorageStats(int pendingCount, int processingCount, int failedCount, long oldestPendingAgeSeconds) {
            this.pendingCount            = pendingCount;
            this.processingCount         = processingCount;
            this.failedCount             = failedCount;
            this.oldestPendingAgeSeconds = oldestPendingAgeSeconds;
        }
    }
}
