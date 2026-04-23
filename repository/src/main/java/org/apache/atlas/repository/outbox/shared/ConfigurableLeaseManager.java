package org.apache.atlas.repository.outbox.shared;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.repository.assetsync.LeaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;

/**
 * LWT-based distributed lease manager, parameterised over keyspace and lease
 * table. A drop-in replacement for {@code AssetSyncLeaseManager} that does not
 * hard-code {@code asset_sync_lease} — the caller supplies the table identity.
 *
 * <p>Implements the same {@link LeaseManager} contract PR #6568 introduced,
 * allowing this class to participate anywhere a {@code LeaseManager} is
 * expected. Intended for tag-outbox use today; asset-sync may migrate to this
 * class in a future refactor (at which point {@code AssetSyncLeaseManager}
 * becomes a thin backward-compat wrapper or is deleted).</p>
 *
 * <p>Semantics (unchanged from {@code AssetSyncLeaseManager}):
 * <ul>
 *     <li>Acquire: {@code INSERT ... IF NOT EXISTS USING TTL} — Cassandra Paxos
 *         gives safe mutual exclusion across Atlas pods with no external
 *         coordinator. Re-entrant: a second acquire by the current owner
 *         returns {@code true}.</li>
 *     <li>Heartbeat: {@code UPDATE ... IF owner = ?} — only the holder may
 *         extend. {@code owner} and {@code heartbeat_at} are both re-SET so
 *         they share a fresh TTL. Refreshing only {@code heartbeat_at} would
 *         let the {@code owner} cell expire on its original TTL and cause
 *         reacquire thrashing.</li>
 *     <li>Release: {@code DELETE ... IF owner = ?} — safe no-op if someone
 *         else holds the lease.</li>
 *     <li>Identity check: {@code SELECT owner ... WHERE job_name = ?} — returns
 *         whether this pod currently holds the named lease.</li>
 * </ul>
 *
 * <p>Pod identity is resolved once at construction time from the supplied
 * {@code podId} (typically {@link OutboxPodId#get()}); it is not re-read on
 * every call, so per-call lease operations are one Cassandra round-trip each.</p>
 */
public final class ConfigurableLeaseManager implements LeaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableLeaseManager.class);

    private final CqlSession        session;
    private final String            keyspace;
    private final String            leaseTableName;
    private final String            podId;
    private final PreparedStatement acquireStmt;
    private final PreparedStatement heartbeatStmt;
    private final PreparedStatement releaseStmt;
    private final PreparedStatement selectStmt;

    /**
     * @param session        live Cassandra session; table must already exist
     *                       (see {@code TagOutboxSchema.bootstrap}).
     * @param keyspace       Cassandra keyspace containing the lease table.
     * @param leaseTableName name of the lease table within {@code keyspace}.
     *                       Must have columns (job_name text PRIMARY KEY,
     *                       owner text, acquired_at timestamp, heartbeat_at
     *                       timestamp).
     * @param podId          this pod's identity string — the value stored as
     *                       the lease {@code owner}. Must be stable for the
     *                       pod's lifetime.
     */
    public ConfigurableLeaseManager(CqlSession session,
                                    String keyspace,
                                    String leaseTableName,
                                    String podId) {
        this.session        = Objects.requireNonNull(session, "session");
        this.keyspace       = Objects.requireNonNull(keyspace, "keyspace");
        this.leaseTableName = Objects.requireNonNull(leaseTableName, "leaseTableName");
        this.podId          = Objects.requireNonNull(podId, "podId");

        final String qualified = keyspace + "." + leaseTableName;

        this.acquireStmt = session.prepare(
                "INSERT INTO " + qualified + " (job_name, owner, acquired_at, heartbeat_at) " +
                "VALUES (?, ?, ?, ?) IF NOT EXISTS USING TTL ?"
        );
        this.heartbeatStmt = session.prepare(
                "UPDATE " + qualified + " USING TTL ? " +
                "SET owner = ?, heartbeat_at = ? WHERE job_name = ? IF owner = ?"
        );
        this.releaseStmt = session.prepare(
                "DELETE FROM " + qualified + " WHERE job_name = ? IF owner = ?"
        );
        this.selectStmt = session.prepare(
                "SELECT owner FROM " + qualified + " WHERE job_name = ?"
        );

        LOG.info("ConfigurableLeaseManager initialized, podId='{}', table='{}'", podId, qualified);
    }

    @Override
    public boolean tryAcquire(String jobName, int ttlSeconds) {
        try {
            Instant now = Instant.now();
            ResultSet rs = session.execute(acquireStmt.bind(jobName, podId, now, now, ttlSeconds));
            if (rs.wasApplied()) {
                LOG.debug("Lease '{}' acquired by pod='{}' (ttl={}s)", jobName, podId, ttlSeconds);
                return true;
            }
            // Re-entrant: the current owner is already this pod.
            Row row = rs.one();
            String currentOwner = row != null ? row.getString("owner") : null;
            if (podId.equals(currentOwner)) {
                LOG.debug("Lease '{}' already held by this pod='{}'", jobName, podId);
                return true;
            }
            LOG.debug("Lease '{}' held by another pod='{}'; this pod backs off", jobName, currentOwner);
            return false;
        } catch (Exception e) {
            LOG.warn("Lease '{}' acquire failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean heartbeat(String jobName, int ttlSeconds) {
        try {
            ResultSet rs = session.execute(
                    heartbeatStmt.bind(ttlSeconds, podId, Instant.now(), jobName, podId));
            return rs.wasApplied();
        } catch (Exception e) {
            LOG.warn("Lease '{}' heartbeat failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    @Override
    public void release(String jobName) {
        try {
            ResultSet rs = session.execute(releaseStmt.bind(jobName, podId));
            if (rs.wasApplied()) {
                LOG.info("Lease '{}' released by pod='{}'", jobName, podId);
            }
        } catch (Exception e) {
            LOG.warn("Lease '{}' release failed: {}", jobName, e.getMessage());
        }
    }

    @Override
    public boolean isHeldByMe(String jobName) {
        try {
            ResultSet rs = session.execute(selectStmt.bind(jobName));
            Row row = rs.one();
            return row != null && podId.equals(row.getString("owner"));
        } catch (Exception e) {
            LOG.warn("Lease '{}' isHeldByMe check failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    @Override
    public String getPodId() { return podId; }
}
