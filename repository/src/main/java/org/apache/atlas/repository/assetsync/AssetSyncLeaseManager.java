package org.apache.atlas.repository.assetsync;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * LWT-based leader election for the asset-sync relay (MS-1010).
 *
 * <p>One row in {@code asset_sync_lease} per logical job. Acquisition uses
 * {@code INSERT ... IF NOT EXISTS USING TTL}; release uses
 * {@code DELETE ... IF owner = ?}. Both go through Cassandra's Paxos path,
 * giving us safe mutual exclusion across Atlas pods with no external
 * coordinator (no ZooKeeper, no etcd).</p>
 *
 * <p>Heartbeating is the responsibility of the holder — call
 * {@link #heartbeat(String)} on a timer interval shorter than the TTL,
 * otherwise the lease will expire and another pod will take over.</p>
 *
 * <p>Pod identity is taken from the {@code HOSTNAME} environment variable
 * (set by Kubernetes to the pod name) with a PID fallback for local dev.</p>
 */
public final class AssetSyncLeaseManager implements LeaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncLeaseManager.class);

    private final CqlSession        session;
    private final String            keyspace;
    private final String            podId;
    private final PreparedStatement acquireStmt;
    private final PreparedStatement heartbeatStmt;
    private final PreparedStatement releaseStmt;
    private final PreparedStatement selectStmt;

    public AssetSyncLeaseManager(CqlSession session) {
        this.session  = session;
        this.keyspace = AtlasConfiguration.ASSET_SYNC_OUTBOX_KEYSPACE.getString();
        this.podId    = resolvePodId();

        this.acquireStmt = session.prepare(
                "INSERT INTO " + keyspace + ".asset_sync_lease (job_name, owner, acquired_at, heartbeat_at) " +
                "VALUES (?, ?, ?, ?) IF NOT EXISTS USING TTL ?"
        );
        // Conditional heartbeat: only the holder may extend.
        // Both owner and heartbeat_at are SET so they get a fresh TTL together —
        // refreshing only heartbeat_at would let the owner column expire on its
        // original TTL, the next heartbeat would see owner=null and fail, and
        // we'd thrash through unnecessary reacquire cycles.
        this.heartbeatStmt = session.prepare(
                "UPDATE " + keyspace + ".asset_sync_lease USING TTL ? " +
                "SET owner = ?, heartbeat_at = ? WHERE job_name = ? IF owner = ?"
        );
        this.releaseStmt = session.prepare(
                "DELETE FROM " + keyspace + ".asset_sync_lease WHERE job_name = ? IF owner = ?"
        );
        this.selectStmt = session.prepare(
                "SELECT owner FROM " + keyspace + ".asset_sync_lease WHERE job_name = ?"
        );

        LOG.info("AssetSyncLeaseManager initialized, podId='{}', keyspace='{}'", podId, keyspace);
    }

    /**
     * Try to acquire a lease. Returns true if this pod now holds it (either
     * fresh acquisition or already-held re-entrant call), false if another
     * pod holds it.
     */
    @Override
    public boolean tryAcquire(String jobName, int ttlSeconds) {
        try {
            Instant now = Instant.now();
            ResultSet rs = session.execute(acquireStmt.bind(jobName, podId, now, now, ttlSeconds));
            boolean applied = rs.wasApplied();
            if (applied) {
                LOG.debug("AssetSyncLease '{}' acquired by pod='{}' (ttl={}s)", jobName, podId, ttlSeconds);
                return true;
            }
            // Re-entrant: we already hold it
            Row row = rs.one();
            String currentOwner = row != null ? row.getString("owner") : null;
            if (podId.equals(currentOwner)) {
                LOG.debug("AssetSyncLease '{}' already held by this pod='{}'", jobName, podId);
                return true;
            }
            LOG.debug("AssetSyncLease '{}' held by another pod='{}'; this pod backs off", jobName, currentOwner);
            return false;
        } catch (Exception e) {
            LOG.warn("AssetSyncLease '{}' acquire failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    /**
     * Extend a lease this pod currently holds. Idempotent: a heartbeat by a
     * pod that has lost the lease (e.g., due to pause + TTL expiry) returns
     * false instead of stealing it back.
     */
    @Override
    public boolean heartbeat(String jobName, int ttlSeconds) {
        try {
            // Bind args mirror the SET clause: ttl, owner (this pod), heartbeat_at, where, IF owner
            ResultSet rs = session.execute(heartbeatStmt.bind(ttlSeconds, podId, Instant.now(), jobName, podId));
            return rs.wasApplied();
        } catch (Exception e) {
            LOG.warn("AssetSyncLease '{}' heartbeat failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    /** Release a lease this pod currently holds. No-op if held by someone else. */
    @Override
    public void release(String jobName) {
        try {
            ResultSet rs = session.execute(releaseStmt.bind(jobName, podId));
            if (rs.wasApplied()) {
                LOG.info("AssetSyncLease '{}' released by pod='{}'", jobName, podId);
            }
        } catch (Exception e) {
            LOG.warn("AssetSyncLease '{}' release failed: {}", jobName, e.getMessage());
        }
    }

    /** True when this pod is the current owner. */
    @Override
    public boolean isHeldByMe(String jobName) {
        try {
            ResultSet rs = session.execute(selectStmt.bind(jobName));
            Row row = rs.one();
            return row != null && podId.equals(row.getString("owner"));
        } catch (Exception e) {
            LOG.warn("AssetSyncLease '{}' isHeldByMe check failed: {}", jobName, e.getMessage());
            return false;
        }
    }

    @Override
    public String getPodId() { return podId; }

    private static String resolvePodId() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) return hostname;
        return "local-" + ProcessHandle.current().pid();
    }
}
