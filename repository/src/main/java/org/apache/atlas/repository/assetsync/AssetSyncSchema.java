package org.apache.atlas.repository.assetsync;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent Cassandra schema bootstrap for the asset-sync outbox (MS-1010).
 *
 * <p>Creates a dedicated keyspace ({@code atlas_asset_sync} by default) so the
 * outbox is isolated from existing Atlas tables — no shared schema, no
 * cross-keyspace coupling.</p>
 *
 * <p>Two tables:
 * <ul>
 *     <li>{@code asset_sync_outbox} — failed-entry queue, partitioned by status
 *         so {@code WHERE status='PENDING'} is a direct partition scan.</li>
 *     <li>{@code asset_sync_lease} — single-row lease table for LWT-based
 *         leader election among Atlas pods.</li>
 * </ul>
 *
 * <p>All operations use {@code IF NOT EXISTS} — safe to run on every pod startup
 * and on schema upgrades.</p>
 */
public final class AssetSyncSchema {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncSchema.class);

    private AssetSyncSchema() {}

    /**
     * Create the keyspace and tables if they don't already exist.
     *
     * @param session a CqlSession not bound to a keyspace (the caller's
     *                shared session is fine — this method does not change
     *                the session's keyspace setting)
     */
    public static void bootstrap(CqlSession session) {
        String keyspace = AtlasConfiguration.ASSET_SYNC_OUTBOX_KEYSPACE.getString();
        int    rf       = AtlasConfiguration.ASSET_SYNC_OUTBOX_REPLICATION_FACTOR.getInt();

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "}" +
                " AND durable_writes = true"
        );

        // Slim, event-only schema (Cassandra is the source of truth — re-derive ES doc at replay).
        // PK ((status), entity_guid) — partitioned by status for efficient PENDING scans.
        // Multiple post-commit-verify misses for the same entity GUID natural-dedupe to one row.
        // The relay re-derives the ES doc from Cassandra/JG via RepairIndex.restoreByIds.
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + keyspace + ".asset_sync_outbox (" +
                "  status            text," +
                "  entity_guid       text," +
                "  attempt_count     int," +
                "  created_at        timestamp," +
                "  last_attempted_at timestamp," +
                "  next_attempt_at   timestamp," +
                "  claimed_by        text," +
                "  claimed_until     timestamp," +
                "  PRIMARY KEY ((status), entity_guid)" +
                ") WITH gc_grace_seconds = 3600" +
                "  AND default_time_to_live = " + AtlasConfiguration.ASSET_SYNC_OUTBOX_TTL_SECONDS.getInt()
        );

        // Single-row lease table; one row per logical job (today only "asset-sync-relay").
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + keyspace + ".asset_sync_lease (" +
                "  job_name      text PRIMARY KEY," +
                "  owner         text," +
                "  acquired_at   timestamp," +
                "  heartbeat_at  timestamp" +
                ")"
        );

        LOG.info("AssetSyncSchema: bootstrapped keyspace='{}' (rf={}) with tables asset_sync_outbox, asset_sync_lease",
                keyspace, rf);
    }
}
