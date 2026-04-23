package org.apache.atlas.repository.tagoutbox;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent Cassandra schema bootstrap for the tag-outbox subsystem.
 *
 * <p>Creates a dedicated keyspace ({@code atlas_tag_outbox} by default) containing
 * two tables:</p>
 * <ul>
 *     <li>{@code tag_outbox} — the failed-entry queue, partitioned by {@code status}
 *         so {@code WHERE status = 'PENDING'} is a direct partition scan.
 *         Column-identical to {@code asset_sync_outbox}.</li>
 *     <li>{@code tag_outbox_lease} — single-row lease table for LWT-based leader
 *         election among Atlas pods (mirrors {@code asset_sync_lease}).</li>
 * </ul>
 *
 * <p>All operations use {@code IF NOT EXISTS}, so this method is safe to run on
 * every pod startup, and safe to run multiple times concurrently (the schema
 * agreement protocol serialises them).</p>
 *
 * <p>Keyspace name, both table names, replication factor, and TTL all come from
 * {@link TagOutboxConfig} — this class has no hard-coded identifiers.</p>
 */
public final class TagOutboxSchema {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxSchema.class);

    private TagOutboxSchema() {}

    /**
     * Bootstrap keyspace and tables, if missing. Safe to call on every pod start.
     *
     * @param session a {@link CqlSession} not bound to any keyspace. This method
     *                does not change the session's keyspace setting; all statements
     *                use fully-qualified identifiers.
     * @param config  the tag-outbox configuration record — provides keyspace name,
     *                both table names, replication factor, and TTL.
     */
    public static void bootstrap(CqlSession session, TagOutboxConfig config) {
        final String keyspace   = config.keyspace();
        final int    rf         = config.replicationFactor();
        final String outboxTbl  = keyspace + "." + config.outboxTableName();
        final String leaseTbl   = keyspace + "." + config.leaseTableName();
        final int    ttlSeconds = config.ttlSeconds();

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "}" +
                " AND durable_writes = true"
        );

        // Column-identical to asset_sync_outbox. Partitioning by status keeps
        // PENDING scans single-partition. gc_grace_seconds tuned down to 1h
        // since this is a transient queue table — long tombstone windows gain
        // nothing and hurt compaction.
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + outboxTbl + " (" +
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
                "  AND default_time_to_live = " + ttlSeconds
        );

        // Single-row lease table — one row per logical job (today only
        // "tag-outbox-relay"). LWT INSERT IF NOT EXISTS USING TTL gives us
        // leader election with automatic takeover on crash.
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + leaseTbl + " (" +
                "  job_name      text PRIMARY KEY," +
                "  owner         text," +
                "  acquired_at   timestamp," +
                "  heartbeat_at  timestamp" +
                ")"
        );

        LOG.info("TagOutboxSchema: bootstrapped keyspace='{}' (rf={}), tables {} and {}",
                keyspace, rf, outboxTbl, leaseTbl);
    }
}
