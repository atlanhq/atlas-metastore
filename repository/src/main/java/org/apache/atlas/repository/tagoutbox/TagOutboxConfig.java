package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.AtlasConfiguration;

/**
 * Immutable value object carrying every tag-outbox tunable and schema identifier.
 *
 * <p>Every component in the tag-outbox subsystem ({@code TagOutbox},
 * {@code TagOutboxProcessor}, {@code TagOutboxReconciler}, {@code TagOutboxSchema},
 * the lease manager, the consumer) receives this record through its constructor.
 * No component reads {@link AtlasConfiguration} directly — all values flow
 * through this record, centralising configuration access in one place.</p>
 *
 * <p>Three benefits follow from this shape:</p>
 * <ul>
 *     <li><b>Testability.</b> Unit tests pass a hand-built {@code TagOutboxConfig}
 *         with controlled values. No mocking of {@code AtlasConfiguration} is required.</li>
 *     <li><b>Schema flexibility.</b> {@code keyspace}, {@code outboxTableName},
 *         and {@code leaseTableName} are parameters, not literals — operators can
 *         rename the Cassandra objects via {@code atlas.tag.outbox.*} properties
 *         without any Java code change.</li>
 *     <li><b>Observability clarity.</b> Thread names and the metrics prefix
 *         are carried on the config, so test harnesses and custom deployments
 *         can disambiguate instances cleanly.</li>
 * </ul>
 *
 * <p>Production code obtains an instance via {@link #fromAtlasConfiguration()}.
 * Tests either build a record directly or use that factory and override specific
 * values when needed.</p>
 */
public record TagOutboxConfig(
        // Subsystem master switches
        boolean enabled,
        boolean reconcilerEnabled,

        // Cassandra identifiers — intentionally parameterised for schema flexibility
        String  keyspace,
        int     replicationFactor,
        String  outboxTableName,
        String  leaseTableName,
        int     ttlSeconds,

        // Retry semantics
        int     maxAttempts,

        // Lease (shared between relay and reconciler within this subsystem)
        int     leaseTtlSeconds,
        int     leaseHeartbeatSeconds,

        // Relay — adaptive idle/drain polling
        int     idlePollSeconds,
        int     drainPollSeconds,
        int     idleBatchSize,
        int     drainBatchSize,
        int     claimTtlSeconds,
        long    backoffBaseMs,
        long    backoffMaxMs,

        // Reconciler (hourly sweeper)
        int     reconcilerIntervalSeconds,
        int     reconcilerJitterSeconds,
        int     reconcilerBatchSize,
        int     reconcilerStuckPendingThresholdSeconds,

        // Consumer (matches TagDenormDLQReplayService.REPAIR_BATCH_SIZE semantics)
        int     consumerRepairBatchSize,

        // Cosmetic identifiers — not AtlasConfiguration-backed because renaming them
        // would break existing Prometheus queries and log filters. Kept on the record
        // so tests can still override when running parallel fixtures.
        String  leaseName,
        String  relayThreadName,
        String  reconcilerThreadName,
        String  metricsPrefix) {

    /** Lease key used for leader election. Separate from asset-sync-relay. */
    public static final String DEFAULT_LEASE_NAME             = "tag-outbox-relay";
    /** Thread name for the relay scheduler; surfaces in thread dumps and structured logs. */
    public static final String DEFAULT_RELAY_THREAD_NAME      = "tag-outbox-relay";
    /** Thread name for the reconciler scheduler. */
    public static final String DEFAULT_RECONCILER_THREAD_NAME = "tag-outbox-reconciler";
    /** Prometheus metric-name prefix. All tag-outbox metrics live under this namespace. */
    public static final String DEFAULT_METRICS_PREFIX         = "atlas_tag_outbox_";

    /**
     * Build a production-ready config by reading every {@code atlas.tag.outbox.*}
     * property through {@link AtlasConfiguration}. Cosmetic identifiers
     * ({@code leaseName}, thread names, metrics prefix) use the defaults above.
     */
    public static TagOutboxConfig fromAtlasConfiguration() {
        return new TagOutboxConfig(
                AtlasConfiguration.TAG_OUTBOX_ENABLED.getBoolean(),
                AtlasConfiguration.TAG_OUTBOX_RECONCILER_ENABLED.getBoolean(),
                AtlasConfiguration.TAG_OUTBOX_KEYSPACE.getString(),
                AtlasConfiguration.TAG_OUTBOX_REPLICATION_FACTOR.getInt(),
                AtlasConfiguration.TAG_OUTBOX_TABLE_NAME.getString(),
                AtlasConfiguration.TAG_OUTBOX_LEASE_TABLE_NAME.getString(),
                AtlasConfiguration.TAG_OUTBOX_TTL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_MAX_ATTEMPTS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_LEASE_TTL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_LEASE_HEARTBEAT_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_IDLE_POLL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_DRAIN_POLL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_IDLE_BATCH_SIZE.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_DRAIN_BATCH_SIZE.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_CLAIM_TTL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_BACKOFF_BASE_MS.getLong(),
                AtlasConfiguration.TAG_OUTBOX_RELAY_BACKOFF_MAX_MS.getLong(),
                AtlasConfiguration.TAG_OUTBOX_RECONCILER_INTERVAL_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RECONCILER_JITTER_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RECONCILER_BATCH_SIZE.getInt(),
                AtlasConfiguration.TAG_OUTBOX_RECONCILER_STUCK_PENDING_THRESHOLD_SECONDS.getInt(),
                AtlasConfiguration.TAG_OUTBOX_CONSUMER_REPAIR_BATCH_SIZE.getInt(),
                DEFAULT_LEASE_NAME,
                DEFAULT_RELAY_THREAD_NAME,
                DEFAULT_RECONCILER_THREAD_NAME,
                DEFAULT_METRICS_PREFIX);
    }
}
