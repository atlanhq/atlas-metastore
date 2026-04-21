package org.apache.atlas.repository.assetsync;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.repository.graphdb.cassandra.CassandraSessionProvider;
import org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig;
import org.apache.atlas.util.RepairIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Spring-managed bootstrap of the asset-sync outbox subsystem (MS-1010, Option B).
 *
 * <p>Wires the post-commit-verify path:</p>
 * <ol>
 *     <li>Acquires the shared Cassandra session.</li>
 *     <li>Bootstraps the {@code atlas_asset_sync} keyspace + tables (idempotent).</li>
 *     <li>Builds the {@link PostCommitEsVerifier} (exposed via {@link #getVerifier()}
 *         so {@code EntityMutationService} can hand it the committed GUIDs after
 *         each commit).</li>
 *     <li>Starts the {@link AssetSyncOutboxProcessor} background relay (single-leader
 *         via LWT lease) which reads PENDING outbox rows and re-fires ES indexing
 *         via {@link RepairIndex#restoreByIds}.</li>
 * </ol>
 *
 * <p>Disabled entirely via {@code atlas.asset.sync.outbox.enabled=false}.</p>
 */
@Service
public final class AssetSyncOutboxService {
    private static final Logger LOG = LoggerFactory.getLogger(AssetSyncOutboxService.class);

    private final RepairIndex repairIndex;

    private AssetSyncOutboxProcessor processor;
    private PostCommitEsVerifier verifier;
    private boolean started = false;

    @Autowired
    public AssetSyncOutboxService(RepairIndex repairIndex) {
        this.repairIndex = repairIndex;
    }

    @PostConstruct
    public void init() {
        if (!AtlasConfiguration.ASSET_SYNC_OUTBOX_ENABLED.getBoolean()) {
            LOG.info("AssetSyncOutboxService: disabled via atlas.asset.sync.outbox.enabled=false");
            return;
        }
        try {
            CqlSession session = acquireSession();
            AssetSyncSchema.bootstrap(session);

            Outbox<EntityGuidRef>          outbox       = new AssetSyncOutbox(session);
            OutboxConsumer<EntityGuidRef>  consumer     = new AssetSyncReindexConsumer(repairIndex);
            AssetSyncLeaseManager          leaseManager = new AssetSyncLeaseManager(session);

            processor = new AssetSyncOutboxProcessor(outbox, consumer, leaseManager);

            AssetSyncSink sink = new AssetSyncSink(outbox);
            verifier = new PostCommitEsVerifier(sink);
            // Install as the process-wide singleton so EntityMutationService can call
            // PostCommitEsVerifier.postCommit(...) without a Spring DI cycle.
            PostCommitEsVerifier.install(verifier);

            processor.start();
            started = true;
            LOG.info("AssetSyncOutboxService: started — post-commit verify + outbox + relay are live");
        } catch (Exception e) {
            LOG.error("AssetSyncOutboxService: startup failed — outbox is NOT active for this pod", e);
        }
    }

    /**
     * Returns the post-commit verifier, or {@code null} if the service is
     * disabled or failed to start. {@code EntityMutationService} calls this
     * via the @Autowired bean reference and routes committed GUIDs in.
     */
    public PostCommitEsVerifier getVerifier() {
        return verifier;
    }

    @PreDestroy
    public void shutdown() {
        if (!started) return;
        try {
            PostCommitEsVerifier.install(null);
            if (verifier != null) verifier.shutdown();
            if (processor != null) processor.stop();
            LOG.info("AssetSyncOutboxService: shutdown complete");
        } catch (Exception e) {
            LOG.warn("AssetSyncOutboxService: error during shutdown", e);
        }
    }

    private static CqlSession acquireSession() throws Exception {
        String hostname = ApplicationProperties.get().getString(
                CassandraTagConfig.CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        int port = ApplicationProperties.get().getInt(
                CassandraTagConfig.CASSANDRA_PORT_PROPERTY, 9042);
        return CassandraSessionProvider.getSharedSession(hostname, port, "datacenter1");
    }
}
