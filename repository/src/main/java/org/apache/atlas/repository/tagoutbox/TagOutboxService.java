package org.apache.atlas.repository.tagoutbox;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.cassandra.CassandraSessionProvider;
import org.apache.atlas.repository.outbox.shared.ConfigurableLeaseManager;
import org.apache.atlas.repository.outbox.shared.OutboxMetrics;
import org.apache.atlas.repository.outbox.shared.OutboxPodId;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.ESWriteFailureRegistry;
import org.apache.atlas.repository.store.graph.v2.EntityCreateOrUpdateMutationPostProcessor;
import org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Spring-managed bootstrap for the tag-outbox subsystem.
 *
 * <p>On {@code @PostConstruct} init():</p>
 * <ol>
 *     <li>Check {@code atlas.tag.outbox.enabled}; short-circuit if disabled.</li>
 *     <li>Build {@link TagOutboxConfig} from {@code AtlasConfiguration}.</li>
 *     <li>Acquire a shared Cassandra session via {@link CassandraSessionProvider}.</li>
 *     <li>Bootstrap schema idempotently (keyspace + two tables).</li>
 *     <li>Build the shared-package components: {@link OutboxMetrics},
 *         {@link ConfigurableLeaseManager}.</li>
 *     <li>Build the tag-package components: {@link TagOutbox}, {@link TagOutboxConsumer},
 *         {@link TagOutboxProcessor}, {@link TagOutboxSink}.</li>
 *     <li>Install <b>Surface 1 sink</b>: {@code TagOutboxSink.install(sink)} so
 *         {@code EntityGraphMapper.flushTagDenormToES} can reach the sink via
 *         static accessor.</li>
 *     <li>Install <b>Surface 2 sink</b>: {@code ESWriteFailureRegistry.setSink(new TagOutboxFailureSink(...))}
 *         so direct-tag-attachment failures in
 *         {@code EntityMutationService.executeESPostProcessing} flow into the
 *         same outbox.</li>
 *     <li>Start processor, then (if enabled) start reconciler.</li>
 * </ol>
 *
 * <p>On {@code @PreDestroy} shutdown():</p>
 * <ol>
 *     <li>Uninstall both sinks (replay-time writes become no-ops).</li>
 *     <li>Stop reconciler, then processor (so the relay releases its lease promptly).</li>
 * </ol>
 *
 * <p>Disabled entirely via {@code atlas.tag.outbox.enabled=false} — returns
 * early from {@code init()}, leaving both sinks in their default no-op state.</p>
 */
@Service
public final class TagOutboxService {
    private static final Logger LOG = LoggerFactory.getLogger(TagOutboxService.class);

    private final AtlasGraph                                graph;
    private final AtlasEntityStore                          entityStore;
    private final EntityCreateOrUpdateMutationPostProcessor postProcessor;

    private TagOutboxProcessor  processor;
    private TagOutboxReconciler reconciler;
    private TagOutboxSink       sink;
    private TagOutbox           outbox;
    private TagOutboxConfig     activeConfig;
    private boolean             started = false;

    @Autowired
    public TagOutboxService(AtlasGraph graph,
                            AtlasEntityStore entityStore,
                            EntityCreateOrUpdateMutationPostProcessor postProcessor) {
        this.graph         = graph;
        this.entityStore   = entityStore;
        this.postProcessor = postProcessor;
    }

    @PostConstruct
    public void init() {
        TagOutboxConfig config;
        try {
            config = TagOutboxConfig.fromAtlasConfiguration();
        } catch (Exception e) {
            LOG.error("TagOutboxService: failed to load configuration — subsystem will not start", e);
            return;
        }

        if (!config.enabled()) {
            LOG.info("TagOutboxService: disabled via atlas.tag.outbox.enabled=false");
            return;
        }

        try {
            CqlSession session = acquireSession();
            TagOutboxSchema.bootstrap(session, config);

            OutboxMetrics             metrics       = new OutboxMetrics(MetricUtils.getMeterRegistry(), config.metricsPrefix());
            ConfigurableLeaseManager  leaseManager  = new ConfigurableLeaseManager(
                    session, config.keyspace(), config.leaseTableName(), OutboxPodId.get());

            outbox        = new TagOutbox(session, config);
            activeConfig  = config;
            TagOutboxConsumer         consumer      = new TagOutboxConsumer(entityStore, postProcessor, config);

            processor = new TagOutboxProcessor(outbox, consumer, leaseManager, metrics, config);

            // Surface 1 sink — installed as the process-wide singleton so
            // EntityGraphMapper.flushTagDenormToES reaches it statically without
            // a Spring DI cycle.
            sink = new TagOutboxSink(outbox, metrics);
            TagOutboxSink.install(sink);

            // Surface 2 sink — consumes ESWriteFailureRegistry records emitted by
            // EntityMutationService.executeESPostProcessing when direct-attachment
            // tag paths fail the deferred-op flush. Until now, that registry had no
            // consumer and failures were dropped silently.
            ESWriteFailureRegistry.setSink(new TagOutboxFailureSink(graph, sink));

            processor.start();

            if (config.reconcilerEnabled()) {
                try {
                    reconciler = new TagOutboxReconciler(outbox, consumer, leaseManager, metrics, config);
                    reconciler.start();
                } catch (Throwable t) {
                    LOG.error("TagOutboxService: reconciler init failed — relay + both sinks still active. " +
                            "FAILED rows will accumulate until another pod's reconciler runs or this one is restarted.", t);
                }
            } else {
                LOG.info("TagOutboxService: reconciler disabled via atlas.tag.outbox.reconciler.enabled=false");
            }

            started = true;
            LOG.info("TagOutboxService: started — Surface 1 + Surface 2 sinks installed; relay{} live",
                    config.reconcilerEnabled() ? " + reconciler" : "");
        } catch (Exception e) {
            LOG.error("TagOutboxService: startup failed — tag outbox is NOT active for this pod", e);
        }
    }

    /** @return whether the subsystem bootstrapped successfully on this pod. */
    public boolean isStarted() { return started; }

    /**
     * @return the live {@link TagOutbox} instance, or {@code null} if the
     *         subsystem is disabled or failed to start. The admin controller
     *         uses this for status/list/retry operations.
     */
    public TagOutbox getOutbox() { return outbox; }

    /** @return the config the running subsystem was bootstrapped with, or {@code null} if not started. */
    public TagOutboxConfig getActiveConfig() { return activeConfig; }

    @PreDestroy
    public void shutdown() {
        if (!started) return;
        try {
            TagOutboxSink.install(null);
            ESWriteFailureRegistry.setSink(null);
            if (reconciler != null) reconciler.stop();
            if (processor  != null) processor.stop();
            LOG.info("TagOutboxService: shutdown complete");
        } catch (Exception e) {
            LOG.warn("TagOutboxService: error during shutdown", e);
        }
    }

    /**
     * Acquire the shared Cassandra session using the same hostname/port that
     * TagDAO and the asset-sync outbox use. The shared session reuses its
     * connection pool across all subsystems so we don't double-provision.
     */
    private static CqlSession acquireSession() throws Exception {
        String hostname = ApplicationProperties.get().getString(
                CassandraTagConfig.CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        int port = ApplicationProperties.get().getInt(
                CassandraTagConfig.CASSANDRA_PORT_PROPERTY, 9042);
        return CassandraSessionProvider.getSharedSession(hostname, port, "datacenter1");
    }
}
