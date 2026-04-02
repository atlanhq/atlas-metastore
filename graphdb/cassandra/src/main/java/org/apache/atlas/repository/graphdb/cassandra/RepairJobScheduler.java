package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schedules background repair jobs for CassandraGraph consistency.
 *
 * Currently schedules:
 * 1. ES Reconciliation (every 6 hours) — sample-based audit of Cassandra↔ES consistency
 *
 * Orphan vertex/edge cleanup is NOT scheduled here. The atomic vertex+index batch
 * in CassandraGraph.commit() eliminates the W2 failure window that caused orphans.
 * OrphanVertexCleanup and OrphanEdgeCleanup are retained as on-demand tools for
 * post-migration or post-incident repair, not as continuous background loops.
 *
 * All jobs are lease-guarded via {@link JobLeaseManager} — only one pod executes each job
 * at a time. Crashed pods auto-release leases via TTL expiry.
 *
 * Lifecycle: call {@link #start()} once at application startup, {@link #stop()} at shutdown.
 */
public class RepairJobScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RepairJobScheduler.class);

    private static final long ES_RECONCILIATION_INTERVAL_HOURS = 6;
    private static final long ES_RECONCILIATION_INITIAL_DELAY_MINUTES = 10;

    private final CqlSession session;
    private final CassandraGraph graph;
    private final JobLeaseManager leaseManager;
    private final ScheduledExecutorService scheduler;
    private final ESReconciliationJob esReconciliationJob;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RepairJobScheduler(CqlSession session, CassandraGraph graph,
                               JobLeaseManager leaseManager, ESOutboxRepository outboxRepository) {
        this.session = session;
        this.graph = graph;
        this.leaseManager = leaseManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "repair-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.esReconciliationJob = new ESReconciliationJob(session, graph, leaseManager, outboxRepository);
    }

    /**
     * Runs orphan edge cleanup on-demand (single cycle).
     * Scans edges_by_id for edges with missing endpoint vertices and deletes them.
     */
    public void runOrphanEdgeCleanup() {
        LOG.info("Running on-demand orphan edge cleanup");
        OrphanEdgeCleanup cleanup = new OrphanEdgeCleanup(
                session, graph.getVertexRepository(), graph.getEdgeRepository(), graph, leaseManager);
        scheduler.submit(wrapWithErrorHandling("OrphanEdgeCleanup", cleanup));
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            // Orphan edge cleanup: run once at startup (2 min delay), then every 24h
            OrphanEdgeCleanup orphanEdgeCleanup = new OrphanEdgeCleanup(
                    session, graph.getVertexRepository(), graph.getEdgeRepository(), graph, leaseManager);
            scheduler.scheduleWithFixedDelay(
                    wrapWithErrorHandling("OrphanEdgeCleanup", orphanEdgeCleanup),
                    2, 24 * 60, TimeUnit.MINUTES);

            // ES reconciliation: every 6 hours, starting after 10 minutes
            scheduler.scheduleWithFixedDelay(
                    wrapWithErrorHandling("ESReconciliationJob", esReconciliationJob),
                    ES_RECONCILIATION_INITIAL_DELAY_MINUTES, ES_RECONCILIATION_INTERVAL_HOURS * 60,
                    TimeUnit.MINUTES);

            LOG.info("RepairJobScheduler started — ES reconciliation every {}h (lease-guarded)",
                    ES_RECONCILIATION_INTERVAL_HOURS);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("RepairJobScheduler stopped");
        }
    }

    /**
     * Wraps a Runnable to catch and log any unexpected exceptions, preventing
     * ScheduledExecutorService from silently swallowing errors and stopping
     * future executions.
     */
    private Runnable wrapWithErrorHandling(String jobName, Runnable job) {
        return () -> {
            try {
                job.run();
            } catch (Exception e) {
                LOG.error("RepairJobScheduler: {} failed with unexpected error", jobName, e);
            }
        };
    }
}
