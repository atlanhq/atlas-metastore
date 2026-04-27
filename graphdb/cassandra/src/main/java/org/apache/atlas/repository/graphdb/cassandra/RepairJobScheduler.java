package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.commons.configuration.Configuration;
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
 * 1. ES Reconciliation — sample-based audit of Cassandra↔ES consistency
 *
 * Configurable via atlas-application.properties:
 *   atlas.cassandra.graph.reconciliation.interval.hours=6  (default)
 *   atlas.cassandra.graph.reconciliation.initial.delay.minutes=10  (default)
 *
 * All jobs are lease-guarded via {@link JobLeaseManager} — only one pod executes each job
 * at a time. Crashed pods auto-release leases via TTL expiry.
 *
 * Lifecycle: call {@link #start()} once at application startup, {@link #stop()} at shutdown.
 */
public class RepairJobScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RepairJobScheduler.class);

    private static final String CONF_RECONCILIATION_INTERVAL_HOURS =
            "atlas.cassandra.graph.reconciliation.interval.hours";
    private static final String CONF_RECONCILIATION_INITIAL_DELAY_MINUTES =
            "atlas.cassandra.graph.reconciliation.initial.delay.minutes";

    private static final long DEFAULT_INTERVAL_HOURS = 6;
    private static final long DEFAULT_INITIAL_DELAY_MINUTES = 10;

    private final ScheduledExecutorService scheduler;
    private final ESReconciliationJob esReconciliationJob;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final long intervalHours;
    private final long initialDelayMinutes;

    public RepairJobScheduler(CqlSession session, CassandraGraph graph,
                               JobLeaseManager leaseManager, ESOutboxRepository outboxRepository,
                               Configuration configuration) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "repair-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.esReconciliationJob = new ESReconciliationJob(session, graph, leaseManager, outboxRepository);
        this.intervalHours = configuration != null
                ? configuration.getLong(CONF_RECONCILIATION_INTERVAL_HOURS, DEFAULT_INTERVAL_HOURS)
                : DEFAULT_INTERVAL_HOURS;
        this.initialDelayMinutes = configuration != null
                ? configuration.getLong(CONF_RECONCILIATION_INITIAL_DELAY_MINUTES, DEFAULT_INITIAL_DELAY_MINUTES)
                : DEFAULT_INITIAL_DELAY_MINUTES;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(
                    wrapWithErrorHandling("ESReconciliationJob", esReconciliationJob),
                    initialDelayMinutes, intervalHours * 60,
                    TimeUnit.MINUTES);

            LOG.info("RepairJobScheduler started — ES reconciliation every {}h, initial delay {}min (lease-guarded)",
                    intervalHours, initialDelayMinutes);
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
