package org.apache.atlas.repository.graphdb.migrator;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.client.schedules.ScheduleActionExecution;
import io.temporal.client.schedules.ScheduleActionExecutionStartWorkflow;
import io.temporal.client.schedules.ScheduleActionResult;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.client.schedules.ScheduleDescription;
import io.temporal.client.schedules.ScheduleHandle;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that recent successful Cassandra and Elasticsearch backups exist
 * for a tenant by querying the Temporal schedule {@code daily-backup-schedule-<tenant>}.
 *
 * <p>The schedule triggers a {@code TenantBackup} parent workflow with child workflows:
 * ArgoBackup, CassandraBackup, ElasticsearchBackup, PostgresBackup, RedisBackup.
 * This verifier specifically checks that CassandraBackup and ElasticsearchBackup
 * completed successfully — other children are ignored.</p>
 */
public class BackupVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(BackupVerifier.class);

    static final String SCHEDULE_PREFIX          = "daily-backup-schedule-";
    static final String DEFAULT_TEMPORAL_ADDRESS = "temporal-server.atlan.com:443";
    static final String DEFAULT_NAMESPACE        = "default";
    static final int    DEFAULT_RECENCY_HOURS    = 24;

    static final String CASSANDRA_BACKUP     = "CassandraBackup";
    static final String ELASTICSEARCH_BACKUP = "ElasticsearchBackup";

    private final String              tenant;
    private final int                 recencyHours;
    private final ScheduleInfoFetcher fetcher;

    // ---- Abstraction for Temporal operations (mockable in tests) ----

    interface ScheduleInfoFetcher extends AutoCloseable {
        /**
         * Fetch the most recent backup run from the given schedule,
         * including per-child workflow statuses for CassandraBackup and ElasticsearchBackup.
         * @return null if no recent actions exist
         */
        BackupRunInfo fetchLastBackupRun(String scheduleId) throws Exception;

        @Override
        default void close() throws Exception { }
    }

    static class BackupRunInfo {
        final Instant             startedAt;
        final String              workflowId;
        final String              runId;
        final String              parentStatus;
        final Map<String, String> childStatuses; // e.g. {"CassandraBackup": "COMPLETED", "ElasticsearchBackup": "FAILED"}

        BackupRunInfo(Instant startedAt, String workflowId, String runId,
                      String parentStatus, Map<String, String> childStatuses) {
            this.startedAt     = startedAt;
            this.workflowId    = workflowId;
            this.runId         = runId;
            this.parentStatus  = parentStatus;
            this.childStatuses = childStatuses != null ? childStatuses : new HashMap<>();
        }
    }

    // ---- Result ----

    public static class Result {
        private final boolean passed;
        private final String  message;
        private final Instant lastRunTime;
        private final String  workflowStatus;

        private Result(boolean passed, String message, Instant lastRunTime, String workflowStatus) {
            this.passed         = passed;
            this.message        = message;
            this.lastRunTime    = lastRunTime;
            this.workflowStatus = workflowStatus;
        }

        static Result pass(String message, Instant lastRunTime) {
            return new Result(true, message, lastRunTime, "COMPLETED");
        }

        static Result fail(String message) {
            return new Result(false, message, null, null);
        }

        static Result fail(String message, Instant lastRunTime, String status) {
            return new Result(false, message, lastRunTime, status);
        }

        public boolean isPassed()          { return passed; }
        public String  getMessage()        { return message; }
        public Instant getLastRunTime()    { return lastRunTime; }
        public String  getWorkflowStatus() { return workflowStatus; }

        @Override
        public String toString() {
            return (passed ? "PASSED" : "FAILED") + ": " + message;
        }
    }

    // ---- Constructors ----

    /** Production constructor — connects to Temporal. */
    public BackupVerifier(String tenant, int recencyHours, String temporalAddress, String namespace) {
        this(tenant, recencyHours,
             new TemporalScheduleInfoFetcher(
                     temporalAddress != null ? temporalAddress : DEFAULT_TEMPORAL_ADDRESS,
                     namespace       != null ? namespace       : DEFAULT_NAMESPACE));
    }

    public BackupVerifier(String tenant, int recencyHours) {
        this(tenant, recencyHours, DEFAULT_TEMPORAL_ADDRESS, DEFAULT_NAMESPACE);
    }

    /** Package-private constructor for testing with a mock fetcher. */
    BackupVerifier(String tenant, int recencyHours, ScheduleInfoFetcher fetcher) {
        this.tenant       = tenant;
        this.recencyHours = recencyHours;
        this.fetcher      = fetcher;
    }

    // ---- Verification logic ----

    public Result verify() {
        String scheduleId = SCHEDULE_PREFIX + tenant;
        LOG.info("Verifying backup schedule: {} (recency window: {}h)", scheduleId, recencyHours);

        try {
            BackupRunInfo lastRun = fetcher.fetchLastBackupRun(scheduleId);

            if (lastRun == null) {
                return Result.fail("No recent backup runs found for schedule: " + scheduleId);
            }

            LOG.info("Last backup: workflow={}, run={}, started={}, parent={}",
                     lastRun.workflowId, lastRun.runId, lastRun.startedAt, lastRun.parentStatus);

            // Check recency
            Instant cutoff = Instant.now().minus(Duration.ofHours(recencyHours));
            if (lastRun.startedAt == null || lastRun.startedAt.isBefore(cutoff)) {
                return Result.fail(
                        String.format("Last backup ran at %s, older than %d hours", lastRun.startedAt, recencyHours),
                        lastRun.startedAt, "STALE");
            }

            // If parent is still running, we can't verify children yet
            if ("RUNNING".equals(lastRun.parentStatus)) {
                return Result.fail(
                        String.format("Backup is currently running (started %s) — re-check after it completes",
                                      lastRun.startedAt),
                        lastRun.startedAt, "RUNNING");
            }

            // Check CassandraBackup and ElasticsearchBackup child statuses
            String cassandraStatus = lastRun.childStatuses.getOrDefault(CASSANDRA_BACKUP, "NOT_FOUND");
            String esStatus        = lastRun.childStatuses.getOrDefault(ELASTICSEARCH_BACKUP, "NOT_FOUND");

            LOG.info("Child statuses: {}={}, {}={}", CASSANDRA_BACKUP, cassandraStatus, ELASTICSEARCH_BACKUP, esStatus);

            boolean cassandraOk = "COMPLETED".equals(cassandraStatus);
            boolean esOk        = "COMPLETED".equals(esStatus);

            if (cassandraOk && esOk) {
                String msg = String.format(
                        "Backup verified: schedule=%s, lastRun=%s, CassandraBackup=COMPLETED, ElasticsearchBackup=COMPLETED",
                        scheduleId, lastRun.startedAt);
                return Result.pass(msg, lastRun.startedAt);
            }

            // Build failure message with per-component detail
            StringBuilder sb = new StringBuilder();
            sb.append("Backup verification failed for schedule: ").append(scheduleId).append('\n');
            sb.append("  CassandraBackup:     ").append(cassandraStatus).append(cassandraOk ? " ✓" : " ✗").append('\n');
            sb.append("  ElasticsearchBackup: ").append(esStatus).append(esOk ? " ✓" : " ✗");

            return Result.fail(sb.toString(), lastRun.startedAt, lastRun.parentStatus);

        } catch (Exception e) {
            LOG.error("Failed to verify backup schedule: {}", scheduleId, e);
            return Result.fail("Error verifying backup: " + e.getMessage());
        }
    }

    // ---- Real Temporal implementation ----

    private static class TemporalScheduleInfoFetcher implements ScheduleInfoFetcher {
        private final WorkflowServiceStubs stubs;
        private final String               namespace;

        TemporalScheduleInfoFetcher(String temporalAddress, String namespace) {
            this.namespace = namespace;

            WorkflowServiceStubsOptions.Builder builder = WorkflowServiceStubsOptions.newBuilder()
                    .setTarget(temporalAddress);

            // Enable TLS for port-443 endpoints (GrpcSslContexts adds ALPN/HTTP2 negotiation)
            if (temporalAddress.endsWith(":443")) {
                try {
                    builder.setSslContext(
                            io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.forClient().build());
                } catch (SSLException e) {
                    throw new RuntimeException("Failed to create TLS context for Temporal", e);
                }
            }

            this.stubs = WorkflowServiceStubs.newServiceStubs(builder.build());
        }

        @Override
        public BackupRunInfo fetchLastBackupRun(String scheduleId) throws Exception {
            // 1. Describe the schedule to get the latest action
            ScheduleClient scheduleClient = ScheduleClient.newInstance(stubs,
                    ScheduleClientOptions.newBuilder().setNamespace(namespace).build());

            ScheduleHandle handle = scheduleClient.getHandle(scheduleId);
            ScheduleDescription desc = handle.describe();

            List<ScheduleActionResult> recentActions = desc.getInfo().getRecentActions();
            if (recentActions == null || recentActions.isEmpty()) {
                return null;
            }

            ScheduleActionResult lastAction = recentActions.get(recentActions.size() - 1);
            Instant startedAt = lastAction.getStartedAt();

            ScheduleActionExecution action = lastAction.getAction();
            if (!(action instanceof ScheduleActionExecutionStartWorkflow)) {
                throw new IllegalStateException("Last schedule action is not a workflow execution");
            }

            ScheduleActionExecutionStartWorkflow wfAction = (ScheduleActionExecutionStartWorkflow) action;
            String workflowId = wfAction.getWorkflowId();
            String runId      = wfAction.getFirstExecutionRunId();

            // 2. Follow retry chain to the terminal run (max 5 hops)
            String currentRunId = runId;
            String parentStatus = null;
            for (int i = 0; i < 5; i++) {
                DescribeWorkflowExecutionResponse wfResp = stubs.blockingStub()
                        .describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
                                .setNamespace(namespace)
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .setRunId(currentRunId)
                                        .build())
                                .build());

                WorkflowExecutionStatus wfStatus = wfResp.getWorkflowExecutionInfo().getStatus();
                parentStatus = wfStatus.name().replace("WORKFLOW_EXECUTION_STATUS_", "");

                if (!"FAILED".equals(parentStatus)) {
                    break; // COMPLETED, RUNNING, etc. — no retry to follow
                }

                // Check history for retry (newExecutionRunId in the FAILED event)
                String retryRunId = findRetryRunId(workflowId, currentRunId);
                if (retryRunId == null) {
                    break; // No retry — this is the terminal run
                }

                LOG.info("Following retry: {} -> {}", currentRunId, retryRunId);
                currentRunId = retryRunId;
            }

            // 3. Fetch workflow history and parse child workflow statuses
            Map<String, String> childStatuses = parseChildWorkflowStatuses(workflowId, currentRunId);

            return new BackupRunInfo(startedAt, workflowId, currentRunId, parentStatus, childStatuses);
        }

        /**
         * Check the workflow history for a retry run ID (from WorkflowExecutionFailed event).
         */
        private String findRetryRunId(String workflowId, String runId) {
            try {
                GetWorkflowExecutionHistoryResponse historyResp = stubs.blockingStub()
                        .getWorkflowExecutionHistory(GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(namespace)
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .setRunId(runId)
                                        .build())
                                .build());

                for (HistoryEvent event : historyResp.getHistory().getEventsList()) {
                    if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED) {
                        String newRunId = event.getWorkflowExecutionFailedEventAttributes().getNewExecutionRunId();
                        if (newRunId != null && !newRunId.isEmpty()) {
                            return newRunId;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to check retry for workflow {}/{}: {}", workflowId, runId, e.getMessage());
            }
            return null;
        }

        /**
         * Parse workflow history events to extract child workflow statuses.
         * Tracks: CassandraBackup and ElasticsearchBackup.
         */
        private Map<String, String> parseChildWorkflowStatuses(String workflowId, String runId) {
            Map<String, String> childStatuses = new HashMap<>();
            Map<Long, String> initiatedEventIdToType = new HashMap<>(); // eventId -> workflow type name

            try {
                GetWorkflowExecutionHistoryResponse historyResp = stubs.blockingStub()
                        .getWorkflowExecutionHistory(GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(namespace)
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .setRunId(runId)
                                        .build())
                                .build());

                for (HistoryEvent event : historyResp.getHistory().getEventsList()) {
                    switch (event.getEventType()) {
                        case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED: {
                            String typeName = event.getStartChildWorkflowExecutionInitiatedEventAttributes()
                                    .getWorkflowType().getName();
                            if (CASSANDRA_BACKUP.equals(typeName) || ELASTICSEARCH_BACKUP.equals(typeName)) {
                                initiatedEventIdToType.put(event.getEventId(), typeName);
                                childStatuses.put(typeName, "RUNNING"); // default until we see completion/failure
                            }
                            break;
                        }
                        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED: {
                            long initId = event.getChildWorkflowExecutionCompletedEventAttributes().getInitiatedEventId();
                            String typeName = initiatedEventIdToType.get(initId);
                            if (typeName != null) {
                                childStatuses.put(typeName, "COMPLETED");
                            }
                            break;
                        }
                        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED: {
                            long initId = event.getChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
                            String typeName = initiatedEventIdToType.get(initId);
                            if (typeName != null) {
                                childStatuses.put(typeName, "FAILED");
                            }
                            break;
                        }
                        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT: {
                            long initId = event.getChildWorkflowExecutionTimedOutEventAttributes().getInitiatedEventId();
                            String typeName = initiatedEventIdToType.get(initId);
                            if (typeName != null) {
                                childStatuses.put(typeName, "TIMED_OUT");
                            }
                            break;
                        }
                        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED: {
                            long initId = event.getChildWorkflowExecutionCanceledEventAttributes().getInitiatedEventId();
                            String typeName = initiatedEventIdToType.get(initId);
                            if (typeName != null) {
                                childStatuses.put(typeName, "CANCELED");
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse child workflow statuses for {}/{}: {}", workflowId, runId, e.getMessage());
            }

            return childStatuses;
        }

        @Override
        public void close() {
            if (stubs != null) {
                stubs.shutdown();
            }
        }
    }

    // ---- CLI entry point ----

    public static void main(String[] args) {
        String tenant    = null;
        int    recency   = DEFAULT_RECENCY_HOURS;
        String address   = DEFAULT_TEMPORAL_ADDRESS;
        String namespace = DEFAULT_NAMESPACE;
        boolean skip     = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--tenant":
                    tenant = args[++i]; break;
                case "--recency":
                    recency = Integer.parseInt(args[++i]); break;
                case "--temporal-address":
                    address = args[++i]; break;
                case "--namespace":
                    namespace = args[++i]; break;
                case "--skip-backup-check":
                    skip = true; break;
                case "--help": case "-h":
                    printUsage(); System.exit(0); break;
                default:
                    System.err.println("[backup-check] ERROR: Unknown option: " + args[i]);
                    System.exit(2);
            }
        }

        if (skip) {
            System.out.println("[backup-check] WARN: Backup check SKIPPED (--skip-backup-check)");
            System.out.println("[backup-check] WARN: Proceeding WITHOUT verified backups — dev/test only.");
            System.exit(0);
        }

        if (tenant == null || tenant.isEmpty()) {
            System.err.println("[backup-check] ERROR: --tenant <name> is required");
            System.exit(2);
        }

        BackupVerifier verifier = new BackupVerifier(tenant, recency, address, namespace);
        Result result = verifier.verify();

        if (result.isPassed()) {
            System.out.println("[backup-check] PASSED: " + result.getMessage());
            System.exit(0);
        } else {
            System.err.println("[backup-check] FAILED: " + result.getMessage());
            if (result.getLastRunTime() != null) {
                System.err.println("[backup-check]   Last run:   " + result.getLastRunTime());
                System.err.println("[backup-check]   Status:     " + result.getWorkflowStatus());
            }
            System.err.println("[backup-check]   Options:");
            System.err.println("[backup-check]     1. Trigger a backup and re-run this check");
            System.err.println("[backup-check]     2. Increase --recency window (e.g., --recency 48)");
            System.err.println("[backup-check]     3. Use --skip-backup-check for dev/test ONLY");
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: BackupVerifier --tenant <name> [options]");
        System.out.println();
        System.out.println("Verifies recent successful Cassandra and Elasticsearch backups");
        System.out.println("via Temporal schedule API (daily-backup-schedule-<tenant>).");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --tenant <name>            Tenant/cluster name (required)");
        System.out.println("  --recency <hours>          Max backup age in hours (default: 24)");
        System.out.println("  --temporal-address <addr>   Temporal server address (default: temporal-server.atlan.com:443)");
        System.out.println("  --namespace <ns>           Temporal namespace (default: default)");
        System.out.println("  --skip-backup-check        Skip verification (dev/test only)");
        System.out.println();
        System.out.println("Exit codes:");
        System.out.println("  0  Backup verification passed (both CassandraBackup and ElasticsearchBackup completed)");
        System.out.println("  1  Backup verification failed");
        System.out.println("  2  Configuration error");
    }
}
