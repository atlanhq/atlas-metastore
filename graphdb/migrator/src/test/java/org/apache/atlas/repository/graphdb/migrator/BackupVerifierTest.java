package org.apache.atlas.repository.graphdb.migrator;

import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.BackupRunInfo;
import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.Result;
import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.ScheduleInfoFetcher;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

public class BackupVerifierTest {

    private static final String TENANT = "test-tenant";

    // ---- Helpers ----

    private static Map<String, String> childStatuses(String cassandraStatus, String esStatus) {
        Map<String, String> map = new HashMap<>();
        if (cassandraStatus != null) map.put("CassandraBackup", cassandraStatus);
        if (esStatus != null)        map.put("ElasticsearchBackup", esStatus);
        return map;
    }

    private static BackupRunInfo run(Instant startedAt, String parentStatus,
                                     String cassandraStatus, String esStatus) {
        return new BackupRunInfo(startedAt, "daily-backup-wf-123", "run-abc",
                                parentStatus, childStatuses(cassandraStatus, esStatus));
    }

    private static ScheduleInfoFetcher fixedFetcher(BackupRunInfo info) {
        return scheduleId -> info;
    }

    private static ScheduleInfoFetcher failingFetcher(Exception ex) {
        return scheduleId -> { throw ex; };
    }

    // ---- Happy path ----

    @Test
    public void testBothChildrenCompleted_passes() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "COMPLETED", "COMPLETED", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertTrue(result.isPassed());
        assertTrue(result.getMessage().contains("CassandraBackup=COMPLETED"));
        assertTrue(result.getMessage().contains("ElasticsearchBackup=COMPLETED"));
    }

    @Test
    public void testParentFailedButBothChildrenCompleted_passes() {
        // Parent failed (e.g., PostgresBackup failed) but C* and ES are fine
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "FAILED", "COMPLETED", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertTrue(result.isPassed(), "Should pass when C* and ES both completed, even if parent failed");
    }

    // ---- Failures: specific child failed ----

    @Test
    public void testCassandraFailed_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "FAILED", "FAILED", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("CassandraBackup"));
        assertTrue(result.getMessage().contains("FAILED"));
    }

    @Test
    public void testElasticsearchFailed_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "FAILED", "COMPLETED", "FAILED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("ElasticsearchBackup"));
        assertTrue(result.getMessage().contains("FAILED"));
    }

    @Test
    public void testBothChildrenFailed_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "FAILED", "FAILED", "FAILED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("CassandraBackup"));
        assertTrue(result.getMessage().contains("ElasticsearchBackup"));
    }

    @Test
    public void testCassandraTimedOut_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "FAILED", "TIMED_OUT", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("TIMED_OUT"));
    }

    @Test
    public void testChildNotFound_fails() {
        // History parsing didn't find the child workflow at all
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo info = run(oneHourAgo, "COMPLETED", "COMPLETED", null); // ES not in map

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("NOT_FOUND"));
    }

    // ---- Recency checks ----

    @Test
    public void testStaleBackup_fails() {
        Instant twoDaysAgo = Instant.now().minus(Duration.ofHours(49));
        BackupRunInfo info = run(twoDaysAgo, "COMPLETED", "COMPLETED", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertEquals(result.getWorkflowStatus(), "STALE");
        assertTrue(result.getMessage().contains("older than 24 hours"));
    }

    @Test
    public void testCustomRecencyWindow() {
        Instant thirtyHoursAgo = Instant.now().minus(Duration.ofHours(30));
        BackupRunInfo info = run(thirtyHoursAgo, "COMPLETED", "COMPLETED", "COMPLETED");

        assertFalse(new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify().isPassed(),
                    "30h-old backup should fail with 24h window");
        assertTrue(new BackupVerifier(TENANT, 48, fixedFetcher(info)).verify().isPassed(),
                   "30h-old backup should pass with 48h window");
    }

    @Test
    public void testNullStartedAt_fails() {
        BackupRunInfo info = run(null, "COMPLETED", "COMPLETED", "COMPLETED");

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertEquals(result.getWorkflowStatus(), "STALE");
    }

    @Test
    public void testBackupJustInsideWindow_passes() {
        Instant almostStale = Instant.now().minus(Duration.ofHours(24).minusMinutes(1));
        BackupRunInfo info = run(almostStale, "COMPLETED", "COMPLETED", "COMPLETED");

        assertTrue(new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify().isPassed());
    }

    @Test
    public void testBackupJustOutsideWindow_fails() {
        Instant justStale = Instant.now().minus(Duration.ofHours(24).plusMinutes(1));
        BackupRunInfo info = run(justStale, "COMPLETED", "COMPLETED", "COMPLETED");

        assertFalse(new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify().isPassed());
    }

    // ---- Running / no actions / errors ----

    @Test
    public void testRunningWorkflow_fails() {
        Instant fiveMinAgo = Instant.now().minus(Duration.ofMinutes(5));
        BackupRunInfo info = run(fiveMinAgo, "RUNNING", null, null);

        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(info)).verify();

        assertFalse(result.isPassed());
        assertEquals(result.getWorkflowStatus(), "RUNNING");
        assertTrue(result.getMessage().contains("currently running"));
    }

    @Test
    public void testNoRecentActions_fails() {
        Result result = new BackupVerifier(TENANT, 24, fixedFetcher(null)).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("No recent backup runs"));
        assertTrue(result.getMessage().contains("daily-backup-schedule-" + TENANT));
    }

    @Test
    public void testScheduleNotFound_fails() {
        ScheduleInfoFetcher fetcher = failingFetcher(
                new RuntimeException("Schedule not found: daily-backup-schedule-" + TENANT));

        Result result = new BackupVerifier(TENANT, 24, fetcher).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("Schedule not found"));
    }

    @Test
    public void testConnectionFailure_fails() {
        ScheduleInfoFetcher fetcher = failingFetcher(
                new RuntimeException("Connection refused: temporal-server.atlan.com:443"));

        Result result = new BackupVerifier(TENANT, 24, fetcher).verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("Connection refused"));
    }

    @Test
    public void testScheduleIdFormat() {
        Result result = new BackupVerifier("my-prod-tenant", 24, fixedFetcher(null)).verify();

        assertTrue(result.getMessage().contains("daily-backup-schedule-my-prod-tenant"));
    }
}
