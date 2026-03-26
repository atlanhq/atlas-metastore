package org.apache.atlas.repository.graphdb.migrator;

import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.BackupRunInfo;
import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.Result;
import org.apache.atlas.repository.graphdb.migrator.BackupVerifier.ScheduleInfoFetcher;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;

import static org.testng.Assert.*;

public class BackupVerifierTest {

    private static final String TENANT = "test-tenant";

    // ---- Helper to create a fetcher returning a fixed BackupRunInfo ----

    private static ScheduleInfoFetcher fixedFetcher(BackupRunInfo info) {
        return scheduleId -> info;
    }

    private static ScheduleInfoFetcher failingFetcher(Exception ex) {
        return scheduleId -> { throw ex; };
    }

    // ---- Tests ----

    @Test
    public void testRecentCompletedBackup_passes() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo run = new BackupRunInfo(oneHourAgo, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertTrue(result.isPassed(), "Recent completed backup should pass");
        assertEquals(result.getWorkflowStatus(), "COMPLETED");
        assertNotNull(result.getLastRunTime());
        assertTrue(result.getMessage().contains("COMPLETED"));
    }

    @Test
    public void testStaleBackup_fails() {
        Instant twoDaysAgo = Instant.now().minus(Duration.ofHours(49));
        BackupRunInfo run = new BackupRunInfo(twoDaysAgo, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertFalse(result.isPassed(), "Stale backup should fail");
        assertEquals(result.getWorkflowStatus(), "STALE");
        assertTrue(result.getMessage().contains("older than 24 hours"));
    }

    @Test
    public void testFailedWorkflow_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo run = new BackupRunInfo(oneHourAgo, "daily-backup-wf-123", "run-abc", "FAILED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertFalse(result.isPassed(), "Failed workflow should fail verification");
        assertEquals(result.getWorkflowStatus(), "FAILED");
        assertTrue(result.getMessage().contains("failed"));
    }

    @Test
    public void testRunningWorkflow_fails() {
        Instant fiveMinAgo = Instant.now().minus(Duration.ofMinutes(5));
        BackupRunInfo run = new BackupRunInfo(fiveMinAgo, "daily-backup-wf-123", "run-abc", "RUNNING");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertFalse(result.isPassed(), "Running workflow should fail (not yet complete)");
        assertEquals(result.getWorkflowStatus(), "RUNNING");
        assertTrue(result.getMessage().contains("currently running"));
    }

    @Test
    public void testTimedOutWorkflow_fails() {
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        BackupRunInfo run = new BackupRunInfo(oneHourAgo, "daily-backup-wf-123", "run-abc", "TIMED_OUT");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertFalse(result.isPassed());
        assertEquals(result.getWorkflowStatus(), "TIMED_OUT");
    }

    @Test
    public void testNoRecentActions_fails() {
        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(null));
        Result result = verifier.verify();

        assertFalse(result.isPassed(), "No recent actions should fail");
        assertTrue(result.getMessage().contains("No recent backup runs"));
        assertTrue(result.getMessage().contains("daily-backup-schedule-" + TENANT));
    }

    @Test
    public void testScheduleNotFound_fails() {
        ScheduleInfoFetcher fetcher = failingFetcher(
                new RuntimeException("Schedule not found: daily-backup-schedule-" + TENANT));

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fetcher);
        Result result = verifier.verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("Schedule not found"));
    }

    @Test
    public void testConnectionFailure_fails() {
        ScheduleInfoFetcher fetcher = failingFetcher(
                new RuntimeException("Connection refused: temporal-server.atlan.com:443"));

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fetcher);
        Result result = verifier.verify();

        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("Connection refused"));
    }

    @Test
    public void testCustomRecencyWindow() {
        // Backup is 30 hours old — fails with 24h window, passes with 48h window
        Instant thirtyHoursAgo = Instant.now().minus(Duration.ofHours(30));
        BackupRunInfo run = new BackupRunInfo(thirtyHoursAgo, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier strict = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        assertFalse(strict.verify().isPassed(), "30h-old backup should fail with 24h window");

        BackupVerifier relaxed = new BackupVerifier(TENANT, 48, fixedFetcher(run));
        assertTrue(relaxed.verify().isPassed(), "30h-old backup should pass with 48h window");
    }

    @Test
    public void testNullStartedAt_fails() {
        BackupRunInfo run = new BackupRunInfo(null, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        Result result = verifier.verify();

        assertFalse(result.isPassed(), "Null startedAt should fail recency check");
        assertEquals(result.getWorkflowStatus(), "STALE");
    }

    @Test
    public void testScheduleIdUsesCorrectPrefix() {
        // Verify the schedule ID format by checking the message references the tenant
        BackupVerifier verifier = new BackupVerifier("my-production-tenant", 24, fixedFetcher(null));
        Result result = verifier.verify();

        assertTrue(result.getMessage().contains("daily-backup-schedule-my-production-tenant"));
    }

    @Test
    public void testBackupJustInsideRecencyWindow_passes() {
        // Backup is 23h59m old — should pass with 24h window
        Instant almostStale = Instant.now().minus(Duration.ofHours(24).minusMinutes(1));
        BackupRunInfo run = new BackupRunInfo(almostStale, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        assertTrue(verifier.verify().isPassed(), "Backup just inside window should pass");
    }

    @Test
    public void testBackupJustOutsideRecencyWindow_fails() {
        // Backup is 24h1m old — should fail with 24h window
        Instant justStale = Instant.now().minus(Duration.ofHours(24).plusMinutes(1));
        BackupRunInfo run = new BackupRunInfo(justStale, "daily-backup-wf-123", "run-abc", "COMPLETED");

        BackupVerifier verifier = new BackupVerifier(TENANT, 24, fixedFetcher(run));
        assertFalse(verifier.verify().isPassed(), "Backup just outside window should fail");
    }
}
