package org.apache.atlas.repository.outbox.shared;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for {@link OutboxMetrics}. Exercises the prefix-parameterised
 * constructor against a {@link SimpleMeterRegistry} and asserts that every
 * documented meter registers under the provided prefix. The contract: two
 * {@code OutboxMetrics} instances with different prefixes must not collide.
 */
class OutboxMetricsTest {

    private static final String PREFIX_A = "test_outbox_a_";
    private static final String PREFIX_B = "test_outbox_b_";

    @Test
    void registersAllMetersUnderProvidedPrefix() {
        MeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics  = new OutboxMetrics(registry, PREFIX_A);

        // Triggering every surface method once flushes the lazy tagged counters
        // into the registry, so we can verify full coverage.
        metrics.recordWrite();
        metrics.recordWriteError("cassandra_timeout");
        metrics.recordPayloadBytes(256);
        metrics.recordRelayPoll();
        metrics.recordRelayBatchProcessed();
        metrics.recordRelayProcessed(5);
        metrics.recordRelayFailure("retryable");
        metrics.recordRelayPermanentlyFailed(1);
        metrics.recordRelayReclaimedProcessing(1);
        metrics.recordRelayLagMillis(42L);
        metrics.recordRelayBatchSize(10);
        metrics.setLeader(true);
        metrics.recordLeaseHandover();
        metrics.recordLeaseAcquireAttempt("acquired");
        metrics.recordReconcilerRun();
        metrics.recordReconcilerScanned("FAILED", 3);
        metrics.recordReconcilerAlreadyInEs(1);
        metrics.recordReconcilerReindexed(2);
        metrics.recordReconcilerStillMissing(1);
        metrics.recordReconcilerTickError();
        metrics.setReconcilerHealthy(true);
        metrics.setPendingCount(7);
        metrics.setProcessingCount(1);
        metrics.setFailedCount(0);
        metrics.setOldestPendingAgeSeconds(60L);

        List<String> names = registry.getMeters().stream()
                .map(m -> m.getId().getName())
                .collect(Collectors.toList());

        // Core counters / timers / distributions
        assertTrue(names.contains(PREFIX_A + "writes_total"));
        assertTrue(names.contains(PREFIX_A + "write_errors_total"));
        assertTrue(names.contains(PREFIX_A + "write_latency_seconds"));
        assertTrue(names.contains(PREFIX_A + "payload_bytes"));
        assertTrue(names.contains(PREFIX_A + "relay_polls_total"));
        assertTrue(names.contains(PREFIX_A + "relay_batches_processed_total"));
        assertTrue(names.contains(PREFIX_A + "relay_processed_total"));
        assertTrue(names.contains(PREFIX_A + "relay_permanently_failed_total"));
        assertTrue(names.contains(PREFIX_A + "relay_reclaimed_processing_total"));
        assertTrue(names.contains(PREFIX_A + "relay_lag_seconds"));
        assertTrue(names.contains(PREFIX_A + "relay_batch_size"));
        assertTrue(names.contains(PREFIX_A + "lease_handovers_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_runs_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_already_in_es_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_reindexed_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_still_missing_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_tick_errors_total"));

        // Tagged counters resolved on first call
        assertTrue(names.contains(PREFIX_A + "write_errors_by_reason_total"));
        assertTrue(names.contains(PREFIX_A + "relay_failures_total"));
        assertTrue(names.contains(PREFIX_A + "lease_acquire_attempts_total"));
        assertTrue(names.contains(PREFIX_A + "reconciler_scanned_total"));

        // Gauges
        assertTrue(names.contains(PREFIX_A + "pending_count"));
        assertTrue(names.contains(PREFIX_A + "processing_count"));
        assertTrue(names.contains(PREFIX_A + "failed_count"));
        assertTrue(names.contains(PREFIX_A + "oldest_pending_age_seconds"));
        assertTrue(names.contains(PREFIX_A + "relay_leader"));
        assertTrue(names.contains(PREFIX_A + "reconciler_last_run_timestamp_seconds"));
        assertTrue(names.contains(PREFIX_A + "reconciler_healthy"));
    }

    @Test
    void gaugeValuesReflectSetters() {
        MeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics  = new OutboxMetrics(registry, PREFIX_A);

        metrics.setPendingCount(42);
        metrics.setProcessingCount(3);
        metrics.setFailedCount(7);
        metrics.setOldestPendingAgeSeconds(120L);

        assertEquals(42.0, registry.get(PREFIX_A + "pending_count").gauge().value());
        assertEquals(3.0,  registry.get(PREFIX_A + "processing_count").gauge().value());
        assertEquals(7.0,  registry.get(PREFIX_A + "failed_count").gauge().value());
        assertEquals(120.0, registry.get(PREFIX_A + "oldest_pending_age_seconds").gauge().value());
    }

    @Test
    void twoInstancesWithDifferentPrefixesCoexist() {
        MeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics a = new OutboxMetrics(registry, PREFIX_A);
        OutboxMetrics b = new OutboxMetrics(registry, PREFIX_B);

        a.recordWrite();
        a.recordWrite();
        b.recordWrite();

        Counter counterA = (Counter) registry.find(PREFIX_A + "writes_total").meter();
        Counter counterB = (Counter) registry.find(PREFIX_B + "writes_total").meter();

        assertNotNull(counterA);
        assertNotNull(counterB);
        assertEquals(2.0, counterA.count());
        assertEquals(1.0, counterB.count());
    }

    @Test
    void recordMethodsAreNoOpsWhenRegistryIsNull() {
        // Null registry is a supported mode (e.g. tests that don't care about
        // observability). Every method must be a safe no-op.
        OutboxMetrics metrics = new OutboxMetrics(null, PREFIX_A);

        metrics.recordWrite();
        metrics.recordWriteError("foo");
        metrics.recordRelayPoll();
        metrics.recordRelayBatchProcessed();
        metrics.recordRelayProcessed(10);
        metrics.recordRelayFailure("retryable");
        metrics.recordLeaseAcquireAttempt("held_by_other");
        metrics.recordReconcilerScanned("FAILED", 5);
        metrics.setPendingCount(5);
        metrics.setLeader(true);

        // Gauges' backing atomics are still touched, but no registry means no
        // meters to find.
        SimpleMeterRegistry empty = new SimpleMeterRegistry();
        List<Meter> meters = empty.getMeters();
        assertTrue(meters.isEmpty());
        // No exception thrown is the actual assertion — just make it explicit:
        assertFalse(metrics.startWriteTimer() != null);
    }
}
