package org.apache.atlas.repository.store.graph.v2;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Prometheus metrics for the Layer 1 inline ES write hardening (MS-1009).
 *
 * Static-accessible facade over Micrometer meters so {@link ESConnector}
 * (which is a static-everything class with no Spring injection) can record
 * metrics without restructuring. Metrics are lazily registered the first time
 * any accessor is called, and silently no-op if the registry isn't available
 * (e.g., during unit tests or early bootstrap).
 */
public final class ESConnectorMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(ESConnectorMetrics.class);

    private static final String PREFIX = "atlas_es_write_";

    private static volatile boolean initialized = false;
    private static final Object initLock = new Object();

    private static Counter writeAttempts;
    private static Counter writeSuccess;
    private static Counter retries;
    private static Counter bulkPartialFailures;
    private static Counter circuitBreakerTrips;
    private static Counter circuitBreakerShortCircuits;
    private static Timer writeLatency;
    private static DistributionSummary bulkDocCount;

    private static final AtomicInteger circuitBreakerState = new AtomicInteger(0);

    // Per-reason failure counters created lazily so cardinality is bounded by call sites
    private static final ConcurrentMap<String, Counter> failureCountersByReason = new ConcurrentHashMap<>();

    private ESConnectorMetrics() {}

    private static void ensureInit() {
        if (initialized) return;
        synchronized (initLock) {
            if (initialized) return;
            try {
                MeterRegistry r = getMeterRegistry();
                if (r == null) return;

                writeAttempts = Counter.builder(PREFIX + "attempts_total")
                        .description("ES bulk write attempts (each call to writeTagProperties)")
                        .register(r);
                writeSuccess = Counter.builder(PREFIX + "success_total")
                        .description("ES bulk writes that fully succeeded (no per-doc failures, all retries unneeded)")
                        .register(r);
                retries = Counter.builder(PREFIX + "retries_total")
                        .description("ES bulk write retry attempts triggered by transient failures")
                        .register(r);
                bulkPartialFailures = Counter.builder(PREFIX + "bulk_partial_failures_total")
                        .description("Failed documents detected via per-doc parsing of ES bulk responses")
                        .register(r);
                circuitBreakerTrips = Counter.builder("atlas_es_circuit_breaker_trips_total")
                        .description("Number of times the ES circuit breaker transitioned to OPEN")
                        .register(r);
                circuitBreakerShortCircuits = Counter.builder("atlas_es_circuit_breaker_short_circuits_total")
                        .description("ES write requests rejected by the circuit breaker without contacting ES")
                        .register(r);
                writeLatency = Timer.builder(PREFIX + "latency_seconds")
                        .description("End-to-end ES bulk write latency including retries")
                        .register(r);
                bulkDocCount = DistributionSummary.builder("atlas_es_bulk_docs_per_request")
                        .description("Document count per ES bulk request")
                        .register(r);
                Gauge.builder("atlas_es_circuit_breaker_state", circuitBreakerState, AtomicInteger::get)
                        .description("ES circuit breaker state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN")
                        .register(r);

                initialized = true;
            } catch (Exception e) {
                LOG.warn("Failed to register ES connector metrics; metrics will be disabled this run", e);
            }
        }
    }

    public static void recordAttempt() {
        ensureInit();
        if (writeAttempts != null) writeAttempts.increment();
    }

    public static void recordSuccess() {
        ensureInit();
        if (writeSuccess != null) writeSuccess.increment();
    }

    public static void recordRetry() {
        ensureInit();
        if (retries != null) retries.increment();
    }

    public static void recordPartialFailure(int failedDocs) {
        ensureInit();
        if (bulkPartialFailures != null && failedDocs > 0) bulkPartialFailures.increment(failedDocs);
    }

    /**
     * Record a write failure with a low-cardinality reason label.
     * Reason values must be drawn from a bounded set (e.g., "io_exception",
     * "non_retryable_status", "retries_exhausted", "circuit_open", "parse_error").
     */
    public static void recordFailure(String reason) {
        ensureInit();
        if (!initialized) return;
        try {
            failureCountersByReason
                    .computeIfAbsent(reason, k -> Counter.builder(PREFIX + "failures_total")
                            .tag("reason", k)
                            .description("ES bulk write failures by reason")
                            .register(getMeterRegistry()))
                    .increment();
        } catch (Exception e) {
            // metric recording is never allowed to break the write path
        }
    }

    public static Timer.Sample startLatencyTimer() {
        ensureInit();
        return writeLatency != null ? Timer.start() : null;
    }

    public static void stopLatencyTimer(Timer.Sample sample) {
        if (sample != null && writeLatency != null) sample.stop(writeLatency);
    }

    public static void recordBulkDocCount(int count) {
        ensureInit();
        if (bulkDocCount != null && count > 0) bulkDocCount.record(count);
    }

    public static void recordCircuitBreakerTrip() {
        ensureInit();
        if (circuitBreakerTrips != null) circuitBreakerTrips.increment();
    }

    public static void recordCircuitBreakerShortCircuit() {
        ensureInit();
        if (circuitBreakerShortCircuits != null) circuitBreakerShortCircuits.increment();
    }

    public static void setCircuitBreakerState(int state) {
        circuitBreakerState.set(state);
    }
}
