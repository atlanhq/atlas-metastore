package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-pod circuit breaker for ES bulk writes (MS-1009).
 *
 * <p>Protects ES from being hammered by an Atlas pod when ES is degraded or
 * down, and protects the pod's own thread pool from being tied up in retry
 * loops against a sick ES.</p>
 *
 * <p>State machine:
 * <ul>
 *     <li><b>CLOSED</b> — requests pass through; failures are counted.</li>
 *     <li><b>OPEN</b> — requests are rejected immediately without contacting
 *     ES, until the cool-down elapses.</li>
 *     <li><b>HALF_OPEN</b> — after cool-down, a probe request is allowed
 *     through. Success → CLOSED. Failure → OPEN (cool-down resets).</li>
 * </ul>
 *
 * <p>State is per-pod (in-memory). Pods do not coordinate breaker state with
 * each other — aggregate effect of every pod independently protecting itself
 * is sufficient and avoids any shared-state contention point on the ES write
 * path.</p>
 *
 * <p>Disabled entirely when {@code atlas.es.circuit.breaker.enabled=false} —
 * in that case all calls pass through and state transitions never happen.</p>
 */
public final class ESCircuitBreaker {
    private static final Logger LOG = LoggerFactory.getLogger(ESCircuitBreaker.class);

    public enum State {
        CLOSED(0), OPEN(1), HALF_OPEN(2);
        private final int code;
        State(int code) { this.code = code; }
        public int code() { return code; }
    }

    private static volatile State state = State.CLOSED;
    private static final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private static volatile long openedAtMs = 0L;
    private static final Object stateLock = new Object();

    // Configuration is read once at class init. Hot-reload would require a refresh hook.
    private static final boolean ENABLED            = AtlasConfiguration.ES_CIRCUIT_BREAKER_ENABLED.getBoolean();
    private static final int     FAILURE_THRESHOLD  = AtlasConfiguration.ES_CIRCUIT_BREAKER_FAILURE_THRESHOLD.getInt();
    private static final long    COOLDOWN_MS        = AtlasConfiguration.ES_CIRCUIT_BREAKER_COOLDOWN_MS.getLong();

    static {
        if (ENABLED) {
            LOG.info("ES circuit breaker enabled: failureThreshold={}, cooldownMs={}",
                    FAILURE_THRESHOLD, COOLDOWN_MS);
        } else {
            LOG.info("ES circuit breaker is DISABLED (atlas.es.circuit.breaker.enabled=false)");
        }
    }

    private ESCircuitBreaker() {}

    /**
     * Returns true if the caller may proceed with an ES request, false if the
     * breaker is OPEN and the caller must short-circuit. When the cool-down
     * has elapsed, transitions OPEN → HALF_OPEN and allows one probe through.
     */
    public static boolean allowRequest() {
        if (!ENABLED) return true;

        State current = state;
        if (current == State.CLOSED) return true;

        if (current == State.OPEN) {
            if (System.currentTimeMillis() - openedAtMs >= COOLDOWN_MS) {
                synchronized (stateLock) {
                    if (state == State.OPEN) {
                        state = State.HALF_OPEN;
                        ESConnectorMetrics.setCircuitBreakerState(State.HALF_OPEN.code());
                        LOG.info("ES circuit breaker transitioned OPEN -> HALF_OPEN (cool-down {}ms elapsed)", COOLDOWN_MS);
                    }
                }
                return state == State.HALF_OPEN;
            }
            return false;
        }

        // HALF_OPEN: pass requests through; outcome will reset to CLOSED or move back to OPEN.
        return true;
    }

    /**
     * Record a successful ES bulk request. Resets the consecutive-failure
     * counter and, if the breaker was HALF_OPEN, transitions it back to CLOSED.
     */
    public static void recordSuccess() {
        if (!ENABLED) return;
        consecutiveFailures.set(0);
        if (state != State.CLOSED) {
            synchronized (stateLock) {
                if (state != State.CLOSED) {
                    State previous = state;
                    state = State.CLOSED;
                    ESConnectorMetrics.setCircuitBreakerState(State.CLOSED.code());
                    LOG.info("ES circuit breaker recovered: {} -> CLOSED", previous);
                }
            }
        }
    }

    /**
     * Record a failed ES bulk request. Increments the consecutive-failure
     * counter; opens the breaker if the threshold is reached. From HALF_OPEN,
     * a single failure flips back to OPEN immediately.
     */
    public static void recordFailure() {
        if (!ENABLED) return;
        int failures = consecutiveFailures.incrementAndGet();

        if (state == State.HALF_OPEN) {
            synchronized (stateLock) {
                if (state == State.HALF_OPEN) {
                    state = State.OPEN;
                    openedAtMs = System.currentTimeMillis();
                    ESConnectorMetrics.setCircuitBreakerState(State.OPEN.code());
                    ESConnectorMetrics.recordCircuitBreakerTrip();
                    LOG.warn("ES circuit breaker probe failed: HALF_OPEN -> OPEN");
                }
            }
            return;
        }

        if (failures >= FAILURE_THRESHOLD && state == State.CLOSED) {
            synchronized (stateLock) {
                if (state == State.CLOSED && consecutiveFailures.get() >= FAILURE_THRESHOLD) {
                    state = State.OPEN;
                    openedAtMs = System.currentTimeMillis();
                    ESConnectorMetrics.setCircuitBreakerState(State.OPEN.code());
                    ESConnectorMetrics.recordCircuitBreakerTrip();
                    LOG.warn("ES circuit breaker tripped: CLOSED -> OPEN after {} consecutive failures (cool-down {}ms)",
                            FAILURE_THRESHOLD, COOLDOWN_MS);
                }
            }
        }
    }

    public static State getState() { return state; }

    /**
     * Reset state to CLOSED. Test-only — production callers must not invoke this.
     */
    static void resetForTesting() {
        synchronized (stateLock) {
            state = State.CLOSED;
            consecutiveFailures.set(0);
            openedAtMs = 0L;
            ESConnectorMetrics.setCircuitBreakerState(State.CLOSED.code());
        }
    }
}
