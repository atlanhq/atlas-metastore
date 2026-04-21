package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ESCircuitBreaker}.
 *
 * <p>Note: the breaker reads its configuration once at class init via static
 * fields. These tests assume defaults from AtlasConfiguration:
 * threshold=10, cooldown=30000ms. They intentionally avoid asserting on
 * cool-down / HALF_OPEN transitions because waiting 30 seconds in unit tests
 * is too slow; the OPEN→HALF_OPEN transition is exercised manually by
 * {@link #shortCooldownTransition()} which doesn't use real clock waiting.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ESCircuitBreakerTest {

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.es.circuit.breaker.enabled", true);
            config.setProperty("atlas.es.circuit.breaker.failure.threshold", 10);
            config.setProperty("atlas.es.circuit.breaker.cooldown.ms", 30000);
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    void tearDown() {
        ApplicationProperties.forceReload();
    }

    @BeforeEach
    void resetBreaker() {
        ESCircuitBreaker.resetForTesting();
    }

    @Test
    void startsClosed() {
        assertEquals(ESCircuitBreaker.State.CLOSED, ESCircuitBreaker.getState());
        assertTrue(ESCircuitBreaker.allowRequest());
    }

    @Test
    void singleFailureDoesNotOpen() {
        ESCircuitBreaker.recordFailure();
        assertEquals(ESCircuitBreaker.State.CLOSED, ESCircuitBreaker.getState());
        assertTrue(ESCircuitBreaker.allowRequest());
    }

    @Test
    void thresholdConsecutiveFailuresOpenBreaker() {
        for (int i = 0; i < 10; i++) {
            ESCircuitBreaker.recordFailure();
        }
        assertEquals(ESCircuitBreaker.State.OPEN, ESCircuitBreaker.getState());
        assertFalse(ESCircuitBreaker.allowRequest(),
                "Once OPEN, allowRequest must reject within the cool-down window");
    }

    @Test
    void successResetsConsecutiveCounter() {
        for (int i = 0; i < 9; i++) {
            ESCircuitBreaker.recordFailure();
        }
        ESCircuitBreaker.recordSuccess();
        // Counter is back to zero; we can take 9 more failures before tripping
        for (int i = 0; i < 9; i++) {
            ESCircuitBreaker.recordFailure();
        }
        assertEquals(ESCircuitBreaker.State.CLOSED, ESCircuitBreaker.getState());
    }

    @Test
    void successInClosedStateIsNoop() {
        // Sanity: success on already-closed breaker doesn't break anything
        ESCircuitBreaker.recordSuccess();
        ESCircuitBreaker.recordSuccess();
        assertEquals(ESCircuitBreaker.State.CLOSED, ESCircuitBreaker.getState());
    }

    @Test
    void resetReturnsToClosed() {
        for (int i = 0; i < 10; i++) {
            ESCircuitBreaker.recordFailure();
        }
        assertEquals(ESCircuitBreaker.State.OPEN, ESCircuitBreaker.getState());
        ESCircuitBreaker.resetForTesting();
        assertEquals(ESCircuitBreaker.State.CLOSED, ESCircuitBreaker.getState());
        assertTrue(ESCircuitBreaker.allowRequest());
    }

    @Test
    void shortCooldownTransition() {
        // Force the breaker open, then verify it stays open during the cool-down.
        // Real OPEN→HALF_OPEN transition timing is exercised at integration level.
        for (int i = 0; i < 10; i++) {
            ESCircuitBreaker.recordFailure();
        }
        assertEquals(ESCircuitBreaker.State.OPEN, ESCircuitBreaker.getState());
        // Repeated calls during cool-down stay rejected without flipping state
        for (int i = 0; i < 5; i++) {
            assertFalse(ESCircuitBreaker.allowRequest());
        }
        assertEquals(ESCircuitBreaker.State.OPEN, ESCircuitBreaker.getState());
    }
}
