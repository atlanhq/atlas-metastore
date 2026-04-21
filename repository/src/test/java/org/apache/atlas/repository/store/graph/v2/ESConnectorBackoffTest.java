package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ESConnector#computeBackoffMs(int)}.
 *
 * <p>Validates that the exponential backoff doubles each retry, is capped at
 * the configured maximum, and that jitter — when enabled — keeps the result
 * within [capped/2, capped) so callers cannot block longer than the cap.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ESConnectorBackoffTest {

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            // Defaults under test: initial 1s, max 60s, jitter on.
            config.setProperty("atlas.es.retry.delay.ms", 1000);
            config.setProperty("atlas.es.retry.max.delay.ms", 60000);
            config.setProperty("atlas.es.retry.jitter.enabled", true);
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    void tearDown() {
        ApplicationProperties.forceReload();
    }

    @Test
    void firstRetryRespectsInitialDelay() {
        // retryCount=1 → base = initial = 1000ms, jittered ∈ [500, 1000)
        for (int i = 0; i < 50; i++) {
            long delay = ESConnector.computeBackoffMs(1);
            assertTrue(delay >= 500 && delay < 1000,
                    "First retry delay must be in [500, 1000) with jitter on, got " + delay);
        }
    }

    @Test
    void backoffDoublesEachRetry() {
        // retryCount=4 → base = 1000 * 2^3 = 8000ms, jittered ∈ [4000, 8000)
        for (int i = 0; i < 50; i++) {
            long delay = ESConnector.computeBackoffMs(4);
            assertTrue(delay >= 4000 && delay < 8000,
                    "4th retry delay must be in [4000, 8000) with jitter on, got " + delay);
        }
    }

    @Test
    void capsAtMaxDelay() {
        // retryCount=20 → base = 1000 * 2^19 ≫ max(60000), so capped at 60000
        // With jitter: ∈ [30000, 60000)
        for (int i = 0; i < 50; i++) {
            long delay = ESConnector.computeBackoffMs(20);
            assertTrue(delay >= 30000 && delay < 60000,
                    "Capped retry delay must be in [30000, 60000) with jitter on, got " + delay);
        }
    }

    @Test
    void zeroRetryCountClampsToInitial() {
        // retryCount=0 should not produce a negative-power exponent
        for (int i = 0; i < 20; i++) {
            long delay = ESConnector.computeBackoffMs(0);
            assertTrue(delay >= 500 && delay < 1000,
                    "Retry count 0 should clamp to initial delay (1000ms with jitter), got " + delay);
        }
    }

    @Test
    void negativeRetryCountDoesNotExplode() {
        // Defensive: should clamp at initial delay, not produce nonsense
        long delay = ESConnector.computeBackoffMs(-5);
        assertTrue(delay >= 500 && delay < 1000,
                "Negative retry count should clamp to initial delay, got " + delay);
    }
}
