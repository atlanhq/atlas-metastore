package org.apache.atlas.repository.graphdb.cassandra;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

/**
 * Tests for CassandraAsyncThrottle — verifies that concurrency
 * is bounded by the semaphore and permits are released correctly.
 */
public class CassandraAsyncThrottleTest {

    @Test
    public void throttle_capsMaxConcurrent() throws InterruptedException {
        final int maxConcurrent = 3;
        CassandraAsyncThrottle throttle = new CassandraAsyncThrottle(maxConcurrent);

        AtomicInteger peakConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);

        List<CompletableFuture<Void>> controllers = new ArrayList<>();
        List<CompletionStage<Void>> throttled = new ArrayList<>();

        // Fire 10 tasks from separate threads — each blocked on its own controller future
        for (int i = 0; i < 10; i++) {
            CompletableFuture<Void> controller = new CompletableFuture<>();
            controllers.add(controller);

            Thread t = new Thread(() -> {
                CompletionStage<Void> stage = throttle.throttle(() -> {
                    int cur = currentConcurrent.incrementAndGet();
                    peakConcurrent.updateAndGet(prev -> Math.max(prev, cur));
                    return controller.thenRun(currentConcurrent::decrementAndGet);
                });
                synchronized (throttled) {
                    throttled.add(stage);
                }
            });
            t.setDaemon(true);
            t.start();
        }

        // Wait for the first batch to acquire permits
        Thread.sleep(300);

        // Only maxConcurrent should have started; the rest blocked on the semaphore
        assertTrue(currentConcurrent.get() <= maxConcurrent,
                "Current concurrent should not exceed " + maxConcurrent + " but was " + currentConcurrent.get());

        // Complete all controllers to unblock everything
        controllers.forEach(f -> f.complete(null));

        // Wait for all threads to finish
        Thread.sleep(500);

        assertTrue(peakConcurrent.get() <= maxConcurrent,
                "Peak concurrent should not exceed " + maxConcurrent + " but was " + peakConcurrent.get());
        assertEquals(throttle.getAvailablePermits(), maxConcurrent,
                "All permits should be released after completion");
    }

    @Test
    public void throttle_releasesPermitOnSuccess() {
        CassandraAsyncThrottle throttle = new CassandraAsyncThrottle(5);

        CompletionStage<String> result = throttle.throttle(
                () -> CompletableFuture.completedFuture("done"));

        assertEquals(result.toCompletableFuture().join(), "done");
        assertEquals(throttle.getAvailablePermits(), 5);
    }

    @Test
    public void throttle_releasesPermitOnAsyncFailure() {
        CassandraAsyncThrottle throttle = new CassandraAsyncThrottle(5);

        CompletableFuture<String> failing = new CompletableFuture<>();
        failing.completeExceptionally(new RuntimeException("test error"));

        CompletionStage<String> result = throttle.throttle(() -> failing);

        try {
            result.toCompletableFuture().join();
            fail("Expected exception");
        } catch (Exception e) {
            // expected
        }
        assertEquals(throttle.getAvailablePermits(), 5);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void throttle_releasesPermitOnSupplierException() {
        CassandraAsyncThrottle throttle = new CassandraAsyncThrottle(5);

        try {
            throttle.throttle(() -> { throw new RuntimeException("supplier boom"); });
        } finally {
            assertEquals(throttle.getAvailablePermits(), 5,
                    "Permit should be released even when supplier throws synchronously");
        }
    }

    @Test
    public void getMaxConcurrent_returnsConfiguredValue() {
        assertEquals(new CassandraAsyncThrottle(100).getMaxConcurrent(), 100);
        assertEquals(new CassandraAsyncThrottle(1).getMaxConcurrent(), 1);
    }
}
