package org.apache.atlas.repository.graphdb.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Throttles concurrent async Cassandra queries using a semaphore.
 * Prevents spikes of hundreds of in-flight requests to Cassandra
 * by capping concurrency at a configurable limit.
 */
public class CassandraAsyncThrottle {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraAsyncThrottle.class);

    private final Semaphore semaphore;
    private final int maxConcurrent;

    public CassandraAsyncThrottle(int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
        this.semaphore = new Semaphore(maxConcurrent);
    }

    /**
     * Executes an async Cassandra call with bounded concurrency.
     * Blocks the calling thread if maxConcurrent queries are already in-flight.
     * Releases the permit when the async call completes (success or failure).
     */
    public <T> CompletionStage<T> throttle(Supplier<CompletionStage<T>> asyncCall) {
        semaphore.acquireUninterruptibly();
        try {
            return asyncCall.get().whenComplete((r, ex) -> semaphore.release());
        } catch (Exception e) {
            semaphore.release();
            throw e;
        }
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }

    public int getAvailablePermits() {
        return semaphore.availablePermits();
    }
}
