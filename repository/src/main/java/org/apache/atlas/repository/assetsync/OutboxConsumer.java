package org.apache.atlas.repository.assetsync;

import java.util.List;

/**
 * Consumer of {@link OutboxEntry} batches (MS-1010).
 *
 * <p>The relay claims a batch from the {@link Outbox} and hands it to the
 * consumer. The consumer attempts the downstream write (e.g., ES bulk) and
 * returns a {@link ConsumeResult} describing per-entry outcomes. The relay
 * uses that result to call {@link Outbox#markDone}, {@link Outbox#releaseForRetry},
 * or {@link Outbox#markFailed} on each entry.</p>
 *
 * <p>Consumers must be thread-safe — though the relay is single-threaded today,
 * future implementations may parallelize.</p>
 */
public interface OutboxConsumer<T> {

    /**
     * Process a claimed batch. Implementations must classify each entry as
     * succeeded, retryable-failure, or permanent-failure. Implementations
     * must not throw — any unexpected exception should be wrapped in
     * a permanent failure for the affected entries.
     */
    ConsumeResult consume(List<OutboxEntry<T>> batch);
}
