package org.apache.atlas.repository.assetsync;

import java.util.List;

/**
 * Generic failure-only outbox abstraction (MS-1010).
 *
 * <p>Producers of dual-write operations call {@link #enqueue(OutboxEntry)} only when
 * a downstream write (e.g., Elasticsearch) has failed inline retries. A single
 * background relay then claims pending entries, hands them to an
 * {@link OutboxConsumer} for replay, and resolves them based on the result.</p>
 *
 * <p>Implementations are responsible for durability, claim/release semantics, and
 * concurrency safety across pods. Consumers do not need to know whether the
 * underlying store is Cassandra, JanusGraph, or anything else.</p>
 *
 * @param <T> the payload type carried by entries in this outbox
 */
public interface Outbox<T> {

    /**
     * Persist an entry that needs eventual delivery. Idempotency is the
     * implementation's responsibility — duplicate logical entries from
     * multiple writers must not produce duplicate downstream writes.
     */
    void enqueue(OutboxEntry<T> entry);

    /**
     * Claim a batch of pending entries for processing. Implementations must
     * mark claimed entries so concurrent claimers do not pick the same rows.
     * Empty list signals "nothing to do."
     *
     * @param batchSize maximum number of entries to claim
     */
    List<OutboxEntry<T>> claim(int batchSize);

    /**
     * Mark an entry permanently delivered. The implementation removes it
     * from the outbox.
     */
    void markDone(OutboxEntryId id);

    /**
     * Mark an entry as failed permanently (max retries exhausted, or a
     * non-retryable error). The implementation moves it to a FAILED state
     * for downstream triage by reconciliation.
     */
    void markFailed(OutboxEntryId id, int finalAttemptCount, Throwable cause);

    /**
     * Increment the attempt count and release the claim so the entry is
     * eligible to be re-claimed after its backoff window. Used for
     * retryable failures (5xx, 429, IO).
     */
    void releaseForRetry(OutboxEntryId id, int newAttemptCount);

    /**
     * Release a claim without changing the attempt count or status. Used
     * during graceful shutdown to hand entries back to the next claimer.
     */
    void releaseClaim(OutboxEntryId id);
}
