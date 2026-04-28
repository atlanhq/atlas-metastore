package org.apache.atlas.repository.assetsync;

import java.time.Instant;
import java.util.Objects;

/**
 * A single entry in the failure-only outbox (MS-1010).
 *
 * <p>An entry represents one logical operation that failed inline and needs
 * eventual delivery. The {@code payload} is opaque to the outbox itself — it
 * is the consumer's job to interpret it and replay the downstream write.</p>
 *
 * @param <T> the consumer-defined payload type
 */
public final class OutboxEntry<T> {

    private final OutboxEntryId id;
    private final T             payload;
    private final int           attemptCount;
    private final Instant       createdAt;
    private final Instant       lastAttemptedAt;

    public OutboxEntry(OutboxEntryId id, T payload, int attemptCount,
                       Instant createdAt, Instant lastAttemptedAt) {
        this.id              = Objects.requireNonNull(id, "id");
        this.payload         = payload;
        this.attemptCount    = attemptCount;
        this.createdAt       = createdAt != null ? createdAt : Instant.now();
        this.lastAttemptedAt = lastAttemptedAt;
    }

    public OutboxEntryId getId()              { return id; }
    public T             getPayload()         { return payload; }
    public int           getAttemptCount()    { return attemptCount; }
    public Instant       getCreatedAt()       { return createdAt; }
    public Instant       getLastAttemptedAt() { return lastAttemptedAt; }
}
