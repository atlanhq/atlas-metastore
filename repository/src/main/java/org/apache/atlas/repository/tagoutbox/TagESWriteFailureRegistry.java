package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.model.ESDeferredOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Tag-scoped surface point for ES write failures originating from tag paths.
 *
 * <p>Parallel to {@code ESWriteFailureRegistry} (PR #6568, MS-1010) but dedicated
 * to the tag subsystem. The two registries are <b>fully independent</b>: separate
 * classes, separate static sink slots, separate payload types. A tag-path failure
 * recorded here can never reach an asset-sync consumer, and vice versa. This is
 * the mechanical guarantee that prevents cross-routing between the tag outbox
 * and the asset-sync outbox.</p>
 *
 * <p>Integration contract, identical to the asset-sync registry:</p>
 * <ul>
 *     <li>Producers (tag-related ES-failure sites in {@code EntityMutationService}
 *         and {@code EntityGraphMapper}) call {@link #record(TagESWriteFailure)}
 *         when an inline ES write has failed and they want the failure durably
 *         captured for replay.</li>
 *     <li>One consumer ({@code TagOutboxFailureSink}, wired by
 *         {@code TagOutboxService}) installs itself via {@link #setSink(FailureSink)}.
 *         Single-slot by design — the tag subsystem has exactly one failure
 *         consumer (the outbox). If that changes later, evolve this class then.</li>
 *     <li>Until a sink is installed, the default no-op sink silently drops
 *         failures. Safe fallback for tests and for pods where the tag subsystem
 *         is disabled via {@code atlas.tag.outbox.enabled=false}.</li>
 * </ul>
 *
 * <p>The sink contract: it must be fast and never throw. Any exception thrown
 * by a sink is caught here so the original write path (inside a
 * {@code catch} block that's already handling a failure) is unaffected.</p>
 */
public final class TagESWriteFailureRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TagESWriteFailureRegistry.class);

    private static volatile FailureSink sink = failure -> {
        // Default no-op until TagOutboxService installs the tag-outbox sink.
    };

    private TagESWriteFailureRegistry() {}

    /**
     * Install a sink for surfaced failures. {@code TagOutboxService} calls this
     * during bootstrap to wire the tag-outbox consumer in. Unit tests may also
     * use this to intercept recorded failures. Passing {@code null} reverts to
     * the default no-op sink.
     */
    public static void setSink(FailureSink newSink) {
        sink = (newSink != null) ? newSink : failure -> {};
    }

    /**
     * Surface a failure to the registered sink. Always safe to call — exceptions
     * from the sink are caught and logged so the caller's write path is never
     * affected by sink misbehaviour.
     */
    public static void record(TagESWriteFailure failure) {
        if (failure == null) return;
        try {
            sink.accept(failure);
        } catch (Throwable t) {
            LOG.error("TagESWriteFailureRegistry sink threw — failure not durably captured (stage={}, vertices={})",
                    failure.stage, failure.failedVertexIds.size(), t);
        }
    }

    @FunctionalInterface
    public interface FailureSink {
        void accept(TagESWriteFailure failure);
    }

    /**
     * Snapshot of a failed tag-related ES write. Carries enough information for
     * the outbox sink to extract entity IDs and enqueue a replay.
     *
     * <p>Intentionally a separate type from the asset-sync registry's
     * {@code ESWriteFailure} even though the fields line up — separate types
     * prevent accidental cross-registry routing at the compiler level.</p>
     */
    public static final class TagESWriteFailure {
        public final List<ESDeferredOperation> operations;
        public final List<String>              failedVertexIds;
        public final Throwable                 cause;
        public final String                    stage;
        public final long                      timestampMs;

        public TagESWriteFailure(List<ESDeferredOperation> operations,
                                 List<String> failedVertexIds,
                                 Throwable cause,
                                 String stage) {
            this.operations      = operations      != null ? operations      : Collections.emptyList();
            this.failedVertexIds = failedVertexIds != null ? failedVertexIds : Collections.emptyList();
            this.cause           = cause;
            this.stage           = stage != null ? stage : "unknown";
            this.timestampMs     = System.currentTimeMillis();
        }
    }
}
