package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.ESDeferredOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Surface point for ES write failures that have exhausted inline retries.
 *
 * <p>This is the integration boundary between Layer 1 (inline hardening,
 * MS-1009) and Layer 2 (failure-only outbox, MS-1010). When the inline ES
 * write path determines that a request has finally failed, it calls
 * {@link #record(ESWriteFailure)} instead of silently logging-and-discarding
 * the exception.</p>
 *
 * <p>Until MS-1010 lands, the default sink is a no-op (failures are still
 * logged at the call site, and Layer 2's Kafka DLQ continues to handle the
 * propagation path). MS-1010 will install a Cassandra-outbox sink via
 * {@link #setSink(FailureSink)}.</p>
 *
 * <p>The sink contract: it must be fast and never throw. Any exception
 * thrown by a sink is caught here so the original write path is unaffected.</p>
 */
public final class ESWriteFailureRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ESWriteFailureRegistry.class);

    private static volatile FailureSink sink = failure -> {
        // Default no-op until MS-1010 installs the Cassandra outbox sink.
    };

    private ESWriteFailureRegistry() {}

    /**
     * Install a sink for surfaced failures. MS-1010 calls this during bootstrap
     * to wire the Cassandra outbox writer in. Unit tests may also use this.
     */
    public static void setSink(FailureSink newSink) {
        sink = (newSink != null) ? newSink : failure -> {};
    }

    /**
     * Surface a failure to the registered sink. Always safe to call — exceptions
     * from the sink are caught and logged so the caller's write path is never
     * affected by sink misbehaviour.
     */
    public static void record(ESWriteFailure failure) {
        if (failure == null) return;
        try {
            sink.accept(failure);
        } catch (Throwable t) {
            LOG.error("ESWriteFailureRegistry sink threw — failure not durably captured (stage={}, vertices={})",
                    failure.stage, failure.failedVertexIds.size(), t);
        }
    }

    @FunctionalInterface
    public interface FailureSink {
        void accept(ESWriteFailure failure);
    }

    /**
     * Snapshot of a failed ES write. Carries enough information for the outbox
     * to replay the operation without re-reading from JanusGraph.
     */
    public static final class ESWriteFailure {
        public final List<ESDeferredOperation> operations;
        public final List<String>              failedVertexIds;
        public final Throwable                 cause;
        public final String                    stage;
        public final long                      timestampMs;

        public ESWriteFailure(List<ESDeferredOperation> operations,
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
