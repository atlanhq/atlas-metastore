package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.ESDeferredOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ESWriteFailureRegistry}.
 *
 * <p>Validates the sink installation contract used by MS-1010 to plug the
 * Cassandra outbox writer in: a sink can be installed; nulls reset to the
 * default no-op; sink exceptions are caught so the caller's write path is
 * never affected.</p>
 */
class ESWriteFailureRegistryTest {

    @AfterEach
    void resetSink() {
        // Restore default no-op sink so tests are independent
        ESWriteFailureRegistry.setSink(null);
    }

    @Test
    void defaultSinkIsNoop() {
        // Should not throw with no sink installed
        ESWriteFailureRegistry.record(new ESWriteFailureRegistry.ESWriteFailure(
                Collections.emptyList(), Collections.emptyList(), new RuntimeException("x"), "test"));
    }

    @Test
    void installedSinkReceivesFailure() {
        AtomicReference<ESWriteFailureRegistry.ESWriteFailure> received = new AtomicReference<>();
        ESWriteFailureRegistry.setSink(received::set);

        RuntimeException cause = new RuntimeException("ES exploded");
        ESWriteFailureRegistry.record(new ESWriteFailureRegistry.ESWriteFailure(
                Collections.emptyList(), List.of("v1", "v2"), cause, "post-processing"));

        ESWriteFailureRegistry.ESWriteFailure f = received.get();
        assertNotNull(f);
        assertEquals("post-processing", f.stage);
        assertSame(cause, f.cause);
        assertEquals(2, f.failedVertexIds.size());
        assertTrue(f.timestampMs > 0);
    }

    @Test
    void sinkExceptionsAreSwallowed() {
        ESWriteFailureRegistry.setSink(failure -> {
            throw new IllegalStateException("sink is broken");
        });

        // Caller must not see the sink's exception — the write path's safety depends on this
        assertDoesNotThrow(() -> ESWriteFailureRegistry.record(new ESWriteFailureRegistry.ESWriteFailure(
                Collections.emptyList(), Collections.emptyList(), new RuntimeException(), "test")));
    }

    @Test
    void recordingNullFailureIsNoop() {
        AtomicReference<ESWriteFailureRegistry.ESWriteFailure> received = new AtomicReference<>();
        ESWriteFailureRegistry.setSink(received::set);

        ESWriteFailureRegistry.record(null);

        assertNull(received.get());
    }

    @Test
    void settingNullSinkResetsToNoop() {
        AtomicReference<ESWriteFailureRegistry.ESWriteFailure> received = new AtomicReference<>();
        ESWriteFailureRegistry.setSink(received::set);
        ESWriteFailureRegistry.setSink(null);

        ESWriteFailureRegistry.record(new ESWriteFailureRegistry.ESWriteFailure(
                Collections.emptyList(), Collections.emptyList(), new RuntimeException(), "test"));

        assertNull(received.get(), "After setSink(null), records must not reach the previous sink");
    }

    @Test
    void failureCarriesOperationsForReplay() {
        AtomicReference<ESWriteFailureRegistry.ESWriteFailure> received = new AtomicReference<>();
        ESWriteFailureRegistry.setSink(received::set);

        ESDeferredOperation op = new ESDeferredOperation(
                ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS,
                "entity-1",
                Collections.emptyMap());
        ESWriteFailureRegistry.record(new ESWriteFailureRegistry.ESWriteFailure(
                List.of(op), Collections.emptyList(), new RuntimeException(), "post-processing"));

        ESWriteFailureRegistry.ESWriteFailure f = received.get();
        assertEquals(1, f.operations.size());
        assertEquals("entity-1", f.operations.get(0).getEntityId());
    }
}
