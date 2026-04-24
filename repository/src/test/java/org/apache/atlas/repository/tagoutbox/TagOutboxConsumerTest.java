package org.apache.atlas.repository.tagoutbox;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.repository.assetsync.ConsumeResult;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityCreateOrUpdateMutationPostProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Classification tests for {@link TagOutboxConsumer}. Verifies:
 * <ul>
 *     <li>A clean run classifies all GUIDs as {@code succeeded} and flushes
 *         deferred ES ops once per chunk.</li>
 *     <li>Per-GUID errors returned by {@code repairClassificationMappingsV2}
 *         are classified as {@code permanentlyFailed}.</li>
 *     <li>A thrown {@code AtlasBaseException} classifies every GUID in the
 *         chunk as {@code retryable}.</li>
 *     <li>{@code RequestContext.getESDeferredOperations().clear()} always
 *         runs in the finally block — deferred ops never leak between chunks.</li>
 *     <li>Chunking honours {@code config.consumerRepairBatchSize()}.</li>
 * </ul>
 */
class TagOutboxConsumerTest {

    private AtlasEntityStore                       entityStore;
    private EntityCreateOrUpdateMutationPostProcessor postProcessor;

    @BeforeEach
    void setUp() {
        // Fresh RequestContext per test — the consumer touches it directly.
        RequestContext.clear();
        RequestContext.get();

        entityStore   = mock(AtlasEntityStore.class);
        postProcessor = mock(EntityCreateOrUpdateMutationPostProcessor.class);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    /** Happy path: every GUID classified succeeded. One flush per chunk. */
    @Test
    void allSuccessClassification() throws Exception {
        TagOutboxConfig config = testConfig(/*repairBatch=*/ 100);
        TagOutboxConsumer consumer = new TagOutboxConsumer(entityStore, postProcessor, config);

        // repair returns an empty error map → all succeeded
        when(entityStore.repairClassificationMappingsV2(anyList())).thenReturn(Collections.emptyMap());

        // Stage a deferred op so the flush path is exercised
        stageDeferredOp("v1");

        List<OutboxEntry<EntityGuidRef>> batch = buildBatch("g1", "g2", "g3");
        ConsumeResult result = consumer.consume(batch);

        assertEquals(3, result.getSucceeded().size());
        assertTrue(result.getRetryable().isEmpty());
        assertTrue(result.getPermanentlyFailed().isEmpty());

        verify(entityStore, times(1)).repairClassificationMappingsV2(anyList());
        // Flush was called because deferred ops existed
        verify(postProcessor, times(1)).executeESOperations(anyList());
        // Finally block always clears
        assertTrue(RequestContext.get().getESDeferredOperations().isEmpty());
    }

    /** Per-GUID errors → permanentlyFailed. Un-errored GUIDs → succeeded. */
    @Test
    void perGuidErrorsClassifiedPermanent() throws Exception {
        TagOutboxConfig config = testConfig(/*repairBatch=*/ 100);
        TagOutboxConsumer consumer = new TagOutboxConsumer(entityStore, postProcessor, config);

        Map<String, String> errors = new HashMap<>();
        errors.put("g2", "mapping error");
        when(entityStore.repairClassificationMappingsV2(anyList())).thenReturn(errors);

        List<OutboxEntry<EntityGuidRef>> batch = buildBatch("g1", "g2", "g3");
        ConsumeResult result = consumer.consume(batch);

        assertEquals(2, result.getSucceeded().size());   // g1, g3
        assertEquals(1, result.getPermanentlyFailed().size()); // g2
        assertTrue(result.getRetryable().isEmpty());
    }

    /** Throw → all GUIDs in chunk classified retryable. Deferred ops still cleared. */
    @Test
    void thrownExceptionClassifiesAllRetryable() throws Exception {
        TagOutboxConfig config = testConfig(/*repairBatch=*/ 100);
        TagOutboxConsumer consumer = new TagOutboxConsumer(entityStore, postProcessor, config);

        doThrow(new AtlasBaseException("cassandra down"))
                .when(entityStore).repairClassificationMappingsV2(anyList());

        // Deferred op staged — verify it still gets cleared even on failure.
        stageDeferredOp("v1");

        List<OutboxEntry<EntityGuidRef>> batch = buildBatch("g1", "g2");
        ConsumeResult result = consumer.consume(batch);

        assertEquals(0, result.getSucceeded().size());
        assertEquals(2, result.getRetryable().size());
        assertTrue(result.getPermanentlyFailed().isEmpty());

        // Post-processor NEVER reached because repair threw before the flush point
        verify(postProcessor, never()).executeESOperations(anyList());
        // Load-bearing: finally clears RequestContext even on throw
        assertTrue(RequestContext.get().getESDeferredOperations().isEmpty());
    }

    /** Chunk boundary: 3 GUIDs with batch size 2 → two calls to repair. */
    @Test
    void chunksAccordingToRepairBatchSize() throws Exception {
        TagOutboxConfig config = testConfig(/*repairBatch=*/ 2);
        TagOutboxConsumer consumer = new TagOutboxConsumer(entityStore, postProcessor, config);

        when(entityStore.repairClassificationMappingsV2(anyList())).thenReturn(Collections.emptyMap());

        List<OutboxEntry<EntityGuidRef>> batch = buildBatch("g1", "g2", "g3");
        ConsumeResult result = consumer.consume(batch);

        assertEquals(3, result.getSucceeded().size());

        // Two separate calls: chunk [g1,g2] and chunk [g3]
        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(entityStore, times(2)).repairClassificationMappingsV2(captor.capture());
        assertEquals(2, captor.getAllValues().get(0).size());
        assertEquals(1, captor.getAllValues().get(1).size());
    }

    /** Empty batch → empty result. No calls. */
    @Test
    void emptyBatchReturnsEmptyResult() {
        TagOutboxConfig config = testConfig(100);
        TagOutboxConsumer consumer = new TagOutboxConsumer(entityStore, postProcessor, config);

        ConsumeResult result = consumer.consume(Collections.emptyList());

        assertTrue(result.getSucceeded().isEmpty());
        assertTrue(result.getRetryable().isEmpty());
        assertTrue(result.getPermanentlyFailed().isEmpty());
    }

    // -- helpers ------------------------------------------------------------

    private static List<OutboxEntry<EntityGuidRef>> buildBatch(String... guids) {
        List<OutboxEntry<EntityGuidRef>> batch = new ArrayList<>();
        Instant now = Instant.now();
        for (String g : guids) {
            batch.add(new OutboxEntry<>(
                    new OutboxEntryId(g, ""),
                    new EntityGuidRef(g),
                    0,
                    now,
                    null));
        }
        return batch;
    }

    private static void stageDeferredOp(String vertexId) {
        Map<String, Map<String, Object>> payload = new HashMap<>();
        payload.put(vertexId, Collections.emptyMap());
        RequestContext.get().addESDeferredOperation(new ESDeferredOperation(
                ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS,
                vertexId,
                payload));
    }

    /**
     * Minimal config — only the fields the consumer actually reads. Records
     * require every component set, so the others are filled with harmless
     * values.
     */
    private static TagOutboxConfig testConfig(int repairBatch) {
        return new TagOutboxConfig(
                true, true,
                "test_ks", 1, "test_outbox", "test_lease", 86400,
                10,
                30, 10,
                30, 2, 100, 500, 60,
                1000L, 60000L,
                3600, 300, 500, 1800,
                repairBatch,
                "test-relay", "test-relay", "test-reconciler", "test_");
    }
}
