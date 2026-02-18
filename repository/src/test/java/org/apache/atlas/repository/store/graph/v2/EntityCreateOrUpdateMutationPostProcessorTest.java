package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.model.ESDeferredOperation.OperationType;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for EntityCreateOrUpdateMutationPostProcessor.
 *
 * Tests the MS-456 fix: when multiple ES operations target the same entity,
 * only the operation with the most complete data should be written.
 * Priority: ADD > UPDATE > DELETE (ADD runs last in commitChanges, has final state)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityCreateOrUpdateMutationPostProcessorTest {

    private EntityCreateOrUpdateMutationPostProcessor processor;
    private EntityCreateOrUpdateMutationPostProcessor.ESTagWriter esTagWriter;

    @BeforeAll
    void initApplicationProperties() throws AtlasException {
        // Ensure AtlasConfiguration can initialize in IDE/Surefire runs where atlas.conf is not set.
        Configuration testConfig = new PropertiesConfiguration();
        ApplicationProperties.set(testConfig);
    }

    @BeforeEach
    void setUp() {
        esTagWriter = mock(EntityCreateOrUpdateMutationPostProcessor.ESTagWriter.class);
        processor = new EntityCreateOrUpdateMutationPostProcessor(mock(TagDAO.class), esTagWriter);
    }

    @AfterAll
    void resetApplicationProperties() {
        ApplicationProperties.forceReload();
    }

    // =================== Single Operation Tests ===================

    @Test
    void testSingleAddOperation_writesWithUpsertTrue() {
        ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

        processor.executeESOperations(List.of(addOp));

        // ADD operations use upsert=true
        verify(esTagWriter, times(1)).writeTagProperties(anyMap(), eq(true));
        verify(esTagWriter, never()).writeTagProperties(anyMap(), eq(false));
    }

    @Test
    void testSingleDeleteOperation_writesWithUpsertFalse() {
        ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

        processor.executeESOperations(List.of(deleteOp));

        // DELETE operations use upsert=false
        verify(esTagWriter, times(1)).writeTagProperties(anyMap(), eq(false));
        verify(esTagWriter, never()).writeTagProperties(anyMap(), eq(true));
    }

    @Test
    void testSingleUpdateOperation_writesWithUpsertFalse() {
        ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");

        processor.executeESOperations(List.of(updateOp));

        // UPDATE operations use upsert=false
        verify(esTagWriter, times(1)).writeTagProperties(anyMap(), eq(false));
        verify(esTagWriter, never()).writeTagProperties(anyMap(), eq(true));
    }

    // =================== Same Entity Deduplication Tests ===================

    @Test
    void testDeleteThenAdd_sameEntity_onlyAddWritten() {
        // Simulates: tags [A,C] -> [B] where DELETE runs before ADD in commitChanges
        // DELETE(A) creates payload "", ADD(B) creates payload "|B|"
        // Only ADD should be written (has complete final state)
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");
        ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

        processor.executeESOperations(List.of(deleteOp, addOp));

        // Only ADD batch written (upsert=true)
        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(true));
        verify(esTagWriter, never()).writeTagProperties(anyMap(), eq(false));

        Map<String, Map<String, Object>> writtenPayload = payloadCaptor.getValue();
        assertEquals("|TagB|", writtenPayload.get("e1").get("__classificationNames"));
    }

    @Test
    void testAddThenDelete_sameEntity_onlyAddWritten() {
        // Order independence: even if ADD comes first in the list, it should still win
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");
        ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

        processor.executeESOperations(List.of(addOp, deleteOp));

        // ADD wins by priority, not by position
        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(true));
        assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    @Test
    void testMultipleDeletes_sameEntity_lastDeleteWritten() {
        // Multiple DELETEs for same entity: later one has more recent Cassandra state
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation delete1 = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "|TagC|");
        ESDeferredOperation delete2 = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

        processor.executeESOperations(List.of(delete1, delete2));

        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(false));
        // Last DELETE's payload should be used
        assertEquals("", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    @Test
    void testDeleteThenUpdate_sameEntity_updateWins() {
        // UPDATE runs after DELETE in commitChanges, so UPDATE has more recent state
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");
        ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");

        processor.executeESOperations(List.of(deleteOp, updateOp));

        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(false));
        assertEquals("|TagA|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    @Test
    void testUpdateThenDelete_sameEntity_updateWins() {
        // UPDATE has higher priority than DELETE regardless of order
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");
        ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

        processor.executeESOperations(List.of(updateOp, deleteOp));

        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(false));
        assertEquals("|TagA|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    @Test
    void testUpdateThenAdd_sameEntity_addWins() {
        // ADD has highest priority
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");
        ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

        processor.executeESOperations(List.of(updateOp, addOp));

        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(true));
        assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    // =================== Multiple Entity Tests ===================

    @Test
    void testMultipleEntities_eachDeduplicatedIndependently() {
        // e1: DELETE + UPDATE -> UPDATE wins
        // e2: ADD only -> ADD written
        // e3: DELETE only -> DELETE written
        ArgumentCaptor<Map<String, Map<String, Object>>> addCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Map<String, Object>>> otherCaptor = ArgumentCaptor.forClass(Map.class);

        List<ESDeferredOperation> ops = List.of(
                createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", ""),
                createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e2", "|TagX|"),
                createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|"),
                createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e3", "")
        );

        processor.executeESOperations(ops);

        // ADD batch (e2) with upsert=true
        verify(esTagWriter, times(1)).writeTagProperties(addCaptor.capture(), eq(true));
        // Other batch (e1 UPDATE, e3 DELETE) with upsert=false
        verify(esTagWriter, times(1)).writeTagProperties(otherCaptor.capture(), eq(false));

        Map<String, Map<String, Object>> addPayload = addCaptor.getValue();
        assertEquals(1, addPayload.size());
        assertEquals("|TagX|", addPayload.get("e2").get("__classificationNames"));

        Map<String, Map<String, Object>> otherPayload = otherCaptor.getValue();
        assertEquals(2, otherPayload.size());
        assertEquals("|TagA|", otherPayload.get("e1").get("__classificationNames"));
        assertEquals("", otherPayload.get("e3").get("__classificationNames"));
    }

    // =================== Edge Cases ===================

    @Test
    void testEmptyOperationsList_noESCalls() {
        processor.executeESOperations(Collections.emptyList());

        verifyNoInteractions(esTagWriter);
    }

    @Test
    void testNullOperationsList_noESCalls() {
        processor.executeESOperations(null);

        verifyNoInteractions(esTagWriter);
    }

    @Test
    void testAllThreeOperationTypes_sameEntity_addWins() {
        // DELETE -> UPDATE -> ADD for same entity: ADD should win
        ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

        List<ESDeferredOperation> ops = List.of(
                createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", ""),
                createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|"),
                createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|")
        );

        processor.executeESOperations(ops);

        // Only ADD batch
        verify(esTagWriter, times(1)).writeTagProperties(payloadCaptor.capture(), eq(true));
        verify(esTagWriter, never()).writeTagProperties(anyMap(), eq(false));

        assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
    }

    private ESDeferredOperation createOperation(OperationType type, String entityId, String classificationNames) {
        Map<String, Object> entityPayload = new HashMap<>();
        entityPayload.put("__classificationNames", classificationNames);

        Map<String, Map<String, Object>> payload = new HashMap<>();
        payload.put(entityId, entityPayload);

        return new ESDeferredOperation(type, entityId, payload);
    }
}
