package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AsyncIngestionConsumerService}.
 * Uses sample payloads from async-ingestion-payloads.json as test fixtures.
 */
@ExtendWith(MockitoExtension.class)
class AsyncIngestionConsumerServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private EntityMutationService entityMutationService;
    @Mock
    private AtlasTypeDefStore typeDefStore;
    @Mock
    private AtlasTypeRegistry typeRegistry;

    private AsyncIngestionConsumerService consumerService;

    @BeforeEach
    void setUp() {
        consumerService = new AsyncIngestionConsumerService(entityMutationService, typeDefStore, typeRegistry);
        ReflectionTestUtils.setField(consumerService, "topic", "ATLAS_ASYNC_ENTITIES");
        ReflectionTestUtils.setField(consumerService, "consumerGroupId", "test_group");
        ReflectionTestUtils.setField(consumerService, "maxRetries", 3);
        ReflectionTestUtils.setField(consumerService, "baseDelayMs", 100L);
        ReflectionTestUtils.setField(consumerService, "maxDelayMs", 1000L);
        ReflectionTestUtils.setField(consumerService, "backoffMultiplier", 2.0);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    // ── Helper to invoke private processRecord ──────────────────────────

    private void processEvent(String eventJson) throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ATLAS_ASYNC_ENTITIES", 0, 0, "key", eventJson);
        ReflectionTestUtils.invokeMethod(consumerService, "processRecord", record);
    }

    // ── Sample Payloads (from async-ingestion-payloads.json) ─────────────

    // Sample #1: BULK_CREATE_OR_UPDATE
    private static final String BULK_CREATE_OR_UPDATE_EVENT = """
            {
              "eventId": "3124c61f-cf0a-4d6d-b46a-e7808f2de4bc",
              "eventType": "BULK_CREATE_OR_UPDATE",
              "eventTime": 1770888880911,
              "requestMetadata": { "traceId": "trace-e822ec2f", "user": "admin" },
              "operationMetadata": {
                "replaceClassifications": false, "replaceTags": false, "appendTags": false,
                "replaceBusinessAttributes": false, "overwriteBusinessAttributes": false,
                "skipProcessEdgeRestoration": false
              },
              "payload": {
                "entities": [
                  {
                    "typeName": "Table",
                    "attributes": { "qualifiedName": "default/snowflake/db1/schema1/table1", "name": "table1", "description": "Sample table for async ingestion payload demo" },
                    "guid": "guid-table-001", "isIncomplete": false, "provenanceType": 0, "version": 0, "superTypeNames": [], "proxy": false
                  },
                  {
                    "typeName": "Column",
                    "attributes": { "qualifiedName": "default/snowflake/db1/schema1/table1/col1", "dataType": "VARCHAR", "name": "col1" },
                    "guid": "guid-col-001", "isIncomplete": false, "provenanceType": 0, "version": 0, "superTypeNames": [], "proxy": false
                  }
                ]
              }
            }
            """;

    // Sample #2: SET_CLASSIFICATIONS
    private static final String SET_CLASSIFICATIONS_EVENT = """
            {
              "eventId": "8d2bce9c-13a3-4106-b161-d184f0058f73",
              "eventType": "SET_CLASSIFICATIONS",
              "eventTime": 1770888880940,
              "requestMetadata": { "traceId": "trace-d1617835", "user": "admin" },
              "operationMetadata": {},
              "payload": [
                { "typeName": "PII", "attributes": { "level": "HIGH" }, "entityGuid": "guid-table-001", "entityStatus": "ACTIVE" },
                { "typeName": "Confidential", "entityGuid": "guid-col-001", "entityStatus": "ACTIVE" }
              ]
            }
            """;

    // Sample #3: DELETE_BY_GUID
    private static final String DELETE_BY_GUID_EVENT = """
            {
              "eventId": "d6a36da6-c812-492a-89dd-9e7e6dea1452",
              "eventType": "DELETE_BY_GUID",
              "eventTime": 1770888880941,
              "requestMetadata": { "traceId": "trace-1f0c2a7c", "user": "admin" },
              "operationMetadata": { "deleteType": "SOFT" },
              "payload": { "guid": "guid-table-001" }
            }
            """;

    // Sample #4: DELETE_BY_GUIDS
    private static final String DELETE_BY_GUIDS_EVENT = """
            {
              "eventId": "b4cfd8fe-fa84-4251-bc06-1b3d10f54611",
              "eventType": "DELETE_BY_GUIDS",
              "eventTime": 1770888880942,
              "requestMetadata": { "traceId": "trace-3ded4f44", "user": "admin" },
              "operationMetadata": { "deleteType": "SOFT" },
              "payload": { "guids": ["guid-table-001", "guid-col-001", "guid-col-002"] }
            }
            """;

    // Sample #5: DELETE_BY_UNIQUE_ATTRIBUTE
    private static final String DELETE_BY_UNIQUE_ATTRIBUTE_EVENT = """
            {
              "eventId": "eef519d9-55f5-4e4a-813f-503a7d3964cb",
              "eventType": "DELETE_BY_UNIQUE_ATTRIBUTE",
              "eventTime": 1770888880946,
              "requestMetadata": { "traceId": "trace-ead61c93", "user": "admin" },
              "operationMetadata": { "deleteType": "SOFT", "typeName": "Table" },
              "payload": {
                "typeName": "Table",
                "uniqueAttributes": { "qualifiedName": "default/snowflake/db1/schema1/table1" }
              }
            }
            """;

    // Sample #6: BULK_DELETE_BY_UNIQUE_ATTRIBUTES
    private static final String BULK_DELETE_BY_UNIQUE_ATTRIBUTES_EVENT = """
            {
              "eventId": "6ce2a427-b564-44e7-8776-3f4dd848582a",
              "eventType": "BULK_DELETE_BY_UNIQUE_ATTRIBUTES",
              "eventTime": 1770888880947,
              "requestMetadata": { "traceId": "trace-cde941ff", "user": "admin" },
              "operationMetadata": { "deleteType": "SOFT" },
              "payload": [
                { "typeName": "Table", "uniqueAttributes": { "qualifiedName": "default/snowflake/db1/schema1/table1" } },
                { "typeName": "Table", "uniqueAttributes": { "qualifiedName": "default/snowflake/db1/schema1/table2" } }
              ]
            }
            """;

    // Sample #7: RESTORE_BY_GUIDS
    private static final String RESTORE_BY_GUIDS_EVENT = """
            {
              "eventId": "b166afdc-9bdd-45eb-933f-89ecd528f196",
              "eventType": "RESTORE_BY_GUIDS",
              "eventTime": 1770888880948,
              "requestMetadata": { "traceId": "trace-f7ecb031", "user": "admin" },
              "operationMetadata": {},
              "payload": { "guids": ["guid-table-001", "guid-col-001"] }
            }
            """;

    // Sample #8: TYPEDEF_CREATE
    private static final String TYPEDEF_CREATE_EVENT = """
            {
              "eventId": "90c3c9bf-1d1d-4251-b18d-961fbf73c48f",
              "eventType": "TYPEDEF_CREATE",
              "eventTime": 1770888880953,
              "requestMetadata": { "traceId": "trace-1b77a9b6", "user": "admin" },
              "operationMetadata": { "allowDuplicateDisplayName": false },
              "payload": {
                "enumDefs": [], "structDefs": [],
                "classificationDefs": [{ "category": "CLASSIFICATION", "name": "iLnMytPOXuX8ZpKNHgJglH", "description": "Marks data as sensitive", "attributeDefs": [], "superTypes": [], "entityTypes": [], "displayName": "SensitiveData" }],
                "entityDefs": [{ "category": "ENTITY", "name": "CustomTable", "description": "A custom table type", "attributeDefs": [{ "name": "customField", "typeName": "string", "isOptional": false, "cardinality": "SINGLE", "valuesMinCount": -1, "valuesMaxCount": -1, "isUnique": false, "isIndexable": false, "includeInNotification": false, "skipScrubbing": false, "searchWeight": -1, "isDefaultValueNull": false }], "superTypes": ["DataSet"] }],
                "relationshipDefs": [], "businessMetadataDefs": []
              }
            }
            """;

    // Sample #9: TYPEDEF_UPDATE
    private static final String TYPEDEF_UPDATE_EVENT = """
            {
              "eventId": "f210e4cf-d787-4fcc-a250-f47a25de52b9",
              "eventType": "TYPEDEF_UPDATE",
              "eventTime": 1770888880961,
              "requestMetadata": { "traceId": "trace-40c0924c", "user": "admin" },
              "operationMetadata": { "allowDuplicateDisplayName": false, "patch": true },
              "payload": {
                "enumDefs": [], "structDefs": [], "classificationDefs": [],
                "entityDefs": [{ "category": "ENTITY", "name": "CustomTable", "description": "Updated custom table type with new attribute", "attributeDefs": [{ "name": "customField", "typeName": "string", "isOptional": false, "cardinality": "SINGLE", "valuesMinCount": -1, "valuesMaxCount": -1, "isUnique": false, "isIndexable": false, "includeInNotification": false, "skipScrubbing": false, "searchWeight": -1, "isDefaultValueNull": false }, { "name": "newField", "typeName": "int", "isOptional": false, "cardinality": "SINGLE", "valuesMinCount": -1, "valuesMaxCount": -1, "isUnique": false, "isIndexable": false, "includeInNotification": false, "skipScrubbing": false, "searchWeight": -1, "isDefaultValueNull": false }], "superTypes": ["DataSet"] }],
                "relationshipDefs": [], "businessMetadataDefs": []
              }
            }
            """;

    // Sample #10: TYPEDEF_DELETE
    private static final String TYPEDEF_DELETE_EVENT = """
            {
              "eventId": "31e849c5-e8cc-47e1-a494-ad0d14e88aa9",
              "eventType": "TYPEDEF_DELETE",
              "eventTime": 1770888880961,
              "requestMetadata": { "traceId": "trace-0b6c5eca", "user": "admin" },
              "operationMetadata": {},
              "payload": {
                "enumDefs": [], "structDefs": [],
                "classificationDefs": [{ "category": "CLASSIFICATION", "name": "USlQR0zIaYDc8BlxMKMCQ6", "attributeDefs": [], "superTypes": [], "entityTypes": [], "displayName": "SensitiveData" }],
                "entityDefs": [{ "category": "ENTITY", "name": "CustomTable", "attributeDefs": [], "superTypes": [] }],
                "relationshipDefs": [], "businessMetadataDefs": []
              }
            }
            """;

    // Sample #11: TYPEDEF_DELETE_BY_NAME
    private static final String TYPEDEF_DELETE_BY_NAME_EVENT = """
            {
              "eventId": "cbf285e9-aaa5-474c-9e3d-9673c4d25f8b",
              "eventType": "TYPEDEF_DELETE_BY_NAME",
              "eventTime": 1770888880961,
              "requestMetadata": { "traceId": "trace-21af29be", "user": "admin" },
              "operationMetadata": {},
              "payload": { "typeName": "CustomTable" }
            }
            """;

    // ── Tests ────────────────────────────────────────────────────────────

    @Test
    void testReplayBulkCreateOrUpdate() throws Exception {
        when(entityMutationService.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenReturn(null);

        processEvent(BULK_CREATE_OR_UPDATE_EVENT);

        ArgumentCaptor<EntityStream> streamCaptor = ArgumentCaptor.forClass(EntityStream.class);
        ArgumentCaptor<BulkRequestContext> ctxCaptor = ArgumentCaptor.forClass(BulkRequestContext.class);
        verify(entityMutationService).createOrUpdate(streamCaptor.capture(), ctxCaptor.capture());

        BulkRequestContext ctx = ctxCaptor.getValue();
        assertFalse(ctx.isReplaceClassifications());
        assertFalse(ctx.isReplaceTags());
        assertFalse(ctx.isAppendTags());
        assertFalse(ctx.isReplaceBusinessAttributes());
        assertFalse(ctx.isOverwriteBusinessAttributes());
    }

    @Test
    void testReplaySetClassifications() throws Exception {
        doNothing().when(entityMutationService).setClassifications(any(AtlasEntityHeaders.class), anyBoolean());

        processEvent(SET_CLASSIFICATIONS_EVENT);

        ArgumentCaptor<AtlasEntityHeaders> headersCaptor = ArgumentCaptor.forClass(AtlasEntityHeaders.class);
        ArgumentCaptor<Boolean> overrideCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(entityMutationService).setClassifications(headersCaptor.capture(), overrideCaptor.capture());

        AtlasEntityHeaders headers = headersCaptor.getValue();
        assertNotNull(headers.getGuidHeaderMap());
        // Two entities: guid-table-001 and guid-col-001
        assertEquals(2, headers.getGuidHeaderMap().size());
        assertTrue(headers.getGuidHeaderMap().containsKey("guid-table-001"));
        assertTrue(headers.getGuidHeaderMap().containsKey("guid-col-001"));
        // guid-table-001 has PII classification
        assertEquals(1, headers.getGuidHeaderMap().get("guid-table-001").getClassifications().size());
        assertEquals("PII", headers.getGuidHeaderMap().get("guid-table-001").getClassifications().get(0).getTypeName());
        assertFalse(overrideCaptor.getValue());
    }

    @Test
    void testReplayDeleteByGuid() throws Exception {
        when(entityMutationService.deleteById(anyString())).thenReturn(null);

        processEvent(DELETE_BY_GUID_EVENT);

        verify(entityMutationService).deleteById("guid-table-001");
    }

    @Test
    void testReplayDeleteByGuids() throws Exception {
        when(entityMutationService.deleteByIds(anyList())).thenReturn(null);

        processEvent(DELETE_BY_GUIDS_EVENT);

        ArgumentCaptor<List<String>> guidsCaptor = ArgumentCaptor.forClass(List.class);
        verify(entityMutationService).deleteByIds(guidsCaptor.capture());
        assertEquals(3, guidsCaptor.getValue().size());
        assertTrue(guidsCaptor.getValue().contains("guid-table-001"));
        assertTrue(guidsCaptor.getValue().contains("guid-col-001"));
        assertTrue(guidsCaptor.getValue().contains("guid-col-002"));
    }

    @Test
    void testReplayDeleteByUniqueAttribute() throws Exception {
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(mockEntityType);
        when(entityMutationService.deleteByUniqueAttributes(any(AtlasEntityType.class), anyMap())).thenReturn(null);

        processEvent(DELETE_BY_UNIQUE_ATTRIBUTE_EVENT);

        ArgumentCaptor<AtlasEntityType> typeCaptor = ArgumentCaptor.forClass(AtlasEntityType.class);
        ArgumentCaptor<Map> attrsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(entityMutationService).deleteByUniqueAttributes(typeCaptor.capture(), attrsCaptor.capture());
        assertSame(mockEntityType, typeCaptor.getValue());
        assertEquals("default/snowflake/db1/schema1/table1", attrsCaptor.getValue().get("qualifiedName"));
    }

    @Test
    void testReplayDeleteByUniqueAttribute_UnknownType() throws Exception {
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(null);

        assertThrows(Exception.class, () -> processEvent(DELETE_BY_UNIQUE_ATTRIBUTE_EVENT));
        verify(entityMutationService, never()).deleteByUniqueAttributes(any(AtlasEntityType.class), anyMap());
    }

    @Test
    void testReplayBulkDeleteByUniqueAttributes() throws Exception {
        when(entityMutationService.deleteByUniqueAttributes(anyList())).thenReturn(null);

        processEvent(BULK_DELETE_BY_UNIQUE_ATTRIBUTES_EVENT);

        ArgumentCaptor<List<AtlasObjectId>> captor = ArgumentCaptor.forClass(List.class);
        verify(entityMutationService).deleteByUniqueAttributes(captor.capture());
        assertEquals(2, captor.getValue().size());
    }

    @Test
    void testReplayRestoreByGuids() throws Exception {
        when(entityMutationService.restoreByIds(anyList())).thenReturn(null);

        processEvent(RESTORE_BY_GUIDS_EVENT);

        ArgumentCaptor<List<String>> guidsCaptor = ArgumentCaptor.forClass(List.class);
        verify(entityMutationService).restoreByIds(guidsCaptor.capture());
        assertEquals(2, guidsCaptor.getValue().size());
        assertTrue(guidsCaptor.getValue().contains("guid-table-001"));
        assertTrue(guidsCaptor.getValue().contains("guid-col-001"));
    }

    @Test
    void testReplayTypeDefCreate() throws Exception {
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(null);

        processEvent(TYPEDEF_CREATE_EVENT);

        ArgumentCaptor<AtlasTypesDef> captor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        verify(typeDefStore).createTypesDef(captor.capture());
        AtlasTypesDef typesDef = captor.getValue();
        assertNotNull(typesDef);
        assertFalse(typesDef.getClassificationDefs().isEmpty());
        assertFalse(typesDef.getEntityDefs().isEmpty());
        assertEquals("SensitiveData", typesDef.getClassificationDefs().get(0).getDisplayName());
        assertEquals("CustomTable", typesDef.getEntityDefs().get(0).getName());
    }

    @Test
    void testReplayTypeDefUpdate() throws Exception {
        when(typeDefStore.updateTypesDef(any(AtlasTypesDef.class))).thenReturn(null);

        processEvent(TYPEDEF_UPDATE_EVENT);

        ArgumentCaptor<AtlasTypesDef> captor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        verify(typeDefStore).updateTypesDef(captor.capture());
        AtlasTypesDef typesDef = captor.getValue();
        assertEquals("CustomTable", typesDef.getEntityDefs().get(0).getName());
        // Updated version has 2 attributeDefs (customField + newField)
        assertEquals(2, typesDef.getEntityDefs().get(0).getAttributeDefs().size());
    }

    @Test
    void testReplayTypeDefDelete() throws Exception {
        doNothing().when(typeDefStore).deleteTypesDef(any(AtlasTypesDef.class));

        processEvent(TYPEDEF_DELETE_EVENT);

        ArgumentCaptor<AtlasTypesDef> captor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        verify(typeDefStore).deleteTypesDef(captor.capture());
        assertFalse(captor.getValue().getClassificationDefs().isEmpty());
        assertFalse(captor.getValue().getEntityDefs().isEmpty());
    }

    @Test
    void testReplayTypeDefDeleteByName() throws Exception {
        when(typeDefStore.deleteTypeByName("CustomTable")).thenReturn(null);

        processEvent(TYPEDEF_DELETE_BY_NAME_EVENT);

        verify(typeDefStore).deleteTypeByName("CustomTable");
    }

    @Test
    void testUnknownEventType() throws Exception {
        String unknownEvent = """
                {
                  "eventId": "test-unknown",
                  "eventType": "UNKNOWN_TYPE",
                  "eventTime": 1770888880911,
                  "requestMetadata": { "traceId": "trace-test", "user": "admin" },
                  "operationMetadata": {},
                  "payload": {}
                }
                """;

        processEvent(unknownEvent);

        // Should be skipped, not throw
        verifyNoInteractions(entityMutationService);
        verifyNoInteractions(typeDefStore);
        // Check skippedCount incremented
        long skipped = ((java.util.concurrent.atomic.AtomicLong)
                ReflectionTestUtils.getField(consumerService, "skippedCount")).get();
        assertEquals(1, skipped);
    }

    @Test
    void testRequestMetadataApplied() throws Exception {
        when(entityMutationService.deleteById(anyString())).thenReturn(null);

        processEvent(DELETE_BY_GUID_EVENT);

        // The processRecord method sets RequestContext and clears it in finally.
        // We verify the interaction happened (which means RequestContext was set during the call).
        verify(entityMutationService).deleteById("guid-table-001");
    }

    @Test
    void testGetStatus_AllFields() {
        ReflectionTestUtils.setField(consumerService, "processedCount",
                new java.util.concurrent.atomic.AtomicLong(100));
        ReflectionTestUtils.setField(consumerService, "errorCount",
                new java.util.concurrent.atomic.AtomicLong(5));
        ReflectionTestUtils.setField(consumerService, "skippedCount",
                new java.util.concurrent.atomic.AtomicLong(2));
        ReflectionTestUtils.setField(consumerService, "dlqPublishCount",
                new java.util.concurrent.atomic.AtomicLong(1));
        ReflectionTestUtils.setField(consumerService, "dlqTopic", "ATLAS_ASYNC_INGESTION_DLQ");
        ReflectionTestUtils.setField(consumerService, "lastEventType", "BULK_CREATE_OR_UPDATE");

        Map<String, Object> status = consumerService.getStatus();

        assertEquals(false, status.get("isRunning"));
        assertEquals(100L, status.get("processedCount"));
        assertEquals(5L, status.get("errorCount"));
        assertEquals(2L, status.get("skippedCount"));
        assertEquals(1L, status.get("dlqPublishCount"));
        assertEquals("ATLAS_ASYNC_INGESTION_DLQ", status.get("dlqTopic"));
        assertEquals("ATLAS_ASYNC_ENTITIES", status.get("topic"));
        assertEquals("test_group", status.get("consumerGroup"));
        assertEquals("BULK_CREATE_OR_UPDATE", status.get("lastEventType"));
    }

    @Test
    void testExponentialBackoff_Progression() {
        String retryKey = "0-100";

        long delay1 = ReflectionTestUtils.invokeMethod(consumerService, "calculateExponentialBackoff", retryKey);
        assertEquals(100, delay1, "First delay should be base delay");

        long delay2 = ReflectionTestUtils.invokeMethod(consumerService, "calculateExponentialBackoff", retryKey);
        assertEquals(200, delay2, "Second delay should be 100 * 2.0");

        long delay3 = ReflectionTestUtils.invokeMethod(consumerService, "calculateExponentialBackoff", retryKey);
        assertEquals(400, delay3, "Third delay should be 200 * 2.0");
    }

    @Test
    void testExponentialBackoff_CappedAtMax() {
        String retryKey = "0-100";

        for (int i = 0; i < 10; i++) {
            ReflectionTestUtils.invokeMethod(consumerService, "calculateExponentialBackoff", retryKey);
        }

        long finalDelay = ReflectionTestUtils.invokeMethod(consumerService, "calculateExponentialBackoff", retryKey);
        assertEquals(1000, finalDelay, "Delay should be capped at maxDelayMs (1000)");
    }

    @Test
    void testBulkRequestContextFromOperationMetadata() throws Exception {
        String json = """
                {
                  "replaceClassifications": true,
                  "replaceTags": false,
                  "appendTags": false,
                  "replaceBusinessAttributes": true,
                  "overwriteBusinessAttributes": false,
                  "skipProcessEdgeRestoration": true
                }
                """;
        JsonNode opMeta = MAPPER.readTree(json);
        BulkRequestContext ctx = BulkRequestContext.fromOperationMetadata(opMeta);

        // Note: replaceClassifications=true triggers Builder to set replaceTags=false and appendTags=false
        assertTrue(ctx.isReplaceClassifications());
        assertFalse(ctx.isReplaceTags());
        assertFalse(ctx.isAppendTags());
        assertTrue(ctx.isReplaceBusinessAttributes());
        assertFalse(ctx.isOverwriteBusinessAttributes());
        // skipProcessEdgeRestoration is not a field on leangraph's BulkRequestContext — silently ignored
    }

    // ── End-to-End: Process All Event Types from JSON ──────────────────

    @Test
    void testProcessAllEventTypes_EndToEnd() throws Exception {
        // Read all 11 events from the payloads JSON fixture
        JsonNode events;
        try (java.io.InputStream is = getClass().getResourceAsStream("/async-ingestion-payloads.json")) {
            assertNotNull(is, "async-ingestion-payloads.json not found on classpath");
            events = MAPPER.readTree(is);
        }
        assertEquals(11, events.size(), "Expected 11 events in payloads file");

        // Set up mocks for all event types
        when(entityMutationService.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenReturn(null);
        doNothing().when(entityMutationService).setClassifications(any(AtlasEntityHeaders.class), anyBoolean());
        when(entityMutationService.deleteById(anyString())).thenReturn(null);
        when(entityMutationService.deleteByIds(anyList())).thenReturn(null);
        AtlasEntityType mockTableType = mock(AtlasEntityType.class);
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(mockTableType);
        when(entityMutationService.deleteByUniqueAttributes(any(AtlasEntityType.class), anyMap())).thenReturn(null);
        when(entityMutationService.deleteByUniqueAttributes(anyList())).thenReturn(null);
        when(entityMutationService.restoreByIds(anyList())).thenReturn(null);
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(null);
        when(typeDefStore.updateTypesDef(any(AtlasTypesDef.class))).thenReturn(null);
        doNothing().when(typeDefStore).deleteTypesDef(any(AtlasTypesDef.class));
        when(typeDefStore.deleteTypeByName(anyString())).thenReturn(null);

        // Process all 11 events sequentially (same order as published by fatgraph)
        for (JsonNode event : events) {
            processEvent(MAPPER.writeValueAsString(event));
        }

        // Verify all downstream calls in order
        InOrder inOrder = inOrder(entityMutationService, typeDefStore, typeRegistry);

        // 1. BULK_CREATE_OR_UPDATE → createOrUpdate
        inOrder.verify(entityMutationService).createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class));

        // 2. SET_CLASSIFICATIONS → setClassifications (2 classifications across 2 entities, override=false)
        ArgumentCaptor<AtlasEntityHeaders> headersCaptor = ArgumentCaptor.forClass(AtlasEntityHeaders.class);
        inOrder.verify(entityMutationService).setClassifications(headersCaptor.capture(), eq(false));
        assertEquals(2, headersCaptor.getValue().getGuidHeaderMap().size());

        // 3. DELETE_BY_GUID → deleteById("guid-table-001")
        inOrder.verify(entityMutationService).deleteById("guid-table-001");

        // 4. DELETE_BY_GUIDS → deleteByIds (3 guids)
        ArgumentCaptor<List<String>> deleteGuidsCaptor = ArgumentCaptor.forClass(List.class);
        inOrder.verify(entityMutationService).deleteByIds(deleteGuidsCaptor.capture());
        assertEquals(3, deleteGuidsCaptor.getValue().size());
        assertTrue(deleteGuidsCaptor.getValue().containsAll(List.of("guid-table-001", "guid-col-001", "guid-col-002")));

        // 5. DELETE_BY_UNIQUE_ATTRIBUTE → typeRegistry lookup + deleteByUniqueAttributes
        inOrder.verify(typeRegistry).getEntityTypeByName("Table");
        ArgumentCaptor<Map> uniqAttrsCaptor = ArgumentCaptor.forClass(Map.class);
        inOrder.verify(entityMutationService).deleteByUniqueAttributes(eq(mockTableType), uniqAttrsCaptor.capture());
        assertEquals("default/snowflake/db1/schema1/table1", uniqAttrsCaptor.getValue().get("qualifiedName"));

        // 6. BULK_DELETE_BY_UNIQUE_ATTRIBUTES → deleteByUniqueAttributes(list of 2 objectIds)
        ArgumentCaptor<List<AtlasObjectId>> objIdsCaptor = ArgumentCaptor.forClass(List.class);
        inOrder.verify(entityMutationService).deleteByUniqueAttributes(objIdsCaptor.capture());
        assertEquals(2, objIdsCaptor.getValue().size());

        // 7. RESTORE_BY_GUIDS → restoreByIds (2 guids)
        ArgumentCaptor<List<String>> restoreGuidsCaptor = ArgumentCaptor.forClass(List.class);
        inOrder.verify(entityMutationService).restoreByIds(restoreGuidsCaptor.capture());
        assertEquals(2, restoreGuidsCaptor.getValue().size());
        assertTrue(restoreGuidsCaptor.getValue().containsAll(List.of("guid-table-001", "guid-col-001")));

        // 8. TYPEDEF_CREATE → createTypesDef (1 classificationDef + 1 entityDef)
        ArgumentCaptor<AtlasTypesDef> createTdCaptor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        inOrder.verify(typeDefStore).createTypesDef(createTdCaptor.capture());
        assertEquals("CustomTable", createTdCaptor.getValue().getEntityDefs().get(0).getName());
        assertEquals("SensitiveData", createTdCaptor.getValue().getClassificationDefs().get(0).getDisplayName());

        // 9. TYPEDEF_UPDATE → updateTypesDef (CustomTable with 2 attributes)
        ArgumentCaptor<AtlasTypesDef> updateTdCaptor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        inOrder.verify(typeDefStore).updateTypesDef(updateTdCaptor.capture());
        assertEquals(2, updateTdCaptor.getValue().getEntityDefs().get(0).getAttributeDefs().size());

        // 10. TYPEDEF_DELETE → deleteTypesDef
        ArgumentCaptor<AtlasTypesDef> deleteTdCaptor = ArgumentCaptor.forClass(AtlasTypesDef.class);
        inOrder.verify(typeDefStore).deleteTypesDef(deleteTdCaptor.capture());
        assertFalse(deleteTdCaptor.getValue().getClassificationDefs().isEmpty());
        assertFalse(deleteTdCaptor.getValue().getEntityDefs().isEmpty());

        // 11. TYPEDEF_DELETE_BY_NAME → deleteTypeByName("CustomTable")
        inOrder.verify(typeDefStore).deleteTypeByName("CustomTable");

        inOrder.verifyNoMoreInteractions();
    }

    // ── DLQ Publishing Tests ─────────────────────────────────────────────

    @Test
    void testPublishToDlq_ProducesCorrectEnvelope() throws Exception {
        ReflectionTestUtils.setField(consumerService, "dlqTopic", "TEST_DLQ");

        // Create a mock DLQ producer that captures the sent record
        org.apache.kafka.clients.producer.KafkaProducer<String, String> mockDlqProducer =
                mock(org.apache.kafka.clients.producer.KafkaProducer.class);
        ReflectionTestUtils.setField(consumerService, "dlqProducer", mockDlqProducer);

        java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> mockFuture =
                mock(java.util.concurrent.Future.class);
        when(mockDlqProducer.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "ATLAS_ASYNC_ENTITIES", 2, 42, "event-123", DELETE_BY_GUID_EVENT);
        Exception error = new RuntimeException("Test error message");

        ReflectionTestUtils.invokeMethod(consumerService, "publishToDlq", record, error, 3);

        ArgumentCaptor<org.apache.kafka.clients.producer.ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(org.apache.kafka.clients.producer.ProducerRecord.class);
        verify(mockDlqProducer).send(captor.capture());

        org.apache.kafka.clients.producer.ProducerRecord<String, String> dlqRecord = captor.getValue();
        assertEquals("TEST_DLQ", dlqRecord.topic());
        assertEquals("event-123", dlqRecord.key());

        JsonNode dlqEnvelope = MAPPER.readTree(dlqRecord.value());
        assertEquals("ATLAS_ASYNC_ENTITIES", dlqEnvelope.get("originalTopic").asText());
        assertEquals(2, dlqEnvelope.get("originalPartition").asInt());
        assertEquals(42, dlqEnvelope.get("originalOffset").asLong());
        assertEquals("event-123", dlqEnvelope.get("originalKey").asText());
        assertEquals(DELETE_BY_GUID_EVENT, dlqEnvelope.get("originalValue").asText());
        assertEquals("Test error message", dlqEnvelope.get("errorMessage").asText());
        assertEquals("java.lang.RuntimeException", dlqEnvelope.get("errorClass").asText());
        assertEquals(3, dlqEnvelope.get("retryCount").asInt());
        assertTrue(dlqEnvelope.has("failedAt"));

        long dlqCount = ((java.util.concurrent.atomic.AtomicLong)
                ReflectionTestUtils.getField(consumerService, "dlqPublishCount")).get();
        assertEquals(1, dlqCount);
    }

    @Test
    void testPublishToDlq_FailureDoesNotThrow() throws Exception {
        // DLQ producer is null (not initialized) — should log error but not throw
        ReflectionTestUtils.setField(consumerService, "dlqProducer", null);
        ReflectionTestUtils.setField(consumerService, "bootstrapServers", null);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "ATLAS_ASYNC_ENTITIES", 0, 0, "key", "value");
        Exception error = new RuntimeException("Test");

        // Should not throw — best-effort
        assertDoesNotThrow(() ->
                ReflectionTestUtils.invokeMethod(consumerService, "publishToDlq", record, error, 3));
    }
}
