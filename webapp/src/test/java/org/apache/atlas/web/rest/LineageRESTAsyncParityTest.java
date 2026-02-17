/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.async.AsyncExecutorService;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.web.rest.validator.LineageListRequestValidator;
import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Parity tests verifying that sync and async execution paths in LineageREST.getLineageGraph()
 * produce identical results when direction is BOTH. The async path is gated by DynamicConfigStore.isAsyncExecutionEnabled().
 */
class LineageRESTAsyncParityTest {

    private AtlasLineageService lineageService;
    private AsyncExecutorService asyncExecutorService;

    private LineageREST lineageREST;
    private MockedStatic<DynamicConfigStore> configStoreMock;
    private MockedStatic<ApplicationProperties> appPropsMock;

    private static final String BASE_GUID = "base-entity-guid";
    private static final int DEPTH = 3;

    @BeforeEach
    void setUp() {
        // Mock ApplicationProperties to prevent static initializer failures in shared JVM
        appPropsMock = mockStatic(ApplicationProperties.class);
        Configuration mockConfig = mock(Configuration.class);
        lenient().when(mockConfig.getString(anyString())).thenReturn("");
        lenient().when(mockConfig.getString(anyString(), anyString())).thenAnswer(inv -> inv.getArgument(1));
        lenient().when(mockConfig.getInt(anyString(), anyInt())).thenAnswer(inv -> inv.getArgument(1));
        lenient().when(mockConfig.getBoolean(anyString(), anyBoolean())).thenAnswer(inv -> inv.getArgument(1));
        appPropsMock.when(ApplicationProperties::get).thenReturn(mockConfig);

        AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
        lineageService = mock(AtlasLineageService.class);
        LineageListRequestValidator lineageListRequestValidator = mock(LineageListRequestValidator.class);
        asyncExecutorService = mock(AsyncExecutorService.class);

        lineageREST = new LineageREST(typeRegistry, lineageService, lineageListRequestValidator, asyncExecutorService);

        // Make asyncExecutorService.supplyAsync() execute synchronously
        when(asyncExecutorService.supplyAsync(any(Supplier.class), anyString()))
                .thenAnswer(invocation -> {
                    Supplier<?> supplier = invocation.getArgument(0);
                    return CompletableFuture.completedFuture(supplier.get());
                });
        when(asyncExecutorService.getDefaultTimeoutMs()).thenReturn(30000L);
        when(asyncExecutorService.withTimeout(any(CompletableFuture.class), any(Duration.class), anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        configStoreMock = mockStatic(DynamicConfigStore.class);
    }

    @AfterEach
    void tearDown() {
        if (configStoreMock != null) {
            configStoreMock.close();
        }
        if (appPropsMock != null) {
            appPropsMock.close();
        }
    }

    @Test
    void getLineageGraph_BOTH_syncAndAsync_produceIdenticalResults() throws AtlasBaseException {
        // Arrange: combined BOTH result for sync path
        AtlasLineageInfo bothResult = createLineageInfo(BASE_GUID, LineageDirection.BOTH);
        addEntityToLineage(bothResult, BASE_GUID, "Table", "baseTable");
        addEntityToLineage(bothResult, "upstream-1", "Table", "upstreamTable");
        addEntityToLineage(bothResult, "downstream-1", "Table", "downstreamTable");
        addEntityToLineage(bothResult, "process-1", "Process", "etlProcess");
        addRelation(bothResult, "upstream-1", "process-1");
        addRelation(bothResult, "process-1", BASE_GUID);
        addRelation(bothResult, BASE_GUID, "downstream-1");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false))
                .thenReturn(bothResult);

        // Arrange: INPUT and OUTPUT results for async path
        AtlasLineageInfo inputResult = createLineageInfo(BASE_GUID, LineageDirection.INPUT);
        addEntityToLineage(inputResult, BASE_GUID, "Table", "baseTable");
        addEntityToLineage(inputResult, "upstream-1", "Table", "upstreamTable");
        addEntityToLineage(inputResult, "process-1", "Process", "etlProcess");
        addRelation(inputResult, "upstream-1", "process-1");
        addRelation(inputResult, "process-1", BASE_GUID);

        AtlasLineageInfo outputResult = createLineageInfo(BASE_GUID, LineageDirection.OUTPUT);
        addEntityToLineage(outputResult, BASE_GUID, "Table", "baseTable");
        addEntityToLineage(outputResult, "downstream-1", "Table", "downstreamTable");
        addRelation(outputResult, BASE_GUID, "downstream-1");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false))
                .thenReturn(inputResult);
        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.OUTPUT, DEPTH, false, -1, -1, false))
                .thenReturn(outputResult);

        // Act: sync path
        setAsyncFlag(false);
        AtlasLineageInfo syncOutput = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false);

        // Act: async path
        setAsyncFlag(true);
        AtlasLineageInfo asyncOutput = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false);

        // Assert: same entity maps
        assertNotNull(syncOutput);
        assertNotNull(asyncOutput);
        assertEquals(syncOutput.getGuidEntityMap().keySet(), asyncOutput.getGuidEntityMap().keySet(),
                "Entity GUIDs should match between sync and async");

        // Assert: same relations
        assertEquals(syncOutput.getRelations().size(), asyncOutput.getRelations().size(),
                "Relation count should match");

        // Assert: base entity GUID preserved
        assertEquals(syncOutput.getBaseEntityGuid(), asyncOutput.getBaseEntityGuid());
    }

    @Test
    void getLineageGraph_INPUT_alwaysUsesSyncPath() throws AtlasBaseException {
        // Arrange: non-BOTH direction should skip async even when flag is ON
        AtlasLineageInfo inputResult = createLineageInfo(BASE_GUID, LineageDirection.INPUT);
        addEntityToLineage(inputResult, BASE_GUID, "Table", "baseTable");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false))
                .thenReturn(inputResult);

        // Act: with async ON
        setAsyncFlag(true);
        AtlasLineageInfo result = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false);

        // Assert: used sync path (single call with INPUT direction)
        verify(lineageService).getAtlasLineageInfo(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false);
        verify(lineageService, never()).getAtlasLineageInfo(eq(BASE_GUID), eq(LineageDirection.OUTPUT), anyInt(), anyBoolean(), anyInt(), anyInt(), anyBoolean());
        assertNotNull(result);
    }

    @Test
    void getLineageGraph_BOTH_emptyLineage_handledIdentically() throws AtlasBaseException {
        // Arrange: empty lineage (no relations, only base entity)
        AtlasLineageInfo emptyBoth = createLineageInfo(BASE_GUID, LineageDirection.BOTH);
        addEntityToLineage(emptyBoth, BASE_GUID, "Table", "lonelyTable");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false))
                .thenReturn(emptyBoth);

        AtlasLineageInfo emptyInput = createLineageInfo(BASE_GUID, LineageDirection.INPUT);
        addEntityToLineage(emptyInput, BASE_GUID, "Table", "lonelyTable");
        AtlasLineageInfo emptyOutput = createLineageInfo(BASE_GUID, LineageDirection.OUTPUT);
        addEntityToLineage(emptyOutput, BASE_GUID, "Table", "lonelyTable");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false))
                .thenReturn(emptyInput);
        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.OUTPUT, DEPTH, false, -1, -1, false))
                .thenReturn(emptyOutput);

        // Act
        setAsyncFlag(false);
        AtlasLineageInfo syncOutput = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false);

        setAsyncFlag(true);
        AtlasLineageInfo asyncOutput = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false);

        // Assert: both handle empty lineage
        assertNotNull(syncOutput);
        assertNotNull(asyncOutput);
        assertTrue(syncOutput.getRelations() == null || syncOutput.getRelations().isEmpty());
        assertTrue(asyncOutput.getRelations() == null || asyncOutput.getRelations().isEmpty());
        assertEquals(syncOutput.getGuidEntityMap().size(), asyncOutput.getGuidEntityMap().size());
    }

    @Test
    void getLineageGraph_BOTH_mergeDeduplicatesEntities() throws AtlasBaseException {
        // Arrange: base entity appears in both INPUT and OUTPUT results
        // The merged async result should not duplicate it
        AtlasLineageInfo inputResult = createLineageInfo(BASE_GUID, LineageDirection.INPUT);
        addEntityToLineage(inputResult, BASE_GUID, "Table", "baseTable");
        addEntityToLineage(inputResult, "upstream-1", "Table", "upstreamTable");
        addRelation(inputResult, "upstream-1", BASE_GUID);

        AtlasLineageInfo outputResult = createLineageInfo(BASE_GUID, LineageDirection.OUTPUT);
        addEntityToLineage(outputResult, BASE_GUID, "Table", "baseTable"); // same base entity
        addEntityToLineage(outputResult, "downstream-1", "Table", "downstreamTable");
        addRelation(outputResult, BASE_GUID, "downstream-1");

        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.INPUT, DEPTH, false, -1, -1, false))
                .thenReturn(inputResult);
        when(lineageService.getAtlasLineageInfo(BASE_GUID, LineageDirection.OUTPUT, DEPTH, false, -1, -1, false))
                .thenReturn(outputResult);

        // Act: async path
        setAsyncFlag(true);
        AtlasLineageInfo asyncOutput = lineageREST.getLineageGraph(BASE_GUID, LineageDirection.BOTH, DEPTH, false, -1, -1, false);

        // Assert: base entity not duplicated — guidEntityMap is a Map so keys are unique
        assertNotNull(asyncOutput);
        assertEquals(3, asyncOutput.getGuidEntityMap().size(),
                "Should have 3 unique entities: base, upstream, downstream");
        assertTrue(asyncOutput.getGuidEntityMap().containsKey(BASE_GUID));
        assertTrue(asyncOutput.getGuidEntityMap().containsKey("upstream-1"));
        assertTrue(asyncOutput.getGuidEntityMap().containsKey("downstream-1"));

        // Assert: relations from both directions are present
        assertEquals(2, asyncOutput.getRelations().size(),
                "Should have 2 relations: upstream->base and base->downstream");
    }

    // --- Helpers ---

    private void setAsyncFlag(boolean enabled) {
        configStoreMock.when(DynamicConfigStore::isAsyncExecutionEnabled)
                .thenReturn(enabled);
    }

    private AtlasLineageInfo createLineageInfo(String baseGuid, LineageDirection direction) {
        AtlasLineageInfo info = new AtlasLineageInfo();
        info.setBaseEntityGuid(baseGuid);
        info.setLineageDirection(direction);
        info.setLineageDepth(DEPTH);
        info.setLimit(-1);
        info.setOffset(-1);
        info.setGuidEntityMap(new HashMap<>());
        info.setRelations(new HashSet<>());
        return info;
    }

    private void addEntityToLineage(AtlasLineageInfo info, String guid, String typeName, String name) {
        AtlasEntityHeader header = new AtlasEntityHeader(typeName);
        header.setGuid(guid);
        header.setDisplayText(name);
        info.getGuidEntityMap().put(guid, header);
    }

    private void addRelation(AtlasLineageInfo info, String fromGuid, String toGuid) {
        info.getRelations().add(new LineageRelation(fromGuid, toGuid, "rel-" + fromGuid + "-" + toGuid));
    }
}
