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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.async.AsyncExecutorService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.repository.store.graph.v2.repair.AtlasRepairAttributeService;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.RepairIndex;
import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Parity tests verifying that sync and async execution paths in EntityREST.getByGuids()
 * produce identical results. The async path is gated by the ENABLE_ASYNC_EXECUTION feature flag.
 */
class EntityRESTAsyncParityTest {

    private AtlasEntityStore entitiesStore;
    private AsyncExecutorService asyncExecutorService;

    private EntityREST entityREST;
    private MockedStatic<FeatureFlagStore> featureFlagMock;
    private MockedStatic<ApplicationProperties> appPropsMock;

    @BeforeEach
    void setUp() {
        // Mock ApplicationProperties FIRST — required before ESBasedAuditRepository class loads
        appPropsMock = mockStatic(ApplicationProperties.class);
        Configuration mockConfig = mock(Configuration.class);
        lenient().when(mockConfig.getString(anyString())).thenReturn("");
        lenient().when(mockConfig.getString(anyString(), anyString())).thenAnswer(inv -> inv.getArgument(1));
        lenient().when(mockConfig.getInt(anyString(), anyInt())).thenAnswer(inv -> inv.getArgument(1));
        lenient().when(mockConfig.getBoolean(anyString(), anyBoolean())).thenAnswer(inv -> inv.getArgument(1));
        appPropsMock.when(ApplicationProperties::get).thenReturn(mockConfig);

        // Now safe to create mocks for classes with static initializers
        AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
        entitiesStore = mock(AtlasEntityStore.class);
        ESBasedAuditRepository esBasedAuditRepository = mock(ESBasedAuditRepository.class);
        EntityGraphRetriever entityGraphRetriever = mock(EntityGraphRetriever.class);
        EntityMutationService entityMutationService = mock(EntityMutationService.class);
        AtlasRepairAttributeService repairAttributeService = mock(AtlasRepairAttributeService.class);
        RepairIndex repairIndex = mock(RepairIndex.class);
        asyncExecutorService = mock(AsyncExecutorService.class);

        entityREST = new EntityREST(typeRegistry, entitiesStore, esBasedAuditRepository,
                entityGraphRetriever, entityMutationService, repairAttributeService,
                repairIndex, asyncExecutorService);

        // Make asyncExecutorService.supplyAsync() execute synchronously
        when(asyncExecutorService.supplyAsync(any(Supplier.class), anyString()))
                .thenAnswer(invocation -> {
                    Supplier<?> supplier = invocation.getArgument(0);
                    return CompletableFuture.completedFuture(supplier.get());
                });
        when(asyncExecutorService.getDefaultTimeoutMs()).thenReturn(30000L);
        when(asyncExecutorService.withTimeout(any(CompletableFuture.class), any(Duration.class), anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        featureFlagMock = mockStatic(FeatureFlagStore.class);
    }

    @AfterEach
    void tearDown() {
        if (featureFlagMock != null) {
            featureFlagMock.close();
        }
        if (appPropsMock != null) {
            appPropsMock.close();
        }
    }

    @Test
    void getByGuids_syncAndAsync_produceIdenticalEntities() throws AtlasBaseException {
        // Arrange: 3 entities with referred entities
        List<String> guids = Arrays.asList("guid-1", "guid-2", "guid-3");

        AtlasEntity entity1 = createEntity("guid-1", "Table", "table1");
        AtlasEntity entity2 = createEntity("guid-2", "Table", "table2");
        AtlasEntity entity3 = createEntity("guid-3", "Column", "col1");
        AtlasEntity referred1 = createEntity("ref-1", "Schema", "schema1");
        AtlasEntity referred2 = createEntity("ref-2", "Database", "db1");

        // Sync path: entitiesStore.getByIds() returns bulk result
        AtlasEntitiesWithExtInfo syncResult = new AtlasEntitiesWithExtInfo();
        syncResult.addEntity(entity1);
        syncResult.addEntity(entity2);
        syncResult.addEntity(entity3);
        syncResult.addReferredEntity(referred1);
        syncResult.addReferredEntity(referred2);
        when(entitiesStore.getByIds(guids, false, false)).thenReturn(syncResult);

        // Async path: entitiesStore.getById() returns individual results
        AtlasEntityWithExtInfo ext1 = new AtlasEntityWithExtInfo(entity1);
        ext1.addReferredEntity(referred1);
        AtlasEntityWithExtInfo ext2 = new AtlasEntityWithExtInfo(entity2);
        ext2.addReferredEntity(referred2);
        AtlasEntityWithExtInfo ext3 = new AtlasEntityWithExtInfo(entity3);
        when(entitiesStore.getById("guid-1", false, false)).thenReturn(ext1);
        when(entitiesStore.getById("guid-2", false, false)).thenReturn(ext2);
        when(entitiesStore.getById("guid-3", false, false)).thenReturn(ext3);

        // Act: sync path (flag OFF)
        setAsyncFlag(false);
        AtlasEntitiesWithExtInfo syncOutput = entityREST.getByGuids(guids, false, false);

        // Act: async path (flag ON)
        setAsyncFlag(true);
        AtlasEntitiesWithExtInfo asyncOutput = entityREST.getByGuids(guids, false, false);

        // Assert: same entities
        assertNotNull(syncOutput);
        assertNotNull(asyncOutput);
        assertEquals(syncOutput.getEntities().size(), asyncOutput.getEntities().size(),
                "Entity count should match");

        // Assert: same entity GUIDs in same order
        for (int i = 0; i < syncOutput.getEntities().size(); i++) {
            assertEquals(syncOutput.getEntities().get(i).getGuid(),
                    asyncOutput.getEntities().get(i).getGuid(),
                    "Entity GUID at index " + i + " should match");
        }

        // Assert: same referred entities
        Map<String, AtlasEntity> syncReferred = syncOutput.getReferredEntities();
        Map<String, AtlasEntity> asyncReferred = asyncOutput.getReferredEntities();
        assertNotNull(syncReferred);
        assertNotNull(asyncReferred);
        assertEquals(syncReferred.keySet(), asyncReferred.keySet(),
                "Referred entity GUIDs should match");
    }

    @Test
    void getByGuids_singleGuid_alwaysUsesSyncPath() throws AtlasBaseException {
        // Arrange: single GUID — should always use sync path even with async ON
        List<String> guids = Collections.singletonList("guid-1");
        AtlasEntity entity1 = createEntity("guid-1", "Table", "table1");

        AtlasEntitiesWithExtInfo syncResult = new AtlasEntitiesWithExtInfo();
        syncResult.addEntity(entity1);
        when(entitiesStore.getByIds(guids, false, false)).thenReturn(syncResult);

        // Act: with async ON
        setAsyncFlag(true);
        AtlasEntitiesWithExtInfo result = entityREST.getByGuids(guids, false, false);

        // Assert: used sync path (getByIds), not getById
        verify(entitiesStore).getByIds(guids, false, false);
        verify(entitiesStore, never()).getById(anyString(), anyBoolean(), anyBoolean());
        assertNotNull(result);
        assertEquals(1, result.getEntities().size());
    }

    @Test
    void getByGuids_entityOrder_preserved() throws AtlasBaseException {
        // Arrange: specific order of GUIDs
        List<String> guids = Arrays.asList("guid-C", "guid-A", "guid-B");

        AtlasEntity entityC = createEntity("guid-C", "Table", "tableC");
        AtlasEntity entityA = createEntity("guid-A", "Table", "tableA");
        AtlasEntity entityB = createEntity("guid-B", "Table", "tableB");

        // Sync path
        AtlasEntitiesWithExtInfo syncResult = new AtlasEntitiesWithExtInfo();
        syncResult.addEntity(entityC);
        syncResult.addEntity(entityA);
        syncResult.addEntity(entityB);
        when(entitiesStore.getByIds(guids, false, false)).thenReturn(syncResult);

        // Async path: individual fetches
        when(entitiesStore.getById("guid-C", false, false)).thenReturn(new AtlasEntityWithExtInfo(entityC));
        when(entitiesStore.getById("guid-A", false, false)).thenReturn(new AtlasEntityWithExtInfo(entityA));
        when(entitiesStore.getById("guid-B", false, false)).thenReturn(new AtlasEntityWithExtInfo(entityB));

        // Act: async path
        setAsyncFlag(true);
        AtlasEntitiesWithExtInfo asyncOutput = entityREST.getByGuids(guids, false, false);

        // Assert: order preserved (C, A, B)
        assertNotNull(asyncOutput);
        assertEquals(3, asyncOutput.getEntities().size());
        assertEquals("guid-C", asyncOutput.getEntities().get(0).getGuid());
        assertEquals("guid-A", asyncOutput.getEntities().get(1).getGuid());
        assertEquals("guid-B", asyncOutput.getEntities().get(2).getGuid());
    }

    @Test
    void getByGuids_emptyGuids_throwsSameError() {
        // Both paths should throw INSTANCE_GUID_NOT_FOUND for empty guids

        // Sync path
        setAsyncFlag(false);
        AtlasBaseException syncEx = assertThrows(AtlasBaseException.class,
                () -> entityREST.getByGuids(Collections.emptyList(), false, false));

        // Async path
        setAsyncFlag(true);
        AtlasBaseException asyncEx = assertThrows(AtlasBaseException.class,
                () -> entityREST.getByGuids(Collections.emptyList(), false, false));

        assertEquals(syncEx.getAtlasErrorCode(), asyncEx.getAtlasErrorCode(),
                "Both paths should throw the same error code");
        assertEquals(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, syncEx.getAtlasErrorCode());
    }

    // --- Helpers ---

    private void setAsyncFlag(boolean enabled) {
        featureFlagMock.when(() -> FeatureFlagStore.evaluate(
                eq(FeatureFlag.ENABLE_ASYNC_EXECUTION.getKey()), eq("true")))
                .thenReturn(enabled);
    }

    private AtlasEntity createEntity(String guid, String typeName, String name) {
        AtlasEntity entity = new AtlasEntity(typeName);
        entity.setGuid(guid);
        entity.setAttribute("name", name);
        entity.setAttribute("qualifiedName", "test://" + name);
        return entity;
    }
}
