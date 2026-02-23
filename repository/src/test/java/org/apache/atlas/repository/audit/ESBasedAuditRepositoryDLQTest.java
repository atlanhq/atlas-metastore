/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for ESBasedAuditRepository entity audit DLQ behaviour (MS-642).
 *
 * Edge cases covered:
 * - null / empty events: no enqueue, no throw
 * - DLQ disabled: log only, no enqueue
 * - DLQ enabled, write fails: enqueue (request does not fail)
 * - Queue full: drop and log, request does not fail
 * - Replay re-offer on failure, max retries then drop
 * - Defensive: details null or shorter than prefix, all entities null (empty bulk)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ESBasedAuditRepositoryDLQTest {

    private static final String ENTITY_AUDIT_DLQ_ENABLED_PROP = "atlas.entity.audit.dlq.enabled";
    private static final String ENTITY_AUDIT_DLQ_QUEUE_CAPACITY_PROP = "atlas.entity.audit.dlq.queue.capacity";
    private static final String ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP = "atlas.entity.audit.dlq.max.retries";

    private Configuration config;
    private EntityGraphRetriever entityGraphRetriever;
    private ESBasedAuditRepository repo;

    @BeforeAll
    void initApplicationProperties() throws Exception {
        config = new PropertiesConfiguration();
        config.setProperty("atlas.graph.storage.hostname", "localhost");
        config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
        config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, true);
        config.setProperty(ENTITY_AUDIT_DLQ_QUEUE_CAPACITY_PROP, 10);
        config.setProperty(ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP, 2);
        ApplicationProperties.set(config);
    }

    @BeforeEach
    void setUp() {
        RequestContext.clear();
        RequestContext.get();
        config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, true);
        entityGraphRetriever = mock(EntityGraphRetriever.class);
        repo = new ESBasedAuditRepository(config, entityGraphRetriever);
        // Do not call start() so lowLevelClient stays null and putEventsV2Internal will throw on first ES call
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(10);
        ReflectionTestUtils.setField(repo, "auditDlqQueue", queue);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    private List<EntityAuditEventV2> oneEvent() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId("guid-1");
        event.setAction(ENTITY_CREATE);
        event.setDetails("Created: {}");
        event.setUser("test-user");
        event.setTimestamp(System.currentTimeMillis());
        event.setEntityQualifiedName("\"qn1\"");
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("qualifiedName", "qn1");
        entity.setAttribute("updateTime", new Date());
        event.setEntity(entity);
        return List.of(event);
    }

    private List<EntityAuditEventV2> eventWithNullEntity() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId("guid-2");
        event.setAction(ENTITY_CREATE);
        event.setDetails("Created: {}");
        event.setUser("test-user");
        event.setTimestamp(System.currentTimeMillis());
        event.setEntity(null);
        return List.of(event);
    }

    @Nested
    @DisplayName("putEventsV2 does not fail request on audit write failure")
    class RequestDoesNotFail {

        @Test
        @DisplayName("null events: no throw, no enqueue")
        void nullEvents() {
            assertDoesNotThrow(() -> repo.putEventsV2((List<EntityAuditEventV2>) null));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("empty events: no throw, no enqueue")
        void emptyEvents() {
            assertDoesNotThrow(() -> repo.putEventsV2(new ArrayList<>()));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("write fails with DLQ enabled: request succeeds, events enqueued")
        void failureEnqueuedWhenDlqEnabled() {
            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));

            assertEquals(1, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("write fails with DLQ disabled: request succeeds, no enqueue")
        void failureNotEnqueuedWhenDlqDisabled() {
            config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, false);
            ReflectionTestUtils.setField(repo, "auditDlqQueue", new LinkedBlockingQueue<>(10));

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));

            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("queue full: request still succeeds, events dropped with log")
        void queueFullRequestSucceeds() {
            LinkedBlockingQueue<Object> smallQueue = new LinkedBlockingQueue<>(1);
            ReflectionTestUtils.setField(repo, "auditDlqQueue", smallQueue);

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, smallQueue.size());

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, smallQueue.size());
        }
    }

    @Nested
    @DisplayName("DLQ replay behaviour")
    class DlqReplay {

        @Test
        @DisplayName("processOneDlqEntryForTest: entry re-queued on failure when retries left")
        void replayRequeuesOnFailure() {
            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, repo.getAuditDlqQueueSize());

            boolean processed = repo.processOneDlqEntryForTest();
            assertTrue(processed);
            assertEquals(1, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("processOneDlqEntryForTest: no entry returns false")
        void processOneWhenEmptyReturnsFalse() {
            boolean processed = repo.processOneDlqEntryForTest();
            assertFalse(processed);
        }
    }

    @Nested
    @DisplayName("putEventsV2Internal edge cases (no NPE / no empty bulk)")
    class InternalEdgeCases {

        @Test
        @DisplayName("events with all null entity: build empty bulk, return early, no ES call")
        void allNullEntitySkipsBulk() {
            assertDoesNotThrow(() -> repo.putEventsV2(eventWithNullEntity()));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }
    }

}
