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
package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for MS-710: connectorName missing from ENTITY_UPDATE events.
 *
 * <p>Verifies that attributes with {@code includeInNotification=true} (like
 * {@code connectorName}) are present in notification event payloads even when
 * those attributes are not part of the update request.</p>
 *
 * <p>Root cause (pre-fix): The entity cached in RequestContext during
 * create/update only contained the attributes from the incoming payload.
 * When the notification builder later retrieved this cached entity and
 * checked {@code includeInNotification}, attributes like {@code connectorName}
 * were absent because they weren't in the original request.</p>
 *
 * <p>Fix (PR #6405): {@code constructHeader()} now reads all primitive/enum
 * attributes from the graph vertex and merges them into the cached entity,
 * ensuring {@code includeInNotification} attributes survive into notifications.</p>
 *
 * <p>Run with:
 * <pre>
 * mvn install -pl webapp -am -DskipTests -Drat.skip=true
 * mvn test -pl webapp -Dtest=IncludeInNotificationAttributeTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IncludeInNotificationAttributeTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(IncludeInNotificationAttributeTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String KAFKA_TOPIC = "ATLAS_ENTITIES";
    private static final long KAFKA_POLL_TIMEOUT_MS = 15000;
    private static final String CONNECTOR_NAME = "snowflake";

    private final long testId = System.currentTimeMillis();

    private String tableGuid;
    private String tableQualifiedName;

    @Test
    @Order(1)
    void testCreateEntityWithConnectorName_ShouldIncludeInCreateNotification() throws Exception {
        LOG.info("=== Step 1: Create Table with connectorName ===");

        KafkaConsumer<String, String> consumer = createEntityNotificationConsumer();
        long operationStartTime = System.currentTimeMillis();

        AtlasEntity table = new AtlasEntity("Table");
        tableQualifiedName = "test://ms710/table/" + testId;
        table.setAttribute("name", "ms710-table-" + testId);
        table.setAttribute("qualifiedName", tableQualifiedName);
        table.setAttribute("connectorName", CONNECTOR_NAME);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(table));

        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Table should be created");
        tableGuid = created.getGuid();
        assertNotNull(tableGuid, "Table GUID should not be null");
        LOG.info("Created Table with GUID: {}", tableGuid);

        // Wait briefly for async notification delivery
        Thread.sleep(3000);

        List<JsonNode> createNotifications = collectNotifications(
                consumer, tableGuid, "ENTITY_CREATE", operationStartTime, KAFKA_POLL_TIMEOUT_MS);

        assertFalse(createNotifications.isEmpty(),
                "Should receive ENTITY_CREATE notification for the Table");

        JsonNode createNotif = createNotifications.get(0);
        JsonNode entityNode = createNotif.get("entity");
        assertNotNull(entityNode, "Notification should contain entity");

        JsonNode attributes = entityNode.get("attributes");
        assertNotNull(attributes, "Entity in notification should have attributes");

        assertTrue(attributes.has("connectorName"),
                "MS-710: ENTITY_CREATE notification must include connectorName " +
                "(includeInNotification=true). Attributes present: " + attributes.fieldNames());

        assertEquals(CONNECTOR_NAME, attributes.get("connectorName").asText(),
                "connectorName value should match what was set on the entity");

        LOG.info("=== ENTITY_CREATE notification correctly includes connectorName ===");
        consumer.close();
    }

    @Test
    @Order(2)
    void testUpdateEntityDescription_ShouldStillIncludeConnectorNameInNotification() throws Exception {
        LOG.info("=== Step 2: Update description only — connectorName should still appear in ENTITY_UPDATE ===");
        assertNotNull(tableGuid, "Table must exist from previous test");

        KafkaConsumer<String, String> consumer = createEntityNotificationConsumer();
        long operationStartTime = System.currentTimeMillis();

        // Update ONLY the description — do NOT include connectorName in the payload
        AtlasEntity updateEntity = new AtlasEntity("Table");
        updateEntity.setGuid(tableGuid);
        updateEntity.setAttribute("qualifiedName", tableQualifiedName);
        updateEntity.setAttribute("name", "ms710-table-" + testId);
        updateEntity.setAttribute("description", "Updated description at " + System.currentTimeMillis());

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(updateEntity));
        assertNotNull(response, "Update response should not be null");

        List<AtlasEntityHeader> updatedEntities = response.getUpdatedEntities();
        assertNotNull(updatedEntities, "Should have updated entities");
        assertFalse(updatedEntities.isEmpty(), "Table should appear as updated");

        // Wait briefly for async notification delivery
        Thread.sleep(3000);

        List<JsonNode> updateNotifications = collectNotifications(
                consumer, tableGuid, "ENTITY_UPDATE", operationStartTime, KAFKA_POLL_TIMEOUT_MS);

        assertFalse(updateNotifications.isEmpty(),
                "Should receive ENTITY_UPDATE notification for the Table");

        JsonNode updateNotif = updateNotifications.get(0);
        JsonNode entityNode = updateNotif.get("entity");
        assertNotNull(entityNode, "Notification should contain entity");

        JsonNode attributes = entityNode.get("attributes");
        assertNotNull(attributes, "Entity in notification should have attributes");

        // KEY ASSERTION: connectorName must be present even though it was NOT in the update payload
        assertTrue(attributes.has("connectorName"),
                "MS-710 BUG: ENTITY_UPDATE notification must include connectorName " +
                "(includeInNotification=true) even when it was not part of the update payload. " +
                "The notification builder should read persisted attributes from the graph vertex. " +
                "Attributes present: " + attributes.fieldNames());

        assertEquals(CONNECTOR_NAME, attributes.get("connectorName").asText(),
                "connectorName value should match the originally persisted value");

        LOG.info("=== ENTITY_UPDATE notification correctly includes connectorName ===");
        consumer.close();
    }

    @Test
    @Order(3)
    void testUpdateConnectorName_ShouldReflectNewValueInNotification() throws Exception {
        LOG.info("=== Step 3: Update connectorName itself — notification should reflect new value ===");
        assertNotNull(tableGuid, "Table must exist from previous test");

        KafkaConsumer<String, String> consumer = createEntityNotificationConsumer();
        long operationStartTime = System.currentTimeMillis();

        String newConnectorName = "bigquery";

        AtlasEntity updateEntity = new AtlasEntity("Table");
        updateEntity.setGuid(tableGuid);
        updateEntity.setAttribute("qualifiedName", tableQualifiedName);
        updateEntity.setAttribute("name", "ms710-table-" + testId);
        updateEntity.setAttribute("connectorName", newConnectorName);

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(updateEntity));
        assertNotNull(response, "Update response should not be null");

        // Wait briefly for async notification delivery
        Thread.sleep(3000);

        List<JsonNode> updateNotifications = collectNotifications(
                consumer, tableGuid, "ENTITY_UPDATE", operationStartTime, KAFKA_POLL_TIMEOUT_MS);

        assertFalse(updateNotifications.isEmpty(),
                "Should receive ENTITY_UPDATE notification for the Table");

        JsonNode updateNotif = updateNotifications.get(0);
        JsonNode entityNode = updateNotif.get("entity");
        assertNotNull(entityNode, "Notification should contain entity");

        JsonNode attributes = entityNode.get("attributes");
        assertNotNull(attributes, "Entity in notification should have attributes");

        assertTrue(attributes.has("connectorName"),
                "ENTITY_UPDATE notification must include connectorName when it is explicitly updated");

        assertEquals(newConnectorName, attributes.get("connectorName").asText(),
                "connectorName should reflect the new value after explicit update");

        LOG.info("=== ENTITY_UPDATE notification correctly reflects updated connectorName ===");
        consumer.close();
    }

    // ==================== Kafka Helper Methods ====================

    private KafkaConsumer<String, String> createEntityNotificationConsumer() throws Exception {
        String bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ms710-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        return consumer;
    }

    /**
     * Polls Kafka for notifications matching the given entity GUID and operation type.
     * Filters by startTime to avoid stale messages from earlier tests.
     */
    private List<JsonNode> collectNotifications(
            KafkaConsumer<String, String> consumer,
            String targetGuid,
            String targetOperationType,
            long startTime,
            long timeoutMs) {

        List<JsonNode> matched = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode kafkaMessage = MAPPER.readTree(record.value());

                    if (!kafkaMessage.has("message")) {
                        continue;
                    }

                    JsonNode message = kafkaMessage.get("message");
                    long eventTime = message.has("eventTime") ? message.get("eventTime").asLong() : 0;

                    if (startTime > 0 && eventTime > 0 && eventTime < startTime) {
                        continue;
                    }

                    String opType = message.has("operationType") ? message.get("operationType").asText() : "";
                    String guid = message.has("entity") && message.get("entity").has("guid")
                            ? message.get("entity").get("guid").asText() : "";

                    if (targetOperationType.equals(opType) && targetGuid.equals(guid)) {
                        matched.add(message);
                        LOG.debug("Found {} for target GUID {}", targetOperationType, targetGuid);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse Kafka message: {}", e.getMessage());
                }
            }

            if (!matched.isEmpty()) {
                break;
            }

            if (records.isEmpty()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return matched;
    }
}
