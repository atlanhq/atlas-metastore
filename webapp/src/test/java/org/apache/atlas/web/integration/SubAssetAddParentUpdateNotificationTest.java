package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for MS-701: Main asset events missing for sub-asset add.
 *
 * Scenario (first example from the ticket):
 * 1. Create a Table entity (parent/main asset)
 * 2. Send a bulk createOrUpdate with:
 *    - The SAME Table (unchanged attributes)
 *    - A NEW Process with inputs referencing the Table (sub-asset add)
 * 3. Expect: Table should appear as UPDATED in both the REST response and Kafka notifications,
 *    because its relationship (inputs/outputs) changed.
 *
 * Bug: The Table is marked as "unchanged" by the diff check and added to entitiesToSkipUpdate.
 * When the Process creates a relationship edge back to the Table, the Table's update event
 * is suppressed by RequestContext.recordEntityUpdate() checking the skip set.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
public class SubAssetAddParentUpdateNotificationTest extends AtlasDockerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SubAssetAddParentUpdateNotificationTest.class);

    private static final String KAFKA_TOPIC = "ATLAS_ENTITIES";
    private static final int NOTIFICATION_TIMEOUT_MS = 20000;

    @Test
    @DisplayName("MS-701: Adding sub-asset (Process) should emit UPDATE event for parent (Table)")
    void testAddSubAsset_ParentShouldAppearAsUpdated() throws Exception {
        LOG.info("=== TEST: MS-701 - Sub-asset add should trigger parent UPDATE ===");

        // Step 1: Create a Table entity
        String tableQualifiedName = "ms701_parent_table_" + System.currentTimeMillis();
        LOG.info("Creating parent Table: {}", tableQualifiedName);

        String tableGuid = createTable(tableQualifiedName);
        LOG.info("Created parent Table, GUID: {}", tableGuid);
        assertNotNull(tableGuid, "Table GUID should not be null");

        // Small delay to ensure entity is fully persisted
        Thread.sleep(2000);

        // Step 2: Create Kafka consumer BEFORE the second bulk request
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Thread.sleep(1000);

        long operationStartTime = System.currentTimeMillis();

        // Step 3: Send a bulk request with:
        //   - The SAME Table (no attribute changes → will be marked as "unchanged")
        //   - A NEW Process with inputs=[Table] (creates a relationship edge back to the Table)
        String processQualifiedName = "ms701_child_process_" + System.currentTimeMillis();
        LOG.info("Sending bulk request with unchanged Table + new Process: {}", processQualifiedName);

        String bulkPayload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "%s",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s",
                            "connectionQualifiedName": "default/bigquery/1709805089",
                            "connectorName": "bigquery"
                        }
                    },
                    {
                        "typeName": "Process",
                        "guid": "-1",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s",
                            "connectionQualifiedName": "default/snowflake/1736926905",
                            "connectorName": "snowflake"
                        },
                        "relationshipAttributes": {
                            "inputs": [
                                {
                                    "guid": "%s",
                                    "typeName": "Table"
                                }
                            ]
                        }
                    }
                ]
            }""", tableGuid, tableQualifiedName, tableQualifiedName,
                processQualifiedName, processQualifiedName, tableGuid);

        HttpResponse<String> response = sendBulkEntityRequest(bulkPayload);
        assertEquals(200, response.statusCode(), "Bulk request should succeed. Response: " + response.body());

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        LOG.info("Bulk response: {}", response.body());

        // Step 4: Verify REST response — Table should be in mutatedEntities.UPDATE
        JsonNode mutatedEntities = result.get("mutatedEntities");
        assertNotNull(mutatedEntities, "Response should have mutatedEntities");

        // Process should be CREATED
        assertTrue(mutatedEntities.has("CREATE"), "Should have CREATE entries for new Process");

        // Collect all UPDATED entity GUIDs from the response
        Set<String> updatedGuids = new HashSet<>();
        if (mutatedEntities.has("UPDATE")) {
            for (JsonNode entity : mutatedEntities.get("UPDATE")) {
                String guid = entity.get("guid").asText();
                String typeName = entity.get("typeName").asText();
                updatedGuids.add(guid);
                LOG.info("UPDATED entity in response: {} ({})", typeName, guid);
            }
        }

        LOG.info("Updated GUIDs in REST response: {}", updatedGuids);

        // KEY ASSERTION: The Table should appear as UPDATED because its relationship changed
        // (a new Process now references it via inputs)
        assertTrue(updatedGuids.contains(tableGuid),
            "MS-701 BUG: Table should appear as UPDATED in REST response when a new sub-asset " +
            "(Process) creates a relationship to it, even though the Table's own attributes are unchanged. " +
            "Table GUID: " + tableGuid + ", Updated GUIDs: " + updatedGuids);

        // Step 5: Verify Kafka notifications — Table should have ENTITY_UPDATE notification
        Thread.sleep(3000);
        LOG.info("=== Collecting Kafka notifications ===");
        List<JsonNode> allNotifications = collectAllNotifications(consumer, NOTIFICATION_TIMEOUT_MS, operationStartTime);

        Set<String> ourGuids = new HashSet<>(Arrays.asList(tableGuid));
        List<JsonNode> tableUpdateNotifications = new ArrayList<>();

        for (JsonNode notif : allNotifications) {
            if (notif.has("entity") && notif.get("entity").has("guid")) {
                String guid = notif.get("entity").get("guid").asText();
                String opType = notif.has("operationType") ? notif.get("operationType").asText() : "UNKNOWN";

                if (tableGuid.equals(guid) && "ENTITY_UPDATE".equals(opType)) {
                    tableUpdateNotifications.add(notif);
                    LOG.info("Found Table UPDATE notification: {}", notif);
                }
            }
        }

        // KEY ASSERTION: Table should have received an ENTITY_UPDATE Kafka notification
        assertFalse(tableUpdateNotifications.isEmpty(),
            "MS-701 BUG: Table should receive ENTITY_UPDATE Kafka notification when a new " +
            "sub-asset (Process) creates a relationship to it. Table GUID: " + tableGuid);

        LOG.info("=== TEST PASSED ===");
        consumer.close();
    }

    // ==================== Helper Methods ====================

    private String createTable(String qualifiedName) throws Exception {
        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "-1",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s",
                            "connectionQualifiedName": "default/bigquery/1709805089",
                            "connectorName": "bigquery"
                        }
                    }
                ]
            }""", qualifiedName, qualifiedName);

        HttpResponse<String> response = sendBulkEntityRequest(payload);
        assertEquals(200, response.statusCode(), "Table creation should succeed");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);

        if (result.has("guidAssignments") && result.get("guidAssignments").has("-1")) {
            return result.get("guidAssignments").get("-1").asText();
        }

        return result.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();
    }

    private HttpResponse<String> sendBulkEntityRequest(String payload) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

        String url = ATLAS_BASE_URL + "/entity/bulk";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:" + kafka.getFirstMappedPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
            "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        return consumer;
    }

    private List<JsonNode> collectAllNotifications(
            KafkaConsumer<String, String> consumer,
            long timeoutMs,
            long startTime) throws Exception {

        List<JsonNode> notifications = new ArrayList<>();
        long collectionStartTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - collectionStartTime) < timeoutMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    ObjectNode kafkaMessage = mapper.readValue(record.value(), ObjectNode.class);

                    if (kafkaMessage.has("message")) {
                        JsonNode message = kafkaMessage.get("message");
                        long msgTime = 0;

                        if (message.has("eventTime")) {
                            msgTime = message.get("eventTime").asLong();
                        }

                        if (startTime > 0 && msgTime > 0 && msgTime < startTime) {
                            continue;
                        }

                        notifications.add(message);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse Kafka message: {}", e.getMessage());
                }
            }

            if (records.count() == 0) {
                Thread.sleep(500);
            }
        }

        return notifications;
    }
}
