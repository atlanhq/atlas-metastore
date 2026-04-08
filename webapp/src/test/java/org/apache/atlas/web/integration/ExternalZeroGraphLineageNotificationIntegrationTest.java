package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExternalZeroGraphLineageNotificationIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalZeroGraphLineageNotificationIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ATLAS_URL = System.getProperty("atlas.url", "http://localhost:21000");
    private static final String ATLAS_USERNAME = System.getProperty("atlas.username", "admin");
    private static final String ATLAS_PASSWORD = System.getProperty("atlas.password", "admin");
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty("atlas.kafka.bootstrap.servers", "localhost:9093");
    private static final String FULL_ENTITY_TOPIC = "ATLAS_ENTITIES_FULL";
    private static final long NOTIFICATION_TIMEOUT_MS = 20000L;

    @Test
    void fullEntityNotificationsCoverCreateAndUpdateRelationshipEdgeCases() throws Exception {
        clearKafkaTopic(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC);

        AtlasClientV2 atlasClient = new AtlasClientV2(
                new String[]{normalizeAtlasBaseUrl(ATLAS_URL)},
                new String[]{ATLAS_USERNAME, ATLAS_PASSWORD}
        );

        long testId = System.currentTimeMillis();
        String sourceQualifiedName = "test://zerograph/lineage/source/" + testId;
        String targetQualifiedName = "test://zerograph/lineage/target/" + testId;
        String appendedTargetQualifiedName = "test://zerograph/lineage/target-appended/" + testId;
        String processQualifiedName = "test://zerograph/lineage/process/" + testId;

        String sourceGuid = createDataset(atlasClient, "zerograph-source-" + testId, sourceQualifiedName);
        String targetGuid = createDataset(atlasClient, "zerograph-target-" + testId, targetQualifiedName);
        String appendedTargetGuid = createDataset(atlasClient, "zerograph-target-appended-" + testId, appendedTargetQualifiedName);
        String processGuid;

        try (KafkaConsumer<String, String> consumer = createConsumer(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC)) {
            processGuid = createProcess(atlasClient, testId, processQualifiedName, sourceGuid, targetGuid);

            List<JsonNode> createMessages = collectAllMessages(consumer, NOTIFICATION_TIMEOUT_MS);
            JsonNode createdProcess = latestEntityMessageByOperation(createMessages, "ENTITY_CREATE", processGuid);
            assertNotNull(createdProcess, "Expected a full ENTITY_CREATE notification for process " + processGuid);
            assertProcessRelationships(createdProcess.path("entity"), List.of(sourceGuid), List.of(targetGuid),
                    "Create notification should include process relationships from the request payload");
        }

        AtlasLineageInfo lineageInfo = atlasClient.getLineageInfo(targetGuid, LineageDirection.INPUT, 3);
        assertNotNull(lineageInfo, "Lineage response should not be null");
        assertNotNull(lineageInfo.getGuidEntityMap(), "Lineage map should not be null");
        assertTrue(lineageInfo.getGuidEntityMap().containsKey(sourceGuid), "Lineage should contain the source dataset");
        assertTrue(lineageInfo.getGuidEntityMap().containsKey(targetGuid), "Lineage should contain the target dataset");
        assertTrue(lineageInfo.getGuidEntityMap().containsKey(processGuid), "Lineage should contain the process");

        clearKafkaTopic(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC);
        try (KafkaConsumer<String, String> consumer = createConsumer(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC)) {
            updateProcessDescriptionOnly(atlasClient, processGuid, processQualifiedName, testId);

            List<JsonNode> descriptionOnlyUpdateMessages = collectAllMessages(consumer, NOTIFICATION_TIMEOUT_MS);
            JsonNode updatedProcess = latestEntityMessageByOperation(descriptionOnlyUpdateMessages, "ENTITY_UPDATE", processGuid);
            assertNotNull(updatedProcess, "Expected a full ENTITY_UPDATE notification for process " + processGuid);
            assertProcessRelationships(updatedProcess.path("entity"), List.of(sourceGuid), List.of(targetGuid),
                    "Description-only update should backfill process relationships from persisted state");
        }

        clearKafkaTopic(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC);
        try (KafkaConsumer<String, String> consumer = createConsumer(KAFKA_BOOTSTRAP_SERVERS, FULL_ENTITY_TOPIC)) {
            updateProcessOutputs(atlasClient, processGuid, targetGuid, appendedTargetGuid);

            List<JsonNode> allMessages = collectAllMessages(consumer, NOTIFICATION_TIMEOUT_MS);
            Map<String, JsonNode> latestMessagesByGuid = latestEntityMessagesByGuid(allMessages);
            assertUpdatedEntitiesHaveAllRelationships(latestMessagesByGuid, sourceGuid, targetGuid, appendedTargetGuid, processGuid);
        }
    }

    private static String createDataset(AtlasClientV2 atlasClient, String name, String qualifiedName) throws AtlasServiceException {
        AtlasEntity dataset = new AtlasEntity("Table");
        dataset.setAttribute("name", name);
        dataset.setAttribute("qualifiedName", qualifiedName);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(dataset));
        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Dataset should be created");

        return created.getGuid();
    }

    private static String createProcess(AtlasClientV2 atlasClient, long testId, String qualifiedName,
                                        String sourceGuid, String targetGuid) throws AtlasServiceException {
        AtlasEntity process = new AtlasEntity("Process");
        process.setAttribute("name", "zerograph-process-" + testId);
        process.setAttribute("qualifiedName", qualifiedName);
        process.setAttribute("description", "Initial process state for ZeroGraph integration test");
        process.setAttribute("inputs", Collections.singletonList(new AtlasObjectId(sourceGuid, "Table")));
        process.setAttribute("outputs", Collections.singletonList(new AtlasObjectId(targetGuid, "Table")));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(process));
        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Process should be created");

        return created.getGuid();
    }

    private static void updateProcessDescriptionOnly(AtlasClientV2 atlasClient, String processGuid,
                                                     String processQualifiedName, long testId) throws AtlasServiceException {
        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(processGuid);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("qualifiedName", processQualifiedName);
        entity.setAttribute("description", "Description-only process update for ZeroGraph integration test " + testId);

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response, "Description-only update response should not be null");

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(processGuid);
        assertEquals("Description-only process update for ZeroGraph integration test " + testId,
                updated.getEntity().getAttribute("description"));
    }

    private static void updateProcessOutputs(AtlasClientV2 atlasClient, String processGuid,
                                             String existingTargetGuid, String appendedTargetGuid) throws AtlasServiceException {
        EntityMutationResponse response = atlasClient.partialUpdateEntityByGuid(processGuid, List.of(
                new AtlasObjectId(existingTargetGuid, "Table"),
                new AtlasObjectId(appendedTargetGuid, "Table")
        ), "outputs");
        assertNotNull(response, "Update response should not be null");

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(processGuid);
        assertProcessRelationshipsFromEntity(updated.getEntity(), List.of(existingTargetGuid, appendedTargetGuid),
                "Committed process state should include both outputs after relationship update");
    }

    private static KafkaConsumer<String, String> createConsumer(String kafkaBootstrap, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(kafkaBootstrap));
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(Duration.ofMillis(500));
        return consumer;
    }

    private static KafkaConsumer<String, String> createAssignedConsumer(String kafkaBootstrap) {
        return new KafkaConsumer<>(consumerProperties(kafkaBootstrap));
    }

    private static Properties consumerProperties(String kafkaBootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zerograph-lineage-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static void clearKafkaTopic(String kafkaBootstrap, String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);

        try (AdminClient adminClient = AdminClient.create(props)) {
            if (!adminClient.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic)) {
                LOG.info("Kafka topic {} does not exist yet; skipping clear", topic);
                return;
            }

            Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions =
                    adminClient.describeTopics(Collections.singletonList(topic)).all().get(10, TimeUnit.SECONDS);

            org.apache.kafka.clients.admin.TopicDescription description = descriptions.get(topic);
            List<TopicPartition> partitions = new ArrayList<>();
            for (org.apache.kafka.common.TopicPartitionInfo partitionInfo : description.partitions()) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> endOffsets;
            try (KafkaConsumer<String, String> consumer = createAssignedConsumer(kafkaBootstrap)) {
                consumer.assign(partitions);
                endOffsets = consumer.endOffsets(partitions);
            }

            Map<TopicPartition, RecordsToDelete> truncation = new HashMap<>();
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.getOrDefault(partition, 0L);
                truncation.put(partition, RecordsToDelete.beforeOffset(endOffset));
            }

            DeleteRecordsResult result = adminClient.deleteRecords(truncation);
            result.all().get(10, TimeUnit.SECONDS);
            LOG.info("Cleared Kafka topic {} across {} partitions", topic, description.partitions().size());
        } catch (UnknownTopicOrPartitionException e) {
            LOG.info("Kafka topic {} does not exist yet; skipping clear", topic);
        }
    }

    private static List<JsonNode> collectAllMessages(KafkaConsumer<String, String> consumer,
                                                     long timeoutMs) {
        List<JsonNode> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode payload = MAPPER.readTree(record.value());
                    JsonNode message = payload.has("message") ? payload.path("message") : payload;
                    JsonNode entity = message.path("entity");

                    if (message.isMissingNode() || entity.isMissingNode()) {
                        continue;
                    }

                    messages.add(message);
                } catch (Exception e) {
                    LOG.warn("Unable to parse Kafka notification: {}", record.value(), e);
                }
            }
        }

        return messages;
    }

    private static Map<String, JsonNode> latestEntityMessagesByOperation(List<JsonNode> allMessages, String operation) {
        Map<String, JsonNode> latestUpdates = new LinkedHashMap<>();
        for (JsonNode message : allMessages) {
            JsonNode entity = message.path("entity");
            String operationType = message.path("operationType").asText("");
            String entityGuid = entity.path("guid").asText("");

            if (operation.equals(operationType) && !entityGuid.isEmpty()) {
                latestUpdates.put(entityGuid, message);
                LOG.info("Matched {} notification for entity {}: {}", operation, entityGuid, message);
            }
        }
        return latestUpdates;
    }

    private static JsonNode latestEntityMessageByOperation(List<JsonNode> allMessages, String operation, String guid) {
        return latestEntityMessagesByOperation(allMessages, operation).get(guid);
    }

    private static Map<String, JsonNode> latestEntityMessagesByGuid(List<JsonNode> allMessages) {
        Map<String, JsonNode> latestMessages = new LinkedHashMap<>();

        for (JsonNode message : allMessages) {
            JsonNode entity = message.path("entity");
            String entityGuid = entity.path("guid").asText("");

            if (!entityGuid.isEmpty()) {
                latestMessages.put(entityGuid, message);
                LOG.info("Matched {} notification for entity {}: {}",
                        message.path("operationType").asText(""),
                        entityGuid,
                        message);
            }
        }

        return latestMessages;
    }

    private static void assertUpdatedEntitiesHaveAllRelationships(Map<String, JsonNode> latestMessagesByGuid,
                                                                  String sourceGuid,
                                                                  String targetGuid,
                                                                  String appendedTargetGuid,
                                                                  String processGuid) {
        assertFalse(latestMessagesByGuid.isEmpty(), "Expected at least one full notification");
        assertTrue(latestMessagesByGuid.containsKey(processGuid), "Expected a full notification for process " + processGuid);

        assertEquals("ENTITY_UPDATE", latestMessagesByGuid.get(processGuid).path("operationType").asText(""),
                "Process should still emit an ENTITY_UPDATE full notification");

        JsonNode processEntity = latestMessagesByGuid.get(processGuid).path("entity");
        assertProcessRelationships(processEntity, List.of(sourceGuid), List.of(targetGuid, appendedTargetGuid),
                "Relationship-changing update should preserve the process relationship changes from the payload");

        assertEntityRelationshipIfNotified(latestMessagesByGuid, sourceGuid, "inputToProcesses", List.of(processGuid),
                "Source dataset should reference the updated process as an input relationship");
        assertEntityRelationshipIfNotified(latestMessagesByGuid, targetGuid, "outputFromProcesses", List.of(processGuid),
                "Original target dataset should reference the updated process as an output relationship");
        assertEntityRelationshipIfNotified(latestMessagesByGuid, appendedTargetGuid, "outputFromProcesses", List.of(processGuid),
                "Appended target dataset should reference the updated process as an output relationship");
    }

    private static void assertProcessRelationships(JsonNode processEntity,
                                                   List<String> expectedInputGuids,
                                                   List<String> expectedOutputGuids,
                                                   String messagePrefix) {
        assertRelationshipGuids(getRelationshipArray(processEntity, "inputs"),
                expectedInputGuids,
                messagePrefix + ": process inputs mismatch");
        assertRelationshipGuids(getRelationshipArray(processEntity, "outputs"),
                expectedOutputGuids,
                messagePrefix + ": process outputs mismatch");
    }

    private static void assertProcessRelationshipsFromEntity(AtlasEntity processEntity,
                                                             List<String> expectedOutputGuids,
                                                             String message) {
        Object outputs = getRelationshipValue(processEntity, "outputs");
        assertTrue(outputs instanceof List, message + ": outputs relationship is missing");

        List<String> actualGuids = new ArrayList<>();
        for (Object output : (List<?>) outputs) {
            String guid = extractGuid(output);
            if (guid != null) {
                actualGuids.add(guid);
            }
        }

        assertEquals(expectedOutputGuids.size(), actualGuids.size(), message + ": unexpected output count");
        assertTrue(actualGuids.containsAll(expectedOutputGuids), message + ": expected GUIDs " + expectedOutputGuids + " but got " + actualGuids);
    }

    private static void assertEntityRelationshipIfNotified(Map<String, JsonNode> latestMessagesByGuid,
                                                           String entityGuid,
                                                           String relationshipAttributeName,
                                                           List<String> expectedGuids,
                                                           String message) {
        JsonNode entityMessage = latestMessagesByGuid.get(entityGuid);

        if (entityMessage == null) {
            LOG.info("No full notification observed for entity {}. Skipping assertion: {}", entityGuid, message);
            return;
        }

        JsonNode entity = entityMessage.path("entity");
        assertRelationshipGuids(getRelationshipArray(entity, relationshipAttributeName),
                expectedGuids,
                message);
    }

    private static JsonNode getRelationshipArray(JsonNode entity, String relationshipName) {
        JsonNode relationshipArray = entity.path("relationshipAttributes").path(relationshipName);

        if (relationshipArray.isArray()) {
            return relationshipArray;
        }

        return entity.path("attributes").path(relationshipName);
    }

    private static Object getRelationshipValue(AtlasEntity entity, String relationshipName) {
        Object relationshipValue = entity.getRelationshipAttribute(relationshipName);

        if (relationshipValue != null) {
            return relationshipValue;
        }

        return entity.getAttribute(relationshipName);
    }

    private static String extractGuid(Object relationshipValue) {
        if (relationshipValue instanceof AtlasObjectId) {
            return ((AtlasObjectId) relationshipValue).getGuid();
        }

        if (relationshipValue instanceof Map) {
            Object guid = ((Map<?, ?>) relationshipValue).get("guid");
            return guid != null ? Objects.toString(guid, null) : null;
        }

        return null;
    }

    private static void assertRelationshipGuids(JsonNode relationshipArray,
                                                List<String> expectedGuids,
                                                String message) {
        assertTrue(relationshipArray.isArray(), message + ": relationship array is missing");

        List<String> actualGuids = new ArrayList<>();
        for (JsonNode relationship : relationshipArray) {
            String guid = relationship.path("guid").asText();
            if (!guid.isEmpty()) {
                actualGuids.add(guid);
            }
        }

        assertEquals(expectedGuids.size(), actualGuids.size(), message + ": unexpected relationship count");
        assertTrue(actualGuids.containsAll(expectedGuids), message + ": expected GUIDs " + expectedGuids + " but got " + actualGuids);
    }

    private static String normalizeAtlasBaseUrl(String atlasUrl) {
        String trimmed = atlasUrl.trim();

        if (trimmed.endsWith("/api/atlas/v2")) {
            return trimmed.substring(0, trimmed.length() - "/api/atlas/v2".length());
        }

        if (trimmed.endsWith("/")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }

        return trimmed;
    }
}
