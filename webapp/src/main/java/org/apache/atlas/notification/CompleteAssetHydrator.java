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
package org.apache.atlas.notification;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.atlas.repository.graphdb.cassandra.CassandraSessionProvider;
import org.apache.atlas.repository.graphdb.cassandra.JobLeaseManager;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.repository.graph.GraphHelper.isInternalType;

/**
 * Bootstraps the full-entity topic from ZeroGraph Cassandra, then continuously
 * rehydrates it from entity notifications.
 *
 * This process is singleton-guarded by a Cassandra-backed lease so multiple
 * nodes can start concurrently while only one active hydrator runs at a time.
 */
public class CompleteAssetHydrator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CompleteAssetHydrator.class);

    private static final String KAFKA_PROPERTY_PREFIX = "atlas.kafka";

    private static final String ENABLED_PROPERTY = "atlas.notification.complete.assets.hydrator.enabled";
    private static final String LEASE_NAME_PROPERTY = "atlas.notification.complete.assets.hydrator.lease.name";
    private static final String LEASE_TTL_SECONDS_PROPERTY = "atlas.notification.complete.assets.hydrator.lease.ttl.seconds";
    private static final String LEASE_RETRY_MS_PROPERTY = "atlas.notification.complete.assets.hydrator.lease.retry.ms";
    private static final String CONSUMER_GROUP_ID_PROPERTY = "atlas.notification.complete.assets.hydrator.consumer.group.id";
    private static final String CONSUMER_POLL_MS_PROPERTY = "atlas.notification.complete.assets.hydrator.consumer.poll.ms";
    private static final String CASSANDRA_SCAN_PAGE_SIZE_PROPERTY = "atlas.notification.complete.assets.hydrator.cassandra.scan.page.size";

    private static final String DEFAULT_LEASE_NAME = "atlas_complete_asset_hydrator";
    private static final int DEFAULT_LEASE_TTL_SECONDS = 60;
    private static final int DEFAULT_LEASE_RETRY_MS = 5000;
    private static final int DEFAULT_CONSUMER_POLL_MS = 1000;
    private static final int DEFAULT_CASSANDRA_SCAN_PAGE_SIZE = 500;

    private static final String PROCESS_INPUTS_EDGE = "__Process.inputs";
    private static final String PROCESS_OUTPUTS_EDGE = "__Process.outputs";

    private final Configuration configuration;
    private final CqlSession session;
    private final JobLeaseManager leaseManager;
    private final AtlasFullEntityNotificationPublisher publisher;
    private final Properties kafkaProperties;
    private final String entityNotificationsTopic;
    private final String fullEntityTopic;
    private final String leaseName;
    private final String consumerGroupId;
    private final int leaseTtlSeconds;
    private final int leaseRetryMs;
    private final int consumerPollMs;
    private final int cassandraScanPageSize;

    private final PreparedStatement selectVertexByIdStmt;
    private final PreparedStatement selectEdgesOutByVertexStmt;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean leaseHeld = new AtomicBoolean(false);

    private volatile KafkaConsumer<String, String> consumer;
    private volatile ScheduledExecutorService leaseRenewer;

    public CompleteAssetHydrator(Configuration configuration) {
        this.configuration = configuration;
        this.session = CassandraSessionProvider.getSession(configuration);
        this.leaseManager = new JobLeaseManager(session);
        this.fullEntityTopic = AtlasConfiguration.NOTIFICATION_ENTITIES_FULL_TOPIC_NAME.getString();
        this.publisher = new AtlasFullEntityNotificationPublisher(configuration, fullEntityTopic);
        this.kafkaProperties = createKafkaProperties(configuration);
        this.entityNotificationsTopic = AtlasConfiguration.NOTIFICATION_ENTITIES_TOPIC_NAME.getString();
        this.leaseName = configuration.getString(LEASE_NAME_PROPERTY, DEFAULT_LEASE_NAME);
        this.consumerGroupId = configuration.getString(CONSUMER_GROUP_ID_PROPERTY, DEFAULT_LEASE_NAME);
        this.leaseTtlSeconds = configuration.getInt(LEASE_TTL_SECONDS_PROPERTY, DEFAULT_LEASE_TTL_SECONDS);
        this.leaseRetryMs = configuration.getInt(LEASE_RETRY_MS_PROPERTY, DEFAULT_LEASE_RETRY_MS);
        this.consumerPollMs = configuration.getInt(CONSUMER_POLL_MS_PROPERTY, DEFAULT_CONSUMER_POLL_MS);
        this.cassandraScanPageSize = configuration.getInt(CASSANDRA_SCAN_PAGE_SIZE_PROPERTY, DEFAULT_CASSANDRA_SCAN_PAGE_SIZE);

        this.selectVertexByIdStmt = session.prepare(
                "SELECT vertex_id, properties, type_name, state FROM vertices WHERE vertex_id = ?"
        );
        this.selectEdgesOutByVertexStmt = session.prepare(
                "SELECT edge_id, in_vertex_id, edge_label, properties, state FROM edges_out WHERE out_vertex_id = ?"
        );
    }

    public static boolean isEnabled(Configuration configuration) {
        return configuration.getBoolean(ENABLED_PROPERTY, false);
    }

    public void runUntilStopped() {
        LOG.info("Starting complete asset hydrator. fullEntityTopic={}, entityNotificationsTopic={}, consumerGroupId={}, leaseName={}",
                fullEntityTopic, entityNotificationsTopic, consumerGroupId, leaseName);

        while (running.get()) {
            if (!tryAcquireLease()) {
                sleep(leaseRetryMs);
                continue;
            }

            try {
                runAsLeader();
            } catch (WakeupException e) {
                if (running.get()) {
                    LOG.warn("Complete asset hydrator consumer woke unexpectedly", e);
                }
            } catch (Exception e) {
                LOG.error("Complete asset hydrator failed while acting as leader", e);
            } finally {
                stopLeaseRenewer();
                leaseHeld.set(false);
                leaseManager.release(leaseName);
                closeConsumer();
            }
        }
    }

    public void stop() {
        running.set(false);
        stopLeaseRenewer();
        closeConsumer();
    }

    @Override
    public void close() {
        stop();
        publisher.close();
    }

    private boolean tryAcquireLease() {
        boolean acquired = leaseManager.tryAcquire(leaseName, leaseTtlSeconds);

        if (acquired) {
            leaseHeld.set(true);
            LOG.info("Acquired complete asset hydrator lease '{}'", leaseName);
        }

        return acquired;
    }

    private void runAsLeader() {
        startLeaseRenewer();

        Map<TopicPartition, Long> bootstrapResumeOffsets = null;
        if (isFullEntityTopicEmpty()) {
            bootstrapResumeOffsets = fetchTopicEndOffsets(entityNotificationsTopic);
            LOG.info("Full entity topic {} is empty. Bootstrapping from ZeroGraph Cassandra and resuming entity notifications from offsets {}",
                    fullEntityTopic, bootstrapResumeOffsets);
            bootstrapFromZeroGraph();
        } else {
            LOG.info("Full entity topic {} already has data. Skipping Cassandra bootstrap.", fullEntityTopic);
        }

        consumeEntityNotifications(bootstrapResumeOffsets);
    }

    private void startLeaseRenewer() {
        stopLeaseRenewer();

        leaseRenewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "complete-asset-hydrator-lease-renewer");
            thread.setDaemon(true);
            return thread;
        });

        long renewEveryMs = Math.max(1000L, TimeUnit.SECONDS.toMillis(leaseTtlSeconds) / 3);
        leaseRenewer.scheduleAtFixedRate(() -> {
            if (!running.get() || !leaseHeld.get()) {
                return;
            }

            if (!leaseManager.renew(leaseName, leaseTtlSeconds)) {
                LOG.warn("Lost complete asset hydrator lease '{}'; stopping current leader run", leaseName);
                leaseHeld.set(false);
                closeConsumer();
            }
        }, renewEveryMs, renewEveryMs, TimeUnit.MILLISECONDS);
    }

    private void stopLeaseRenewer() {
        ScheduledExecutorService renewer = leaseRenewer;
        leaseRenewer = null;

        if (renewer != null) {
            renewer.shutdownNow();
        }
    }

    private void bootstrapFromZeroGraph() {
        SimpleStatement stmt = SimpleStatement.builder(
                        "SELECT vertex_id, properties, type_name, state FROM vertices")
                .setPageSize(cassandraScanPageSize)
                .build();

        int scanned = 0;
        int published = 0;

        for (Row row : session.execute(stmt)) {
            if (!running.get() || !leaseHeld.get()) {
                return;
            }

            scanned++;

            AtlasEntity entity = buildEntitySnapshot(row);
            if (entity == null) {
                continue;
            }

            if (entity.getStatus() == AtlasEntity.Status.ACTIVE) {
                publisher.publishEntity(entity, OperationType.ENTITY_CREATE, System.currentTimeMillis(), Collections.emptyMap());
                published++;
            }

            if (published % 1000 == 0) {
                LOG.info("Complete asset hydrator bootstrap progress: scanned={} published={}", scanned, published);
            }
        }

        LOG.info("Complete asset hydrator bootstrap completed: scanned={} published={}", scanned, published);
    }

    private AtlasEntity buildEntitySnapshot(Row row) {
        String vertexId = row.getString("vertex_id");
        Map<String, Object> props = parseNormalizedProperties(row.getString("properties"));

        if (MapUtils.isEmpty(props)) {
            return null;
        }

        String guid = getString(props.get("__guid"));
        String typeName = StringUtils.defaultIfEmpty(getString(props.get("__typeName")), row.getString("type_name"));

        if (StringUtils.isBlank(guid) || StringUtils.isBlank(typeName) || isInternalType(typeName)) {
            return null;
        }

        AtlasEntity entity = new AtlasEntity(typeName, new HashMap<>());
        entity.setGuid(guid);
        entity.setStatus(toEntityStatus(StringUtils.defaultIfEmpty(getString(props.get("__state")), row.getString("state"))));

        entity.setCreatedBy(getString(props.get("__createdBy")));
        entity.setUpdatedBy(getString(props.get("__modifiedBy")));
        entity.setCreateTime(toDate(props.get("__timestamp")));
        entity.setUpdateTime(toDate(props.get("__modificationTimestamp")));
        entity.setDocId(toDocId(vertexId));

        Set<String> superTypes = toStringSet(props.get("__superTypeNames"));
        if (CollectionUtils.isNotEmpty(superTypes)) {
            entity.setSuperTypeNames(superTypes);
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            if (shouldPublishAttribute(entry.getKey(), entry.getValue())) {
                entity.setAttribute(entry.getKey(), entry.getValue());
            }
        }

        Map<String, Object> relationshipAttributes = buildRelationshipAttributes(vertexId, typeName);
        if (MapUtils.isNotEmpty(relationshipAttributes)) {
            entity.setRelationshipAttributes(relationshipAttributes);
        }

        return entity;
    }

    private Map<String, Object> buildRelationshipAttributes(String vertexId, String typeName) {
        if (!shouldLoadProcessRelationships(typeName)) {
            return Collections.emptyMap();
        }

        List<AtlasRelatedObjectId> inputs = new ArrayList<>();
        List<AtlasRelatedObjectId> outputs = new ArrayList<>();

        ResultSet edgeRows = session.execute(selectEdgesOutByVertexStmt.bind(vertexId));
        for (Row edgeRow : edgeRows) {
            String edgeLabel = edgeRow.getString("edge_label");
            if (!PROCESS_INPUTS_EDGE.equals(edgeLabel) && !PROCESS_OUTPUTS_EDGE.equals(edgeLabel)) {
                continue;
            }

            AtlasRelatedObjectId relatedObjectId = toRelatedObjectId(edgeRow.getString("in_vertex_id"), edgeRow);
            if (relatedObjectId == null) {
                continue;
            }

            if (PROCESS_INPUTS_EDGE.equals(edgeLabel)) {
                inputs.add(relatedObjectId);
            } else {
                outputs.add(relatedObjectId);
            }
        }

        Map<String, Object> relationshipAttributes = new LinkedHashMap<>();
        if (!inputs.isEmpty()) {
            relationshipAttributes.put("inputs", inputs);
        }
        if (!outputs.isEmpty()) {
            relationshipAttributes.put("outputs", outputs);
        }

        return relationshipAttributes;
    }

    private AtlasRelatedObjectId toRelatedObjectId(String targetVertexId, Row edgeRow) {
        if (StringUtils.isBlank(targetVertexId)) {
            return null;
        }

        Row vertexRow = session.execute(selectVertexByIdStmt.bind(targetVertexId)).one();
        if (vertexRow == null) {
            return null;
        }

        Map<String, Object> props = parseNormalizedProperties(vertexRow.getString("properties"));
        if (MapUtils.isEmpty(props)) {
            return null;
        }

        String guid = getString(props.get("__guid"));
        String typeName = StringUtils.defaultIfEmpty(getString(props.get("__typeName")), vertexRow.getString("type_name"));
        if (StringUtils.isBlank(guid) || StringUtils.isBlank(typeName) || isInternalType(typeName)) {
            return null;
        }

        Map<String, Object> uniqueAttributes = new HashMap<>();
        if (props.containsKey("qualifiedName")) {
            uniqueAttributes.put("qualifiedName", props.get("qualifiedName"));
        }

        String displayText = getString(props.get("name"));
        if (StringUtils.isBlank(displayText)) {
            displayText = getString(props.get("qualifiedName"));
        }

        Map<String, Object> edgeProps = parseRawProperties(edgeRow.getString("properties"));
        String relationshipGuid = getString(edgeProps.get("r:__guid"));

        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId(
                guid,
                typeName,
                toEntityStatus(StringUtils.defaultIfEmpty(getString(props.get("__state")), vertexRow.getString("state"))),
                uniqueAttributes,
                displayText,
                relationshipGuid,
                toRelationshipStatus(StringUtils.defaultIfEmpty(getString(edgeProps.get("__state")), edgeRow.getString("state"))),
                null
        );

        relatedObjectId.setRelationshipType(edgeRow.getString("edge_label"));
        relatedObjectId.setAttributes(new AtlasObjectId(guid, typeName, uniqueAttributes).getAttributes());

        return relatedObjectId;
    }

    private void consumeEntityNotifications(Map<TopicPartition, Long> bootstrapResumeOffsets) {
        while (running.get() && leaseHeld.get()) {
            try {
                KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(createConsumerProperties());
                consumer = kafkaConsumer;

                List<TopicPartition> partitions = waitForTopicPartitions(kafkaConsumer, entityNotificationsTopic);
                if (partitions.isEmpty()) {
                    closeConsumer();
                    sleep(leaseRetryMs);
                    continue;
                }

                kafkaConsumer.assign(partitions);
                initializeConsumerOffsets(kafkaConsumer, partitions, bootstrapResumeOffsets);

                while (running.get() && leaseHeld.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(consumerPollMs));
                    if (records.isEmpty()) {
                        continue;
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, String> record : records) {
                        processEntityNotification(record.value());
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    }

                    if (!offsets.isEmpty()) {
                        kafkaConsumer.commitSync(offsets);
                    }
                }

                return;
            } catch (WakeupException e) {
                if (!running.get() || !leaseHeld.get()) {
                    return;
                }

                throw e;
            } catch (Exception e) {
                LOG.error("Failed while consuming entity notifications for complete asset hydration", e);
                closeConsumer();
                sleep(leaseRetryMs);
            }
        }
    }

    private void processEntityNotification(String json) {
        if (StringUtils.isBlank(json)) {
            return;
        }

        Map<String, Object> payload = AtlasType.fromJson(json, Map.class);
        if (MapUtils.isEmpty(payload) || !payload.containsKey("entity")) {
            return;
        }

        Object operationTypeValue = payload.get("operationType");
        Map<String, Object> entityMap = payload.get("entity") instanceof Map ? (Map<String, Object>) payload.get("entity") : null;
        if (entityMap == null) {
            return;
        }

        AtlasEntity entity = toAtlasEntity(entityMap);
        if (entity == null || StringUtils.isBlank(entity.getGuid()) || isInternalType(entity.getTypeName())) {
            return;
        }

        OperationType operationType = operationTypeValue != null ? OperationType.valueOf(String.valueOf(operationTypeValue)) : null;
        if (operationType == OperationType.ENTITY_DELETE) {
            publisher.publishEntity(toAtlasEntity(entityMap), OperationType.ENTITY_DELETE, System.currentTimeMillis(), Collections.emptyMap());
        } else {
            AtlasEntity fullEntity = rehydrateEntity(entityMap);
            if (fullEntity != null) {
                publisher.publishEntity(fullEntity, operationType != null ? operationType : OperationType.ENTITY_UPDATE,
                        System.currentTimeMillis(), Collections.emptyMap());
            }
        }
    }

    private boolean isFullEntityTopicEmpty() {
        Map<TopicPartition, Long> endOffsets = fetchTopicEndOffsets(fullEntityTopic);
        if (endOffsets.isEmpty()) {
            return true;
        }

        for (Long offset : endOffsets.values()) {
            if (offset != null && offset > 0L) {
                return false;
            }
        }

        return true;
    }

    private AtlasEntity rehydrateEntity(Map<String, Object> entityMap) {
        AtlasEntity entity = toAtlasEntity(entityMap);
        if (entity == null) {
            return null;
        }

        String docId = entity.getDocId();
        if (StringUtils.isNotBlank(docId)) {
            Row row = findVertexRowByDocId(docId);
            if (row != null) {
                AtlasEntity hydrated = buildEntitySnapshot(row);
                if (hydrated != null) {
                    return hydrated;
                }
            }
        }

        return entity;
    }

    private Row findVertexRowByDocId(String docId) {
        try {
            long vertexId = LongEncoding.decode(docId);
            return session.execute(selectVertexByIdStmt.bind(String.valueOf(vertexId))).one();
        } catch (Exception e) {
            LOG.debug("Unable to resolve vertex row from docId {}", docId, e);
            return null;
        }
    }

    private static AtlasEntity toAtlasEntity(Map<String, Object> entityMap) {
        if (MapUtils.isEmpty(entityMap)) {
            return null;
        }

        return AtlasType.fromJson(AtlasType.toJson(entityMap), AtlasEntity.class);
    }

    private static String toDocId(String vertexId) {
        if (StringUtils.isBlank(vertexId)) {
            return null;
        }

        try {
            return LongEncoding.encode(Long.parseLong(vertexId));
        } catch (Exception e) {
            return null;
        }
    }

    private Map<TopicPartition, Long> fetchTopicEndOffsets(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaProperties);
             KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(createConsumerProperties())) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                return Collections.emptyMap();
            }

            List<TopicPartition> partitions = getTopicPartitions(kafkaConsumer, topicName);
            if (partitions.isEmpty()) {
                return Collections.emptyMap();
            }

            kafkaConsumer.assign(partitions);
            kafkaConsumer.seekToEnd(partitions);

            Map<TopicPartition, Long> endOffsets = new LinkedHashMap<>();
            for (TopicPartition partition : partitions) {
                endOffsets.put(partition, kafkaConsumer.position(partition));
            }

            return endOffsets;
        } catch (Exception e) {
            LOG.warn("Unable to determine end offsets for topic {}", topicName, e);
            return Collections.emptyMap();
        }
    }

    private List<TopicPartition> waitForTopicPartitions(KafkaConsumer<String, String> kafkaConsumer, String topicName) {
        while (running.get() && leaseHeld.get()) {
            List<TopicPartition> partitions = getTopicPartitions(kafkaConsumer, topicName);
            if (!partitions.isEmpty()) {
                return partitions;
            }

            LOG.info("Waiting for topic {} to become available for complete asset hydration", topicName);
            sleep(leaseRetryMs);
        }

        return Collections.emptyList();
    }

    private List<TopicPartition> getTopicPartitions(KafkaConsumer<String, String> kafkaConsumer, String topicName) {
        try {
            List<TopicPartition> partitions = new ArrayList<>();
            kafkaConsumer.partitionsFor(topicName).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())));
            return partitions;
        } catch (Exception e) {
            LOG.warn("Unable to fetch partitions for topic {}", topicName, e);
            return Collections.emptyList();
        }
    }

    private void initializeConsumerOffsets(KafkaConsumer<String, String> kafkaConsumer,
                                           List<TopicPartition> partitions,
                                           Map<TopicPartition, Long> bootstrapResumeOffsets) {
        if (bootstrapResumeOffsets != null && !bootstrapResumeOffsets.isEmpty()) {
            for (TopicPartition partition : partitions) {
                kafkaConsumer.seek(partition, bootstrapResumeOffsets.getOrDefault(partition, 0L));
            }
            return;
        }

        boolean foundCommittedOffsets = false;
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata committed = kafkaConsumer.committed(partition);
            if (committed != null) {
                kafkaConsumer.seek(partition, committed.offset());
                foundCommittedOffsets = true;
            }
        }

        if (!foundCommittedOffsets) {
            kafkaConsumer.seekToEnd(partitions);
        }
    }

    private void closeConsumer() {
        KafkaConsumer<String, String> kafkaConsumer = consumer;
        if (kafkaConsumer != null) {
            consumer = null;

            try {
                kafkaConsumer.wakeup();
            } catch (Exception ignored) {
                // nothing to do
            }

            try {
                kafkaConsumer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOG.warn("Failed to close complete asset hydrator consumer cleanly", e);
            }
        }
    }

    private Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        return properties;
    }

    private static Properties createKafkaProperties(Configuration atlasProperties) {
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(atlasProperties, KAFKA_PROPERTY_PREFIX);
        Properties properties = ConfigurationConverter.getProperties(kafkaConf);
        KafkaUtils.setKafkaJAASProperties(atlasProperties, properties);
        return properties;
    }

    private static boolean shouldPublishAttribute(String key, Object value) {
        if (StringUtils.isBlank(key) || value == null) {
            return false;
        }

        if (key.startsWith("__")) {
            return false;
        }

        return !key.startsWith("r:");
    }

    private static boolean shouldLoadProcessRelationships(String typeName) {
        if (StringUtils.isBlank(typeName)) {
            return false;
        }

        String lower = typeName.toLowerCase();
        return lower.contains("process") || lower.contains("airflowtask");
    }

    private static Map<String, Object> parseNormalizedProperties(String json) {
        Map<String, Object> raw = parseRawProperties(json);
        if (MapUtils.isEmpty(raw)) {
            return Collections.emptyMap();
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            normalized.put(normalizePropertyName(entry.getKey()), entry.getValue());
        }
        return normalized;
    }

    private static Map<String, Object> parseRawProperties(String json) {
        if (StringUtils.isBlank(json)) {
            return Collections.emptyMap();
        }

        Map<String, Object> properties = AtlasType.fromJson(json, Map.class);
        return properties != null ? properties : Collections.emptyMap();
    }

    private static String normalizePropertyName(String name) {
        if (name == null || name.startsWith("__")) {
            return name;
        }

        int dotIndex = name.indexOf('.');
        if (dotIndex > 0 && dotIndex < name.length() - 1) {
            return name.substring(dotIndex + 1);
        }

        return name;
    }

    private static AtlasEntity.Status toEntityStatus(String value) {
        if (StringUtils.isBlank(value)) {
            return AtlasEntity.Status.ACTIVE;
        }

        try {
            return AtlasEntity.Status.valueOf(value);
        } catch (IllegalArgumentException e) {
            return AtlasEntity.Status.ACTIVE;
        }
    }

    private static org.apache.atlas.model.instance.AtlasRelationship.Status toRelationshipStatus(String value) {
        if (StringUtils.isBlank(value)) {
            return org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
        }

        try {
            return org.apache.atlas.model.instance.AtlasRelationship.Status.valueOf(value);
        } catch (IllegalArgumentException e) {
            return org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
        }
    }

    private static Set<String> toStringSet(Object value) {
        if (!(value instanceof Collection)) {
            return Collections.emptySet();
        }

        Set<String> values = new HashSet<>();
        for (Object entry : (Collection<?>) value) {
            if (entry != null) {
                values.add(String.valueOf(entry));
            }
        }

        return values;
    }

    private static Date toDate(Object value) {
        if (value instanceof Date) {
            return (Date) value;
        }

        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }

        if (value instanceof String && StringUtils.isNotBlank((String) value)) {
            try {
                return new Date(Long.parseLong((String) value));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }

        return null;
    }

    private static String getString(Object value) {
        return value != null ? String.valueOf(value) : null;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
