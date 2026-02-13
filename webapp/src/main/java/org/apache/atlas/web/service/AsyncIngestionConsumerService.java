package org.apache.atlas.web.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka consumer for async ingestion events published by the fatgraph stack.
 * <p>
 * Reads from the {@code ATLAS_ASYNC_ENTITIES} topic and replays entity/typedef
 * mutations through leangraph's {@link EntityMutationService} and
 * {@link AtlasTypeDefStore}. Since {@code LEAN_GRAPH_ENABLED=true} on this stack,
 * writes automatically go to JanusGraph (5 core props) + Cassandra assets + ES.
 * <p>
 * Design principles (modeled after {@link DLQReplayService}):
 * <ul>
 *   <li>Pause/resume pattern to avoid {@code max.poll.interval.ms} timeout</li>
 *   <li>Manual offset commits for at-least-once delivery</li>
 *   <li>Exponential backoff for transient failures</li>
 *   <li>Poison pill handling: skip after max retries</li>
 *   <li>Rebalance listener for cleanup of retry/backoff trackers</li>
 * </ul>
 */
@Service
public class AsyncIngestionConsumerService {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIngestionConsumerService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Configuration ────────────────────────────────────────────────────
    private String bootstrapServers;

    @Value("${atlas.async.ingestion.consumer.enabled:false}")
    private boolean consumerEnabled;

    @Value("${atlas.async.ingestion.consumer.topic:ATLAS_ASYNC_ENTITIES}")
    private String topic;

    @Value("${atlas.async.ingestion.consumer.group.id:atlas_async_ingestion_consumer}")
    private String consumerGroupId;

    @Value("${atlas.async.ingestion.consumer.maxPollRecords:10}")
    private int maxPollRecords;

    @Value("${atlas.async.ingestion.consumer.maxPollIntervalMs:600000}")
    private int maxPollIntervalMs;

    @Value("${atlas.async.ingestion.consumer.sessionTimeoutMs:90000}")
    private int sessionTimeoutMs;

    @Value("${atlas.async.ingestion.consumer.heartbeatIntervalMs:30000}")
    private int heartbeatIntervalMs;

    @Value("${atlas.async.ingestion.consumer.pollTimeoutSeconds:15}")
    private int pollTimeoutSeconds;

    @Value("${atlas.async.ingestion.consumer.maxRetries:3}")
    private int maxRetries;

    @Value("${atlas.async.ingestion.consumer.retryDelayMs:5000}")
    private long retryDelayMs;

    @Value("${atlas.async.ingestion.consumer.exponentialBackoff.baseDelayMs:1000}")
    private long baseDelayMs;

    @Value("${atlas.async.ingestion.consumer.exponentialBackoff.maxDelayMs:60000}")
    private long maxDelayMs;

    @Value("${atlas.async.ingestion.consumer.exponentialBackoff.multiplier:2.0}")
    private double backoffMultiplier;

    @Value("${atlas.async.ingestion.consumer.shutdownWaitSeconds:60}")
    private int shutdownWaitSeconds;

    @Value("${atlas.async.ingestion.consumer.consumerCloseTimeoutSeconds:30}")
    private int consumerCloseTimeoutSeconds;

    @Value("${atlas.async.ingestion.consumer.trackerCleanupIntervalMs:300000}")
    private long trackerCleanupIntervalMs;

    @Value("${atlas.async.ingestion.consumer.trackerMaxAgeMs:3600000}")
    private long trackerMaxAgeMs;

    @Value("${atlas.async.ingestion.consumer.dlq.topic:ATLAS_ASYNC_INGESTION_DLQ}")
    private String dlqTopic;

    @Value("${atlas.async.ingestion.consumer.entityNotificationsEnabled:false}")
    private boolean entityNotificationsEnabled;

    // ── Dependencies ─────────────────────────────────────────────────────
    private final EntityMutationService entityMutationService;
    private final AtlasEntityStore entitiesStore;
    private final AtlasTypeDefStore typeDefStore;
    private final AtlasTypeRegistry typeRegistry;

    // ── State ────────────────────────────────────────────────────────────
    private volatile KafkaConsumer<String, String> consumer;
    private volatile KafkaProducer<String, String> dlqProducer;
    private volatile Thread consumerThread;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong skippedCount = new AtomicLong(0);
    private final AtomicLong dlqPublishCount = new AtomicLong(0);
    private volatile long lastProcessedTime = 0;
    private volatile String lastEventType = null;

    // Retry & backoff trackers keyed by "partition-offset"
    private final ConcurrentHashMap<String, RetryTrackerEntry> retryTracker = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> backoffTracker = new ConcurrentHashMap<>();
    private volatile long lastTrackerCleanupTime = System.currentTimeMillis();

    private static class RetryTrackerEntry {
        int retryCount;
        long lastAttemptTime;

        RetryTrackerEntry() {
            this.retryCount = 0;
            this.lastAttemptTime = System.currentTimeMillis();
        }
    }

    @Inject
    public AsyncIngestionConsumerService(EntityMutationService entityMutationService,
                                         AtlasEntityStore entitiesStore,
                                         AtlasTypeDefStore typeDefStore,
                                         AtlasTypeRegistry typeRegistry) {
        this.entityMutationService = entityMutationService;
        this.entitiesStore = entitiesStore;
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    @PostConstruct
    public void init() {
        if (!consumerEnabled) {
            LOG.info("AsyncIngestionConsumer is disabled (atlas.async.ingestion.consumer.enabled=false)");
            return;
        }
        start();
    }

    public synchronized void start() {
        if (isRunning.get()) {
            LOG.warn("AsyncIngestionConsumer is already running");
            return;
        }

        try {
            this.bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers");
        } catch (Exception e) {
            LOG.error("Failed to read Kafka bootstrap servers from config", e);
            return;
        }

        try {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMs));

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    LOG.info("Partitions revoked: {}", partitions);
                    for (TopicPartition tp : partitions) {
                        String prefix = tp.partition() + "-";
                        retryTracker.keySet().removeIf(k -> k.startsWith(prefix));
                        backoffTracker.keySet().removeIf(k -> k.startsWith(prefix));
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    LOG.info("Partitions assigned: {}", partitions);
                    for (TopicPartition tp : partitions) {
                        try {
                            long endOffset = consumer.endOffsets(Collections.singleton(tp)).getOrDefault(tp, -1L);
                            long position = consumer.position(tp);
                            LOG.info("Partition {} - position: {}, endOffset: {}, lag: {}",
                                    tp.partition(), position, endOffset, endOffset - position);
                        } catch (Exception e) {
                            LOG.warn("Failed to log offset info for partition {}", tp, e);
                        }
                    }
                }
            });

            isRunning.set(true);
            isHealthy.set(true);

            consumerThread = new Thread(this::processMessages, "AsyncIngestion-Consumer-Thread");
            consumerThread.setDaemon(false);
            consumerThread.start();

            LOG.info("AsyncIngestionConsumer started - topic: {}, group: {}", topic, consumerGroupId);

        } catch (Exception e) {
            LOG.error("Failed to start AsyncIngestionConsumer", e);
            isHealthy.set(false);
        }
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (!isRunning.get()) {
            return;
        }

        LOG.info("AsyncIngestionConsumer shutting down...");
        isRunning.set(false);

        // Wakeup consumer to interrupt poll()
        if (consumer != null) {
            try {
                consumer.wakeup();
            } catch (Exception e) {
                LOG.warn("Error waking up consumer", e);
            }
        }

        // Wait for consumer thread to finish
        if (consumerThread != null) {
            try {
                consumerThread.join(TimeUnit.SECONDS.toMillis(shutdownWaitSeconds));
                if (consumerThread.isAlive()) {
                    LOG.error("AsyncIngestionConsumer thread did not terminate within {} seconds", shutdownWaitSeconds);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for consumer thread to stop");
            }
        }

        // Close consumer
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(consumerCloseTimeoutSeconds));
                LOG.info("AsyncIngestionConsumer: Kafka consumer closed");
            } catch (Exception e) {
                LOG.warn("Error closing Kafka consumer", e);
            }
            consumer = null;
        }

        // Close DLQ producer
        if (dlqProducer != null) {
            try {
                dlqProducer.close(Duration.ofSeconds(10));
                LOG.info("AsyncIngestionConsumer: DLQ producer closed");
            } catch (Exception e) {
                LOG.warn("Error closing DLQ producer", e);
            }
            dlqProducer = null;
        }

        isHealthy.set(false);
        LOG.info("AsyncIngestionConsumer stopped - processed: {}, errors: {}, skipped: {}, dlqPublished: {}",
                processedCount.get(), errorCount.get(), skippedCount.get(), dlqPublishCount.get());
    }

    // ── Core Processing Loop ─────────────────────────────────────────────

    private void processMessages() {
        try {
            while (isRunning.get()) {
                try {
                    cleanupStaleTrackersIfNeeded();

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));
                    if (records.isEmpty()) {
                        continue;
                    }

                    // Pause all partitions to prevent max.poll.interval.ms timeout
                    Set<TopicPartition> assignment = consumer.assignment();
                    consumer.pause(assignment);

                    TopicPartition failedPartition = null;
                    long failedOffset = -1;

                    try {
                        for (ConsumerRecord<String, String> record : records) {
                            String trackerKey = record.partition() + "-" + record.offset();

                            try {
                                processRecord(record);

                                // Success — clean up trackers, commit offset
                                retryTracker.remove(trackerKey);
                                backoffTracker.remove(trackerKey);
                                consumer.commitSync(Collections.singletonMap(
                                        new TopicPartition(record.topic(), record.partition()),
                                        new OffsetAndMetadata(record.offset() + 1)));

                                processedCount.incrementAndGet();
                                lastProcessedTime = System.currentTimeMillis();

                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                RetryTrackerEntry entry = retryTracker.computeIfAbsent(trackerKey, k -> new RetryTrackerEntry());
                                entry.retryCount++;
                                entry.lastAttemptTime = System.currentTimeMillis();

                                if (entry.retryCount >= maxRetries) {
                                    // Poison pill — publish to DLQ, then skip
                                    publishToDlq(record, e, entry.retryCount);
                                    LOG.error("AsyncIngestionConsumer: skipping poison pill at partition={} offset={} " +
                                                    "after {} retries. EventId: {}", record.partition(), record.offset(),
                                            entry.retryCount, record.key(), e);
                                    skippedCount.incrementAndGet();
                                    retryTracker.remove(trackerKey);
                                    backoffTracker.remove(trackerKey);
                                    consumer.commitSync(Collections.singletonMap(
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset() + 1)));
                                } else {
                                    // Retry with backoff
                                    long backoff = calculateExponentialBackoff(trackerKey);
                                    LOG.warn("AsyncIngestionConsumer: retryable failure at partition={} offset={}, " +
                                                    "retry {}/{}, backoff {}ms", record.partition(), record.offset(),
                                            entry.retryCount, maxRetries, backoff, e);
                                    failedPartition = new TopicPartition(record.topic(), record.partition());
                                    failedOffset = record.offset();

                                    try {
                                        Thread.sleep(backoff);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                    break; // Break out of record loop, will seek back
                                }
                            }
                        }
                    } finally {
                        // Seek back to failed offset if needed
                        if (failedPartition != null && failedOffset >= 0) {
                            consumer.seek(failedPartition, failedOffset);
                        }
                        // Resume all partitions
                        consumer.resume(assignment);
                    }

                } catch (WakeupException e) {
                    if (isRunning.get()) {
                        LOG.warn("AsyncIngestionConsumer: unexpected wakeup", e);
                    }
                    // Expected during shutdown
                } catch (Exception e) {
                    LOG.error("AsyncIngestionConsumer: unexpected error in processing loop", e);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("AsyncIngestionConsumer: fatal error, marking unhealthy", e);
            isHealthy.set(false);
        }
    }

    // ── Record Processing ────────────────────────────────────────────────

    private void processRecord(ConsumerRecord<String, String> record) throws Exception {
        JsonNode envelope = MAPPER.readTree(record.value());

        String eventType = envelope.path("eventType").asText();
        String eventId = envelope.path("eventId").asText();
        JsonNode requestMetadata = envelope.path("requestMetadata");
        JsonNode operationMetadata = envelope.path("operationMetadata");
        JsonNode payload = envelope.path("payload");

        lastEventType = eventType;

        LOG.info("AsyncIngestionConsumer: processing event {} type={} partition={} offset={}",
                eventId, eventType, record.partition(), record.offset());

        // Set request context from the original request metadata
        setupRequestContext(requestMetadata);

        try {
            routeAndProcess(eventType, payload, operationMetadata);
        } finally {
            RequestContext.clear();
        }
    }

    private void setupRequestContext(JsonNode requestMetadata) {
        RequestContext ctx = RequestContext.get();
        if (requestMetadata.has("user")) {
            ctx.setUser(requestMetadata.get("user").asText(), null);
        }
        if (requestMetadata.has("traceId")) {
            ctx.setTraceId(requestMetadata.get("traceId").asText());
        }
        ctx.setSkipEntityChangeNotification(!entityNotificationsEnabled);
    }

    private void routeAndProcess(String eventType, JsonNode payload, JsonNode operationMetadata) throws Exception {
        switch (eventType) {
            // ── Entity mutations ─────────────────────────────────────
            case "BULK_CREATE_OR_UPDATE":
                replayCreateOrUpdate(payload, operationMetadata);
                break;
            case "DELETE_BY_GUID":
                replayDeleteByGuid(payload);
                break;
            case "DELETE_BY_GUIDS":
                replayDeleteByGuids(payload);
                break;
            case "RESTORE_BY_GUIDS":
                replayRestoreByGuids(payload);
                break;
            case "DELETE_BY_UNIQUE_ATTRIBUTE":
                replayDeleteByUniqueAttribute(payload, operationMetadata);
                break;
            case "BULK_DELETE_BY_UNIQUE_ATTRIBUTES":
                replayBulkDeleteByUniqueAttributes(payload);
                break;
            case "SET_CLASSIFICATIONS":
                replaySetClassifications(payload, operationMetadata);
                break;

            // ── Classification mutations (to be added to fatgraph producer) ──
            case "ADD_CLASSIFICATIONS":
                replayAddClassifications(payload);
                break;
            case "UPDATE_CLASSIFICATIONS":
                replayUpdateClassifications(payload);
                break;
            case "DELETE_CLASSIFICATION":
                replayDeleteClassification(payload, operationMetadata);
                break;
            case "ADD_CLASSIFICATION_BULK":
                replayAddClassificationBulk(payload);
                break;

            // ── Relationship mutations (to be added to fatgraph producer) ──
            case "DELETE_RELATIONSHIP":
                replayDeleteRelationship(payload);
                break;
            case "DELETE_RELATIONSHIPS":
                replayDeleteRelationships(payload);
                break;

            // ── Partial update mutations ─────────────────────────────
            case "PARTIAL_UPDATE_BY_GUID":
                replayPartialUpdateByGuid(payload, operationMetadata);
                break;
            case "UPDATE_BY_UNIQUE_ATTRIBUTES":
                replayUpdateByUniqueAttributes(payload, operationMetadata);
                break;

            // ── Labels ─────────────────────────────────────────────────
            case "ADD_LABELS":
                replayAddLabels(payload);
                break;

            // ── TypeDef mutations ────────────────────────────────────
            case "TYPEDEF_CREATE":
                replayTypeDefCreate(payload);
                break;
            case "TYPEDEF_UPDATE":
                replayTypeDefUpdate(payload, operationMetadata);
                break;
            case "TYPEDEF_DELETE":
                replayTypeDefDelete(payload);
                break;
            case "TYPEDEF_DELETE_BY_NAME":
                replayTypeDefDeleteByName(payload);
                break;

            default:
                LOG.warn("AsyncIngestionConsumer: unknown event type '{}', skipping", eventType);
                skippedCount.incrementAndGet();
        }
    }

    // ── Entity Replay Methods ────────────────────────────────────────────

    /**
     * BULK_CREATE_OR_UPDATE: payload = {"entities": [{typeName, attributes, guid, ...}, ...]}
     */
    private void replayCreateOrUpdate(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        AtlasEntitiesWithExtInfo entities = AtlasType.fromJson(payload.toString(), AtlasEntitiesWithExtInfo.class);
        BulkRequestContext ctx = BulkRequestContext.fromOperationMetadata(operationMetadata);
        entityMutationService.createOrUpdate(new AtlasEntityStream(entities), ctx);
    }

    /**
     * DELETE_BY_GUID: payload = {"guid": "guid-table-001"}
     */
    private void replayDeleteByGuid(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        entityMutationService.deleteById(guid);
    }

    /**
     * DELETE_BY_GUIDS: payload = {"guids": ["guid1", "guid2", ...]}
     */
    private void replayDeleteByGuids(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        entityMutationService.deleteByIds(guids);
    }

    /**
     * RESTORE_BY_GUIDS: payload = {"guids": ["guid1", "guid2"]}
     */
    private void replayRestoreByGuids(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        entityMutationService.restoreByIds(guids);
    }

    /**
     * DELETE_BY_UNIQUE_ATTRIBUTE: payload = {"typeName": "Table", "uniqueAttributes": {"qualifiedName": "..."}}
     * operationMetadata = {"deleteType": "SOFT", "typeName": "Table"}
     */
    private void replayDeleteByUniqueAttribute(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String typeName = payload.get("typeName").asText();
        Map<String, Object> uniqAttrs = MAPPER.convertValue(payload.get("uniqueAttributes"),
                new TypeReference<Map<String, Object>>() {});
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException("Unknown entity type: " + typeName);
        }
        entityMutationService.deleteByUniqueAttributes(entityType, uniqAttrs);
    }

    /**
     * BULK_DELETE_BY_UNIQUE_ATTRIBUTES: payload = [{typeName, uniqueAttributes}, ...]
     */
    private void replayBulkDeleteByUniqueAttributes(JsonNode payload) throws AtlasBaseException {
        List<AtlasObjectId> objectIds = MAPPER.convertValue(payload, new TypeReference<List<AtlasObjectId>>() {});
        entityMutationService.deleteByUniqueAttributes(objectIds);
    }

    /**
     * SET_CLASSIFICATIONS: payload = [{typeName, attributes, entityGuid, entityStatus}, ...]
     * operationMetadata = {"overrideClassifications": false}
     */
    private void replaySetClassifications(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        // The payload is an array of classification entries with entityGuid.
        // We need to convert them to AtlasEntityHeaders (guidHeaderMap).
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();
        for (JsonNode entry : payload) {
            String entityGuid = entry.get("entityGuid").asText();
            AtlasEntityHeader header = guidHeaderMap.computeIfAbsent(entityGuid, g -> {
                AtlasEntityHeader h = new AtlasEntityHeader();
                h.setGuid(g);
                h.setClassifications(new ArrayList<>());
                return h;
            });
            AtlasClassification classification = AtlasType.fromJson(entry.toString(), AtlasClassification.class);
            header.getClassifications().add(classification);
        }
        AtlasEntityHeaders entityHeaders = new AtlasEntityHeaders(guidHeaderMap);
        boolean override = operationMetadata.path("overrideClassifications").asBoolean(false);
        entityMutationService.setClassifications(entityHeaders, override);
    }

    // ── Classification Replay Methods (stubs for future fatgraph producer events) ──

    /**
     * ADD_CLASSIFICATIONS: payload = {"guid": "...", "classifications": [...]}
     */
    private void replayAddClassifications(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        List<AtlasClassification> classifications = MAPPER.convertValue(
                payload.get("classifications"), new TypeReference<List<AtlasClassification>>() {});
        entityMutationService.addClassifications(guid, classifications);
    }

    /**
     * UPDATE_CLASSIFICATIONS: payload = {"guid": "...", "classifications": [...]}
     */
    private void replayUpdateClassifications(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        List<AtlasClassification> classifications = MAPPER.convertValue(
                payload.get("classifications"), new TypeReference<List<AtlasClassification>>() {});
        entityMutationService.updateClassifications(guid, classifications);
    }

    /**
     * DELETE_CLASSIFICATION: payload = {"guid": "...", "classificationName": "..."}
     * operationMetadata may contain {"associatedEntityGuid": "..."}
     */
    private void replayDeleteClassification(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        String classificationName = payload.get("classificationName").asText();
        String associatedEntityGuid = operationMetadata.has("associatedEntityGuid")
                ? operationMetadata.get("associatedEntityGuid").asText() : null;

        if (associatedEntityGuid != null) {
            entityMutationService.deleteClassification(guid, classificationName, associatedEntityGuid);
        } else {
            entityMutationService.deleteClassification(guid, classificationName);
        }
    }

    /**
     * ADD_CLASSIFICATION_BULK: payload = {"guids": [...], "classification": {...}}
     */
    private void replayAddClassificationBulk(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        AtlasClassification classification = AtlasType.fromJson(
                payload.get("classification").toString(), AtlasClassification.class);
        entityMutationService.addClassification(guids, classification);
    }

    // ── Relationship Replay Methods (stubs for future fatgraph producer events) ──

    /**
     * DELETE_RELATIONSHIP: payload = {"guid": "..."}
     */
    private void replayDeleteRelationship(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        entityMutationService.deleteRelationshipById(guid);
    }

    /**
     * DELETE_RELATIONSHIPS: payload = {"guids": [...]}
     */
    private void replayDeleteRelationships(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        entityMutationService.deleteRelationshipsByIds(guids);
    }

    // ── Partial Update & Attribute Replay Methods ─────────────────────────

    /**
     * PARTIAL_UPDATE_BY_GUID: payload = {"guid": "...", "attrName": "...", "attrValue": ...}
     */
    private void replayPartialUpdateByGuid(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        String attrName = payload.get("attrName").asText();
        Object attrValue = MAPPER.convertValue(payload.get("attrValue"), Object.class);
        entitiesStore.updateEntityAttributeByGuid(guid, attrName, attrValue);
    }

    /**
     * UPDATE_BY_UNIQUE_ATTRIBUTES: payload = AtlasEntityWithExtInfo JSON
     * operationMetadata = {"typeName": "Table", "uniqueAttributes": {"qualifiedName": "..."}}
     */
    private void replayUpdateByUniqueAttributes(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String typeName = operationMetadata.get("typeName").asText();
        Map<String, Object> uniqAttrs = MAPPER.convertValue(
                operationMetadata.get("uniqueAttributes"), new TypeReference<Map<String, Object>>() {});
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException("Unknown entity type: " + typeName);
        }
        AtlasEntity.AtlasEntityWithExtInfo entityInfo = AtlasType.fromJson(
                payload.toString(), AtlasEntity.AtlasEntityWithExtInfo.class);
        entityMutationService.updateByUniqueAttributes(entityType, uniqAttrs, entityInfo);
    }

    /**
     * ADD_LABELS: payload = {"guid": "...", "labels": ["label1", "label2"]}
     */
    private void replayAddLabels(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        Set<String> labels = MAPPER.convertValue(
                payload.get("labels"), new TypeReference<Set<String>>() {});
        entitiesStore.addLabels(guid, labels);
    }

    // ── TypeDef Replay Methods ───────────────────────────────────────────

    /**
     * TYPEDEF_CREATE: payload = AtlasTypesDef JSON
     */
    private void replayTypeDefCreate(JsonNode payload) throws AtlasBaseException {
        AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
        typeDefStore.createTypesDef(typesDef);
    }

    /**
     * TYPEDEF_UPDATE: payload = AtlasTypesDef JSON
     * operationMetadata may contain {"allowDuplicateDisplayName": false, "patch": true}
     */
    private void replayTypeDefUpdate(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
        typeDefStore.updateTypesDef(typesDef);
    }

    /**
     * TYPEDEF_DELETE: payload = AtlasTypesDef JSON
     */
    private void replayTypeDefDelete(JsonNode payload) throws AtlasBaseException {
        AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
        typeDefStore.deleteTypesDef(typesDef);
    }

    /**
     * TYPEDEF_DELETE_BY_NAME: payload = {"typeName": "CustomTable"}
     */
    private void replayTypeDefDeleteByName(JsonNode payload) throws AtlasBaseException {
        String typeName = payload.get("typeName").asText();
        typeDefStore.deleteTypeByName(typeName);
    }

    // ── DLQ Publishing ─────────────────────────────────────────────────

    /**
     * Publish a failed record to the DLQ topic with error context.
     * Best-effort: logs errors but does not throw.
     */
    private void publishToDlq(ConsumerRecord<String, String> record, Exception error, int retryCount) {
        try {
            KafkaProducer<String, String> producer = getOrCreateDlqProducer();
            if (producer == null) {
                LOG.error("AsyncIngestionConsumer: DLQ producer unavailable, cannot publish failed record " +
                        "partition={} offset={}", record.partition(), record.offset());
                return;
            }

            com.fasterxml.jackson.databind.node.ObjectNode dlqEnvelope = MAPPER.createObjectNode();
            dlqEnvelope.put("originalTopic", record.topic());
            dlqEnvelope.put("originalPartition", record.partition());
            dlqEnvelope.put("originalOffset", record.offset());
            dlqEnvelope.put("originalKey", record.key());
            dlqEnvelope.put("originalValue", record.value());
            dlqEnvelope.put("errorMessage", error.getMessage());
            dlqEnvelope.put("errorClass", error.getClass().getName());
            dlqEnvelope.put("retryCount", retryCount);
            dlqEnvelope.put("failedAt", System.currentTimeMillis());

            String json = MAPPER.writeValueAsString(dlqEnvelope);
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, record.key(), json);
            producer.send(dlqRecord).get(10, TimeUnit.SECONDS);

            dlqPublishCount.incrementAndGet();
            LOG.info("AsyncIngestionConsumer: published failed record to DLQ topic={} partition={} offset={}",
                    dlqTopic, record.partition(), record.offset());

        } catch (Exception e) {
            LOG.error("AsyncIngestionConsumer: failed to publish to DLQ (non-fatal) partition={} offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    private KafkaProducer<String, String> getOrCreateDlqProducer() {
        if (dlqProducer == null) {
            synchronized (this) {
                if (dlqProducer == null) {
                    try {
                        Properties props = new Properties();
                        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringSerializer");
                        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringSerializer");
                        props.put(ProducerConfig.ACKS_CONFIG, "all");

                        dlqProducer = new KafkaProducer<>(props);
                        LOG.info("AsyncIngestionConsumer: DLQ producer created for topic {}", dlqTopic);
                    } catch (Exception e) {
                        LOG.error("AsyncIngestionConsumer: failed to create DLQ producer", e);
                        return null;
                    }
                }
            }
        }
        return dlqProducer;
    }

    // ── Backoff & Tracker Helpers ────────────────────────────────────────

    private long calculateExponentialBackoff(String trackerKey) {
        Long currentDelay = backoffTracker.get(trackerKey);
        if (currentDelay == null) {
            currentDelay = baseDelayMs;
        }
        long nextDelay = Math.min((long) (currentDelay * backoffMultiplier), maxDelayMs);
        backoffTracker.put(trackerKey, nextDelay);
        return currentDelay;
    }

    private void cleanupStaleTrackersIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastTrackerCleanupTime < trackerCleanupIntervalMs) {
            return;
        }
        lastTrackerCleanupTime = now;

        int removedRetry = 0;
        Iterator<Map.Entry<String, RetryTrackerEntry>> it = retryTracker.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RetryTrackerEntry> entry = it.next();
            if (now - entry.getValue().lastAttemptTime > trackerMaxAgeMs) {
                it.remove();
                backoffTracker.remove(entry.getKey());
                removedRetry++;
            }
        }

        if (removedRetry > 0) {
            LOG.info("AsyncIngestionConsumer: cleaned up {} stale tracker entries", removedRetry);
        }
    }

    // ── Status & Health ──────────────────────────────────────────────────

    public boolean isHealthy() {
        return isHealthy.get() && consumerThread != null && consumerThread.isAlive();
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("isRunning", isRunning.get());
        status.put("isHealthy", isHealthy());
        status.put("processedCount", processedCount.get());
        status.put("errorCount", errorCount.get());
        status.put("skippedCount", skippedCount.get());
        status.put("dlqPublishCount", dlqPublishCount.get());
        status.put("dlqTopic", dlqTopic);
        status.put("topic", topic);
        status.put("consumerGroup", consumerGroupId);
        status.put("lastProcessedTime", lastProcessedTime > 0 ? new Date(lastProcessedTime).toString() : "N/A");
        status.put("lastEventType", lastEventType != null ? lastEventType : "N/A");
        status.put("activeRetries", retryTracker.size());
        status.put("activeBackoffs", backoffTracker.size());
        status.put("maxRetries", maxRetries);

        Map<String, Object> backoffConfig = new LinkedHashMap<>();
        backoffConfig.put("baseDelayMs", baseDelayMs);
        backoffConfig.put("maxDelayMs", maxDelayMs);
        backoffConfig.put("multiplier", backoffMultiplier);
        status.put("exponentialBackoffConfig", backoffConfig);

        return status;
    }

    /**
     * Returns per-partition consumer lag. Useful for switchover readiness checks.
     */
    public Map<String, Object> getConsumerLag() {
        Map<String, Object> lagInfo = new LinkedHashMap<>();
        if (consumer == null || !isRunning.get()) {
            lagInfo.put("totalLag", -1);
            lagInfo.put("error", "Consumer is not running");
            return lagInfo;
        }

        try {
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            Map<String, Long> partitionLags = new LinkedHashMap<>();
            long totalLag = 0;

            for (TopicPartition tp : assignment) {
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long committed = 0;
                OffsetAndMetadata committedMeta = consumer.committed(tp);
                if (committedMeta != null) {
                    committed = committedMeta.offset();
                }
                long lag = Math.max(0, endOffset - committed);
                partitionLags.put(String.valueOf(tp.partition()), lag);
                totalLag += lag;
            }

            lagInfo.put("totalLag", totalLag);
            lagInfo.put("partitions", partitionLags);
        } catch (Exception e) {
            LOG.warn("Failed to compute consumer lag", e);
            lagInfo.put("totalLag", -1);
            lagInfo.put("error", e.getMessage());
        }

        return lagInfo;
    }

    // TODO: Once ordering by entity GUID is implemented on the producer side,
    // validate partition-level ordering in the consumer for correctness.
}
