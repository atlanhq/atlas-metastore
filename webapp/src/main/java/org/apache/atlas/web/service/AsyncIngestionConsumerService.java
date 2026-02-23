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
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.service.redis.RedisService;
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
import org.janusgraph.diskstorage.TemporaryBackendException;
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
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
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

    @Value("${atlas.async.ingestion.consumer.autoOffsetReset:earliest}")
    private String autoOffsetReset;

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
    private final AtlasRelationshipStore relationshipStore;
    private final AtlasTypeDefStore typeDefStore;
    private final AtlasTypeRegistry typeRegistry;
    private final RedisService redisService;
    private String typeDefLock;

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

    // Cached lag info — updated on the consumer thread, safe to read from HTTP threads
    private volatile Map<String, Object> cachedLagInfo = Collections.emptyMap();

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
                                         AtlasRelationshipStore relationshipStore,
                                         AtlasTypeDefStore typeDefStore,
                                         AtlasTypeRegistry typeRegistry,
                                         RedisService redisService) {
        this.entityMutationService = entityMutationService;
        this.entitiesStore = entitiesStore;
        this.relationshipStore = relationshipStore;
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
        this.redisService = redisService;

        try {
            this.typeDefLock = ApplicationProperties.get().getString(ApplicationProperties.TYPEDEF_LOCK_NAME, "atlas:type-def:lock");
        } catch (Exception e) {
            this.typeDefLock = "atlas:type-def:lock";
        }
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
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
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

                    // Update lag info on consumer thread (KafkaConsumer is not thread-safe)
                    updateCachedLagInfo();

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

                            } catch (TemporaryBackendException tbe) {
                                // Transient backend error — exponential backoff without counting toward max retries
                                errorCount.incrementAndGet();
                                long backoff = calculateExponentialBackoff(trackerKey);
                                LOG.warn("AsyncIngestionConsumer: temporary backend exception at partition={} offset={}, " +
                                                "backoff {}ms. Will retry without incrementing retry count.",
                                        record.partition(), record.offset(), backoff, tbe);
                                failedPartition = new TopicPartition(record.topic(), record.partition());
                                failedOffset = record.offset();

                                try {
                                    Thread.sleep(backoff);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                break; // Break out of record loop, will seek back
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
        } catch (Exception e) {
            LOG.error("AsyncIngestion: failed to process eventType={}, eventId={}, error={}",
                    eventType, eventId, e.getMessage(), e);
            throw e;
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
        ctx.setImportInProgress(true);
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

            // ── Relationship mutations ────────────────────────────────
            case "DELETE_RELATIONSHIP_BY_GUID":
                replayDeleteRelationship(payload);
                break;
            case "DELETE_RELATIONSHIPS_BY_GUIDS":
                replayDeleteRelationships(payload);
                break;
            case "RELATIONSHIP_CREATE":
                replayRelationshipCreate(payload);
                break;
            case "RELATIONSHIP_BULK_CREATE_OR_UPDATE":
                replayRelationshipBulkCreateOrUpdate(payload);
                break;
            case "RELATIONSHIP_UPDATE":
                replayRelationshipUpdate(payload);
                break;

            // ── Partial update mutations ─────────────────────────────
            case "PARTIAL_UPDATE_BY_GUID":
                replayPartialUpdateByGuid(payload, operationMetadata);
                break;
            case "UPDATE_BY_UNIQUE_ATTRIBUTE":
                replayUpdateByUniqueAttributes(payload, operationMetadata);
                break;

            // ── Labels ─────────────────────────────────────────────────
            case "ADD_LABELS":
                replayAddLabels(payload);
                break;

            // ── Business metadata mutations ───────────────────────────
            case "ADD_OR_UPDATE_BUSINESS_ATTRIBUTES":
                replayAddOrUpdateBusinessAttributes(payload, operationMetadata);
                break;
            case "ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME":
                replayAddOrUpdateBusinessAttributesByDisplayName(payload, operationMetadata);
                break;
            case "REMOVE_BUSINESS_ATTRIBUTES":
                replayRemoveBusinessAttributes(payload, operationMetadata);
                break;

            // ── TypeDef mutations ────────────────────────────────────
            case "TYPEDEF_CREATE":
                replayTypeDefCreate(payload, operationMetadata);
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
        if (entities == null) {
            throw new AtlasBaseException("Failed to deserialize AtlasEntitiesWithExtInfo from payload");
        }
        normalizeRelationshipAttributes(entities);

        if (entities.getEntities() != null) {
            LOG.info("AsyncIngestion: BULK_CREATE_OR_UPDATE entityCount={}, types={}, guids={}",
                    entities.getEntities().size(),
                    entities.getEntities().stream().map(AtlasEntity::getTypeName).collect(Collectors.toList()),
                    entities.getEntities().stream().map(AtlasEntity::getGuid).collect(Collectors.toList()));
        }

        BulkRequestContext ctx = BulkRequestContext.fromOperationMetadata(operationMetadata);

        if (ctx.isSkipProcessEdgeRestoration()) {
            RequestContext.get().setSkipProcessEdgeRestoration(true);
        }

        entityMutationService.createOrUpdate(new AtlasEntityStream(entities), ctx);
        LOG.info("AsyncIngestion: BULK_CREATE_OR_UPDATE completed successfully");
    }

    /**
     * After JSON deserialization, relationshipAttributes values are LinkedHashMap (not AtlasObjectId).
     * Preprocessors like TermPreProcessor.setAnchor() cast them to AtlasObjectId, which fails.
     * This method normalizes Map values to AtlasObjectId instances.
     */
    @SuppressWarnings("unchecked")
    private void normalizeRelationshipAttributes(AtlasEntitiesWithExtInfo entities) {
        if (entities == null || entities.getEntities() == null) {
            return;
        }
        for (AtlasEntity entity : entities.getEntities()) {
            Map<String, Object> relAttrs = entity.getRelationshipAttributes();
            if (relAttrs == null) {
                continue;
            }
            for (Map.Entry<String, Object> entry : relAttrs.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Map && !(value instanceof AtlasObjectId)) {
                    entry.setValue(new AtlasObjectId((Map) value));
                } else if (value instanceof List) {
                    List<Object> normalized = new ArrayList<>();
                    for (Object item : (List<?>) value) {
                        if (item instanceof Map && !(item instanceof AtlasObjectId)) {
                            normalized.add(new AtlasObjectId((Map) item));
                        } else {
                            normalized.add(item);
                        }
                    }
                    entry.setValue(normalized);
                }
            }
        }
    }

    /**
     * DELETE_BY_GUID: payload = {"guids": ["guid-table-001"]}
     */
    private void replayDeleteByGuid(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        entityMutationService.deleteById(guids.get(0));
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
     * DELETE_BY_UNIQUE_ATTRIBUTE: operationMetadata = {"deleteType": "SOFT", "typeName": "Table"}
     * payload = {"uniqueAttributes": {"qualifiedName": "..."}}
     */
    private void replayDeleteByUniqueAttribute(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String typeName = operationMetadata.get("typeName").asText();
        Map<String, Object> uniqAttrs = MAPPER.convertValue(payload.get("uniqueAttributes"),
                new TypeReference<Map<String, Object>>() {});
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException("Unknown entity type: " + typeName);
        }
        entityMutationService.deleteByUniqueAttributes(entityType, uniqAttrs);
    }

    /**
     * BULK_DELETE_BY_UNIQUE_ATTRIBUTES: payload = {"objectIds": [{typeName, uniqueAttributes}, ...]}
     */
    private void replayBulkDeleteByUniqueAttributes(JsonNode payload) throws AtlasBaseException {
        List<AtlasObjectId> objectIds = MAPPER.convertValue(payload.get("objectIds"), new TypeReference<List<AtlasObjectId>>() {});
        entityMutationService.deleteByUniqueAttributes(objectIds);
    }

    /**
     * SET_CLASSIFICATIONS: payload = AtlasEntityHeaders JSON {"guidHeaderMap": {"guid1": {...}, ...}}
     * operationMetadata = {"overrideClassifications": false}
     */
    private void replaySetClassifications(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        AtlasEntityHeaders entityHeaders = AtlasType.fromJson(payload.toString(), AtlasEntityHeaders.class);
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

        // Diagnostic: dump existing classifications BEFORE update
        try {
            List<AtlasClassification> existingCls = entitiesStore.getClassifications(guid);
            LOG.info("AsyncIngestion: UPDATE_CLASSIFICATIONS BEFORE guid={}, existingCount={}, existingTypes={}",
                    guid, existingCls.size(),
                    existingCls.stream().map(c -> c.getTypeName() + ":" + c.getAttributes()).collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.warn("AsyncIngestion: UPDATE_CLASSIFICATIONS could not fetch existing classifications for guid={}: {}", guid, e.getMessage());
        }

        LOG.info("AsyncIngestion: UPDATE_CLASSIFICATIONS incoming guid={}, classificationCount={}, types={}",
                guid, classifications.size(),
                classifications.stream().map(c -> c.getTypeName() + ":" + c.getAttributes()).collect(Collectors.toList()));

        entityMutationService.updateClassifications(guid, classifications);

        // Diagnostic: dump classifications AFTER update
        try {
            List<AtlasClassification> afterCls = entitiesStore.getClassifications(guid);
            LOG.info("AsyncIngestion: UPDATE_CLASSIFICATIONS AFTER guid={}, count={}, types={}",
                    guid, afterCls.size(),
                    afterCls.stream().map(c -> c.getTypeName() + ":" + c.getAttributes()).collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.warn("AsyncIngestion: UPDATE_CLASSIFICATIONS could not fetch post-update classifications for guid={}: {}", guid, e.getMessage());
        }
    }

    /**
     * DELETE_CLASSIFICATION: payload = {"guid": "...", "classificationName": "..."}
     * or payload = {"guid": "...", "classificationName": "...", "associatedEntityGuid": "..."}
     */
    private void replayDeleteClassification(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        String classificationName = payload.get("classificationName").asText();
        String associatedEntityGuid = payload.has("associatedEntityGuid")
                ? payload.get("associatedEntityGuid").asText() : null;

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

    // ── Relationship Replay Methods ─────────────────────────────────────

    /**
     * DELETE_RELATIONSHIP_BY_GUID: payload = {"guid": "..."}
     */
    private void replayDeleteRelationship(JsonNode payload) throws AtlasBaseException {
        String guid = payload.get("guid").asText();
        entityMutationService.deleteRelationshipById(guid);
    }

    /**
     * DELETE_RELATIONSHIPS_BY_GUIDS: payload = {"guids": [...]}
     */
    private void replayDeleteRelationships(JsonNode payload) throws AtlasBaseException {
        List<String> guids = MAPPER.convertValue(payload.get("guids"), new TypeReference<List<String>>() {});
        entityMutationService.deleteRelationshipsByIds(guids);
    }

    /**
     * RELATIONSHIP_CREATE: payload = AtlasRelationship JSON
     */
    private void replayRelationshipCreate(JsonNode payload) throws AtlasBaseException {
        AtlasRelationship relationship = AtlasType.fromJson(payload.toString(), AtlasRelationship.class);
        relationshipStore.create(relationship);
    }

    /**
     * RELATIONSHIP_BULK_CREATE_OR_UPDATE: payload = List of AtlasRelationship JSON
     */
    private void replayRelationshipBulkCreateOrUpdate(JsonNode payload) throws AtlasBaseException {
        List<AtlasRelationship> relationships = MAPPER.convertValue(payload, new TypeReference<List<AtlasRelationship>>() {});
        relationshipStore.createOrUpdate(relationships);
    }

    /**
     * RELATIONSHIP_UPDATE: payload = AtlasRelationship JSON
     */
    private void replayRelationshipUpdate(JsonNode payload) throws AtlasBaseException {
        AtlasRelationship relationship = AtlasType.fromJson(payload.toString(), AtlasRelationship.class);
        relationshipStore.update(relationship);
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
     * UPDATE_BY_UNIQUE_ATTRIBUTE: operationMetadata = {"typeName": "Table"}
     * payload = {"uniqueAttributes": {"qualifiedName": "..."}, "entity": {AtlasEntityWithExtInfo}}
     */
    private void replayUpdateByUniqueAttributes(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String typeName = operationMetadata.get("typeName").asText();
        Map<String, Object> uniqAttrs = MAPPER.convertValue(
                payload.get("uniqueAttributes"), new TypeReference<Map<String, Object>>() {});
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException("Unknown entity type: " + typeName);
        }
        AtlasEntity.AtlasEntityWithExtInfo entityInfo = AtlasType.fromJson(
                payload.get("entity").toString(), AtlasEntity.AtlasEntityWithExtInfo.class);
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

    // ── Business Metadata Replay Methods ─────────────────────────────────

    /**
     * ADD_OR_UPDATE_BUSINESS_ATTRIBUTES: operationMetadata = {"guid": "...", "isOverwrite": false}
     * optionally operationMetadata may contain {"bmName": "..."} for single BM operations.
     * payload = business attributes map
     */
    private void replayAddOrUpdateBusinessAttributes(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = operationMetadata.get("guid").asText();
        boolean isOverwrite = operationMetadata.path("isOverwrite").asBoolean(false);

        LOG.info("AsyncIngestion: ADD_OR_UPDATE_BM guid={}, isOverwrite={}", guid, isOverwrite);

        if (operationMetadata.has("bmName")) {
            String bmName = operationMetadata.get("bmName").asText();
            Map<String, Object> bmAttrs = MAPPER.convertValue(payload, new TypeReference<Map<String, Object>>() {});
            LOG.info("AsyncIngestion: ADD_OR_UPDATE_BM single bmName={}", bmName);
            entitiesStore.addOrUpdateBusinessAttributes(guid, Collections.singletonMap(bmName, bmAttrs), isOverwrite);
        } else {
            Map<String, Map<String, Object>> bmAttrs = MAPPER.convertValue(payload, new TypeReference<Map<String, Map<String, Object>>>() {});
            LOG.info("AsyncIngestion: ADD_OR_UPDATE_BM calling entitiesStore with bmNames={}", bmAttrs.keySet());
            entitiesStore.addOrUpdateBusinessAttributes(guid, bmAttrs, isOverwrite);
        }
    }

    /**
     * ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME: same format as ADD_OR_UPDATE_BUSINESS_ATTRIBUTES
     * but calls addOrUpdateBusinessAttributesByDisplayName.
     */
    private void replayAddOrUpdateBusinessAttributesByDisplayName(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = operationMetadata.get("guid").asText();
        boolean isOverwrite = operationMetadata.path("isOverwrite").asBoolean(false);
        Map<String, Map<String, Object>> bmAttrs = MAPPER.convertValue(payload, new TypeReference<Map<String, Map<String, Object>>>() {});
        LOG.info("AsyncIngestion: ADD_OR_UPDATE_BM_BY_DISPLAY_NAME guid={}, isOverwrite={}, displayNames={}",
                guid, isOverwrite, bmAttrs.keySet());
        entitiesStore.addOrUpdateBusinessAttributesByDisplayName(guid, bmAttrs, isOverwrite);
    }

    /**
     * REMOVE_BUSINESS_ATTRIBUTES: operationMetadata = {"guid": "..."}
     * optionally operationMetadata may contain {"bmName": "..."} for single BM operations.
     * payload = business attributes map
     */
    private void replayRemoveBusinessAttributes(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        String guid = operationMetadata.get("guid").asText();
        if (operationMetadata.has("bmName")) {
            String bmName = operationMetadata.get("bmName").asText();
            Map<String, Object> bmAttrs = MAPPER.convertValue(payload, new TypeReference<Map<String, Object>>() {});
            entitiesStore.removeBusinessAttributes(guid, Collections.singletonMap(bmName, bmAttrs));
        } else {
            Map<String, Map<String, Object>> bmAttrs = MAPPER.convertValue(payload, new TypeReference<Map<String, Map<String, Object>>>() {});
            entitiesStore.removeBusinessAttributes(guid, bmAttrs);
        }
    }

    // ── TypeDef Replay Methods ───────────────────────────────────────────

    /**
     * TYPEDEF_CREATE: payload = AtlasTypesDef JSON
     * operationMetadata may contain {"allowDuplicateDisplayName": false}
     */
    private void replayTypeDefCreate(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        Lock lock = null;
        try {
            try {
                lock = redisService.acquireDistributedLockV2(typeDefLock);
            } catch (Exception e) {
                throw new AtlasBaseException("AsyncIngestion: Error acquiring typedef lock for TYPEDEF_CREATE", e);
            }
            if (lock == null) {
                throw new AtlasBaseException("AsyncIngestion: Failed to acquire typedef lock for TYPEDEF_CREATE");
            }

            if (operationMetadata != null && operationMetadata.has("allowDuplicateDisplayName")) {
                RequestContext.get().setAllowDuplicateDisplayName(
                        operationMetadata.get("allowDuplicateDisplayName").asBoolean(false));
            }

            AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
            typeDefStore.createTypesDef(typesDef);

            incrementTypeDefVersion();
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
        }
    }

    /**
     * TYPEDEF_UPDATE: payload = AtlasTypesDef JSON
     * operationMetadata may contain {"allowDuplicateDisplayName": false, "patch": true}
     */
    private void replayTypeDefUpdate(JsonNode payload, JsonNode operationMetadata) throws AtlasBaseException {
        Lock lock = null;
        try {
            try {
                lock = redisService.acquireDistributedLockV2(typeDefLock);
            } catch (Exception e) {
                throw new AtlasBaseException("AsyncIngestion: Error acquiring typedef lock for TYPEDEF_UPDATE", e);
            }
            if (lock == null) {
                throw new AtlasBaseException("AsyncIngestion: Failed to acquire typedef lock for TYPEDEF_UPDATE");
            }

            if (operationMetadata != null && operationMetadata.has("allowDuplicateDisplayName")) {
                RequestContext.get().setAllowDuplicateDisplayName(
                        operationMetadata.get("allowDuplicateDisplayName").asBoolean(false));
            }
            if (operationMetadata != null && operationMetadata.has("patch")) {
                RequestContext.get().setInTypePatching(
                        operationMetadata.get("patch").asBoolean(false));
            }

            AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
            typeDefStore.updateTypesDef(typesDef);

            incrementTypeDefVersion();
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
        }
    }

    /**
     * TYPEDEF_DELETE: payload = AtlasTypesDef JSON
     */
    private void replayTypeDefDelete(JsonNode payload) throws AtlasBaseException {
        Lock lock = null;
        try {
            try {
                lock = redisService.acquireDistributedLockV2(typeDefLock);
            } catch (Exception e) {
                throw new AtlasBaseException("AsyncIngestion: Error acquiring typedef lock for TYPEDEF_DELETE", e);
            }
            if (lock == null) {
                throw new AtlasBaseException("AsyncIngestion: Failed to acquire typedef lock for TYPEDEF_DELETE");
            }

            AtlasTypesDef typesDef = AtlasType.fromJson(payload.toString(), AtlasTypesDef.class);
            typeDefStore.deleteTypesDef(typesDef);

            incrementTypeDefVersion();
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
        }
    }

    /**
     * TYPEDEF_DELETE_BY_NAME: payload = {"typeName": "CustomTable"}
     */
    private void replayTypeDefDeleteByName(JsonNode payload) throws AtlasBaseException {
        Lock lock = null;
        try {
            try {
                lock = redisService.acquireDistributedLockV2(typeDefLock);
            } catch (Exception e) {
                throw new AtlasBaseException("AsyncIngestion: Error acquiring typedef lock for TYPEDEF_DELETE_BY_NAME", e);
            }
            if (lock == null) {
                throw new AtlasBaseException("AsyncIngestion: Failed to acquire typedef lock for TYPEDEF_DELETE_BY_NAME");
            }

            String typeName = payload.get("typeName").asText();
            typeDefStore.deleteTypeByName(typeName);

            incrementTypeDefVersion();
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
        }
    }

    private void incrementTypeDefVersion() {
        try {
            long latestVersion = Long.parseLong(redisService.getValue(
                    AtlasTypeDefStoreInitializer.TYPEDEF_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(AtlasTypeDefStoreInitializer.TYPEDEF_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(latestVersion);
            LOG.info("AsyncIngestion: Incremented TYPEDEF_VERSION to {}", latestVersionStr);
        } catch (Exception e) {
            LOG.error("AsyncIngestion: Failed to increment typedef version in Redis", e);
        }
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
     * Returns a cached snapshot computed on the consumer thread (thread-safe).
     */
    public Map<String, Object> getConsumerLag() {
        if (!isRunning.get()) {
            Map<String, Object> lagInfo = new LinkedHashMap<>();
            lagInfo.put("totalLag", -1);
            lagInfo.put("error", "Consumer is not running");
            return lagInfo;
        }
        return cachedLagInfo;
    }

    /**
     * Compute consumer lag on the consumer thread (KafkaConsumer is not thread-safe).
     * Must only be called from the consumer thread.
     */
    private void updateCachedLagInfo() {
        try {
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()) {
                return;
            }
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(assignment);
            Map<String, Long> partitionLags = new LinkedHashMap<>();
            long totalLag = 0;

            for (TopicPartition tp : assignment) {
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long committed = 0;
                OffsetAndMetadata committedMeta = committedOffsets.get(tp);
                if (committedMeta != null) {
                    committed = committedMeta.offset();
                }
                long lag = Math.max(0, endOffset - committed);
                partitionLags.put(String.valueOf(tp.partition()), lag);
                totalLag += lag;
            }

            Map<String, Object> lagInfo = new LinkedHashMap<>();
            lagInfo.put("totalLag", totalLag);
            lagInfo.put("partitions", partitionLags);
            lagInfo.put("updatedAt", System.currentTimeMillis());
            cachedLagInfo = lagInfo;
        } catch (Exception e) {
            LOG.warn("Failed to compute consumer lag", e);
        }
    }

    // TODO: Once ordering by entity GUID is implemented on the producer side,
    // validate partition-level ordering in the consumer for correctness.
}
