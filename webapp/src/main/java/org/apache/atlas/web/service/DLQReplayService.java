package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.dlq.DLQEntry;
import org.janusgraph.diskstorage.dlq.SerializableIndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.IndexInfoRetriever;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for replaying DLQ messages back to Elasticsearch.
 * Uses Kafka consumer groups to ensure only one instance processes messages.
 */
@Service
public class DLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(DLQReplayService.class);

    private String bootstrapServers;
    @Value("${atlas.kafka.dlq.topic:ATLAS_ES_DLQ}")
    private String dlqTopic="ATLAS_ES_DLQ";

    @Value("${atlas.kafka.dlq.consumerGroupId:atlas_dq_replay_group}")
    private String consumerGroupId= "atlas_dq_replay_group";

    @Value("${atlas.kafka.dlq.maxRetries:3}")
    private int maxRetries = 3;

    // Kafka consumer configuration
    @Value("${atlas.kafka.dlq.maxPollRecords:10}")
    private int maxPollRecords = 10;

    @Value("${atlas.kafka.dlq.maxPollIntervalMs:600000}")
    private int maxPollIntervalMs = 600000; // 10 minutes

    @Value("${atlas.kafka.dlq.sessionTimeoutMs:90000}")
    private int sessionTimeoutMs = 90000; // 90 seconds

    @Value("${atlas.kafka.dlq.heartbeatIntervalMs:30000}")
    private int heartbeatIntervalMs = 30000; // 30 seconds

    // Timing configuration
    @Value("${atlas.kafka.dlq.pollTimeoutSeconds:5}")
    private int pollTimeoutSeconds = 5;

    @Value("${atlas.kafka.dlq.shutdownWaitMs:1000}")
    private int shutdownWaitMs = 1000;

    @Value("${atlas.kafka.dlq.consumerCloseTimeoutSeconds:30}")
    private int consumerCloseTimeoutSeconds = 30;

    @Value("${atlas.kafka.dlq.errorBackoffMs:10000}")
    private int errorBackoffMs = 10000; // 10 seconds

    private final ElasticSearchIndex esIndex;
    protected final IndexSerializer indexSerializer;
    private final ObjectMapper mapper;
    private GraphDatabaseConfiguration graphConfig;
    private StandardJanusGraph standardJanusGraph;

    // Track retry attempts per partition-offset to handle poison pills (in-memory)
    private final Map<String, Integer> retryTracker = new ConcurrentHashMap<>();

    private volatile KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private volatile Thread replayThread;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger skippedCount = new AtomicInteger(0);

    public DLQReplayService(AtlasJanusGraph graph) throws AtlasException {
        this.mapper = configureMapper();
        // Extract ES configuration from existing graph
        this.graphConfig = ((StandardJanusGraph)graph.getGraph()).getConfiguration();
        this.standardJanusGraph = ((StandardJanusGraph)graph.getGraph());
        this.bootstrapServers = ApplicationProperties.get().getString("atlas.graph.kafka.bootstrap.servers");
        Configuration fullConfig = graphConfig.getConfiguration();
        IndexProvider indexProvider = Backend.getImplementationClass(fullConfig.restrictTo("search"), fullConfig.get(GraphDatabaseConfiguration.INDEX_BACKEND,"search"),
                StandardIndexProvider.getAllProviderClasses());
        StoreFeatures storeFeatures = graphConfig.getBackend().getStoreFeatures();
        this.indexSerializer = new IndexSerializer(fullConfig, graphConfig.getSerializer(),
                graphConfig.getBackend().getIndexInformation(), storeFeatures.isDistributed() && storeFeatures.isKeyOrdered());
        esIndex = (ElasticSearchIndex) indexProvider;
    }

    /**
     * Start replaying DLQ messages
     */
    @PostConstruct
    public synchronized void startReplay() {
        if (isRunning.get()) {
            log.warn("DLQ replay is already running");
            return;
        }

        log.info("Starting DLQ replay service for topic: {} with consumer group: {}", dlqTopic, consumerGroupId);
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit after success
        // Use "latest" to avoid reprocessing all historical messages if consumer group state is lost
        // On first startup with no committed offsets, will only process new DLQ entries
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Optimized settings for long-running message processing with pause/resume pattern
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMs));

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dlqTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("Consumer group partitions revoked. Partitions: {}", partitions);
                
                // Clean up retry tracker for revoked partitions to prevent memory leak
                for (TopicPartition partition : partitions) {
                    retryTracker.keySet().removeIf(key -> key.startsWith(partition.partition() + "-"));
                    log.info("Cleaned up retry tracker for revoked partition: {}", partition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Consumer group partitions assigned: {}", partitions);

                // Log offset information for each partition
                for (TopicPartition partition : partitions) {
                    try {
                        long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                        long committedOffset = -1;
                        OffsetAndMetadata committed = consumer.committed(Collections.singleton(partition)).get(partition);
                        if (committed != null) {
                            committedOffset = committed.offset();
                        }
                        long position = consumer.position(partition);

                        log.info("Partition {} - End offset: {}, Committed offset: {}, Current position: {}, " +
                                        "Messages available: {}",
                                partition, endOffset, committedOffset, position,
                                endOffset - position);
                    } catch (Exception e) {
                        log.error("Error checking offsets for partition: " + partition, e);
                    }
                }
            }
        });

        isRunning.set(true);
        isHealthy.set(true);

        // Start processing in a separate thread
        replayThread = new Thread(this::processMessages, "DLQ-Replay-Thread");
        replayThread.setDaemon(true);
        replayThread.start();

        log.info("DLQ replay service started successfully");
    }

    /**
     * Gracefully shutdown the DLQ replay service
     */
    @PreDestroy
    public synchronized void shutdown() {
        if (!isRunning.get()) {
            log.info("DLQ replay service is not running, nothing to shutdown");
            return;
        }

        log.info("Shutting down DLQ replay service...");
        isRunning.set(false);

        // Close consumer - this will trigger final offset commit and leave the consumer group
        if (consumer != null) {
            try {
                consumer.wakeup(); // Interrupt any ongoing poll()
                // Give thread time to finish current iteration
                Thread.sleep(shutdownWaitMs);
                consumer.close(Duration.ofSeconds(consumerCloseTimeoutSeconds)); // Graceful close with timeout
                log.info("Kafka consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing Kafka consumer during shutdown", e);
            }
        }

        log.info("DLQ replay service shutdown complete. Total processed: {}, Total errors: {}, Total skipped: {}",
                processedCount.get(), errorCount.get(), skippedCount.get());
    }

    /**
     * Process messages from the DLQ topic using pause/resume pattern
     */
    private void processMessages() {
        log.info("DLQ replay thread started, polling for messages...");

        try {
            while (isRunning.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

                if (records.isEmpty()) {
                    // Log why we got no records
                    for (TopicPartition partition : consumer.assignment()) {
                        try {
                            long currentPosition = consumer.position(partition);
                            long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                            if (currentPosition >= endOffset) {
                                log.debug("No messages available - Partition {} at end (Position: {}, End: {})",
                                        partition, currentPosition, endOffset);
                            } else {
                                log.info("No messages returned despite availability - Partition {} (Position: {}, End: {}, Available: {})",
                                        partition, currentPosition, endOffset, endOffset - currentPosition);
                            }
                        } catch (Exception e) {
                            log.error("Error checking position after empty poll", e);
                        }
                    }
                    continue;
                }

                log.debug("Received {} DLQ messages to replay", records.count());

                // PAUSE consumption immediately to prevent timeout during processing
                Set<TopicPartition> pausedPartitions = consumer.assignment();
                consumer.pause(pausedPartitions);
                log.info("Paused consumption on partitions: {} to process messages", pausedPartitions);

                try {
                    // Now process without time pressure - heartbeats continue automatically
                    for (ConsumerRecord<String, String> record : records) {
                        String retryKey = record.partition() + "-" + record.offset();
                        
                        try {
                            log.info("Processing DLQ entry at offset: {} from partition: {}",
                                    record.offset(), record.partition());

                            long processingStartTime = System.currentTimeMillis();
                            replayDLQEntry(record.value());
                            long processingTime = System.currentTimeMillis() - processingStartTime;

                            processedCount.incrementAndGet();

                            // Success - remove from retry tracker and commit offset
                            retryTracker.remove(retryKey);
                            
                            Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1)
                            );
                            consumer.commitSync(offsets);

                            log.debug("Successfully replayed DLQ entry (offset: {}, partition: {}) in {}ms",
                                    record.offset(), record.partition(), processingTime);

                        } catch (TemporaryBackendException temporaryBackendException) {
                            // Treat temporary backend exceptions as transient - will retry
                            errorCount.incrementAndGet();
                            log.warn("Temporary backend exception while replaying DLQ entry (offset: {}, partition: {}). " +
                                    "Will retry on next poll. Error: {}",
                                    record.offset(), record.partition(), temporaryBackendException.getMessage());
                            Thread.sleep(errorBackoffMs);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            
                            // Track retry attempts for this specific offset (in-memory)
                            int retryCount = retryTracker.getOrDefault(retryKey, 0) + 1;
                            retryTracker.put(retryKey, retryCount);
                            
                            if (retryCount >= maxRetries) {
                                // Poison pill detected - skip this message to unblock the partition
                                log.error("DLQ entry at offset {} partition {} failed {} times (max retries reached). " +
                                        "SKIPPING this message to prevent partition blockage. Error: {}",
                                        record.offset(), record.partition(), retryCount, e.getMessage(), e);
                                
                                skippedCount.incrementAndGet();
                                retryTracker.remove(retryKey);
                                
                                // Commit offset to move past poison pill
                                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                                        new TopicPartition(record.topic(), record.partition()),
                                        new OffsetAndMetadata(record.offset() + 1)
                                );
                                try {
                                    consumer.commitSync(offsets);
                                    log.warn("Committed offset {} to skip poison pill", record.offset() + 1);
                                } catch (Exception commitEx) {
                                    log.error("Failed to commit offset after skipping poison pill", commitEx);
                                }
                            } else {
                                // Will retry this message on next poll - don't commit offset
                                log.warn("Failed to replay DLQ entry (offset: {}, partition: {}). Retry {}/{}. " +
                                        "Will retry on next poll. Error: {}",
                                        record.offset(), record.partition(), retryCount, maxRetries, e.getMessage());
                            }
                            // CRITICAL: Don't break - continue processing remaining records in batch
                        }
                    }
                } finally {
                    // RESUME consumption - always do this even if processing failed
                    consumer.resume(pausedPartitions);
                    log.info("Resumed consumption on partitions: {}", pausedPartitions);
                }

                } catch (WakeupException e) {
                    // Expected during shutdown - exit gracefully
                    log.info("Kafka consumer wakeup called, exiting processing loop");
                    break;
                } catch (Exception e) {
                    log.error("Error in DLQ replay processing loop", e);
                    try {
                        // Back off before retry
                        Thread.sleep(errorBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("DLQ replay thread interrupted during error recovery");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in DLQ replay thread - marking service as unhealthy", e);
            isHealthy.set(false);
        } finally {
            boolean wasRunning = isRunning.get();
            isRunning.set(false);
            
            if (wasRunning && !isHealthy.get()) {
                log.error("DLQ replay thread terminated unexpectedly! Service is unhealthy. Pod should be restarted.");
            }
            
            log.info("DLQ replay thread finished. Total processed: {}, Total errors: {}, Total skipped: {}",
                    processedCount.get(), errorCount.get(), skippedCount.get());
        }
    }

    /**
     * Replay a single DLQ entry
     */
    private void replayDLQEntry(String dlqJson) throws Exception {
        long startTime = System.currentTimeMillis();
        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        try {
            DLQEntry entry = mapper.readValue(dlqJson, DLQEntry.class);
            log.info("Replaying DLQ entry for index: {}, store: {}", entry.getIndexName(), entry.getStoreName());

            // Create key information retriever
            log.info("Creating key information retriever for index: {}", entry.getIndexName());
            StandardJanusGraphTx standardJanusGraphTx = (StandardJanusGraphTx) this.standardJanusGraph.newTransaction();
            IndexInfoRetriever keyInfo = this.indexSerializer.getIndexInfoRetriever(standardJanusGraphTx);


            // Reconstruct mutations from serialized form
            log.info("Starting mutation reconstruction for index: {}", entry.getIndexName());
            Map<String, Map<String, IndexMutation>> mutations = reconstructMutations(entry, keyInfo.get("search"));
            log.info("Completed mutation reconstruction in {}ms", System.currentTimeMillis() - startTime);

            // Create a new transaction for replay
            log.info("Beginning transaction for index: {}", entry.getIndexName());
            BaseTransaction replayTx = esIndex.beginTransaction(
                    new StandardBaseTransactionConfig.Builder().commitTime(Instant.now()).build()
            );

            try {
                // This is the same method that originally failed - now we're replaying it!
                log.info("Starting ES mutation for index: {}", entry.getIndexName());
                long mutateStartTime = System.currentTimeMillis();
                esIndex.mutate(mutations, keyInfo.get("search"), replayTx);
                log.info("ES mutation completed in {}ms, committing transaction", System.currentTimeMillis() - mutateStartTime);

                long commitStartTime = System.currentTimeMillis();
                replayTx.commit();
                log.info("Transaction commit completed in {}ms", System.currentTimeMillis() - commitStartTime);

                long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                long memoryUsed = memoryAfter - memoryBefore;
                long totalTime = System.currentTimeMillis() - startTime;

                log.info("Successfully replayed mutation for index: {}. Total time: {}ms, Memory used: {}MB, Current heap usage: {}MB",
                        entry.getIndexName(), totalTime, memoryUsed / (1024 * 1024),
                        (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));

            } catch (Exception e) {
                log.warn("Error replaying mutation for index: {}, rolling back transaction",
                        entry.getIndexName(), e);
                try {
                    replayTx.rollback();
                } catch (Exception rollbackException) {
                    log.error("Failed to rollback transaction for index: {}", entry.getIndexName(), rollbackException);
                }
                throw new Exception("Failed to replay mutation for index: " + entry.getIndexName(), e);
            }
        } catch (IOException e) {
            log.error("Failed to deserialize DLQ entry JSON", e);
            throw e;
        }
    }

    /**
     * Reconstruct IndexMutation objects from serialized form
     */
    private Map<String, Map<String, IndexMutation>> reconstructMutations(DLQEntry entry, KeyInformation.IndexRetriever indexRetriever) {
        Map<String, Map<String, IndexMutation>> result = new HashMap<>();

        for (Map.Entry<String, Map<String, SerializableIndexMutation>> storeEntry :
                entry.getMutations().entrySet()) {

            String storeName = storeEntry.getKey();
            Map<String, IndexMutation> storeMutations = new HashMap<>();

            for (Map.Entry<String, SerializableIndexMutation> docEntry :
                    storeEntry.getValue().entrySet()) {

                String docId = docEntry.getKey();
                SerializableIndexMutation serMut = docEntry.getValue();

                // Reconstruct IndexMutation
                IndexMutation mutation = new IndexMutation(
                        indexRetriever.get(storeName),
                        serMut.isNew(),
                        serMut.isDeleted()
                );

                // Add additions
                for (SerializableIndexMutation.SerializableIndexEntry add : serMut.getAdditions()) {
                    mutation.addition(new IndexEntry(add.getField(), add.getValue()));
                }

                // Add deletions
                for (SerializableIndexMutation.SerializableIndexEntry del : serMut.getDeletions()) {
                    mutation.deletion(new IndexEntry(del.getField(), del.getValue()));
                }

                storeMutations.put(docId, mutation);
            }

            result.put(storeName, storeMutations);
        }

        return result;
    }

    /**
     * Health check for liveness/readiness probes
     * @return true if the replay thread is healthy and running
     */
    public boolean isHealthy() {
        // Check if thread is alive and healthy
        boolean threadAlive = replayThread != null && replayThread.isAlive();
        return isHealthy.get() && threadAlive;
    }

    /**
     * Get replay status
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isRunning", isRunning.get());
        status.put("isHealthy", isHealthy());
        status.put("threadAlive", replayThread != null && replayThread.isAlive());
        status.put("processedCount", processedCount.get());
        status.put("errorCount", errorCount.get());
        status.put("skippedCount", skippedCount.get());
        status.put("topic", dlqTopic);
        status.put("consumerGroup", consumerGroupId);
        status.put("maxRetries", maxRetries);
        status.put("activeRetries", retryTracker.size());
        
        return status;
    }

    private ObjectMapper configureMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Configure to handle property name differences
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Add custom deserializer for SerializableIndexMutation
        SimpleModule module = new SimpleModule();
        module.addDeserializer(SerializableIndexMutation.class, new JsonDeserializer<>() {
            @Override
            public SerializableIndexMutation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode node = p.getCodec().readTree(p);

                // Handle both "new" and "isNew" fields
                boolean isNew = node.has("new") ? node.get("new").asBoolean() :
                        node.has("isNew") ? node.get("isNew").asBoolean() : false;

                boolean isDeleted = node.has("isDeleted") ? node.get("isDeleted").asBoolean() : false;

                List<SerializableIndexMutation.SerializableIndexEntry> additions = new ArrayList<>();
                List<SerializableIndexMutation.SerializableIndexEntry> deletions = new ArrayList<>();

                if (node.has("additions") && node.get("additions").isArray()) {
                    for (JsonNode entry : node.get("additions")) {
                        additions.add(new SerializableIndexMutation.SerializableIndexEntry(
                                entry.get("field").asText(),
                                mapper.treeToValue(entry.get("value"), Object.class)
                        ));
                    }
                }

                if (node.has("deletions") && node.get("deletions").isArray()) {
                    for (JsonNode entry : node.get("deletions")) {
                        deletions.add(new SerializableIndexMutation.SerializableIndexEntry(
                                entry.get("field").asText(),
                                mapper.treeToValue(entry.get("value"), Object.class)
                        ));
                    }
                }

                return new SerializableIndexMutation(isNew, isDeleted, additions, deletions);
            }
        });
        mapper.registerModule(module);
        return mapper;
    }

}