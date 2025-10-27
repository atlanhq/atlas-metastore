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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.dlq.DLQEntry;
import org.janusgraph.diskstorage.dlq.SerializableIndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service for replaying DLQ messages back to Elasticsearch.
 * Uses Kafka consumer groups to ensure only one instance processes messages.
 */
@Service
public class DLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(DLQReplayService.class);

    private String bootstrapServers;
    @Value("${atlas.kafka.dlq.topic:ATLAS_ES_DLQ}")

    private final String dlqTopic="ATLAS_ES_DLQ";
    @Value("${atlas.kafka.dlq.consumerGroupId:atlas_dq_replay_group}")

    private final String consumerGroupId= "atlas_dq_replay_group";
    private final ElasticSearchIndex esIndex;
    private final ObjectMapper mapper;
    
    private ObjectMapper configureMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Configure to handle property name differences
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Add custom deserializer for SerializableIndexMutation
        SimpleModule module = new SimpleModule();
        module.addDeserializer(SerializableIndexMutation.class, new JsonDeserializer<SerializableIndexMutation>() {
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
    private volatile KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    public DLQReplayService(AtlasJanusGraph graph) throws BackendException, AtlasException {
        this.mapper = configureMapper();
        // Extract ES configuration from existing graph
        GraphDatabaseConfiguration graphConfig = ((StandardJanusGraph)graph.getGraph()).getConfiguration();
        this.bootstrapServers = ApplicationProperties.get().getString("atlas.graph.kafka.bootstrap.servers");
        Configuration fullConfig = graphConfig.getConfiguration();
        IndexProvider indexProvider = Backend.getImplementationClass(fullConfig.restrictTo("search"), fullConfig.get(GraphDatabaseConfiguration.INDEX_BACKEND,"search"),
                StandardIndexProvider.getAllProviderClasses());
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
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from beginning
        // Use reasonable batch size and timeouts
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // 5 minutes
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"); // 30 seconds
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"); // 3 seconds

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dlqTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("Consumer group partitions revoked. This might indicate processing is too slow. Partitions: {}", partitions);
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

        // Start processing in a separate thread
        Thread replayThread = new Thread(this::processMessages, "DLQ-Replay-Thread");
        replayThread.setDaemon(true);
        replayThread.start();

        log.info("DLQ replay service started successfully");
    }

    /**
     * Stop replaying DLQ messages
     */
    public synchronized void stopReplay() {
        if (!isRunning.get()) {
            log.warn("DLQ replay is not running");
            return;
        }

        log.info("Stopping DLQ replay service...");
        isRunning.set(false);

        if (consumer != null) {
            consumer.close();
            consumer = null;
        }

        log.info("DLQ replay service stopped. Processed: {}, Errors: {}",
                processedCount.get(), errorCount.get());
    }

    /**
     * Process messages from the DLQ topic
     */
    private void processMessages() {
        log.info("DLQ replay thread started, polling for messages...");
        long lastPollTime = System.currentTimeMillis();
        long lastCommitTime = System.currentTimeMillis();
        int processedInBatch = 0;

        while (isRunning.get()) {
            try {
                long now = System.currentTimeMillis();
                long timeSinceLastPoll = now - lastPollTime;
                
                // If we're taking too long between polls, log a warning
                if (timeSinceLastPoll > 60000) { // 1 minute
                    log.warn("Long delay between polls: {}ms. This could lead to consumer group removal.", 
                            timeSinceLastPoll);
                }
                
                // Commit any pending offsets if we haven't in a while
                if (now - lastCommitTime > 30000) { // 30 seconds
                    try {
                        consumer.commitSync();
                        lastCommitTime = now;
                        log.debug("Committed offsets after timeout");
                    } catch (Exception e) {
                        log.error("Failed to commit offsets", e);
                    }
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                if (records.isEmpty()) {
                    // Log why we got no records
                    for (TopicPartition partition : consumer.assignment()) {
                        try {
                            long currentPosition = consumer.position(partition);
                            long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                            if (currentPosition >= endOffset) {
                                log.info("No messages available - Partition {} at end (Position: {}, End: {})", 
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

                log.info("Received {} DLQ messages to replay", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        replayDLQEntry(record.value());
                        
                        // Track processing time and commit more frequently
                        processedInBatch++;
                        processedCount.incrementAndGet();
                        
                        // Commit every 2 messages or if it's been too long
                        if (processedInBatch >= 2 || (now - lastCommitTime > 30000)) {
                            consumer.commitSync();
                            lastCommitTime = now;
                            processedInBatch = 0;
                            log.debug("Committed offset after batch or timeout");
                        }
                        
                        lastPollTime = System.currentTimeMillis(); // Reset poll timer after successful processing
                        log.info("Successfully replayed DLQ entry (offset: {})", record.offset());

                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        log.error("Failed to replay DLQ entry (offset: {}), will retry later",
                                record.offset(), e);

                        // Don't commit - message will be reprocessed
                        break; // Stop processing this batch to retry later
                    }
                }

            } catch (Exception e) {
                log.error("Error in DLQ replay processing. Consumer might be removed from group due to slow processing", e);
                try {
                    // Give more time for recovery
                    Thread.sleep(30000); // 30 seconds before retrying
                    
                    // Try to rejoin the consumer group
                    consumer.enforceRebalance();
                    log.info("Enforced consumer group rebalance after error");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception re) {
                    log.error("Failed to recover consumer after error", re);
                    stopReplay(); // Stop processing if we can't recover
                    break;
                }
            }
        }

        log.info("DLQ replay thread finished");
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

            // Reconstruct mutations from serialized form
            log.info("Starting mutation reconstruction for index: {}", entry.getIndexName());
            Map<String, Map<String, IndexMutation>> mutations = reconstructMutations(entry);
            log.info("Completed mutation reconstruction in {}ms", System.currentTimeMillis() - startTime);

            // Create key information retriever
            log.info("Creating key information retriever for index: {}", entry.getIndexName());
            KeyInformation.IndexRetriever keyInfo = createKeyInfoRetriever(entry);

            // Create a new transaction for replay
            log.info("Beginning transaction for index: {}", entry.getIndexName());
            BaseTransaction replayTx = esIndex.beginTransaction(
                    new StandardBaseTransactionConfig.Builder().commitTime(Instant.now()).build()
            );

            try {
                // This is the same method that originally failed - now we're replaying it!
                log.info("Starting ES mutation for index: {}", entry.getIndexName());
                long mutateStartTime = System.currentTimeMillis();
                esIndex.mutate(mutations, keyInfo, replayTx);
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
                replayTx.rollback();
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
    private Map<String, Map<String, IndexMutation>> reconstructMutations(DLQEntry entry) {
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
                        createStoreRetriever(storeName), // This is simplified - you may need more context
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
     * Create key information retriever for replay
     */
    private KeyInformation.IndexRetriever createKeyInfoRetriever(DLQEntry entry) {
        return new KeyInformation.IndexRetriever() {
            @Override
            public KeyInformation.StoreRetriever get(String store) {
                return new KeyInformation.StoreRetriever() {
                    @Override
                    public KeyInformation get(String key) {
                        // This is a simplified implementation
                        // In practice, you might need to store more schema information in the DLQ
                        return createKeyInformation(store);
                    }
                };
            }

            @Override
            public KeyInformation get(String store, String key) {
                return createKeyInformation(store);
            }

            @Override
            public void invalidate(String store) {
                // No-op for replay
            }
        };
    }

    /**
     * Create a basic KeyInformation object
     */
    private KeyInformation createKeyInformation(String store) {
        // This is a simplified implementation
        // You might need to enhance this based on your schema requirements
        return new KeyInformation() {
            @Override
            public Class<?> getDataType() {
                return String.class;
            }

            @Override
            public Parameter[] getParameters() {
                return new Parameter[0];
            }

            @Override
            public Cardinality getCardinality() {
                return Cardinality.SINGLE;
            }
        };
    }

    /**
     * Create a store retriever for IndexMutation
     */
    private KeyInformation.StoreRetriever createStoreRetriever(String store) {
        return new KeyInformation.StoreRetriever() {
            @Override
            public KeyInformation get(String key) {
                return createKeyInformation(store);
            }
        };
    }

    /**
     * Get replay status
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isRunning", isRunning.get());
        status.put("processedCount", processedCount.get());
        status.put("errorCount", errorCount.get());
        status.put("topic", dlqTopic);
        status.put("consumerGroup", consumerGroupId);
        return status;
    }

    /**
     * Reset counters
     */
    public void resetCounters() {
        processedCount.set(0);
        errorCount.set(0);
        log.info("DLQ replay counters reset");
    }
}
