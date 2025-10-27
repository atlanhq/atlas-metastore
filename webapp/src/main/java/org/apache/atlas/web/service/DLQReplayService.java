package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    public DLQReplayService(AtlasJanusGraph graph) throws BackendException, AtlasException {
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
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Process in small batches

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dlqTopic));

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

        while (isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                if (records.isEmpty()) {
                    continue; // No messages, continue polling
                }

                log.info("Received {} DLQ messages to replay", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        replayDLQEntry(record.value());
                        consumer.commitSync(); // Commit only after successful replay
                        processedCount.incrementAndGet();

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
                log.error("Error in DLQ replay processing", e);
                try {
                    Thread.sleep(5000); // Wait before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
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
        DLQEntry entry = mapper.readValue(dlqJson, DLQEntry.class);

        log.debug("Replaying DLQ entry for index: {}, store: {}", entry.getIndexName(), entry.getStoreName());

        // Reconstruct mutations from serialized form
        Map<String, Map<String, IndexMutation>> mutations = reconstructMutations(entry);

        // Create key information retriever
        KeyInformation.IndexRetriever keyInfo = createKeyInfoRetriever(entry);

        // Create a new transaction for replay
        BaseTransaction replayTx = esIndex.beginTransaction(
                new StandardBaseTransactionConfig.Builder().build()
        );

        try {
            // This is the same method that originally failed - now we're replaying it!
            esIndex.mutate(mutations, keyInfo, replayTx);
            replayTx.commit();

            log.debug("Successfully replayed mutation for index: {}", entry.getIndexName());

        } catch (Exception e) {
            replayTx.rollback();
            throw new Exception("Failed to replay mutation for index: " + entry.getIndexName(), e);
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
