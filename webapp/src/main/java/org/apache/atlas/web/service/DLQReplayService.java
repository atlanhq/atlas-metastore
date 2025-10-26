package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.dlq.DLQEntry;
import org.janusgraph.diskstorage.dlq.SerializableIndexMutation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import io.prometheus.client.Gauge;
import io.prometheus.client.Counter;
import javax.annotation.PostConstruct;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for replaying DLQ messages back to Elasticsearch.
 * Uses Kafka consumer groups to ensure only one instance processes messages.
 */
@Service
public class DLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(DLQReplayService.class);

    private static final Gauge DLQ_MESSAGES_REMAINING = Gauge.build()
            .name("atlas_dlq_messages_remaining")
            .help("Number of messages remaining to be processed in the DLQ topic")
            .register();

    private static final Counter DLQ_MESSAGES_PROCESSED = Counter.build()
            .name("atlas_dlq_messages_processed_total")
            .help("Total number of DLQ messages successfully processed")
            .register();

    private static final Counter DLQ_PROCESSING_ERRORS = Counter.build()
            .name("atlas_dlq_processing_errors_total")
            .help("Total number of errors encountered while processing DLQ messages")
            .register();

    @Value("${atlas.kafka.bootstrap.servers:localhost:9092}")
    private final String bootstrapServers = "localhost:9092";
    @Value("${atlas.kafka.dlq.topic:ATLAS_ES_DLQ}")
    private final String dlqTopic="ATLAS_ES_DLQ";
    @Value("${atlas.kafka.dlq.consumerGroupId:atlas_dq_replay_group}")
    private final String consumerGroupId= "atlas_dq_replay_group";

    @Value("${atlas.dlq.poll.timeout.seconds:60}")
    private int pollTimeoutSeconds;

    @Value("${atlas.dlq.poll.timeout.max.seconds:120}")
    private int maxPollTimeoutSeconds;

    @Value("${atlas.dlq.backoff.initial.seconds:1}")
    private int initialBackoffSeconds;

    @Value("${atlas.dlq.sleep.caught.up.multiplier:1000}")
    private int caughtUpSleepMultiplier;

    @Value("${atlas.dlq.sleep.checking.multiplier:100}")
    private int checkingSleepMultiplier;

    @Value("${atlas.dlq.batch.size:10}")
    private int batchSize;
    private ElasticSearchIndex esIndex;
    private final ObjectMapper mapper;

    private volatile Thread processingThread;
    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public DLQReplayService() throws BackendException {
        this.esIndex = new ElasticSearchIndex(Configuration.EMPTY);
        this.mapper = new ObjectMapper();
    }

    /**
     * Get replay status
     */
    @PostConstruct
    public void startDLQProcessing() {
        // Start processing in a separate thread
        processingThread = new Thread(this::processDLQMessages, "DLQ-Processing-Thread");
        processingThread.setDaemon(true);
        processingThread.start();
        log.info("Started DLQ processing thread");
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("topic", dlqTopic);
        status.put("consumerGroup", consumerGroupId);
        status.put("isProcessing", processingThread != null && processingThread.isAlive());
        return status;
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

    private void processDLQMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // Process in small batches
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(maxPollTimeoutSeconds * 1000));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollTimeoutSeconds * 2500));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(dlqTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.debug("DLQ partitions revoked from this consumer: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.debug("DLQ partitions assigned to this consumer: {}", partitions);
                    if (partitions.isEmpty()) {
                        log.debug("No partitions assigned to this consumer - another pod is handling the DLQ processing");
                    } else {
                        // Update remaining messages count when we get partition assignment
                        try {
                            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
                            Map<TopicPartition, Long> committed = new HashMap<>();
                            for (TopicPartition partition : partitions) {
                                OffsetAndMetadata offsetMeta = consumer.committed(Collections.singleton(partition)).get(partition);
                                committed.put(partition, offsetMeta != null ? offsetMeta.offset() : 0L);
                            }
                            
                            long remaining = 0;
                            for (TopicPartition partition : partitions) {
                                remaining += endOffsets.get(partition) - committed.get(partition);
                            }
                            DLQ_MESSAGES_REMAINING.set(remaining);
                        } catch (Exception e) {
                            log.error("Failed to calculate remaining messages", e);
                            DLQ_MESSAGES_REMAINING.set(-1);
                        }
                    }
                }
            });
            
            log.info("Subscribed to DLQ topic: {} with consumer group: {}", dlqTopic, consumerGroupId);

            Duration pollTimeout = Duration.ofSeconds(pollTimeoutSeconds);
            Duration backoffInterval = Duration.ofSeconds(initialBackoffSeconds);
            int emptyPollCount = 0;
            boolean caughtUp = false;
            Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                    
                    if (records.isEmpty()) {
                        emptyPollCount++;
                        if (!caughtUp) {
                            // Check if we've caught up with the latest offset
                            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                            Map<TopicPartition, Long> currentOffsets = new HashMap<>();
                            for (TopicPartition partition : consumer.assignment()) {
                                currentOffsets.put(partition, consumer.position(partition));
                            }
                            
                            caughtUp = endOffsets.entrySet().stream()
                                .allMatch(e -> e.getValue() <= currentOffsets.get(e.getKey()));
                                
                            if (caughtUp) {
                                log.debug("Caught up with latest offset, entering low-power mode");
                                pollTimeout = Duration.ofSeconds(maxPollTimeoutSeconds);
                                
                                // Pause partitions to reduce CPU usage
                                consumer.pause(consumer.assignment());
                            }
                        }
                        
                        // Exponential backoff with proper sleep when no messages
                        if (emptyPollCount > 1) {
                            // Calculate backoff time (exponential up to max)
                            long backoffSeconds = Math.min(backoffInterval.getSeconds() * 2, maxPollTimeoutSeconds);
                            backoffInterval = Duration.ofSeconds(backoffSeconds);
                            
                            if (caughtUp) {
                                // In low-power mode, sleep for longer periods
                                Thread.sleep(backoffSeconds * caughtUpSleepMultiplier); // Longer sleep when caught up
                                Thread.yield(); // Explicitly yield CPU
                            } else {
                                // When not caught up, use shorter sleep
                                Thread.sleep(Math.min(1000, backoffSeconds * checkingSleepMultiplier));
                            }
                        }
                        continue;
                    }
                    
                    // Reset backoff when we get messages
                    emptyPollCount = 0;
                    backoffInterval = Duration.ofSeconds(initialBackoffSeconds);
                    
                    // Resume partitions if they were paused
                    if (caughtUp) {
                        caughtUp = false;
                        pollTimeout = Duration.ofSeconds(pollTimeoutSeconds);
                        consumer.resume(consumer.assignment());
                    }
                    
                    log.info("Processing {} DLQ messages", records.count());
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            replayDLQEntry(record.value());
                            
                            // Store offset for batch commit
                            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                            pendingOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                            
                            // Update metrics immediately for accurate tracking
                            DLQ_MESSAGES_PROCESSED.inc();
                            DLQ_MESSAGES_REMAINING.dec();
                            
                            // Commit if we've reached batch size or it's the last record in the poll
                            boolean isLastRecord = record.equals(records.records(partition).get(records.records(partition).size() - 1));
                            if (pendingOffsets.size() >= batchSize || isLastRecord) {
                                consumer.commitSync(pendingOffsets);
                                log.debug("Committed batch of {} offsets", pendingOffsets.size());
                                pendingOffsets.clear();
                            }
                            
                            log.debug("Successfully replayed DLQ entry (offset: {})", record.offset());
                            
                        } catch (Exception e) {
                            DLQ_PROCESSING_ERRORS.inc();
                            log.error("Failed to replay DLQ entry (offset: {})", record.offset(), e);
                            
                            // On error, commit any successful offsets before retrying
                            if (!pendingOffsets.isEmpty()) {
                                try {
                                    consumer.commitSync(pendingOffsets);
                                    log.debug("Committed {} successful offsets before error handling", pendingOffsets.size());
                                    pendingOffsets.clear();
                                } catch (Exception commitEx) {
                                    log.error("Failed to commit offsets after error", commitEx);
                                }
                            }
                            
                            Thread.sleep(pollTimeoutSeconds * 1000); // Wait before retrying
                        }
                        }
                } catch (Exception e) {
                    log.error("Error in DLQ processing", e);
                    Thread.sleep(pollTimeoutSeconds * 1000); // Wait before retrying
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in DLQ processing thread", e);
        }
    }
}
