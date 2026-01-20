package org.apache.atlas.web.service;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.atlas.service.metrics.MetricUtils;
import io.micrometer.core.instrument.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Service
public class KafkaMetadataConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataConsumer.class);
    private static final String METRIC_PREFIX = "kafka.metadata.consumer";

    private final EntityMutationService entityMutationService;
    private final AtomicInteger runningGauge = new AtomicInteger(0);
    private final AtomicInteger inFlightGauge = new AtomicInteger(0);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong lastProcessedTimestampMs = new AtomicLong(0);
    private final AtomicLong lastProcessDurationMs = new AtomicLong(0);
    private volatile Thread consumerThread;
    private volatile ParallelStreamProcessor<String, String> parallelConsumer;
    private volatile KafkaProducer<String, String> errorProducer;

    private String topic;
    private String errorTopic;
    private String bootstrapServers;
    private int maxConcurrency;
    private long commitEveryMs;
    private Properties consumerProperties;
    private Properties errorProducerProperties;

    @Inject
    public KafkaMetadataConsumer(EntityMutationService entityMutationService) {
        this.entityMutationService = entityMutationService;
    }

    @PostConstruct
    public void start() {
        try {
            Configuration configuration = ApplicationProperties.get();
            boolean enabled = configuration.getBoolean("atlas.kafka.metadata.enabled", false);
            if (!enabled) {
                LOG.info("KafkaMetadataConsumer is disabled via atlas.kafka.metadata.enabled");
                return;
            }
            topic = configuration.getString("atlas.kafka.metadata.topic", "events-topic");
            errorTopic = configuration.getString("atlas.kafka.metadata.error.topic", "events-topic-errors");
            commitEveryMs = configuration.getLong("atlas.kafka.metadata.commitEveryMs", 1000L);
            maxConcurrency = configuration.getInt("atlas.kafka.metadata.maxConcurrency", 16);
            bootstrapServers = configuration.getString(
                    "atlas.graph.kafka.bootstrap.servers",
                    configuration.getString("atlas.kafka.bootstrap.servers", "localhost:9092")
            );
            consumerProperties = buildConsumerProperties(bootstrapServers, configuration);
            errorProducerProperties = buildErrorProducerProperties(bootstrapServers);
            ensureTopic(bootstrapServers, topic, 10, (short) 1);
            ensureTopic(bootstrapServers, errorTopic, 10, (short) 1);
        } catch (AtlasException e) {
            LOG.error("Failed to load Kafka metadata consumer configuration", e);
            return;
        }

        initMetrics();
        runningGauge.set(1);
        consumerThread = new Thread(this::runPoll, "kafka-metadata-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @PreDestroy
    public void stop() {
        runningGauge.set(0);
        ParallelStreamProcessor<String, String> consumerRef = this.parallelConsumer;
        if (consumerRef != null) {
            consumerRef.closeDrainFirst(Duration.ofSeconds(30));
        }
        KafkaProducer<String, String> errorProducerRef = errorProducer;
        if (errorProducerRef != null) {
            errorProducerRef.close();
        }
        Thread threadRef = consumerThread;
        if (threadRef != null) {
            try {
                threadRef.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runPoll() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.PARTITION)
                .maxConcurrency(maxConcurrency)
                .commitInterval(Duration.ofMillis(commitEveryMs))
                .meterRegistry(MetricUtils.getMeterRegistry())
                .build();
        parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(options);
        parallelConsumer.subscribe(Collections.singletonList(topic));

        parallelConsumer.poll(context -> {
            ConsumerRecord<String, String> record = context.getSingleConsumerRecord();
            long startNs = System.nanoTime();
            inFlightGauge.incrementAndGet();
            try {
                processRecord(record, entityMutationService);
                processedCount.incrementAndGet();
            } catch (Exception e) {
                LOG.error("KafkaMetadataConsumer failed {}-{}@{}: {}",
                        record.topic(), record.partition(), record.offset(), e.getMessage(), e);
                sendToErrorTopic(record, e);
                failedCount.incrementAndGet();
            } finally {
                lastProcessedTimestampMs.set(System.currentTimeMillis());
                lastProcessDurationMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
                inFlightGauge.decrementAndGet();
            }
        });
    }

    private void initMetrics() {
        Gauge.builder(METRIC_PREFIX + ".running", runningGauge, AtomicInteger::get)
                .description("Kafka metadata consumer running state")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".in_flight", inFlightGauge, AtomicInteger::get)
                .description("Kafka metadata consumer in-flight records")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".processed.count", processedCount, AtomicLong::get)
                .description("Kafka metadata consumer processed records")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".failed.count", failedCount, AtomicLong::get)
                .description("Kafka metadata consumer failed records")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".last_processed_timestamp_ms", lastProcessedTimestampMs, AtomicLong::get)
                .description("Kafka metadata consumer last processed timestamp (ms)")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".last_process_duration_ms", lastProcessDurationMs, AtomicLong::get)
                .description("Kafka metadata consumer last process duration (ms)")
                .register(MetricUtils.getMeterRegistry());
    }

    private static Properties buildConsumerProperties(String bootstrapServers, Configuration configuration) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String groupId = configuration.getString("atlas.kafka.metadata.consumerGroupId", "atlas-metadata-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        int maxPollRecords = configuration.getInt("atlas.kafka.metadata.maxPollRecords", 50);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));

        int maxPollIntervalMs = configuration.getInt("atlas.kafka.metadata.maxPollIntervalMs", 900000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(maxPollIntervalMs));

        String autoOffsetReset = configuration.getString("atlas.kafka.metadata.autoOffsetReset", "latest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return props;
    }

    private static Properties buildErrorProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static void ensureTopic(String bootstrapServers, String topicName, int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            if (existingTopics.contains(topicName)) return;

            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singleton(topic)).all().get();
            LOG.info("KafkaMetadataConsumer created topic {} with {} partitions", topicName, partitions);
        } catch (Exception e) {
            LOG.error("KafkaMetadataConsumer failed to create topic {}", topicName, e);
        }
    }

    private KafkaProducer<String, String> getOrCreateErrorProducer() {
        KafkaProducer<String, String> producerRef = errorProducer;
        if (producerRef == null) {
            synchronized (this) {
                producerRef = errorProducer;
                if (producerRef == null) {
                    producerRef = new KafkaProducer<>(errorProducerProperties);
                    errorProducer = producerRef;
                }
            }
        }
        return producerRef;
    }

    private void sendToErrorTopic(ConsumerRecord<String, String> record, Exception e) {
        if (errorTopic == null || errorTopic.isEmpty()) {
            return;
        }
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("errorMessage", e.getMessage());
            envelope.put("errorType", e.getClass().getName());
            envelope.put("failedAt", System.currentTimeMillis());
            envelope.put("topic", record.topic());
            envelope.put("partition", record.partition());
            envelope.put("offset", record.offset());
            envelope.put("timestamp", record.timestamp());
            envelope.put("key", record.key());
            envelope.put("value", record.value());

            String value = AtlasType.toJson(envelope);
            String key = record.key() != null ? record.key() : String.valueOf(record.offset());
            ProducerRecord<String, String> errorRecord = new ProducerRecord<>(errorTopic, key, value);
            getOrCreateErrorProducer().send(errorRecord, (metadata, ex) -> {
                if (ex != null) {
                    LOG.error("KafkaMetadataConsumer failed to publish to error topic {}: {}",
                            errorTopic, ex.getMessage(), ex);
                }
            });
        } catch (Exception ex) {
            LOG.error("KafkaMetadataConsumer failed to serialize error record for {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage(), ex);
        }
    }

    private static int processRecord(ConsumerRecord<String, String> rec, EntityMutationService entityMutationService) throws AtlasBaseException {
        if (entityMutationService == null) {
            LOG.warn("Skipping record because entityMutationService is not configured: {}-{}@{}",
                    rec.topic(), rec.partition(), rec.offset());
            return 0;
        }

        try {
            Map<String, Object> envelope = AtlasType.fromJson(rec.value(), Map.class);
            Object payload = envelope != null && envelope.containsKey("payload") ? envelope.get("payload") : envelope;
            Map<String, Object> metadata = envelope != null ? asMap(envelope.get("metadata")) : null;

            if (payload == null) {
                throw new IllegalArgumentException("Missing payload in Kafka message");
            }
            String payloadJson = payload instanceof String ? (String) payload : AtlasType.toJson(payload);
            AtlasEntitiesWithExtInfo entities = AtlasType.fromJson(payloadJson, AtlasEntitiesWithExtInfo.class);
            if (entities == null) {
                throw new IllegalArgumentException("Failed to parse AtlasEntitiesWithExtInfo from payload");
            }

            BulkRequestContext context = buildBulkRequestContext(metadata);
            EntityMutationResponse response = entityMutationService.createOrUpdate(new AtlasEntityStream(entities), context);
            if (response == null || response.getCreatedEntities() == null) return 0;
            return response.getCreatedEntities().size();
        } finally {
            // RequestContext.clear();
        }
    }

    private static BulkRequestContext buildBulkRequestContext(Map<String, Object> metadata) {
        boolean replaceClassifications = getBoolean(metadata, "replaceClassifications", false);
        boolean replaceTags = getBoolean(metadata, "replaceTags", false);
        boolean appendTags = getBoolean(metadata, "appendTags", false);
        boolean replaceBusinessAttributes = getBoolean(metadata, "replaceBusinessAttributes", false);
        boolean overwriteBusinessAttributes = getBoolean(metadata, "overwriteBusinessAttributes", false);

        if (Stream.of(replaceClassifications, replaceTags, appendTags).filter(Boolean::booleanValue).count() > 1) {
            throw new IllegalArgumentException("Only one of [replaceClassifications, replaceTags, appendTags] can be true");
        }

        return new BulkRequestContext.Builder()
                .setReplaceClassifications(replaceClassifications)
                .setReplaceTags(replaceTags)
                .setAppendTags(appendTags)
                .setReplaceBusinessAttributes(replaceBusinessAttributes)
                .setOverwriteBusinessAttributes(overwriteBusinessAttributes)
                .build();
    }

    private static Map<String, Object> asMap(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    private static boolean getBoolean(Map<String, Object> metadata, String key, boolean defaultValue) {
        if (metadata == null || !metadata.containsKey(key)) {
            return defaultValue;
        }
        Object value = metadata.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }
}
