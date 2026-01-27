package org.apache.atlas.web.service;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.micrometer.core.instrument.Gauge;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.metrics.MetricUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base implementation for Kafka queue consumers.
 *
 * Configuration:
 * - Uses a per-consumer config prefix (for example: "atlas.kafka.metadata").
 * - Required/optional keys (all under the prefix):
 *   - enabled (boolean)
 *   - topic (string)
 *   - error.topic (string)
 *   - consumerGroupId (string)
 *   - commitEveryMs (long)
 *   - maxConcurrency (int)
 *   - maxPollRecords (int)
 *   - maxPollIntervalMs (int)
 *   - autoOffsetReset (string)
 * - Bootstrap servers are resolved from atlas.graph.kafka.bootstrap.servers,
 *   falling back to atlas.kafka.bootstrap.servers.
 */
public abstract class KafkaEventConsumer {
    private static final String BOOTSTRAP_SERVERS_KEY = "atlas.kafka.bootstrap.servers";
    private static final String BOOTSTRAP_SERVERS_GRAPH_KEY = "atlas.graph.kafka.bootstrap.servers";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String configPrefix;
    private final String metricPrefix;
    private final String consumerThreadName;

    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
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

    protected KafkaEventConsumer(String configPrefix, String metricPrefix, String consumerThreadName) {
        this.configPrefix = configPrefix;
        this.metricPrefix = metricPrefix;
        this.consumerThreadName = consumerThreadName;
    }

    public void start() {
        try {
            Configuration configuration = ApplicationProperties.get();
            boolean enabled = configuration.getBoolean(configPrefix + ".enabled", false);
            if (!enabled) {
                log.info("{} is disabled via {}.enabled", getClass().getSimpleName(), configPrefix);
                return;
            }
            topic = configuration.getString(configPrefix + ".topic", defaultTopic());
            errorTopic = configuration.getString(configPrefix + ".error.topic", defaultErrorTopic());
            commitEveryMs = configuration.getLong(configPrefix + ".commitEveryMs", 1000L);
            maxConcurrency = configuration.getInt(configPrefix + ".maxConcurrency", 16);
            bootstrapServers = configuration.getString(
                    BOOTSTRAP_SERVERS_GRAPH_KEY,
                    configuration.getString(BOOTSTRAP_SERVERS_KEY, "localhost:9092")
            );
            consumerProperties = buildConsumerProperties(bootstrapServers, configuration, configPrefix, defaultConsumerGroupId());
            errorProducerProperties = buildErrorProducerProperties(bootstrapServers);
            if (topic != null && !topic.isEmpty()) {
                throw new RuntimeException("Missing topic: " + topic);
            }
            if (errorTopic != null && !errorTopic.isEmpty()) {
                throw new RuntimeException("Missing error topic: " + errorTopic);
            }
        } catch (AtlasException e) {
            log.error("Failed to load Kafka consumer configuration for {}", getClass().getSimpleName(), e);
            return;
        }

        initMetrics();
        consumerThread = new Thread(this::runPoll, consumerThreadName);
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @PreDestroy
    public void stop() {
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

    protected abstract String defaultTopic();

    protected abstract String defaultErrorTopic();

    protected abstract String defaultConsumerGroupId();

    protected abstract void processRecord(ConsumerRecord<String, String> record) throws Exception;

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
            try {
                processRecord(record);
                processedCount.incrementAndGet();
            } catch (Exception e) {
                log.error("{} failed {}-{}@{}: {}",
                        getClass().getSimpleName(), record.topic(), record.partition(), record.offset(), e.getMessage(), e);
                sendToErrorTopic(record, e);
                failedCount.incrementAndGet();
            }
        });
    }

    private void initMetrics() {
        Gauge.builder(metricPrefix + ".processed.count", processedCount, AtomicLong::get)
                .description("Kafka consumer processed records")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(metricPrefix + ".failed.count", failedCount, AtomicLong::get)
                .description("Kafka consumer failed records")
                .register(MetricUtils.getMeterRegistry());
    }

    private static Properties buildConsumerProperties(String bootstrapServers, Configuration configuration,
                                                     String configPrefix, String defaultGroupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String groupId = configuration.getString(configPrefix + ".consumerGroupId", defaultGroupId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        int maxPollRecords = configuration.getInt(configPrefix + ".maxPollRecords", 50);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));

        int maxPollIntervalMs = configuration.getInt(configPrefix + ".maxPollIntervalMs", 900000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(maxPollIntervalMs));

        String autoOffsetReset = configuration.getString(configPrefix + ".autoOffsetReset", "latest");
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
                    log.error("{} failed to publish to error topic {}: {}",
                            getClass().getSimpleName(), errorTopic, ex.getMessage(), ex);
                }
            });
        } catch (Exception ex) {
            log.error("{} failed to serialize error record for {}-{}@{}: {}",
                    getClass().getSimpleName(), record.topic(), record.partition(), record.offset(), ex.getMessage(), ex);
        }
    }
}
