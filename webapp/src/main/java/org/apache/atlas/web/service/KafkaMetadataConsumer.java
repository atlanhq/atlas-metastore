package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.atlas.service.metrics.MetricUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Service
public class KafkaMetadataConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataConsumer.class);
    private static final String METRIC_PREFIX = "kafka.metadata.consumer";

    private String topic;
    private String errorTopic;
    private int pollTimeoutSeconds;
    private long commitEveryMs;
    private int maxInFlightRecords;

    private final EntityMutationService entityMutationService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger runningGauge = new AtomicInteger(0);
    private final AtomicInteger inFlightGauge = new AtomicInteger(0);
    private final AtomicLong lastPollTimestampMs = new AtomicLong(0);
    private final AtomicLong lastCommitTimestampMs = new AtomicLong(0);
    private volatile KafkaConsumer<String, String> consumer;
    private volatile KafkaProducer<String, String> errorProducer;
    private volatile Thread consumerThread;
    private String bootstrapServers;
    private Properties consumerProperties;
    private Counter pollCounter;
    private Counter recordsCounter;
    private Counter processedCounter;
    private Counter failedCounter;
    private Counter commitSuccessCounter;
    private Counter commitFailureCounter;
    private Timer pollTimer;
    private Timer processTimer;

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
            errorTopic = configuration.getString("atlas.kafka.metadata.errorTopic", topic + "-errors");
            pollTimeoutSeconds = configuration.getInt("atlas.kafka.metadata.pollTimeoutSeconds", 1);
            commitEveryMs = configuration.getLong("atlas.kafka.metadata.commitEveryMs", 1000L);
            maxInFlightRecords = configuration.getInt("atlas.kafka.metadata.maxInFlightRecords", 100);
            bootstrapServers = configuration.getString(
                    "atlas.graph.kafka.bootstrap.servers",
                    configuration.getString("atlas.kafka.bootstrap.servers", "localhost:9092")
            );
            consumerProperties = buildConsumerProperties(bootstrapServers, configuration);
            ensureTopic(bootstrapServers, topic, 10, (short) 1);
            ensureTopic(bootstrapServers, errorTopic, 10, (short) 1);
            errorProducer = new KafkaProducer<>(buildProducerProperties(bootstrapServers));
        } catch (AtlasException e) {
            LOG.error("Failed to load Kafka metadata consumer configuration", e);
            return;
        }

        initMetrics();
        running.set(true);
        runningGauge.set(1);
        consumerThread = new Thread(this::runLoop, "kafka-metadata-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private static void ensureTopic(String bootstrapServers, String topicName, int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            if (existingTopics.contains(topicName)) return;

            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singleton(topic)).all().get();
            LOG.info("HttpToKafkaVerticle created topic {} with {} partitions", topicName, partitions);
        } catch (Exception e) {
            LOG.error("HttpToKafkaVerticle failed to recreate topic {}", topicName, e);
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        runningGauge.set(0);
        KafkaConsumer<String, String> consumerRef = this.consumer;
        if (consumerRef != null) {
            consumerRef.wakeup();
        }
        Thread threadRef = consumerThread;
        if (threadRef != null) {
            try {
                threadRef.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        KafkaProducer<String, String> producerRef = errorProducer;
        if (producerRef != null) {
            producerRef.close();
        }
    }

    private void runLoop() {
        consume(entityMutationService, running, consumerRef -> this.consumer = consumerRef);
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

    private static Properties buildProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private void sendToErrorQueue(ConsumerRecord<String, String> record) {
        KafkaProducer<String, String> producer = errorProducer;
        if (producer == null) {
            LOG.warn("KafkaMetadataConsumer error producer not available, dropping failed record");
            return;
        }
        try {
            producer.send(new ProducerRecord<>(errorTopic, record.key(), record.value()));
        } catch (Exception e) {
            LOG.warn("KafkaMetadataConsumer failed to publish to error topic {}", errorTopic, e);
        }
    }

    private void consume(EntityMutationService entityMutationService,
                         AtomicBoolean running,
                         ConsumerSetter consumerSetter) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        if (consumerSetter != null) {
            consumerSetter.set(consumer);
        }
        LOG.info("KafkaMetadataConsumer starting: bootstrap={}, topic={}",
                bootstrapServers, topic);

        Map<TopicPartition, ExecutorService> executors = new ConcurrentHashMap<>();
        Map<TopicPartition, AtomicLong> nextCommitOffset = new ConcurrentHashMap<>();
        Semaphore inFlight = new Semaphore(maxInFlightRecords);
        long lastCommitAt = System.currentTimeMillis();
        AtomicLong createdSinceLastReport = new AtomicLong(0);
        AtomicLong lastReportAt = new AtomicLong(System.currentTimeMillis());

        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOG.info("KafkaMetadataConsumer partitions revoked: {}", partitions);
                commitNow(consumer, nextCommitOffset);

                for (TopicPartition tp : partitions) {
                    ExecutorService ex = executors.remove(tp);
                    if (ex != null) ex.shutdownNow();
                    nextCommitOffset.remove(tp);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOG.info("KafkaMetadataConsumer partitions assigned: {}", partitions);
                for (TopicPartition tp : partitions) {
                    long pos = consumer.position(tp);
                    nextCommitOffset.put(tp, new AtomicLong(pos));
                    executors.put(tp, Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r);
                        t.setName("worker-" + tp.partition());
                        return t;
                    }));
                }
            }
        });

        try {
            while (running.get()) {
                Timer.Sample pollSample = Timer.start(MetricUtils.getMeterRegistry());
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));
                pollSample.stop(pollTimer);
                pollCounter.increment();
                lastPollTimestampMs.set(System.currentTimeMillis());

                if (!records.isEmpty()) {
                    LOG.info("KafkaMetadataConsumer polled {} records", records.count());
                    recordsCounter.increment(records.count());
                }

                for (ConsumerRecord<String, String> rec : records) {
                    TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                    ExecutorService ex = executors.get(tp);
                    AtomicLong next = nextCommitOffset.get(tp);

                    if (ex == null || next == null) continue;

                    if (running.get()) {
                        try {
                            inFlight.acquire();
                            inFlightGauge.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    ex.submit(() -> {
                        Timer.Sample processSample = Timer.start(MetricUtils.getMeterRegistry());
                        try {
                            LOG.debug("KafkaMetadataConsumer processing {}-{}@{} key={}",
                                    rec.topic(), rec.partition(), rec.offset(), rec.key());
                            int created = processRecord(rec, entityMutationService);
                            if (created > 0) {
                                createdSinceLastReport.addAndGet(created);
                            }
                            next.set(rec.offset() + 1);
                            processedCounter.increment();
                        } catch (Exception e) {
                            LOG.error("KafkaMetadataConsumer failed {}-{}@{}: {}",
                                    rec.topic(), rec.partition(), rec.offset(), e.getMessage(), e);
                            sendToErrorQueue(rec);
                            failedCounter.increment();
                        } finally {
                            processSample.stop(processTimer);
                            inFlightGauge.decrementAndGet();
                            inFlight.release();
                        }
                    });
                }

                long now = System.currentTimeMillis();
                if (now - lastCommitAt >= commitEveryMs) {
                    commitNow(consumer, nextCommitOffset);
                    lastCommitAt = now;
                }

                if (now - lastReportAt.get() >= TimeUnit.SECONDS.toMillis(1) &&
                        createdSinceLastReport.get() > 0) {
                    long created = createdSinceLastReport.getAndSet(0);
                    int partitionCount = consumer.assignment().size();
                    LOG.debug("Created entities per second: " + created
                            + "; assigned partitions: " + partitionCount);
                    lastReportAt.set(now);
                }
            }
        } catch (WakeupException we) {
            // expected on shutdown
        } finally {
            try { commitNow(consumer, nextCommitOffset); } catch (Exception ignored) {}

            for (ExecutorService ex : executors.values()) {
                ex.shutdown();
                try { ex.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            }

            consumer.close();
        }
    }

    private void commitNow(KafkaConsumer<String, String> consumer,
                                  Map<TopicPartition, AtomicLong> nextCommitOffset) {
        if (consumer.assignment().isEmpty()) return;

        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            AtomicLong next = nextCommitOffset.get(tp);
            if (next != null) {
                toCommit.put(tp, new OffsetAndMetadata(next.get()));
            }
        }
        if (!toCommit.isEmpty()) {
            try {
                consumer.commitSync(toCommit);
                commitSuccessCounter.increment();
                lastCommitTimestampMs.set(System.currentTimeMillis());
            } catch (RebalanceInProgressException e) {
                LOG.warn("KafkaMetadataConsumer commit skipped due to rebalance in progress");
                commitFailureCounter.increment();
            }
        }
    }

    private static int processRecord(ConsumerRecord<String, String> rec, EntityMutationService entityMutationService) throws AtlasBaseException {
        if (entityMutationService == null) {
            LOG.warn("Skipping record because entityMutationService is not configured: {}-{}@{}",
                    rec.topic(), rec.partition(), rec.offset());
            return 0;
        }

//        RequestContext.get().setSkipProcessEdgeRestoration(false);
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
//            boolean skipProcessEdgeRestoration = getBoolean(metadata, "skipProcessEdgeRestoration", false);
//            RequestContext.get().setSkipProcessEdgeRestoration(skipProcessEdgeRestoration);

            EntityMutationResponse response = entityMutationService.createOrUpdate(new AtlasEntityStream(entities), context);
            if (response == null || response.getCreatedEntities() == null) {
                return 0;
            }
            return response.getCreatedEntities().size();
        } finally {
            RequestContext.clear();
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

    private interface ConsumerSetter {
        void set(KafkaConsumer<String, String> consumer);
    }

    private void initMetrics() {
        Gauge.builder(METRIC_PREFIX + ".running", runningGauge, AtomicInteger::get)
                .description("Kafka metadata consumer running state")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".in_flight", inFlightGauge, AtomicInteger::get)
                .description("Kafka metadata consumer in-flight records")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".last_poll_timestamp_ms", lastPollTimestampMs, AtomicLong::get)
                .description("Kafka metadata consumer last poll timestamp (ms)")
                .register(MetricUtils.getMeterRegistry());
        Gauge.builder(METRIC_PREFIX + ".last_commit_timestamp_ms", lastCommitTimestampMs, AtomicLong::get)
                .description("Kafka metadata consumer last commit timestamp (ms)")
                .register(MetricUtils.getMeterRegistry());

        pollCounter = Counter.builder(METRIC_PREFIX + ".poll.count")
                .description("Kafka metadata consumer poll count")
                .register(MetricUtils.getMeterRegistry());
        recordsCounter = Counter.builder(METRIC_PREFIX + ".records.count")
                .description("Kafka metadata consumer records received")
                .register(MetricUtils.getMeterRegistry());
        processedCounter = Counter.builder(METRIC_PREFIX + ".processed.count")
                .description("Kafka metadata consumer records processed successfully")
                .register(MetricUtils.getMeterRegistry());
        failedCounter = Counter.builder(METRIC_PREFIX + ".failed.count")
                .description("Kafka metadata consumer record processing failures")
                .register(MetricUtils.getMeterRegistry());
        commitSuccessCounter = Counter.builder(METRIC_PREFIX + ".commit.success.count")
                .description("Kafka metadata consumer commit successes")
                .register(MetricUtils.getMeterRegistry());
        commitFailureCounter = Counter.builder(METRIC_PREFIX + ".commit.failure.count")
                .description("Kafka metadata consumer commit failures")
                .register(MetricUtils.getMeterRegistry());

        pollTimer = Timer.builder(METRIC_PREFIX + ".poll.latency")
                .description("Kafka metadata consumer poll latency")
                .register(MetricUtils.getMeterRegistry());
        processTimer = Timer.builder(METRIC_PREFIX + ".process.latency")
                .description("Kafka metadata consumer processing latency")
                .register(MetricUtils.getMeterRegistry());
    }
}
