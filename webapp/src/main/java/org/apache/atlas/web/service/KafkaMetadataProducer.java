package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaMetadataProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataProducer.class);
    private static final String METRIC_PREFIX = "kafka.metadata.producer";

    private String topic;
    private String bootstrapServers;
    private long producerSendTimeoutMs;
    private Properties producerProperties;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger runningGauge = new AtomicInteger(0);
    private volatile KafkaProducer<String, String> producer;
    private Counter sendSuccessCounter;
    private Counter sendFailureCounter;
    private Timer sendTimer;

    @PostConstruct
    public void start() {
        try {
            Configuration configuration = ApplicationProperties.get();
            boolean enabled = configuration.getBoolean("atlas.kafka.metadata.enabled", false);
            if (!enabled) {
                LOG.info("KafkaMetadataProducer is disabled via atlas.kafka.metadata.enabled");
                return;
            }
            topic = configuration.getString("atlas.kafka.metadata.topic", "events-topic");
            producerSendTimeoutMs = configuration.getLong("atlas.kafka.metadata.producerSendTimeoutMs", 10000L);
            bootstrapServers = configuration.getString(
                    "atlas.graph.kafka.bootstrap.servers",
                    configuration.getString("atlas.kafka.bootstrap.servers", "localhost:9092")
            );
            producerProperties = buildProducerProperties(bootstrapServers);
        } catch (AtlasException e) {
            LOG.error("Failed to load Kafka metadata producer configuration", e);
            return;
        }
        initMetrics();
        running.set(true);
        runningGauge.set(1);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        runningGauge.set(0);
        KafkaProducer<String, String> producerRef = producer;
        if (producerRef != null) {
            producerRef.close();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public String sendBulkRequest(AtlasEntitiesWithExtInfo entities, Map<String, Object> metadata) throws Exception {
        if (!running.get()) {
            throw new IllegalStateException("Kafka metadata producer is not running");
        }
        if (StringUtils.isBlank(topic)) {
            throw new IllegalStateException("Kafka metadata topic is not configured");
        }

        KafkaProducer<String, String> producerRef = getOrCreateProducer();
        String eventId = UUID.randomUUID().toString();

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("uuid", eventId);
        envelope.put("eventTime", System.currentTimeMillis());
        envelope.put("metadata", metadata);
        envelope.put("payload", entities);

        String value = AtlasType.toJson(envelope);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventId, value);
        Timer.Sample sendSample = Timer.start(MetricUtils.getMeterRegistry());
        try {
            producerRef.send(record).get(producerSendTimeoutMs, TimeUnit.MILLISECONDS);
            sendSuccessCounter.increment();
            return eventId;
        } catch (Exception e) {
            sendFailureCounter.increment();
            throw e;
        } finally {
            sendSample.stop(sendTimer);
        }
    }

    private KafkaProducer<String, String> getOrCreateProducer() {
        KafkaProducer<String, String> producerRef = producer;
        if (producerRef == null) {
            synchronized (this) {
                producerRef = producer;
                if (producerRef == null) {
                    producerRef = new KafkaProducer<>(producerProperties);
                    producer = producerRef;
                }
            }
        }
        return producerRef;
    }

    private static Properties buildProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private void initMetrics() {
        Gauge.builder(METRIC_PREFIX + ".running", runningGauge, AtomicInteger::get)
                .description("Kafka metadata producer running state")
                .register(MetricUtils.getMeterRegistry());
        sendSuccessCounter = Counter.builder(METRIC_PREFIX + ".send.success.count")
                .description("Kafka metadata producer send successes")
                .register(MetricUtils.getMeterRegistry());
        sendFailureCounter = Counter.builder(METRIC_PREFIX + ".send.failure.count")
                .description("Kafka metadata producer send failures")
                .register(MetricUtils.getMeterRegistry());
        sendTimer = Timer.builder(METRIC_PREFIX + ".send.latency")
                .description("Kafka metadata producer send latency")
                .register(MetricUtils.getMeterRegistry());
    }
}
