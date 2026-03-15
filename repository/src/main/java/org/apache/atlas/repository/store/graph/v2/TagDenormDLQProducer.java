package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka producer for tag denorm DLQ messages.
 *
 * When ES write fails for some vertex IDs during tag denorm sync,
 * this producer emits them to ATLAS_TAG_DENORM_DLQ topic so a consumer
 * can later re-read from Cassandra and repair ES.
 *
 * Best-effort: logs errors, does NOT fail the caller.
 */
@Service
public class TagDenormDLQProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TagDenormDLQProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROPERTY_PREFIX = "atlas.kafka";
    private static final String CONFIG_TOPIC = "atlas.kafka.tag.denorm.dlq.topic";
    private static final String DEFAULT_TOPIC = "ATLAS_TAG_DENORM_DLQ";

    private volatile KafkaProducer<String, String> producer;
    private String topic;

    public TagDenormDLQProducer() {
        // Default constructor for Spring and testability
    }

    public void init() {
        try {
            Configuration appConfig = ApplicationProperties.get();
            this.topic = appConfig.getString(CONFIG_TOPIC, DEFAULT_TOPIC);
        } catch (Exception e) {
            LOG.warn("Failed to read tag denorm DLQ config, using defaults", e);
            this.topic = DEFAULT_TOPIC;
        }
    }

    /**
     * Emits failed vertex IDs with their GUIDs to the tag denorm DLQ topic.
     * Each message contains vertexId→GUID pairs that failed ES sync and need repair.
     * The consumer re-reads Cassandra truth and repairs ES for these vertices.
     */
    public void emitFailedVertices(List<String> failedVertexIds, Map<String, String> vertexIdToGuidMap) {
        if (failedVertexIds == null || failedVertexIds.isEmpty()) {
            return;
        }

        try {
            KafkaProducer<String, String> p = getOrCreateProducer();
            if (p == null) {
                LOG.error("Tag denorm DLQ producer is not initialized, cannot emit {} failed vertices", failedVertexIds.size());
                return;
            }

            ObjectNode message = MAPPER.createObjectNode();
            message.put("type", "TAG_DENORM_SYNC");
            message.put("timestamp", System.currentTimeMillis());

            ObjectNode vertices = message.putObject("vertices");
            for (String vertexId : failedVertexIds) {
                String guid = vertexIdToGuidMap != null ? vertexIdToGuidMap.get(vertexId) : null;
                vertices.put(vertexId, guid != null ? guid : "");
            }

            String key = UUID.randomUUID().toString();
            String value = MAPPER.writeValueAsString(message);

            p.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send tag denorm DLQ message for {} vertices", failedVertexIds.size(), exception);
                } else {
                    LOG.info("Sent tag denorm DLQ message for {} vertices to topic={}, partition={}, offset={}",
                            failedVertexIds.size(), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOG.error("Error emitting tag denorm DLQ message for {} vertices", failedVertexIds.size(), e);
        }
    }

    private KafkaProducer<String, String> getOrCreateProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    try {
                        if (topic == null) {
                            init();
                        }
                        Properties props = buildProducerProperties();
                        producer = new KafkaProducer<>(props);
                        LOG.info("Initialized tag denorm DLQ Kafka producer for topic: {}", topic);
                    } catch (Exception e) {
                        LOG.error("Failed to create tag denorm DLQ Kafka producer", e);
                    }
                }
            }
        }
        return producer;
    }

    private Properties buildProducerProperties() throws Exception {
        Configuration appConfig = ApplicationProperties.get();
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(appConfig, PROPERTY_PREFIX);
        Properties props = ConfigurationConverter.getProperties(kafkaConf);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, "3");
        return props;
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                LOG.warn("Error closing tag denorm DLQ producer", e);
            }
        }
    }
}
