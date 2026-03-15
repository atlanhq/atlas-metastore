package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.store.graph.v2.ESConnector;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.repository.util.TagDeNormAttributesUtil;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes tag denorm DLQ messages and repairs ES from Cassandra truth.
 *
 * For each message containing failed vertex IDs:
 * 1. Read ALL tags from Cassandra for each vertex
 * 2. Compute ALL 5 denorm fields via getAllAttributesForAllTagsForRepair
 * 3. Overwrite ES with the Cassandra snapshot
 *
 * This is the "self-healing" component that ensures ES eventually
 * matches Cassandra even after transient ES write failures.
 */
@Service
public class TagDenormDLQReplayService {

    private static final Logger LOG = LoggerFactory.getLogger(TagDenormDLQReplayService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROPERTY_PREFIX = "atlas.kafka";

    @Value("${atlas.kafka.tag.denorm.dlq.topic:ATLAS_TAG_DENORM_DLQ}")
    private String dlqTopic;

    @Value("${atlas.kafka.tag.denorm.dlq.consumerGroupId:atlas_tag_denorm_dlq_replay_group}")
    private String consumerGroupId;

    @Value("${atlas.kafka.tag.denorm.dlq.enabled:true}")
    private boolean enabled;

    @Value("${atlas.kafka.tag.denorm.dlq.maxRetries:3}")
    private int maxRetries;

    @Value("${atlas.kafka.tag.denorm.dlq.pollTimeoutSeconds:15}")
    private int pollTimeoutSeconds;

    private final AtlasTypeRegistry typeRegistry;
    private final IFullTextMapper fullTextMapperV2;

    private KafkaConsumer<String, String> consumer;
    private Thread replayThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    @Inject
    public TagDenormDLQReplayService(AtlasTypeRegistry typeRegistry, IFullTextMapper fullTextMapperV2) {
        this.typeRegistry = typeRegistry;
        this.fullTextMapperV2 = fullTextMapperV2;
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            LOG.info("Tag denorm DLQ replay service is disabled");
            return;
        }

        try {
            Properties props = buildConsumerProperties();
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(dlqTopic));

            running.set(true);
            replayThread = new Thread(this::processMessages, "tag-denorm-dlq-replay");
            replayThread.setDaemon(true);
            replayThread.start();

            LOG.info("Tag denorm DLQ replay service started, topic={}, group={}", dlqTopic, consumerGroupId);
        } catch (Exception e) {
            LOG.error("Failed to start tag denorm DLQ replay service", e);
        }
    }

    private void processMessages() {
        TagDAO tagDAO = TagDAOCassandraImpl.getInstance();

        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        replayDLQEntry(record.value(), tagDAO);
                        processedCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        LOG.error("Failed to replay tag denorm DLQ entry, offset={}", record.offset(), e);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            } catch (WakeupException e) {
                if (running.get()) {
                    LOG.warn("Consumer wakeup while still running", e);
                }
            } catch (Exception e) {
                LOG.error("Error in tag denorm DLQ replay loop", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Replays a single DLQ entry: reads Cassandra truth for each vertex, writes to ES.
     * DLQ message format: { "type": "TAG_DENORM_SYNC", "vertices": { "vertexId": "guid", ... } }
     */
    private void replayDLQEntry(String dlqJson, TagDAO tagDAO) throws Exception {
        JsonNode message = MAPPER.readTree(dlqJson);
        JsonNode verticesNode = message.get("vertices");

        if (verticesNode == null || !verticesNode.isObject()) {
            LOG.warn("Invalid tag denorm DLQ message: missing vertices object");
            return;
        }

        Map<String, Map<String, Object>> deNormMap = new HashMap<>();

        Iterator<Map.Entry<String, JsonNode>> fields = verticesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String vertexId = field.getKey();
            String entityGuid = field.getValue().asText();

            if (entityGuid == null || entityGuid.isEmpty()) {
                LOG.warn("Missing entity GUID for vertexId={} in DLQ message, skipping", vertexId);
                continue;
            }

            try {
                List<AtlasClassification> currentTags = tagDAO.getAllClassificationsForVertex(vertexId);
                Map<String, Object> denormAttrs = TagDeNormAttributesUtil.getAllAttributesForAllTagsForRepair(
                        entityGuid, currentTags, typeRegistry, fullTextMapperV2);
                deNormMap.put(vertexId, denormAttrs);
            } catch (AtlasBaseException e) {
                LOG.error("Failed to read Cassandra tags for vertexId={}", vertexId, e);
            }
        }

        if (!deNormMap.isEmpty()) {
            ESConnector.writeTagProperties(deNormMap);
            LOG.info("Tag denorm DLQ: repaired {} vertices from Cassandra truth", deNormMap.size());
        }
    }

    private Properties buildConsumerProperties() throws Exception {
        Configuration appConfig = ApplicationProperties.get();
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(appConfig, PROPERTY_PREFIX);
        Properties props = ConfigurationConverter.getProperties(kafkaConf);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return props;
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (replayThread != null) {
            try {
                replayThread.join(30000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(10));
        }
        LOG.info("Tag denorm DLQ replay service stopped. Processed={}, Errors={}", processedCount.get(), errorCount.get());
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("enabled", enabled);
        status.put("running", running.get());
        status.put("topic", dlqTopic);
        status.put("consumerGroup", consumerGroupId);
        status.put("processedCount", processedCount.get());
        status.put("errorCount", errorCount.get());
        return status;
    }
}
