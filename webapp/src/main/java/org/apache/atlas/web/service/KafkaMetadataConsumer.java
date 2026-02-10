package org.apache.atlas.web.service;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.type.AtlasType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Kafka queue consumer for Atlas metadata bulk requests.
 */
@Service
public class KafkaMetadataConsumer extends KafkaEventConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataConsumer.class);

    private final EntityMutationService entityMutationService;

    @Inject
    public KafkaMetadataConsumer(EntityMutationService entityMutationService) {
        super("atlas.kafka.metadata", "kafka.metadata.consumer", "kafka-metadata-consumer");
        this.entityMutationService = entityMutationService;
    }

    @PostConstruct
    public void start() {
        super.start();
    }

    @Override
    protected String defaultTopic() {
        return "events-topic";
    }

    @Override
    protected String defaultErrorTopic() {
        return "events-topic-errors";
    }

    @Override
    protected String defaultConsumerGroupId() {
        return "atlas-metadata-consumer";
    }

    @Override
    protected void processRecord(ConsumerRecord<String, String> rec) throws Exception {
        processMetadataRecord(rec, entityMutationService);
    }

    private void processMetadataRecord(ConsumerRecord<String, String> rec, EntityMutationService entityMutationService) throws AtlasBaseException {
        if (entityMutationService == null) {
            LOG.warn("Skipping record because entityMutationService is not configured: {}-{}@{}",
                    rec.topic(), rec.partition(), rec.offset());
            return;
        }

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
        entityMutationService.createOrUpdate(new AtlasEntityStream(entities), context);
    }

    private BulkRequestContext buildBulkRequestContext(Map<String, Object> metadata) {
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
