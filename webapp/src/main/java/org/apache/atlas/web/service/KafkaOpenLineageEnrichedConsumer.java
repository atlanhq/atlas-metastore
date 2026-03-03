package org.apache.atlas.web.service;

import org.apache.atlas.repository.store.graph.v2.lineage.OpenLineageEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * Kafka consumer for enriched OpenLineage events.
 *
 * Consumes from the enriched topic where openlineage-app publishes events
 * that had missing parent facets but were enriched via Redis mapping cache.
 * Uses the same processing logic as the original OpenLineage consumer.
 */
@Service
public class KafkaOpenLineageEnrichedConsumer extends KafkaEventConsumer {
    private final OpenLineageEventService eventService;

    @Inject
    public KafkaOpenLineageEnrichedConsumer(OpenLineageEventService eventService) {
        super("atlas.kafka.openlineage.enriched", "kafka.openlineage.enriched.consumer", "kafka-openlineage-enriched-consumer");
        this.eventService = eventService;
    }

    @PostConstruct
    public void start() {
        super.start();
    }

    @Override
    protected String defaultTopic() {
        return "openlineage-topic-enriched";
    }

    @Override
    protected String defaultErrorTopic() {
        return "openlineage-topic-enriched-errors";
    }

    @Override
    protected String defaultConsumerGroupId() {
        return "atlas-openlineage-enriched-consumer";
    }

    @Override
    protected void processRecord(ConsumerRecord<String, String> rec) throws Exception {
        eventService.processEvent(rec.value());
    }
}
