package org.apache.atlas.web.service;

import org.apache.atlas.repository.store.graph.v2.lineage.OpenLineageEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * Kafka queue consumer for Atlas metadata bulk requests.
 */
@Service
public class KafkaOpenLineageConsumer extends KafkaEventConsumer {
    private final OpenLineageEventService eventService;

    @Inject
    public KafkaOpenLineageConsumer(OpenLineageEventService eventService) {
        super("atlas.kafka.openlineage", "kafka.openlineage.consumer", "kafka-openlineage-consumer");
        this.eventService = eventService;
    }


    @PostConstruct
    public void start() {
        super.start();
    }

    @Override
    protected String defaultTopic() {
        return "openlineage-topic";
    }

    @Override
    protected String defaultErrorTopic() {
        return "openlineage-topic-errors";
    }

    @Override
    protected String defaultConsumerGroupId() {
        return "atlas-openlineage-consumer";
    }

    @Override
    protected void processRecord(ConsumerRecord<String, String> rec) throws Exception {
        eventService.processEvent(rec.value());
    }
}
