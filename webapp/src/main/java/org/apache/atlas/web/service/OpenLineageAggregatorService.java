package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

@Service
public class OpenLineageAggregatorService {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageAggregatorService.class);

    private static final String CONFIG_PREFIX = "atlas.kafka.openlineage.aggregator";

    private volatile KafkaStreams streams;

    @PostConstruct
    public void start() {
        try {
            Configuration configuration = ApplicationProperties.get();
            boolean enabled = configuration.getBoolean(CONFIG_PREFIX + ".enabled", false);
            if (!enabled) {
                LOG.info("OpenLineageAggregatorService is disabled via {}.enabled", CONFIG_PREFIX);
                return;
            }

            String inputTopic = configuration.getString(CONFIG_PREFIX + ".inputTopic", "openlineageevents");
            String outputTopic = configuration.getString(
                    CONFIG_PREFIX + ".outputTopic",
                    configuration.getString("atlas.kafka.metadata.topic", "events-topic")
            );
            String errorTopic = configuration.getString(CONFIG_PREFIX + ".errorTopic", "openlineageevents-errors");
            String bootstrapServers = configuration.getString(
                    "atlas.graph.kafka.bootstrap.servers",
                    configuration.getString("atlas.kafka.bootstrap.servers", "localhost:9092")
            );
            String applicationId = configuration.getString(CONFIG_PREFIX + ".applicationId", "atlas-openlineage-aggregator");
            String storeTypeConfig = configuration.getString(CONFIG_PREFIX + ".storeType", "ROCK_DB");
            String stateDir = configuration.getString(CONFIG_PREFIX + ".stateDir", null);

            OpenLineageAggregator.LineageTopologyType topologyType = parseTopologyType(storeTypeConfig);
            Topology topology = OpenLineageAggregator.LineageTopology.build(
                    inputTopic,
                    outputTopic,
                    errorTopic,
                    topologyType
            );

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            if (stateDir != null && !stateDir.isEmpty()) {
                props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
            }

            streams = new KafkaStreams(topology, props);
            streams.start();
            LOG.info("OpenLineageAggregatorService started with inputTopic={} outputTopic={} errorTopic={}",
                    inputTopic, outputTopic, errorTopic);
        } catch (AtlasException e) {
            LOG.error("Failed to load OpenLineage aggregator configuration", e);
        } catch (RuntimeException e) {
            LOG.error("Failed to start OpenLineageAggregatorService", e);
            throw e;
        }
    }

    @PreDestroy
    public void stop() {
        KafkaStreams streamsRef = streams;
        if (streamsRef != null) {
            streamsRef.close(Duration.ofSeconds(30));
        }
    }

    private static OpenLineageAggregator.LineageTopologyType parseTopologyType(String value) {
        if (value == null) {
            return OpenLineageAggregator.LineageTopologyType.ROCK_DB;
        }
        try {
            return OpenLineageAggregator.LineageTopologyType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return OpenLineageAggregator.LineageTopologyType.ROCK_DB;
        }
    }
}
