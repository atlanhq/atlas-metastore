/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.instance.AtlasEntityHeaderWithRelations;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AtlasEntitySnapshotPublisher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntitySnapshotPublisher.class);
    private static final String PROPERTY_PREFIX = "atlas.kafka";

    private final Properties kafkaProperties;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private volatile boolean topicEnsured;

    public AtlasEntitySnapshotPublisher(Configuration atlasProperties, String topic) {
        this(createKafkaProducerProperties(atlasProperties), topic);
    }

    @VisibleForTesting
    AtlasEntitySnapshotPublisher(Properties kafkaProperties, String topic) {
        this.topic = topic;
        this.kafkaProperties = new Properties();
        this.kafkaProperties.putAll(kafkaProperties);
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    public void publishSnapshot(AtlasEntityHeaderWithRelations asset) {
        if (asset == null || asset.getGuid() == null) {
            LOG.warn("Skipping snapshot publish because asset or guid is null");
            return;
        }

        try {
            ensureTopicExists();
            send(new ProducerRecord<>(topic, asset.getGuid(), AtlasType.toJson(asset)));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish snapshot for guid=" + asset.getGuid() + " to topic " + topic, e);
        }
    }

    public void publishDelete(String guid) {
        if (guid == null) {
            LOG.warn("Skipping tombstone publish because guid is null");
            return;
        }

        try {
            ensureTopicExists();
            send(new ProducerRecord<>(topic, guid, null));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish tombstone for guid=" + guid + " to topic " + topic, e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close(Duration.ofSeconds(10));
        } catch (Exception e) {
            LOG.warn("Failed to close snapshot publisher cleanly", e);
        }
    }

    private void send(ProducerRecord<String, String> record) throws Exception {
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        LOG.debug("Published snapshot record to {} partition {} offset {}", metadata.topic(), metadata.partition(), metadata.offset());
    }

    private static Properties createKafkaProducerProperties(Configuration atlasProperties) {
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(atlasProperties, PROPERTY_PREFIX);
        Properties props = ConfigurationConverter.getProperties(kafkaConf);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, "5");
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "10");

        KafkaUtils.setKafkaJAASProperties(atlasProperties, props);
        return props;
    }

    private void ensureTopicExists() {
        if (topicEnsured) {
            return;
        }

        synchronized (this) {
            if (topicEnsured) {
                return;
            }

            try (AdminClient adminClient = AdminClient.create(kafkaProperties)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1)
                        .configs(Collections.singletonMap("cleanup.policy", "compact"));
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                topicEnsured = true;
                LOG.info("Ensured compacted snapshot topic exists: {}", topic);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    topicEnsured = true;
                    LOG.debug("Compacted snapshot topic already exists: {}", topic);
                } else {
                    LOG.warn("Unable to create snapshot topic {}. Publish will be retried on the next event.", topic, e);
                }
            } catch (Exception e) {
                LOG.warn("Unable to create snapshot topic {}. Publish will be retried on the next event.", topic, e);
            }
        }
    }
}
