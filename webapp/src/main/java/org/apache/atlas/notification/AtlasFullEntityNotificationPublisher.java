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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AtlasFullEntityNotificationPublisher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasFullEntityNotificationPublisher.class);
    private static final String PROPERTY_PREFIX = "atlas.kafka";
    private static final String NOTIFICATION_TOPICS_PROPERTY = "atlas.notification.topics";

    private final Properties kafkaProperties;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean enabled;
    private volatile boolean topicEnsured;

    public AtlasFullEntityNotificationPublisher(Configuration atlasProperties, String topic) {
        this(createKafkaProducerProperties(atlasProperties), topic, isTopicEnabled(atlasProperties, topic));
    }

    @VisibleForTesting
    AtlasFullEntityNotificationPublisher(Properties kafkaProperties, String topic) {
        this(kafkaProperties, topic, true);
    }

    @VisibleForTesting
    AtlasFullEntityNotificationPublisher(Properties kafkaProperties, String topic, boolean enabled) {
        this.topic = topic;
        this.enabled = enabled;
        this.kafkaProperties = new Properties();
        this.kafkaProperties.putAll(kafkaProperties);
        this.producer = enabled ? new KafkaProducer<>(kafkaProperties) : null;
    }

    public Optional<Future<RecordMetadata>> publishEntity(AtlasEntity entity, OperationType operationType, long eventTime, Map<String, String> headers) {
        if (!enabled) {
            LOG.debug("Skipping full entity publish because {} does not include topic {}", NOTIFICATION_TOPICS_PROPERTY, topic);
            return Optional.empty();
        }

        if (entity == null || entity.getGuid() == null || operationType == null) {
            LOG.warn("Skipping full entity publish because entity, guid, or operationType is null");
            return Optional.empty();
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "ENTITY_NOTIFICATION");
        payload.put("operationType", operationType.name());
        payload.put("eventTime", eventTime);
        payload.put("headers", headers);
        payload.put("entity", entity);

        try {
            ensureTopicExists();
            return Optional.of(send(new ProducerRecord<>(topic, entity.getGuid(), AtlasType.toJson(payload))));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish full entity notification for guid=" + entity.getGuid() + " to topic " + topic, e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void close() {
        if (producer == null) {
            return;
        }

        try {
            producer.close(Duration.ofSeconds(10));
        } catch (Exception e) {
            LOG.warn("Failed to close full entity notification publisher cleanly", e);
        }
    }

    private Future<RecordMetadata> send(ProducerRecord<String, String> record) throws Exception {
        return producer.send(record);
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

    private static boolean isTopicEnabled(Configuration atlasProperties, String topic) {
        if (atlasProperties == null || StringUtils.isBlank(topic)) {
            return false;
        }

        String[] configuredTopics = atlasProperties.getStringArray(NOTIFICATION_TOPICS_PROPERTY);

        if (configuredTopics == null || configuredTopics.length == 0) {
            String configuredTopicsValue = atlasProperties.getString(NOTIFICATION_TOPICS_PROPERTY);
            configuredTopics = StringUtils.split(configuredTopicsValue, ',');
        }

        if (configuredTopics != null) {
            for (String configuredTopic : configuredTopics) {
                if (topic.equals(StringUtils.trim(configuredTopic))) {
                    return true;
                }
            }
        }

        LOG.info("Full entity notifications disabled because topic {} is not present in {}", topic, NOTIFICATION_TOPICS_PROPERTY);
        return false;
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
                adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                topicEnsured = true;
                LOG.info("Ensured full entity notification topic exists: {}", topic);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    topicEnsured = true;
                    LOG.debug("Full entity notification topic already exists: {}", topic);
                } else {
                    LOG.warn("Unable to create full entity notification topic {}. Publish will be retried on the next event.", topic, e);
                }
            } catch (Exception e) {
                LOG.warn("Unable to create full entity notification topic {}. Publish will be retried on the next event.", topic, e);
            }
        }
    }
}
