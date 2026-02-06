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
package org.apache.atlas.web.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Timer;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.AsyncDeleteNotification;
import org.apache.atlas.model.notification.AsyncDeleteNotification.DeletePayload;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer service for processing async delete requests from Kafka.
 * Each consumer thread has its own dedicated KafkaConsumer to avoid thread-safety issues.
 */
@Service
public class AsyncDeleteConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDeleteConsumer.class);

    private final NotificationInterface notificationInterface;
    private final EntityMutationService entityMutationService;
    private final AsyncDeleteProducer asyncDeleteProducer;
    private final DeleteMetrics metrics;
    private final AtlasTypeRegistry typeRegistry;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;
    private int numConsumers;

    @Inject
    public AsyncDeleteConsumer(NotificationInterface notificationInterface,
                               EntityMutationService entityMutationService,
                               AsyncDeleteProducer asyncDeleteProducer,
                               DeleteMetrics metrics,
                               AtlasTypeRegistry typeRegistry) {
        this.notificationInterface = notificationInterface;
        this.entityMutationService = entityMutationService;
        this.asyncDeleteProducer = asyncDeleteProducer;
        this.metrics = metrics;
        this.typeRegistry = typeRegistry;
    }

    @PostConstruct
    public void init() {
        if (!AtlasConfiguration.ASYNC_DELETE_ENABLED.getBoolean()) {
            LOG.info("Async delete consumer is disabled");
            return;
        }

        numConsumers = AtlasConfiguration.ASYNC_DELETE_CONSUMER_THREADS.getInt();

        LOG.info("Initializing async delete consumer with {} threads", numConsumers);

        executorService = Executors.newFixedThreadPool(numConsumers,
                new ThreadFactoryBuilder()
                        .setNameFormat("async-delete-consumer-%d")
                        .setDaemon(true)
                        .build());

        running.set(true);

        // Each thread creates and manages its own consumer
        for (int i = 0; i < numConsumers; i++) {
            final int consumerIndex = i;
            executorService.submit(() -> runConsumerThread(consumerIndex));
        }

        LOG.info("Started {} async delete consumer threads", numConsumers);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down async delete consumer");

        running.set(false);

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("Async delete consumer shutdown complete");
    }

    /**
     * Each thread runs this method with its own dedicated consumer.
     * The consumer is created within the thread to ensure thread ownership.
     */
    private void runConsumerThread(int threadIndex) {
        LOG.info("Async delete consumer thread {} starting", threadIndex);

        // Create a dedicated consumer for this thread
        // createConsumers(type, 1) returns a list with one consumer
        List<NotificationConsumer<AsyncDeleteNotification>> consumerList =
                notificationInterface.createConsumers(NotificationType.ASYNC_DELETE, 1);

        if (consumerList == null || consumerList.isEmpty()) {
            LOG.error("Failed to create consumer for thread {}", threadIndex);
            return;
        }

        NotificationConsumer<AsyncDeleteNotification> consumer = consumerList.get(0);

        LOG.info("Async delete consumer thread {} started with dedicated consumer", threadIndex);

        while (running.get()) {
            try {
                List<AtlasKafkaMessage<AsyncDeleteNotification>> messages = consumer.receive(1000);

                for (AtlasKafkaMessage<AsyncDeleteNotification> message : messages) {
                    processMessage(message, consumer);
                }

            } catch (IllegalStateException e) {
                // Consumer may have been closed
                if (running.get()) {
                    LOG.error("Consumer error in thread {}, will recreate", threadIndex, e);
                    metrics.incrementConsumerErrors();

                    // Try to recreate consumer
                    try {
                        Thread.sleep(1000);
                        consumerList = notificationInterface.createConsumers(NotificationType.ASYNC_DELETE, 1);
                        if (consumerList != null && !consumerList.isEmpty()) {
                            consumer = consumerList.get(0);
                            LOG.info("Recreated consumer for thread {}", threadIndex);
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    LOG.error("Error in async delete consumer thread {}", threadIndex, e);
                    metrics.incrementConsumerErrors();

                    // Brief pause before retrying to avoid tight error loops
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // Close the consumer when thread exits
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.warn("Error closing consumer in thread {}", threadIndex, e);
        }

        LOG.info("Async delete consumer thread {} stopped", threadIndex);
    }

    private void processMessage(AtlasKafkaMessage<AsyncDeleteNotification> message,
                                NotificationConsumer<AsyncDeleteNotification> consumer) {

        AsyncDeleteNotification notification = message.getMessage();
        String requestId = notification.getRequestId();

        LOG.info("Processing async delete: requestId={}, type={}, attempt={}",
                requestId, notification.getEndpointType(), notification.getRetryCount() + 1);

        Timer.Sample sample = metrics.startTimer();

        try {
            // Execute delete
            EntityMutationResponse response = executeDelete(notification);

            // Commit offset
            consumer.commit(message.getTopicPartition(), message.getOffset() + 1);

            // Record success metrics
            metrics.incrementConsumerDeleteSuccess();
            metrics.recordLatency(sample);

            int deletedCount = response.getDeletedEntities() != null ? response.getDeletedEntities().size() : 0;
            LOG.info("Async delete completed: requestId={}, deletedCount={}", requestId, deletedCount);

        } catch (Exception e) {
            LOG.error("Async delete failed: requestId={}, error={}", requestId, e.getMessage(), e);

            metrics.incrementConsumerDeleteFailure();

            handleFailure(notification, e, message, consumer);
        }
    }

    private EntityMutationResponse executeDelete(AsyncDeleteNotification notification) throws AtlasBaseException {
        DeletePayload payload = notification.getPayload();

        // Set request context for the delete operation
        try {
            RequestContext.clear();
            RequestContext.get().setUser(notification.getRequestedBy(), null);

            switch (notification.getEndpointType()) {
                case GUID_DELETE:
                    return entityMutationService.deleteById(payload.getGuids().get(0));

                case BULK_GUID_DELETE:
                    return entityMutationService.deleteByIds(payload.getGuids());

                case UNIQUE_ATTR_DELETE:
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(payload.getTypeName());
                    if (entityType == null) {
                        throw new AtlasBaseException("Unknown entity type: " + payload.getTypeName());
                    }
                    return entityMutationService.deleteByUniqueAttributes(entityType, payload.getUniqueAttributes());

                case BULK_UNIQUE_ATTR_DELETE:
                    if (payload.isSkipHasLineageCalculation()) {
                        RequestContext.get().setSkipHasLineageCalculation(true);
                    }
                    return entityMutationService.deleteByUniqueAttributes(payload.getObjectIds());

                default:
                    throw new AtlasBaseException("Unknown delete endpoint type: " + notification.getEndpointType());
            }
        } finally {
            RequestContext.clear();
        }
    }

    private void handleFailure(AsyncDeleteNotification notification,
                               Exception error,
                               AtlasKafkaMessage<AsyncDeleteNotification> message,
                               NotificationConsumer<AsyncDeleteNotification> consumer) {

        int maxRetries = AtlasConfiguration.ASYNC_DELETE_MAX_RETRIES.getInt();

        if (notification.getRetryCount() < maxRetries) {
            // Retry: republish with incremented retry count
            notification.setRetryCount(notification.getRetryCount() + 1);
            notification.setLastError(error.getMessage());

            try {
                // Delay before retry
                long retryDelayMs = AtlasConfiguration.ASYNC_DELETE_RETRY_DELAY_MS.getLong();
                Thread.sleep(retryDelayMs);

                // Republish for retry
                asyncDeleteProducer.republishForRetry(notification);

                LOG.info("Scheduled retry: requestId={}, retryCount={}",
                        notification.getRequestId(), notification.getRetryCount());

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while scheduling retry, sending to DLQ: requestId={}",
                        notification.getRequestId());
                asyncDeleteProducer.sendToDLQ(notification, error.getMessage());
            } catch (NotificationException e) {
                LOG.error("Failed to schedule retry, sending to DLQ: requestId={}",
                        notification.getRequestId(), e);
                asyncDeleteProducer.sendToDLQ(notification, error.getMessage());
            }
        } else {
            // Max retries exhausted - send to DLQ
            LOG.warn("Max retries exhausted, sending to DLQ: requestId={}, retries={}",
                    notification.getRequestId(), notification.getRetryCount());
            asyncDeleteProducer.sendToDLQ(notification, error.getMessage());
        }

        // Commit offset (message processed, even if failed)
        try {
            consumer.commit(message.getTopicPartition(), message.getOffset() + 1);
        } catch (Exception e) {
            LOG.error("Failed to commit offset after failure: requestId={}",
                    notification.getRequestId(), e);
        }
    }

    /**
     * Check if the consumer is running.
     */
    public boolean isRunning() {
        return running.get();
    }
}
