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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.notification.AsyncDeleteNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

/**
 * Producer service for sending async delete requests to Kafka.
 */
@Service
public class AsyncDeleteProducer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDeleteProducer.class);

    private final NotificationInterface notificationInterface;
    private final DeleteMetrics metrics;

    @Inject
    public AsyncDeleteProducer(NotificationInterface notificationInterface, DeleteMetrics metrics) {
        this.notificationInterface = notificationInterface;
        this.metrics = metrics;
    }

    /**
     * Send an async delete notification to Kafka.
     *
     * @param notification the delete notification to send
     * @throws AtlasBaseException if sending fails
     */
    public void send(AsyncDeleteNotification notification) throws AtlasBaseException {
        try {
            LOG.info("Publishing async delete request: requestId={}, type={}, user={}",
                    notification.getRequestId(),
                    notification.getEndpointType(),
                    notification.getRequestedBy());

            notificationInterface.send(NotificationType.ASYNC_DELETE, notification);

            metrics.incrementProducerPublishSuccess();

            LOG.debug("Successfully published async delete request: requestId={}",
                    notification.getRequestId());

        } catch (NotificationException e) {
            LOG.error("Failed to publish async delete request: requestId={}",
                    notification.getRequestId(), e);

            metrics.incrementProducerPublishFailure();

            throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED,
                    "Failed to queue async delete request: " + e.getMessage());
        }
    }

    /**
     * Send a failed async delete notification to the dead letter queue.
     *
     * @param notification the notification that failed processing
     * @param errorMessage the error message describing the failure
     */
    public void sendToDLQ(AsyncDeleteNotification notification, String errorMessage) {
        try {
            notification.setLastError(errorMessage);

            LOG.warn("Publishing to DLQ: requestId={}, retryCount={}, error={}",
                    notification.getRequestId(),
                    notification.getRetryCount(),
                    errorMessage);

            notificationInterface.send(NotificationType.ASYNC_DELETE_DLQ, notification);

            metrics.incrementDlqPublishes();

            LOG.info("Successfully published to DLQ: requestId={}", notification.getRequestId());

        } catch (NotificationException e) {
            LOG.error("Failed to publish to DLQ: requestId={}",
                    notification.getRequestId(), e);
            // Log but don't throw - DLQ failure shouldn't cause further issues
        }
    }

    /**
     * Republish a notification for retry.
     *
     * @param notification the notification to retry
     * @throws NotificationException if republishing fails
     */
    public void republishForRetry(AsyncDeleteNotification notification) throws NotificationException {
        LOG.info("Republishing for retry: requestId={}, retryCount={}",
                notification.getRequestId(),
                notification.getRetryCount());

        notificationInterface.send(NotificationType.ASYNC_DELETE, notification);

        metrics.incrementRetryCount();
    }
}
