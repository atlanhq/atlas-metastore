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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * Metrics service for tracking async delete operations.
 */
@Service
public class DeleteMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteMetrics.class);

    private static final String SERVICE = "service";
    private static final String ATLAS_METASTORE = "atlas-metastore";
    private static final double[] PERCENTILES = {0.5, 0.95, 0.99};

    // Request counters
    private Counter totalDeleteRequests;
    private Counter asyncDeleteRequests;
    private Counter syncDeleteRequests;

    // Consumer counters
    private Counter consumerDeleteSuccess;
    private Counter consumerDeleteFailure;
    private Counter consumerErrors;
    private Counter retryCount;

    // Producer counters
    private Counter producerPublishSuccess;
    private Counter producerPublishFailure;

    // DLQ counter
    private Counter dlqPublishes;

    // Latency timer
    private Timer consumerLatencyTimer;

    @PostConstruct
    public void init() {
        try {
            Tags commonTags = Tags.of(SERVICE, ATLAS_METASTORE);

            // Request counters
            totalDeleteRequests = Counter.builder("atlas.delete.requests.total")
                    .description("Total delete requests received")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            asyncDeleteRequests = Counter.builder("atlas.delete.requests.async")
                    .description("Async delete requests")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            syncDeleteRequests = Counter.builder("atlas.delete.requests.sync")
                    .description("Sync delete requests")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            // Consumer counters
            consumerDeleteSuccess = Counter.builder("atlas.delete.consumer.success")
                    .description("Successful async delete executions")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            consumerDeleteFailure = Counter.builder("atlas.delete.consumer.failure")
                    .description("Failed async delete executions")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            consumerErrors = Counter.builder("atlas.delete.consumer.errors")
                    .description("Consumer processing errors")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            retryCount = Counter.builder("atlas.delete.consumer.retries")
                    .description("Retry attempts")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            // Producer counters
            producerPublishSuccess = Counter.builder("atlas.delete.producer.success")
                    .description("Successful Kafka publishes")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            producerPublishFailure = Counter.builder("atlas.delete.producer.failure")
                    .description("Failed Kafka publishes")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            // DLQ counter
            dlqPublishes = Counter.builder("atlas.delete.dlq.publishes")
                    .description("Messages sent to DLQ")
                    .tags(commonTags)
                    .register(MetricUtils.getMeterRegistry());

            // Latency timer
            consumerLatencyTimer = Timer.builder("atlas.delete.consumer.latency")
                    .description("Async delete execution latency")
                    .tags(commonTags)
                    .publishPercentiles(PERCENTILES)
                    .register(MetricUtils.getMeterRegistry());

            LOG.info("DeleteMetrics initialized successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize DeleteMetrics", e);
        }
    }

    public void incrementTotalDeleteRequests() {
        if (totalDeleteRequests != null) {
            totalDeleteRequests.increment();
        }
    }

    public void incrementAsyncDeleteRequests() {
        if (asyncDeleteRequests != null) {
            asyncDeleteRequests.increment();
        }
    }

    public void incrementSyncDeleteRequests() {
        if (syncDeleteRequests != null) {
            syncDeleteRequests.increment();
        }
    }

    public void incrementConsumerDeleteSuccess() {
        if (consumerDeleteSuccess != null) {
            consumerDeleteSuccess.increment();
        }
    }

    public void incrementConsumerDeleteFailure() {
        if (consumerDeleteFailure != null) {
            consumerDeleteFailure.increment();
        }
    }

    public void incrementConsumerErrors() {
        if (consumerErrors != null) {
            consumerErrors.increment();
        }
    }

    public void incrementRetryCount() {
        if (retryCount != null) {
            retryCount.increment();
        }
    }

    public void incrementProducerPublishSuccess() {
        if (producerPublishSuccess != null) {
            producerPublishSuccess.increment();
        }
    }

    public void incrementProducerPublishFailure() {
        if (producerPublishFailure != null) {
            producerPublishFailure.increment();
        }
    }

    public void incrementDlqPublishes() {
        if (dlqPublishes != null) {
            dlqPublishes.increment();
        }
    }

    public Timer getConsumerLatencyTimer() {
        return consumerLatencyTimer;
    }

    public Timer.Sample startTimer() {
        return Timer.start(MetricUtils.getMeterRegistry());
    }

    public void recordLatency(Timer.Sample sample) {
        if (sample != null && consumerLatencyTimer != null) {
            sample.stop(consumerLatencyTimer);
        }
    }
}
