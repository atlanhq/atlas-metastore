/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.tasks;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.RequestContext;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskQueueWatcherV2 implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcherV2.class);
    private static final TaskExecutor.TaskLogger TASK_LOG = TaskExecutor.TaskLogger.getLogger();
    private final MetricsRegistry metricRegistry;

    private TaskRegistry registry;
    private final ExecutorService executorService;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;
    private static final String KAFKA_TOPIC_CREATION_LOCK = "atlas:kafka:topic:lock";
    private final RedisService redisService;

    private static long pollInterval = AtlasConfiguration.TASKS_REQUEUE_POLL_INTERVAL.getLong();
    private static final String ATLAS_TASK_LOCK = "atlas:task:lock";
    private final KafkaNotification kafkaNotification;

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public TaskQueueWatcherV2(ExecutorService executorService, TaskRegistry registry,
                              Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                              RedisService redisService, MetricsRegistry metricsRegistry, KafkaNotification kafkaNotification) {

        this.registry = registry;
        this.executorService = executorService;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics = statistics;
        this.redisService = redisService;
        this.metricRegistry = metricsRegistry;
        this.kafkaNotification = kafkaNotification;
    }

    public void shutdown() {
        shouldRun.set(false);
        LOG.info("TaskQueueWatcherV2: Shutdown");
    }

    @Override
    public void run() {
        boolean isMaintenanceMode = AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
        if (isMaintenanceMode) {
            LOG.info("TaskQueueWatcherV2: Maintenance mode is enabled, new tasks will not be loaded into the queue until next restart");
            return;
        }
        shouldRun.set(true);

        if (LOG.isDebugEnabled()) {
            LOG.debug("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
        }

        ensureKafkaTopicExists();

        while (shouldRun.get()) {
            RequestContext requestContext = RequestContext.get();
            requestContext.setMetricRegistry(this.metricRegistry);
            TasksFetcher fetcher = new TasksFetcher(registry);
            try {
                if (!redisService.acquireDistributedLock(ATLAS_TASK_LOCK)) {
                    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);
                    continue;
                }
                LOG.info("TaskQueueWatcher: Acquired distributed lock: {}", ATLAS_TASK_LOCK);
                List<AtlasTask> inProgressTasks = registry.getInProgressTasksES();
                if(inProgressTasks.isEmpty()){
                    List<AtlasTask> tasks = fetcher.getTasks();
                    if (CollectionUtils.isNotEmpty(tasks)) {
                        submitAll(Collections.singletonList(tasks.get(0)));
                    } else {
                        redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
                    }
                }
                Thread.sleep(pollInterval);
            } catch (InterruptedException interruptedException) {
                LOG.error("TaskQueueWatcher: Interrupted: thread is terminated, new tasks will not be loaded into the queue until next restart");
                break;
            } catch (Exception e) {
                LOG.error("TaskQueueWatcher: Exception occurred " + e.getMessage(), e);
            } finally {
                redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
                fetcher.clearTasks();
            }
        }
    }

    private void ensureKafkaTopicExists() {
        boolean lockAcquired = false;
        try {
            lockAcquired = redisService.acquireDistributedLock(KAFKA_TOPIC_CREATION_LOCK);
            if (lockAcquired) {
                LOG.info("Checking Kafka topic for tag propagation");
                String topicName = AtlasConfiguration.NOTIFICATION_OBJ_PROPAGATION_TOPIC_NAME.getString();

                // Get partition count from configuration or default to 10
                int partitionCount = AtlasConfiguration.NOTIFICATION_OBJ_PROPAGATION_TOPIC_PARTITIONS.getInt();

                if (!kafkaNotification.isKafkaTopicExists(topicName)) {
                    LOG.info("Creating Kafka topic: {} with {} partitions", topicName, partitionCount);
                    kafkaNotification.createKafkaTopic(topicName, partitionCount);
                    LOG.info("Successfully created Kafka topic: {}", topicName);
                } else {
                    LOG.info("Kafka topic: {} already exists", topicName);
                }
            } else {
                LOG.info("Another instance is already checking Kafka topic existence");
            }
        } catch (Exception e) {
            LOG.error("Error checking/creating Kafka topic for tag propagation", e);
        } finally {
            if (lockAcquired) {
                try {
                    redisService.releaseDistributedLock(KAFKA_TOPIC_CREATION_LOCK);
                } catch (Exception e) {
                    LOG.error("Error releasing Kafka topic creation lock", e);
                }
            }
        }
    }

    private void waitForTasksToComplete(final CountDownLatch latch) throws InterruptedException {
        if (latch.getCount() != 0) {
            LOG.info("TaskQueueWatcher: Waiting on Latch, current count: {}", latch.getCount());
            latch.await();
            LOG.info("TaskQueueWatcher: Waiting completed on Latch, current count: {}", latch.getCount());
        }
    }

    private void submitAll(List<AtlasTask> tasks) {
        if (CollectionUtils.isNotEmpty(tasks)) {

            for (AtlasTask task : tasks) {
                if (task != null) {
                    TASK_LOG.log(task);
                }
                if (AtlasTask.Status.IN_PROGRESS.equals(task.getStatus())) {
                    LOG.info("TaskQueueWatcherV2: Task {} is in IN_PROGRESS state, will not be submitted to the queue again, stopping queue", task.getGuid());
                    break;
                }
                this.executorService.submit(new TaskExecutor.TaskConsumerV2(task, this.registry, this.taskTypeFactoryMap, this.statistics));
            }

            LOG.info("TasksFetcher: Submitted {} tasks to the queue", tasks.size());
        } else {
            LOG.info("TasksFetcher: No task to queue");
        }
    }

    static class TasksFetcher {
        private TaskRegistry registry;
        private List<AtlasTask> tasks = new ArrayList<>();

        public TasksFetcher(TaskRegistry registry) {
            this.registry = registry;
        }

        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("TasksFetcher: Fetching tasks for queuing");
            }

            this.tasks = registry.getTasksForReQueue();
            RequestContext requestContext = RequestContext.get();
            requestContext.clearCache();
        }

        public List<AtlasTask> getTasks() {
            run();
            return tasks;
        }

        public void clearTasks() {
            this.tasks.clear();
        }
    }

    @PreDestroy
    public void cleanUp() {
        if (!Objects.isNull(this.executorService)) {
            this.redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
            this.executorService.shutdownNow();
            try {
                this.executorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
