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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ICuratorFactory;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.Service;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.repository.Constants.TASK_GUID;

@Component
@Order(7)
public class TaskManagement implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagement.class);
    private final MetricsRegistry metricRegistry;

    private       TaskExecutor              taskExecutor;
    private final Configuration             configuration;
    private final TaskRegistry              registry;
    private final Statistics                statistics;
    private final Map<String, TaskFactory>  taskTypeFactoryMap;
    private final ICuratorFactory curatorFactory;
    private final RedisService redisService;
    private final KafkaNotification kafkaNotification;

    private Thread watcherThread = null;
    private Thread watcherThreadV2 = null;
    private Thread updaterThread = null;

    public enum DeleteType {
        SOFT,
        HARD
    }

    @Inject
    public TaskManagement(Configuration configuration, TaskRegistry taskRegistry, ICuratorFactory curatorFactory, RedisService redisService, MetricsRegistry metricsRegistry, KafkaNotification kafkaNotification) {
        this.configuration      = configuration;
        this.registry           = taskRegistry;
        this.redisService       = redisService;
        this.kafkaNotification = kafkaNotification;
        this.statistics         = new Statistics();
        this.taskTypeFactoryMap = new HashMap<>();
        this.curatorFactory = curatorFactory;
        this.metricRegistry = metricsRegistry;
    }

    @VisibleForTesting
    TaskManagement(Configuration configuration, TaskRegistry taskRegistry, TaskFactory taskFactory, ICuratorFactory curatorFactory, RedisService redisService, KafkaNotification kafkaNotification) {
        this.configuration      = configuration;
        this.registry           = taskRegistry;
        this.kafkaNotification = kafkaNotification;
        this.metricRegistry = null;
        this.redisService       = redisService;
        this.statistics         = new Statistics();
        this.taskTypeFactoryMap = new HashMap<>();
        this.curatorFactory = curatorFactory;

        createTaskTypeFactoryMap(taskTypeFactoryMap, taskFactory);
    }

    /**
     * Updates a property of the current task's vertex in the Atlas graph.
     *
     * This method retrieves the current task from the request context, fetches its corresponding
     * vertex from the Atlas graph, and updates the specified property.
     *
     * Edge cases handled:
     * - Ensures `currentTask` is not null before accessing its properties.
     * - Checks if the vertex exists before attempting updates.
     * - Handles `null` or unexpected `propertyKey` values safely.
     *
     * @param propertyKey The key of the property to update.
     * @param graph The AtlasGraph instance used for querying and updating.
     * @param value The value to set for the specified property.
     * @throws IllegalStateException if no matching vertex is found.
     * @throws IllegalArgumentException if an invalid propertyKey is provided.
     */
    public void updateTaskVertexProperty(String propertyKey, AtlasGraph graph, long value) {
        // Ensure current task exists
        AtlasTask currentTask = RequestContext.get().getCurrentTask();
        if (currentTask == null) {
            throw new IllegalStateException("No current task found in request context.");
        }

        // Get the corresponding vertex
        Iterator vertexIterator = graph.query()
                .has(TASK_GUID, currentTask.getGuid())
                .vertices().iterator();

        if (!vertexIterator.hasNext()) {
            throw new IllegalStateException("No vertex found for task GUID: " + currentTask.getGuid());
        }

        AtlasVertex currentTaskVertex = (AtlasVertex) vertexIterator.next();

        // Ensure propertyKey is valid and update the corresponding task property
        if (Constants.TASK_ASSET_COUNT_TO_PROPAGATE.equals(propertyKey)) {
            if (propertyKey != null) {
                currentTask.setAssetsCountToPropagate(value);
                currentTaskVertex.setProperty(propertyKey, currentTask.getAssetsCountToPropagate());
            } else if (propertyKey != null && Constants.TASK_ASSET_COUNT_PROPAGATED.equals(propertyKey)) {
                currentTask.setAssetsCountPropagated(currentTask.getAssetsCountPropagated() + value);
                currentTaskVertex.setProperty(propertyKey, currentTask.getAssetsCountPropagated());
            } else {
                throw new IllegalArgumentException("Unexpected or null propertyKey: " + propertyKey);
            }
        } else if (propertyKey != null && Constants.TASK_ASSET_COUNT_PROPAGATED.equals(propertyKey)) {
            currentTask.setAssetsCountPropagated(currentTask.getAssetsCountPropagated() + value);
            currentTaskVertex.setProperty(propertyKey, currentTask.getAssetsCountPropagated());
        } else {
            throw new IllegalArgumentException("Unexpected or null propertyKey: " + propertyKey);
        }
    }

    @Override
    public void start() throws AtlasException {
        try {
            if (configuration == null || !HAConfiguration.isHAEnabled(configuration)) {
                startInternal();
            } else {
                LOG.info("TaskManagement.start(): deferring until instance activation");
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public boolean isWatcherActive() {
        return watcherThread != null;
    }
    public boolean isTaskUpdatorActive() {
        return updaterThread != null;
    }

    @Override
    public void stop() throws AtlasException {
        stopQueueWatcher();
        stopTaskUpdater();
        LOG.info("TaskManagement: Stopped!");
    }

    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("==> TaskManagement.instanceIsActive()");

        try {
            startInternal();
        } catch (Exception e) {
            throw e;
        }

        LOG.info("<== TaskManagement.instanceIsActive()");
    }

    @Override
    public void instanceIsPassive() throws AtlasException {
        stopQueueWatcher();
        stopTaskUpdater();
        LOG.info("TaskManagement.instanceIsPassive(): no action needed");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.TASK_MANAGEMENT.getOrder();
    }

    public void addFactory(TaskFactory taskFactory) {
        createTaskTypeFactoryMap(this.taskTypeFactoryMap, taskFactory);
    }

    public AtlasTask createTask(String taskType, String createdBy, Map<String, Object> parameters) {
        return this.registry.createVertex(taskType, createdBy, parameters, null, null);
    }

    public AtlasTask createTask(String taskType, String createdBy, Map<String, Object> parameters, String classificationId, String entityGuid) {
        return this.registry.createVertex(taskType, createdBy, parameters, classificationId, entityGuid);
    }

    public AtlasTask createTask(String taskType, String createdBy, Map<String, Object> parameters, String classificationId, String classificationName, String entityGuid) {
        return this.registry.createVertex(taskType, createdBy, parameters, classificationId, classificationName, entityGuid);
    }

    public List<AtlasTask> getAll() {
        return this.registry.getAll();
    }

    public List<AtlasTask> getAll(List<String> statusList, int offset, int limit) {
        return this.registry.getAll(statusList, offset, limit);
    }

    public List<AtlasTask> getQueuedTasks() {
        return this.registry.getTasksForReQueue();
    }

    public void retryTasks(List<String> taskGuids) throws AtlasBaseException {
        List<AtlasTask> taskToRetry = new ArrayList<>();
        for (String taskGuid : taskGuids) {
            AtlasTask task = getByGuid(taskGuid);

            if (task != null &&
                    (task.getStatus().equals(AtlasTask.Status.FAILED) || task.getStatus().equals(AtlasTask.Status.IN_PROGRESS))) {
                /* Allowing IN_PROGRESS task retry for following scenario
                -> Started a task
                -> before task gets completed Cassandra gets completely down
                -> task is still in IN_PROGRESS state & as Cassandra write is not possible it will never change
                -> Once cassandra is up & Atlas started communicating to Cassandra again such task may be retried
                */
                taskToRetry.add(task);
            }
        }

        if (CollectionUtils.isNotEmpty(taskToRetry)) {
            //addAll(taskToRetry);
        }
    }

    public AtlasTask getByGuid(String guid) throws AtlasBaseException {
        try {
            return this.registry.getById(guid);
        } catch (Exception exception) {
            LOG.error("Error: getByGuid: {}", guid);

            throw new AtlasBaseException(exception);
        }
    }

    public List<AtlasTask> getByGuids(List<String> guids) throws AtlasBaseException {
        List<AtlasTask> ret = new ArrayList<>();

        for (String guid : guids) {
            AtlasTask task = getByGuid(guid);

            if (task != null) {
                ret.add(task);
            }
        }

        return ret;
    }

    public List<AtlasTask> getByGuidsES(List<String> guids) throws AtlasBaseException {
        return registry.getByIdsES(guids);
    }

    public List<AtlasTask> getInProgressTasks() {
        if(AtlasConfiguration.TASKS_IN_PROGRESS_GRAPH_QUERY.getBoolean()) {
            return registry.getInProgressTasks();
        } else {
            return registry.getInProgressTasksES();
        }
    }

    public void deleteByGuid(String guid) throws AtlasBaseException {
        try {
            this.registry.deleteByGuid(guid);
        } catch (Exception exception) {
            throw new AtlasBaseException(exception);
        }
    }

    public void deleteByGuid(String guid, DeleteType deleteType) throws AtlasBaseException {
        try {
            if (deleteType == DeleteType.SOFT) {
                this.registry.softDelete(guid);
            }
            else {
                this.registry.deleteByGuid(guid);
            }
        } catch (Exception exception) {
            throw new AtlasBaseException(exception);
        }
    }

    public void deleteByGuids(List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            return;
        }

        for (String guid : guids) {
            this.registry.deleteByGuid(guid);
        }
    }

    private synchronized void startWatcherThread() {

        if (this.taskExecutor == null) {
            final boolean isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(configuration);
            final String zkRoot = HAConfiguration.getZookeeperProperties(configuration).getZkRoot();
            this.taskExecutor = new TaskExecutor(registry, taskTypeFactoryMap, statistics, curatorFactory, redisService, zkRoot,isActiveActiveHAEnabled, metricRegistry);
        }

        if (watcherThread == null) {
            watcherThread = this.taskExecutor.startWatcherThread();
        }

        this.statistics.print();
    }

    private synchronized void startUpdaterThread() {
        if (updaterThread == null) {
            updaterThread = this.taskExecutor.startUpdaterThread();
        }
    }

    private void startInternal() {
        if (AtlasConfiguration.TASKS_USE_ENABLED.getBoolean() == false) {
            return;
        }

        LOG.info("TaskManagement: Started!");
        if (this.taskTypeFactoryMap.size() == 0) {
            LOG.warn("Not factories registered! Pending tasks will be queued once factories are registered!");
            return;
        }

        try {
            if (AtlasConfiguration.ATLAS_DISTRIBUTED_TASK_MANAGEMENT_ENABLED.getBoolean()) {
                LOG.info("Checking Kafka topic for tag propagation!");
                String topicName = AtlasConfiguration.NOTIFICATION_OBJ_PROPAGATION_TOPIC_NAME.getString();
                if (!kafkaNotification.isKafkaTopicExists(topicName)) {
                    kafkaNotification.createKafkaTopic(topicName, 10);
                }
                LOG.info("TaskManagement: Distributed task management is enabled. Starting Kafka based task management!");
                startUpdaterThread();
                startWatcherThreadV2();
            } else {
                LOG.info("TaskManagement: Distributed task management is disabled. Starting in-mem task management!");
                startWatcherThread();
            }

        } catch (Exception e) {
            LOG.error("TaskManagement: Error while re queue tasks");
            e.printStackTrace();
        }
    }

    private void startWatcherThreadV2() {
        if (this.taskExecutor == null) {
            final boolean isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(configuration);
            final String zkRoot = HAConfiguration.getZookeeperProperties(configuration).getZkRoot();
            this.taskExecutor = new TaskExecutor(registry, taskTypeFactoryMap, statistics, curatorFactory, redisService, zkRoot,isActiveActiveHAEnabled, metricRegistry);
        }

        if (watcherThreadV2 == null) {
            watcherThreadV2 = this.taskExecutor.startWatcherThreadV2();
        }

        this.statistics.print();
    }

    @VisibleForTesting
    static Map<String, TaskFactory> createTaskTypeFactoryMap(Map<String, TaskFactory> taskTypeFactoryMap, TaskFactory factory) {
        List<String> supportedTypes = factory.getSupportedTypes();

        if (CollectionUtils.isEmpty(supportedTypes)) {
            LOG.warn("{}: Supported types returned empty!", factory.getClass());

            return taskTypeFactoryMap;
        }

        for (String type : supportedTypes) {
            taskTypeFactoryMap.put(type, factory);
        }

        return taskTypeFactoryMap;
    }

    private void stopQueueWatcher() {
        taskExecutor.stopQueueWatcher();
        watcherThread = null;
    }

    private void stopTaskUpdater() {
        taskExecutor.stopUpdaterThread();
        updaterThread = null;
    }

    static class Statistics {
        private static final TaskExecutor.TaskLogger logger = TaskExecutor.TaskLogger.getLogger();
        private static final long REPORT_FREQUENCY = 30000L;

        private final AtomicInteger total               = new AtomicInteger(0);
        private final AtomicInteger countSinceLastCheck = new AtomicInteger(0);
        private final AtomicInteger totalWithErrors     = new AtomicInteger(0);
        private final AtomicInteger totalSucceed        = new AtomicInteger(0);
        private       long          lastCheckTime       = System.currentTimeMillis();

        public void error() {
            this.countSinceLastCheck.incrementAndGet();
            this.totalWithErrors.incrementAndGet();
        }

        public void success() {
            this.countSinceLastCheck.incrementAndGet();
            this.totalSucceed.incrementAndGet();
        }

        public void increment() {
            increment(1);
        }

        public void increment(int delta) {
            this.total.addAndGet(delta);
            this.countSinceLastCheck.addAndGet(delta);
        }

        public void print() {
            long now = System.currentTimeMillis();
            long diff = now - this.lastCheckTime;

            if (diff < REPORT_FREQUENCY) {
                return;
            }

            logger.info(String.format("TaskManagement: Processing stats: total=%d, sinceLastStatsReport=%d completedWithErrors=%d, succeded=%d",
                                       this.total.get(), this.countSinceLastCheck.getAndSet(0),
                                       this.totalWithErrors.get(), this.totalSucceed.get()));
            this.lastCheckTime = now;
        }

        public void successPrint() {
            success();
            print();
        }

        @VisibleForTesting
        int getTotal() {
            return this.total.get();
        }

        @VisibleForTesting
        int getTotalSuccess() {
            return this.totalSucceed.get();
        }

        @VisibleForTesting
        int getTotalError() {
            return this.totalWithErrors.get();
        }
    }
}