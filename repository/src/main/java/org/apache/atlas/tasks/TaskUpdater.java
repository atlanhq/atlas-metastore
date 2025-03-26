package org.apache.atlas.tasks;

import com.esotericsoftware.minlog.Log;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.repository.Constants.EDGE_PENDING_TASKS_PROPERTY_KEY;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_ENTITY_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_RELATIONSHIP_EDGE_ID;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;


public class TaskUpdater implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskUpdater.class);

    private static final long TASK_STUCK_THRESHOLD = 600000L; // 10 mins
    private static final long TASK_UPDATER_THREAD_WAIT_TIME = 60000L; // 1 min
    private final TaskRegistry registry;
    private final RedisService redisService;
    private final AtlasGraph graph;
    private final GraphHelper graphHelper;
    private final TransactionInterceptHelper transactionInterceptHelper;
    private static final String ATLAS_TASK_UPDATER_LOCK = "atlas:task:updater:lock";

    public TaskUpdater(TaskRegistry registry, RedisService redisService, AtlasGraph graph, TransactionInterceptHelper transactionInterceptHelper) {
        this.registry = registry;
        this.redisService = redisService;
        this.graph = graph;
        this.transactionInterceptHelper = transactionInterceptHelper;
        graphHelper = new GraphHelper(graph);
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (!redisService.acquireDistributedLock(ATLAS_TASK_UPDATER_LOCK)) {
                    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);
                    continue;
                }
                LOG.info("TaskUpdater: Acquired distributed lock: {}", ATLAS_TASK_UPDATER_LOCK);

                List<AtlasTask> inProgressTasks = registry.getInProgressTasksES();
                Log.debug("TaskUpdater: Found {} in-progress tasks to update", String.valueOf(inProgressTasks.size()));

                if (CollectionUtils.isEmpty(inProgressTasks)) {
                    redisService.releaseDistributedLock(ATLAS_TASK_UPDATER_LOCK);
                } else {
                    for (AtlasTask task : inProgressTasks) {
                        String taskGuid = task.getGuid();
                        int successTaskValue = redisService.getSetSize("task:" + taskGuid + ":success");
                        int failedTaskValue = redisService.getSetSize("task:" + taskGuid + ":failed");
                        task.setAssetsCountPropagated((long) successTaskValue);
                        task.setAssetsFailedToPropagate((long) failedTaskValue);
                        LOG.info("TaskUpdater-> task currently when in taskUpdater -> {}", task);
                        // Check if the task is complete or failed
                        if (task.getAssetsCountPropagated() + task.getAssetsFailedToPropagate() == task.getAssetsCountToPropagate()) {
                            markTaskAsCompleted(task);
                        } else {
                            // Check if task is stuck
                            handleForTaskExpiryTime(task, taskGuid);
                        }
                        saveTaskVertex(task);
                    }
                }
                Thread.sleep(TASK_UPDATER_THREAD_WAIT_TIME); // Sleep for 1 min before next check
            } catch (InterruptedException e) {
                LOG.error("TaskUpdater thread interrupted", e);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Error in TaskUpdater", e);
            } finally {
                redisService.releaseDistributedLock(ATLAS_TASK_UPDATER_LOCK);
            }
        }
    }

    private void handleForTaskExpiryTime(AtlasTask task, String taskGuid) {
        String lastUpdated = redisService.getHashValue(taskGuid, "last_updated");
        if (Objects.nonNull(lastUpdated)) {
            long lastUpdatedTime = Long.parseLong(lastUpdated);
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastUpdatedTime > TASK_STUCK_THRESHOLD) {
                task.setStatus(AtlasTask.Status.FAILED);
                LOG.info("TaskUpdater-> Task expired, status marked as FAILED for task {}", task.getGuid());
                expireRedisKeys(taskGuid);
            }
        }
    }

    private void markTaskAsCompleted(AtlasTask task) throws AtlasBaseException, EntityNotFoundException {
        if (task.getAssetsFailedToPropagate() > 0) {
            LOG.info("TaskUpdater-> STATUS MARKED AS FAILED for task {}", task.getGuid());
            task.setStatus(AtlasTask.Status.FAILED);
        } else {
            LOG.info("TaskUpdater-> STATUS MARKED AS COMPLETE for task {}", task.getGuid());
            task.setStatus(AtlasTask.Status.COMPLETE);
        }

        if (CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE.equals(task.getType())) {
            this.removePendingTaskFromEdge((String) task.getParameters().get(PARAM_RELATIONSHIP_EDGE_ID), task.getGuid());
        } else {
            this.removePendingTaskFromEntity((String) task.getParameters().get(PARAM_ENTITY_GUID), task.getGuid());
        }

        // Expire the Redis keys
        expireRedisKeys(task.getGuid());
    }

    private void removePendingTaskFromEdge(String edgeId, String taskGuid) {
        if (StringUtils.isEmpty(edgeId) || StringUtils.isEmpty(taskGuid)) {
            return;
        }
        AtlasEdge edge = registry.getGraph().getEdge(edgeId);
        if (edge == null) {
            LOG.warn("Error fetching edge: {}", edgeId);
            return;
        }
        AtlasGraphUtilsV2.removeItemFromListProperty(edge, EDGE_PENDING_TASKS_PROPERTY_KEY, taskGuid);
    }

    private void removePendingTaskFromEntity(String entityGuid, String taskGuid) throws EntityNotFoundException {
        if (StringUtils.isEmpty(entityGuid) || StringUtils.isEmpty(taskGuid)) {
            return;
        }
        AtlasVertex entityVertex = graphHelper.getVertexForGUID(entityGuid);
        if (entityVertex == null) {
            LOG.warn("Error fetching vertex: {}", entityVertex);
            return;
        }
        entityVertex.removePropertyValue(PENDING_TASKS_PROPERTY_KEY, taskGuid);
    }

    private void expireRedisKeys(String taskGuid) {
        redisService.expireKey("task:" + taskGuid + ":success");
        redisService.expireKey("task:" + taskGuid + ":failed");
        redisService.expireHash(taskGuid);
    }

    private void saveTaskVertex(AtlasTask task) {
        // Get the vertex for this task
        AtlasVertex taskVertex = registry.getVertex(task.getGuid());
        if (taskVertex != null) {
            // Update the status and attempt count via the existing method
            registry.updateStatus(taskVertex, task);

            // Update the count attributes that aren't handled by updateStatus()
            setEncodedProperty(taskVertex, Constants.TASK_ASSET_COUNT_PROPAGATED, task.getAssetsCountPropagated());
            setEncodedProperty(taskVertex, Constants.TASK_ASSET_COUNT_FAILED, task.getAssetsFailedToPropagate());

            // If the task is complete, consider updating end time and time taken
            if (task.getStatus() == AtlasTask.Status.COMPLETE || task.getStatus() == AtlasTask.Status.FAILED) {
                if (task.getEndTime() == null) {
                    task.setEndTime(new Date());
                    setEncodedProperty(taskVertex, Constants.TASK_END_TIME, task.getEndTime().getTime());

                    if (task.getStartTime() != null) {
                        long timeTaken = TimeUnit.MILLISECONDS.toSeconds(
                                task.getEndTime().getTime() - task.getStartTime().getTime());
                        task.setTimeTakenInSeconds(timeTaken);
                        setEncodedProperty(taskVertex, Constants.TASK_TIME_TAKEN_IN_SECONDS, timeTaken);
                    }
                }
            }

            // Commit the changes
            transactionInterceptHelper.intercept();
            LOG.info("Updated task {} with status {} and counts (success: {}, failed: {})",
                    task.getGuid(), task.getStatus(), task.getAssetsCountPropagated(), task.getAssetsFailedToPropagate());
        } else {
            LOG.warn("Could not find vertex for task {}", task.getGuid());
        }
    }
}