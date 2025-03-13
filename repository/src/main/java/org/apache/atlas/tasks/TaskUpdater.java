package org.apache.atlas.tasks;

import com.esotericsoftware.minlog.Log;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.service.redis.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;


public class TaskUpdater implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskUpdater.class);

    private final TaskRegistry registry;
    private final RedisService redisService;

    public TaskUpdater(TaskRegistry registry, RedisService redisService) {
        this.registry = registry;
        this.redisService = redisService;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<AtlasTask> inProgressTasks = registry.getInProgressTasks();
                Log.debug("TaskUpdater: Found {} in-progress tasks to update", String.valueOf(inProgressTasks.size()));
                // I want to do the following for each task in inProgressTasks
                // 1. fetch the same task from redis
                // 2. fetch assetsCountpropagated for the task from redis
                // 3. update the task with the fetched assetsCountpropagated
                // 4. fetch assetsCountFailed for the task from redis
                // 5. update the task with the fetched assetsCountFailed
                // 6. if assetsCountFailed > 0 mark task as failed
                // 7. if assetsCountpropagated == assetsCountToPropagate mark task as completed
                // 8. for each subtask fetch lastupdated key from redis 'HSET task:123e4567-e89b-12d3-a456-426614174000 last_updated 1709827400'
                // 9. if current time - lastupdated > 10 mins mark task as failed
                for (AtlasTask task : inProgressTasks) {
                    String taskGuid = task.getGuid();
                    int successTaskValue = redisService.getSetSize("task:" + taskGuid + ":success");
                    int failedTaskValue = redisService.getSetSize("task:" + taskGuid + ":failed");
                    task.setAssetsCountPropagated((long) successTaskValue);
                    task.setAssetsFailedToPropagate((long) failedTaskValue);
                    if (task.getAssetsFailedToPropagate() > 0) {
                        task.setStatus(AtlasTask.Status.FAILED);
                    } else if (Objects.equals(task.getAssetsCountPropagated(), task.getAssetsCountToPropagate())) {
                        task.setStatus(AtlasTask.Status.COMPLETE);
                    } else {
                        // Check if task is stuck
                        String lastUpdated = redisService.getHashValue(taskGuid, "last_updated");
                        if (Objects.nonNull(lastUpdated)) {
                            long lastUpdatedTime = Long.parseLong(lastUpdated);
                            long currentTime = System.currentTimeMillis();
                            if (currentTime - lastUpdatedTime > 600000) {
                                task.setStatus(AtlasTask.Status.FAILED);
                            }
                        }
                    }
                }
                Thread.sleep(10000); // Sleep for 10 secs before next check
            } catch (InterruptedException e) {
                LOG.error("TaskUpdater thread interrupted", e);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Error in TaskUpdater", e);
            }
        }
    }
}