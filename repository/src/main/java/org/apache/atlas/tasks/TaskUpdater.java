package org.apache.atlas.tasks;

import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.redis.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
                /*List<AtlasTask> inProgressTasks = registry.getInProgressTasks();
                for (AtlasTask task : inProgressTasks) {
                    AtlasVertex taskVertex = registry.getVertex(task.getGuid());
                    if (taskVertex != null) {
                        String redisData = redisService.getValue(task.getGuid());
                        if (redisData != null) {
                            registry.updateTaskVertex(taskVertex, redisData);
                            LOG.info("Updated task vertex for task: {}", task.getGuid());
                        }
                    }
                }*/
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