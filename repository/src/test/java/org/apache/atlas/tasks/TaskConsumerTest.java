package org.apache.atlas.tasks;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TaskExecutor.TaskConsumer early-exit guards (PR #6515).
 *
 * Covers:
 * 1. Vertex not found → task marked FAILED, no further processing
 * 2. Task already COMPLETE → skipped, no further processing
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskConsumerTest {

    @Mock private TaskRegistry registry;
    @Mock private TaskManagement.Statistics statistics;

    private AutoCloseable closeable;

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get();
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    /**
     * When registry.getVertex() returns null (vertex deleted from graph),
     * the task should be marked FAILED with MAX_ATTEMPT_COUNT and return
     * without calling performTask.
     */
    @Test
    void run_vertexNotFound_marksTaskFailedAndReturns() {
        AtlasTask task = new AtlasTask("CLASSIFICATION_PROPAGATION_ADD", "testUser",
                Collections.emptyMap(), "classif-1", "tagType", "entity-1");
        task.setStatus(AtlasTask.Status.PENDING);

        when(registry.getVertex(task.getGuid())).thenReturn(null);

        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                task, registry, Collections.emptyMap(), statistics, latch);

        consumer.run();

        assertEquals(AtlasTask.Status.FAILED, task.getStatus());
        assertEquals(3, task.getAttemptCount());
        assertEquals("Task vertex not found in graph", task.getErrorMessage());
        // updateStatus should NOT be called since taskVertex is null
        verify(registry, never()).updateStatus(null, task);
        // latch should be counted down in finally block
        assertEquals(0, latch.getCount());
    }

    /**
     * When the task status is already COMPLETE, the consumer should skip
     * execution entirely — no status changes, no performTask call.
     */
    @Test
    void run_taskAlreadyComplete_skipsExecution() {
        AtlasTask task = new AtlasTask("CLASSIFICATION_PROPAGATION_ADD", "testUser",
                Collections.emptyMap(), "classif-1", "tagType", "entity-1");
        task.setStatus(AtlasTask.Status.COMPLETE);

        AtlasVertex taskVertex = mock(AtlasVertex.class);
        when(registry.getVertex(task.getGuid())).thenReturn(taskVertex);

        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                task, registry, Collections.emptyMap(), statistics, latch);

        consumer.run();

        // Status should remain COMPLETE — not changed to FAILED or anything else
        assertEquals(AtlasTask.Status.COMPLETE, task.getStatus());
        // statistics.increment should not be called (task was skipped before that point)
        verify(statistics, never()).increment(1);
        // latch should be counted down
        assertEquals(0, latch.getCount());
    }

    /**
     * When vertex exists and task is PENDING, the consumer should proceed
     * past the guards. Without a matching TaskFactory, it logs an error and
     * returns (no NPE). This verifies the guards don't block valid tasks.
     */
    @Test
    void run_validTask_proceedsPastGuards() {
        AtlasTask task = new AtlasTask("CLASSIFICATION_PROPAGATION_ADD", "testUser",
                Collections.emptyMap(), "classif-1", "tagType", "entity-1");
        task.setStatus(AtlasTask.Status.PENDING);
        task.setAttemptCount(0);

        AtlasVertex taskVertex = mock(AtlasVertex.class);
        when(registry.getVertex(task.getGuid())).thenReturn(taskVertex);

        CountDownLatch latch = new CountDownLatch(1);
        // Empty taskTypeFactoryMap → performTask will log "taskTypeFactoryMap does not contain..."
        // but won't NPE, proving guards were passed successfully
        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                task, registry, Collections.emptyMap(), statistics, latch);

        consumer.run();

        // statistics.increment(1) IS called for valid tasks that pass guards
        verify(statistics).increment(1);
        // registry.commit() is called in finally
        verify(registry).commit();
        assertEquals(0, latch.getCount());
    }
}
