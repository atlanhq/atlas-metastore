package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.notification.TaskNotification;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.notification.task.TaskNotificationSender;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassificationTaskNotificationTest {

    @Mock private AtlasGraph graph;
    @Mock private EntityGraphMapper entityGraphMapper;
    @Mock private DeleteHandlerDelegate deleteDelegate;
    @Mock private AtlasRelationshipStore relationshipStore;
    @Mock private TaskMetricsService taskMetricsService;
    @Mock private TaskNotificationSender taskNotificationSender;

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
    void setup() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        ApplicationProperties.set(new PropertiesConfiguration());
        RequestContext.clear();
        RequestContext.get();
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    @Test
    void testSuccessfulTask_sendsStartedAndCompletedNotifications() throws AtlasBaseException {
        AtlasTask taskDef = createTaskDef("CLASSIFICATION_PROPAGATION_ADD", "test-entity-guid", "PII");

        TestableTask task = new TestableTask(taskDef, false);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            AtlasTask.Status status = task.perform();

            assertEquals(AtlasTask.Status.COMPLETE, status);

            ArgumentCaptor<TaskNotification> captor = ArgumentCaptor.forClass(TaskNotification.class);
            verify(taskNotificationSender, times(2)).sendTaskEvent(captor.capture());

            TaskNotification startedNotif = captor.getAllValues().get(0);
            assertEquals(TaskNotification.Status.STARTED, startedNotif.getStatus());
            assertEquals("test-task-guid", startedNotif.getTaskId());
            assertEquals("CLASSIFICATION_PROPAGATION_ADD", startedNotif.getTaskType());
            assertEquals("test-entity-guid", startedNotif.getEntityGuid());
            assertEquals("PII", startedNotif.getClassificationName());

            TaskNotification completedNotif = captor.getAllValues().get(1);
            assertEquals(TaskNotification.Status.COMPLETED, completedNotif.getStatus());
            assertEquals("test-task-guid", completedNotif.getTaskId());
            assertEquals(5, completedNotif.getAssetsAffected());
            assertNull(completedNotif.getErrorMessage());
            assertTrue(completedNotif.getEndTime() > 0);
        }
    }

    @Test
    void testFailedTask_sendsStartedAndFailedNotifications() throws AtlasBaseException {
        AtlasTask taskDef = createTaskDef("CLASSIFICATION_PROPAGATION_DELETE", "test-entity-guid", "Confidential");

        TestableTask task = new TestableTask(taskDef, true);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            assertThrows(AtlasBaseException.class, task::perform);

            ArgumentCaptor<TaskNotification> captor = ArgumentCaptor.forClass(TaskNotification.class);
            verify(taskNotificationSender, times(2)).sendTaskEvent(captor.capture());

            TaskNotification startedNotif = captor.getAllValues().get(0);
            assertEquals(TaskNotification.Status.STARTED, startedNotif.getStatus());

            TaskNotification failedNotif = captor.getAllValues().get(1);
            assertEquals(TaskNotification.Status.FAILED, failedNotif.getStatus());
            assertEquals("test-task-guid", failedNotif.getTaskId());
            assertEquals("CLASSIFICATION_PROPAGATION_DELETE", failedNotif.getTaskType());
            assertEquals("Simulated failure", failedNotif.getErrorMessage());
            assertEquals(0, failedNotif.getAssetsAffected());
        }
    }

    @Test
    void testNullSender_doesNotThrow() throws AtlasBaseException {
        AtlasTask taskDef = createTaskDef("CLASSIFICATION_PROPAGATION_ADD", "test-entity-guid", "PII");

        TestableTask task = new TestableTask(taskDef, false, null);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            AtlasTask.Status status = task.perform();
            assertEquals(AtlasTask.Status.COMPLETE, status);
        }
    }

    @Test
    void testNotificationPayloadContainsCorrectMetadata() throws AtlasBaseException {
        AtlasTask taskDef = createTaskDef("CLASSIFICATION_REFRESH_PROPAGATION", "entity-123", "Sensitive");

        TestableTask task = new TestableTask(taskDef, false);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            task.perform();

            ArgumentCaptor<TaskNotification> captor = ArgumentCaptor.forClass(TaskNotification.class);
            verify(taskNotificationSender, times(2)).sendTaskEvent(captor.capture());

            TaskNotification completedNotif = captor.getAllValues().get(1);
            assertEquals("entity-123", completedNotif.getEntityGuid());
            assertEquals("Sensitive", completedNotif.getClassificationName());
            assertEquals("CLASSIFICATION_REFRESH_PROPAGATION", completedNotif.getTaskType());
        }
    }

    private AtlasTask createTaskDef(String type, String entityGuid, String tagTypeName) {
        AtlasTask taskDef = new AtlasTask();
        taskDef.setGuid("test-task-guid");
        taskDef.setType(type);
        taskDef.setCreatedBy("admin");
        taskDef.setEntityGuid(entityGuid);
        taskDef.setTagTypeName(tagTypeName);

        Map<String, Object> params = new HashMap<>();
        params.put("entityGuid", entityGuid);
        taskDef.setParameters(params);

        return taskDef;
    }

    private class TestableTask extends ClassificationTask {
        private final boolean shouldFail;

        TestableTask(AtlasTask task, boolean shouldFail) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore,
                    taskMetricsService, taskNotificationSender);
            this.shouldFail = shouldFail;
        }

        TestableTask(AtlasTask task, boolean shouldFail, TaskNotificationSender sender) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore,
                    taskMetricsService, sender);
            this.shouldFail = shouldFail;
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            if (shouldFail) {
                throw new AtlasBaseException("Simulated failure");
            }
            context.incrementAssetsAffected(5);
        }
    }
}
