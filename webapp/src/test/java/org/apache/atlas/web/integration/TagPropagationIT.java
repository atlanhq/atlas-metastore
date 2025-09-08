package org.apache.atlas.web.integration;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.tasks.TaskRegistry;
import org.apache.atlas.web.rest.EntityREST;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TagPropagationIT extends BaseAtlasIT {
    @Autowired
    private TaskManagement taskManagement;

    @Autowired
    private TaskRegistry taskRegistry;

    @Autowired
    private EntityGraphMapper entityGraphMapper;

    @Autowired
    private EntityMutationService entityMutationService;

    @Autowired
    private EntityREST entityREST;

    private static final String CREATE_TABLE_JSON = """
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": -1,
                        "attributes": {
                            "qualifiedName": "table5",
                            "name": "table5",
                            "connectionQualifiedName": "default/snowflake/123456789",
                            "connectorName": "snowflake"
                        }
                    }
                ]
            }
            """;

    private AtlasVertex sourceEntity;
    private EntityStreamUtil.EntityCreationRequest entityRequest;

    @BeforeEach
    public void setupTest() throws Exception {
        clearRequestContext();

        // Create test entities using the REST API
        entityRequest = EntityStreamUtil.fromJson(CREATE_TABLE_JSON);
        BulkRequestContext context = new BulkRequestContext.Builder()
                .setReplaceClassifications(true)
                .setReplaceBusinessAttributes(true)
                .setOverwriteBusinessAttributes(true)
                .build();
        EntityMutationResponse mutationResponse = entityMutationService.createOrUpdate(entityRequest.toEntityStream(typeRegistry), context);
        
        // Update request with created entities
        entityRequest.updateWithMutationResponse(mutationResponse);

        // Verify entity was created
        assertNotNull(mutationResponse.getCreatedEntities());
        assertEquals(1, mutationResponse.getCreatedEntities().size());
        
        AtlasEntityHeader createdEntity = mutationResponse.getCreatedEntities().get(0);
        assertNotNull(createdEntity.getGuid());
        assertEquals("Table", createdEntity.getTypeName());
        assertEquals("table5", createdEntity.getAttribute("name"));

        // Store created vertex for later use
        sourceEntity = AtlasGraphUtilsV2.findByGuid(createdEntity.getGuid());
        assertNotNull(sourceEntity);
    }

    @Test
    public void testEntityCreation() {
        // Basic test to verify entity creation
        assertNotNull(sourceEntity);
        //assertEquals("table5", AtlasGraphUtilsV2.getProperty(sourceEntity, "name"));
        //assertEquals("default/snowflake/123456789", AtlasGraphUtilsV2.getProperty(sourceEntity, "connectionQualifiedName"));
    }

    private List<AtlasTask> waitForTasks(int expectedCount) throws InterruptedException {
        List<AtlasTask> tasks = null;
        int attempts = 0;
        int maxAttempts = 30;

        while (attempts < maxAttempts) {
            tasks = taskRegistry.getTasksForReQueue();
            
            if (tasks != null && tasks.size() >= expectedCount) {
                // Wait a bit more to ensure task processing is complete
                TimeUnit.SECONDS.sleep(2);
                break;
            }

            TimeUnit.SECONDS.sleep(1);
            attempts++;
        }

        if (tasks == null || tasks.size() < expectedCount) {
            throw new RuntimeException("Expected " + expectedCount + " tasks but found " + (tasks == null ? 0 : tasks.size()));
        }

        return tasks;
    }
} 