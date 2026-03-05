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
package org.apache.atlas.web.integration;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Classification (tag) CRUD operations.
 *
 * <p>Exercises classification typedef creation, entity-classification attachment,
 * retrieval, and removal using the in-process Atlas server.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClassificationIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(ClassificationIntegrationTest.class);

    private final long testId = System.currentTimeMillis();
    private final String classificationDisplayName1 = "IntTestClass1_" + testId;
    private final String classificationDisplayName2 = "IntTestClass2_" + testId;
    private String classificationName1;
    private String classificationName2;

    private boolean typedefsCreated = false;
    private String entityGuid;

    @Test
    @Order(1)
    void testCreateClassificationType() throws Exception {
        AtlasClassificationDef classDef1 = new AtlasClassificationDef();
        classDef1.setName(classificationDisplayName1);
        classDef1.setDisplayName(classificationDisplayName1);
        classDef1.setDescription("Test classification 1");
        classDef1.setServiceType("atlas_core");

        AtlasClassificationDef classDef2 = new AtlasClassificationDef();
        classDef2.setName(classificationDisplayName2);
        classDef2.setDisplayName(classificationDisplayName2);
        classDef2.setDescription("Test classification 2");
        classDef2.setServiceType("atlas_core");

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setClassificationDefs(Arrays.asList(classDef1, classDef2));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);

        assertNotNull(created);
        assertNotNull(created.getClassificationDefs());
        assertEquals(2, created.getClassificationDefs().size());

        classificationName1 = created.getClassificationDefs().stream()
                .filter(d -> classificationDisplayName1.equals(d.getDisplayName()))
                .map(AtlasClassificationDef::getName)
                .findFirst()
                .orElse(created.getClassificationDefs().get(0).getName());
        classificationName2 = created.getClassificationDefs().stream()
                .filter(d -> classificationDisplayName2.equals(d.getDisplayName()))
                .map(AtlasClassificationDef::getName)
                .findFirst()
                .orElse(created.getClassificationDefs().get(1).getName());
        LOG.info("Resolved classification names: {} -> {}, {} -> {}",
                classificationDisplayName1, classificationName1,
                classificationDisplayName2, classificationName2);

        // Wait a moment for type registry to be updated
        Thread.sleep(2000);

        // Verify the typedef can be retrieved
        atlasClient.getClassificationDefByName(classificationName1);
        typedefsCreated = true;
        LOG.info("Created and verified 2 classification typedefs");
    }

    @Test
    @Order(2)
    void testGetClassificationType() throws AtlasServiceException {
        assertTrue(typedefsCreated, "Classification typedefs not available");

        AtlasClassificationDef classDef = atlasClient.getClassificationDefByName(classificationName1);

        assertNotNull(classDef);
        assertEquals(classificationName1, classDef.getName());

        LOG.info("Fetched classification def: {}", classDef.getName());
    }

    @Test
    @Order(3)
    void testCreateEntityWithClassification() throws AtlasServiceException {
        assertTrue(typedefsCreated, "Classification typedefs not available");

        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "classified-table-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/classification/table/" + testId);
        entity.setClassifications(Collections.singletonList(
                new AtlasClassification(classificationName1)));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        entityGuid = created.getGuid();
        LOG.info("Created entity with classification: guid={}", entityGuid);
    }

    @Test
    @Order(4)
    void testGetClassificationsOnEntity() throws AtlasServiceException {
        assertNotNull(entityGuid, "Entity not created");

        AtlasClassifications classifications = atlasClient.getClassifications(entityGuid);

        assertNotNull(classifications);
        assertNotNull(classifications.getList());
        assertEquals(1, classifications.getList().size());
        assertEquals(classificationName1, classifications.getList().get(0).getTypeName());

        LOG.info("Entity has {} classifications", classifications.getList().size());
    }

    @Test
    @Order(5)
    void testAddClassificationToExistingEntity() throws AtlasServiceException {
        assertNotNull(entityGuid, "Entity not created");

        atlasClient.addClassifications(entityGuid,
                Collections.singletonList(new AtlasClassification(classificationName2)));

        LOG.info("Added second classification to entity: guid={}", entityGuid);
    }

    @Test
    @Order(6)
    void testGetEntityWithMultipleClassifications() throws AtlasServiceException {
        assertNotNull(entityGuid, "Entity not created");

        AtlasClassifications classifications = atlasClient.getClassifications(entityGuid);

        assertNotNull(classifications);
        assertNotNull(classifications.getList());
        assertEquals(2, classifications.getList().size());

        LOG.info("Entity has {} classifications after addition", classifications.getList().size());
    }

    @Test
    @Order(7)
    void testRemoveClassification() throws AtlasServiceException {
        assertNotNull(entityGuid, "Entity not created");

        atlasClient.deleteClassification(entityGuid, classificationName2);

        LOG.info("Removed classification {} from entity", classificationName2);
    }

    @Test
    @Order(8)
    void testVerifyClassificationRemoved() throws AtlasServiceException {
        assertNotNull(entityGuid, "Entity not created");

        AtlasClassifications classifications = atlasClient.getClassifications(entityGuid);

        assertNotNull(classifications);
        assertNotNull(classifications.getList());
        assertEquals(1, classifications.getList().size());
        assertEquals(classificationName1, classifications.getList().get(0).getTypeName());

        LOG.info("Verified only 1 classification remains after removal");
    }

    @Test
    @Order(9)
    void testDeleteClassificationType() throws AtlasServiceException {
        assertTrue(typedefsCreated, "Classification typedefs not available");

        // First remove test classifications from entity (best-effort), then wait for detach visibility.
        if (entityGuid != null) {
            try {
                atlasClient.deleteClassification(entityGuid, classificationName2);
            } catch (AtlasServiceException e) {
                LOG.debug("Classification {} already removed or missing: {}", classificationName2, e.getMessage());
            }
            try {
                atlasClient.deleteClassification(entityGuid, classificationName1);
            } catch (AtlasServiceException e) {
                LOG.debug("Classification {} already removed or missing: {}", classificationName1, e.getMessage());
            }
        }

        for (int i = 0; i < 5; i++) {
            if (entityGuid == null) {
                break;
            }
            AtlasClassifications remaining = atlasClient.getClassifications(entityGuid);
            boolean hasClass2 = remaining != null && remaining.getList() != null
                    && remaining.getList().stream().anyMatch(c -> classificationName2.equals(c.getTypeName()));
            if (!hasClass2) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for classification detach");
            }
        }

        AtlasTypesDef typesDef = new AtlasTypesDef();
        AtlasClassificationDef classDef = new AtlasClassificationDef();
        classDef.setName(classificationName2);
        typesDef.setClassificationDefs(Collections.singletonList(classDef));

        AtlasServiceException lastConflict = null;
        for (int i = 0; i < 5; i++) {
            try {
                atlasClient.deleteAtlasTypeDefs(typesDef);
                lastConflict = null;
                break;
            } catch (AtlasServiceException e) {
                String msg = e.getMessage();
                if (msg != null && msg.contains("has references")) {
                    lastConflict = e;
                    LOG.warn("Retrying typedef delete for {} due to references still present (attempt {}/5)",
                            classificationName2, i + 1);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        fail("Interrupted while retrying typedef delete");
                    }
                    continue;
                }
                throw e;
            }
        }
        if (lastConflict != null) {
            throw lastConflict;
        }

        LOG.info("Deleted classification typedef: {}", classificationName2);
    }

    @Test
    @Order(10)
    void testAddNonExistentClassification() {
        assertNotNull(entityGuid, "Entity not created");

        assertThrows(AtlasServiceException.class,
                () -> atlasClient.addClassifications(entityGuid,
                        Collections.singletonList(new AtlasClassification("NonExistentClassification_" + testId))),
                "Adding non-existent classification should throw");

        LOG.info("Correctly rejected non-existent classification");
    }
}
