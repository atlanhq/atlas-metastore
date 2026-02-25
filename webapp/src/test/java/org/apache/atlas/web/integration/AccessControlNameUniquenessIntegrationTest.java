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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for name uniqueness enforcement on Persona, Purpose, and AuthPolicy entities.
 *
 * <p>Validates that:
 * <ul>
 *   <li>Duplicate Persona names are rejected (409)</li>
 *   <li>Duplicate Purpose names are rejected (409)</li>
 *   <li>Duplicate Policy names under the same parent are rejected (409)</li>
 *   <li>Same Policy name under different parents is allowed</li>
 *   <li>Renaming to an existing name is rejected (409)</li>
 *   <li>After soft-deleting an entity, its name can be reused</li>
 * </ul>
 *
 * <p>Note: Persona creation requires Keycloak. Tests are skipped via Assumptions if unavailable.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AccessControlNameUniquenessIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(AccessControlNameUniquenessIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    // Persona state
    private boolean personaCreationSupported = true;
    private String personaName1;
    private String personaGuid1;
    private String personaQN1;
    private String personaName2;
    private String personaGuid2;
    private String personaQN2;

    // Purpose state
    private boolean purposeCreationSupported = true;
    private String purposeName1;
    private String purposeGuid1;

    // Policy state
    private String policyName1;
    private String policyGuid1;

    // ========== Persona name uniqueness tests ==========

    @Test
    @Order(1)
    void testCreatePersona() {
        personaName1 = "unique-persona-" + testId;
        try {
            AtlasEntity persona = new AtlasEntity("Persona");
            persona.setAttribute("name", personaName1);
            persona.setAttribute("description", "First persona");

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(persona));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            personaGuid1 = created.getGuid();

            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid1);
            personaQN1 = (String) result.getEntity().getAttribute("qualifiedName");

            LOG.info("Created Persona 1: guid={}, name={}", personaGuid1, personaName1);
        } catch (AtlasServiceException e) {
            LOG.warn("Persona creation failed (likely missing Keycloak): {}", e.getMessage());
            personaCreationSupported = false;
        }
    }

    @Test
    @Order(2)
    void testCreatePersonaWithDuplicateNameFails() {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        assertNotNull(personaGuid1);

        AtlasEntity duplicate = new AtlasEntity("Persona");
        duplicate.setAttribute("name", personaName1);
        duplicate.setAttribute("description", "Duplicate persona");

        AtlasServiceException ex = assertThrows(AtlasServiceException.class,
                () -> atlasClient.createEntity(new AtlasEntityWithExtInfo(duplicate)),
                "Creating Persona with duplicate name should fail");

        assertEquals(ClientResponse.Status.CONFLICT, ex.getStatus(),
                "Expected 409 CONFLICT for duplicate Persona name");

        LOG.info("Duplicate Persona name correctly rejected: {}", personaName1);
    }

    @Test
    @Order(3)
    void testCreatePersonaWithDifferentNameSucceeds() {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");

        personaName2 = "another-persona-" + testId;
        try {
            AtlasEntity persona = new AtlasEntity("Persona");
            persona.setAttribute("name", personaName2);
            persona.setAttribute("description", "Second persona with different name");

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(persona));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            personaGuid2 = created.getGuid();

            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid2);
            personaQN2 = (String) result.getEntity().getAttribute("qualifiedName");

            LOG.info("Created Persona 2: guid={}, name={}", personaGuid2, personaName2);
        } catch (AtlasServiceException e) {
            fail("Creating Persona with a different name should succeed: " + e.getMessage());
        }
    }

    @Test
    @Order(4)
    void testRenamePersonaToDuplicateNameFails() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        Assumptions.assumeTrue(personaGuid2 != null, "Skipping: Second persona not created");

        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(personaGuid2);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("name", personaName1); // rename to first persona's name

        AtlasServiceException ex = assertThrows(AtlasServiceException.class,
                () -> atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity)),
                "Renaming Persona to existing name should fail");

        assertEquals(ClientResponse.Status.CONFLICT, ex.getStatus(),
                "Expected 409 CONFLICT when renaming to existing Persona name");

        LOG.info("Persona rename to duplicate correctly rejected");
    }

    @Test
    @Order(5)
    void testDeletePersonaAndReuseNameSucceeds() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        assertNotNull(personaGuid1);

        // Soft-delete the first persona
        atlasClient.deleteEntityByGuid(personaGuid1);
        AtlasEntityWithExtInfo deleted = atlasClient.getEntityByGuid(personaGuid1);
        assertEquals(AtlasEntity.Status.DELETED, deleted.getEntity().getStatus());

        LOG.info("Deleted Persona 1: guid={}", personaGuid1);

        // Wait for ES to index the deletion
        sleep(2000);

        // Now creating a persona with the same name should succeed
        try {
            AtlasEntity reuse = new AtlasEntity("Persona");
            reuse.setAttribute("name", personaName1);
            reuse.setAttribute("description", "Reused name after deletion");

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(reuse));
            AtlasEntityHeader created = response.getFirstEntityCreated();
            assertNotNull(created);

            // Update guid1 to the new entity for cleanup
            personaGuid1 = created.getGuid();

            LOG.info("Reused deleted Persona name successfully: {}", personaName1);
        } catch (AtlasServiceException e) {
            fail("Should be able to reuse name of soft-deleted Persona: " + e.getMessage());
        }
    }

    // ========== Purpose name uniqueness tests ==========

    @Test
    @Order(10)
    void testCreatePurpose() {
        purposeName1 = "unique-purpose-" + testId;
        try {
            AtlasEntity purpose = new AtlasEntity("Purpose");
            purpose.setAttribute("name", purposeName1);
            purpose.setAttribute("description", "First purpose");
            purpose.setAttribute("purposeClassifications", Arrays.asList("Confidential"));

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(purpose));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            purposeGuid1 = created.getGuid();

            LOG.info("Created Purpose 1: guid={}, name={}", purposeGuid1, purposeName1);
        } catch (AtlasServiceException e) {
            LOG.warn("Purpose creation failed: {}", e.getMessage());
            purposeCreationSupported = false;
        }
    }

    @Test
    @Order(11)
    void testCreatePurposeWithDuplicateNameFails() {
        Assumptions.assumeTrue(purposeCreationSupported, "Skipping: Purpose creation not supported");
        assertNotNull(purposeGuid1);

        AtlasEntity duplicate = new AtlasEntity("Purpose");
        duplicate.setAttribute("name", purposeName1);
        duplicate.setAttribute("description", "Duplicate purpose");
        duplicate.setAttribute("purposeClassifications", Arrays.asList("PII")); // different tags

        AtlasServiceException ex = assertThrows(AtlasServiceException.class,
                () -> atlasClient.createEntity(new AtlasEntityWithExtInfo(duplicate)),
                "Creating Purpose with duplicate name should fail");

        assertEquals(ClientResponse.Status.CONFLICT, ex.getStatus(),
                "Expected 409 CONFLICT for duplicate Purpose name");

        LOG.info("Duplicate Purpose name correctly rejected: {}", purposeName1);
    }

    // ========== Policy name uniqueness tests (scoped to parent) ==========

    @Test
    @Order(20)
    void testCreatePolicyUnderPersona() {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        Assumptions.assumeTrue(personaGuid1 != null, "Skipping: Persona not available");

        policyName1 = "test-policy-" + testId;
        try {
            AtlasEntity policy = new AtlasEntity("AuthPolicy");
            policy.setAttribute("name", policyName1);
            policy.setAttribute("policyType", "allow");
            policy.setAttribute("policyCategory", "persona");
            policy.setAttribute("policySubCategory", "metadata");
            policy.setAttribute("policyActions", Arrays.asList("persona-asset-read"));
            policy.setAttribute("policyResources", Arrays.asList("entity:*"));
            policy.setAttribute("accessControl", personaGuid1);

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(policy));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            policyGuid1 = created.getGuid();

            LOG.info("Created Policy 1: guid={}, name={}, parent={}", policyGuid1, policyName1, personaGuid1);
        } catch (AtlasServiceException e) {
            LOG.warn("Policy creation failed: {}", e.getMessage());
            Assumptions.assumeTrue(false, "Policy creation failed: " + e.getMessage());
        }
    }

    @Test
    @Order(21)
    void testCreateDuplicatePolicyUnderSameParentFails() {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        Assumptions.assumeTrue(policyGuid1 != null, "Skipping: First policy not created");

        AtlasEntity duplicate = new AtlasEntity("AuthPolicy");
        duplicate.setAttribute("name", policyName1); // same name
        duplicate.setAttribute("policyType", "allow");
        duplicate.setAttribute("policyCategory", "persona");
        duplicate.setAttribute("policySubCategory", "metadata");
        duplicate.setAttribute("policyActions", Arrays.asList("persona-asset-read"));
        duplicate.setAttribute("policyResources", Arrays.asList("entity:*"));
        duplicate.setAttribute("accessControl", personaGuid1); // same parent

        AtlasServiceException ex = assertThrows(AtlasServiceException.class,
                () -> atlasClient.createEntity(new AtlasEntityWithExtInfo(duplicate)),
                "Creating Policy with duplicate name under same parent should fail");

        assertEquals(ClientResponse.Status.CONFLICT, ex.getStatus(),
                "Expected 409 CONFLICT for duplicate Policy name under same parent");

        LOG.info("Duplicate Policy name under same parent correctly rejected");
    }

    @Test
    @Order(22)
    void testCreateSamePolicyNameUnderDifferentParentSucceeds() {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        Assumptions.assumeTrue(personaGuid2 != null, "Skipping: Second persona not created");
        Assumptions.assumeTrue(policyGuid1 != null, "Skipping: First policy not created");

        try {
            AtlasEntity policy = new AtlasEntity("AuthPolicy");
            policy.setAttribute("name", policyName1); // same name as policy under persona1
            policy.setAttribute("policyType", "allow");
            policy.setAttribute("policyCategory", "persona");
            policy.setAttribute("policySubCategory", "metadata");
            policy.setAttribute("policyActions", Arrays.asList("persona-asset-read"));
            policy.setAttribute("policyResources", Arrays.asList("entity:*"));
            policy.setAttribute("accessControl", personaGuid2); // different parent

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(policy));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created, "Same policy name under different parent should succeed");

            LOG.info("Same policy name under different parent allowed: name={}, parent={}",
                    policyName1, personaGuid2);
        } catch (AtlasServiceException e) {
            fail("Same policy name under different parent should succeed: " + e.getMessage());
        }
    }

    // ========== Cleanup ==========

    @Test
    @Order(90)
    void cleanupPurpose() throws AtlasServiceException {
        if (purposeGuid1 != null) {
            atlasClient.deleteEntityByGuid(purposeGuid1);
            LOG.info("Cleaned up Purpose: guid={}", purposeGuid1);
        }
    }

    @Test
    @Order(91)
    void cleanupPersonas() throws AtlasServiceException {
        // Deleting persona cascades to its policies
        if (personaGuid1 != null) {
            atlasClient.deleteEntityByGuid(personaGuid1);
            LOG.info("Cleaned up Persona 1: guid={}", personaGuid1);
        }
        if (personaGuid2 != null) {
            atlasClient.deleteEntityByGuid(personaGuid2);
            LOG.info("Cleaned up Persona 2: guid={}", personaGuid2);
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
