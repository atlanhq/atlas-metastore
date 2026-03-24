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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for MS-701: Main asset events missing for sub-asset add.
 *
 * <p>Scenario (first example from the ticket):
 * <ol>
 *   <li>Create a Table entity (parent/main asset)</li>
 *   <li>Send a bulk createOrUpdate with the SAME Table (unchanged attributes)
 *       plus a NEW Process with inputs referencing the Table</li>
 *   <li>Expect: Table should appear as UPDATED in the response because its
 *       relationship (inputs/outputs) changed</li>
 * </ol>
 *
 * <p>Bug: The Table is marked as "unchanged" by the diff check and added to
 * entitiesToSkipUpdate. When the Process creates a relationship edge back to
 * the Table, the Table's update event is suppressed by
 * RequestContext.recordEntityUpdate() checking the skip set.</p>
 *
 * <p>Run with:
 * <pre>
 * mvn install -pl webapp -am -DskipTests -Drat.skip=true
 * mvn test -pl webapp -Dtest=SubAssetAddParentUpdateNotificationTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SubAssetAddParentUpdateNotificationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(SubAssetAddParentUpdateNotificationTest.class);

    private final long testId = System.currentTimeMillis();

    private String tableGuid;
    private String tableQualifiedName;

    @Test
    @Order(1)
    void testCreateParentTable() throws Exception {
        LOG.info("=== Step 1: Create parent Table ===");

        AtlasEntity table = new AtlasEntity("Table");
        tableQualifiedName = "test://ms701/parent-table/" + testId;
        table.setAttribute("name", "ms701-parent-table-" + testId);
        table.setAttribute("qualifiedName", tableQualifiedName);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(table));

        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Table should be created");
        tableGuid = created.getGuid();
        assertNotNull(tableGuid, "Table GUID should not be null");

        LOG.info("Created parent Table, GUID: {}", tableGuid);
    }

    @Test
    @Order(2)
    void testAddSubAsset_ParentShouldAppearAsUpdated() throws Exception {
        LOG.info("=== Step 2: MS-701 - Sub-asset add should trigger parent UPDATE ===");
        assertNotNull(tableGuid, "Table must exist from previous test");

        // Build the bulk payload:
        //   - The SAME Table (no attribute changes → will be marked as "unchanged")
        //   - A NEW Process with inputs=[Table] (creates a relationship edge back to the Table)
        AtlasEntity unchangedTable = new AtlasEntity("Table");
        unchangedTable.setGuid(tableGuid);
        unchangedTable.setAttribute("name", "ms701-parent-table-" + testId);
        unchangedTable.setAttribute("qualifiedName", tableQualifiedName);

        AtlasEntity newProcess = new AtlasEntity("Process");
        newProcess.setAttribute("name", "ms701-child-process-" + testId);
        newProcess.setAttribute("qualifiedName", "test://ms701/child-process/" + testId);
        newProcess.setAttribute("inputs",
                Collections.singletonList(new AtlasObjectId(tableGuid, "Table")));

        AtlasEntitiesWithExtInfo bulkEntities = new AtlasEntitiesWithExtInfo();
        bulkEntities.addEntity(unchangedTable);
        bulkEntities.addEntity(newProcess);

        LOG.info("Sending bulk request with unchanged Table + new Process");
        EntityMutationResponse response = atlasClient.createEntities(bulkEntities);

        assertNotNull(response, "Mutation response should not be null");

        // Process should be CREATED
        List<AtlasEntityHeader> createdEntities = response.getCreatedEntities();
        assertNotNull(createdEntities, "Should have created entities");
        assertFalse(createdEntities.isEmpty(), "Should have at least 1 created entity (Process)");

        boolean processCreated = createdEntities.stream()
                .anyMatch(h -> "Process".equals(h.getTypeName()));
        assertTrue(processCreated, "Process should appear in created entities");

        // Collect all UPDATED entity GUIDs from the response
        Set<String> updatedGuids = new HashSet<>();
        List<AtlasEntityHeader> updatedEntities = response.getUpdatedEntities();
        if (updatedEntities != null) {
            for (AtlasEntityHeader header : updatedEntities) {
                updatedGuids.add(header.getGuid());
                LOG.info("UPDATED entity in response: {} ({})", header.getTypeName(), header.getGuid());
            }
        }

        List<AtlasEntityHeader> partialUpdatedEntities = response.getPartialUpdatedEntities();
        if (partialUpdatedEntities != null) {
            for (AtlasEntityHeader header : partialUpdatedEntities) {
                updatedGuids.add(header.getGuid());
                LOG.info("PARTIAL_UPDATE entity in response: {} ({})", header.getTypeName(), header.getGuid());
            }
        }

        LOG.info("All updated GUIDs in REST response: {}", updatedGuids);

        // KEY ASSERTION: The Table should appear as UPDATED because its relationship changed
        // (a new Process now references it via inputs)
        assertTrue(updatedGuids.contains(tableGuid),
                "MS-701 BUG: Table should appear as UPDATED in REST response when a new " +
                "sub-asset (Process) creates a relationship to it, even though the Table's " +
                "own attributes are unchanged. Table GUID: " + tableGuid +
                ", Updated GUIDs: " + updatedGuids);

        LOG.info("=== TEST PASSED: Table correctly appears as UPDATED ===");
    }
}
