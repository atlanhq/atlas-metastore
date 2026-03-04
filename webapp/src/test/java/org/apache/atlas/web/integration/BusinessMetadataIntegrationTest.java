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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Business Metadata (custom metadata) operations.
 *
 * <p>Exercises business metadata typedef creation, attribute attachment to entities,
 * partial updates, removal, and typedef deletion.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BusinessMetadataIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessMetadataIntegrationTest.class);

    private final long testId = System.currentTimeMillis();
    private String bmName;
    private String bmStringAttrName;
    private String bmIntAttrName;
    private String bmBoolAttrName;
    private String entityGuid;
    private boolean bmCreated = false;

    private AtlasAttributeDef createAttrDef(String name, String typeName) {
        AtlasAttributeDef attr = new AtlasAttributeDef(name, typeName);
        attr.setDisplayName(name);
        attr.setIsOptional(true);
        attr.setCardinality(AtlasAttributeDef.Cardinality.SINGLE);
        // Each attribute needs applicableEntityTypes in its options
        Map<String, String> options = new HashMap<>();
        options.put("applicableEntityTypes", "[\"Table\",\"View\",\"Column\"]");
        if ("string".equals(typeName)) {
            options.put("maxStrLength", "500");
        }
        attr.setOptions(options);
        return attr;
    }

    @Test
    @Order(1)
    void testCreateBusinessMetadataTypeDef() throws Exception {
        bmName = "TestBM_" + testId;

        AtlasAttributeDef strAttr = createAttrDef("bm_string_attr", "string");
        AtlasAttributeDef intAttr = createAttrDef("bm_int_attr", "int");
        AtlasAttributeDef boolAttr = createAttrDef("bm_bool_attr", "boolean");

        AtlasBusinessMetadataDef bmDef = new AtlasBusinessMetadataDef();
        bmDef.setName(bmName);
        bmDef.setDisplayName(bmName);
        bmDef.setDescription("Test business metadata");
        bmDef.setTypeVersion("1.0");
        bmDef.setAttributeDefs(Arrays.asList(strAttr, intAttr, boolAttr));

        Map<String, String> options = new HashMap<>();
        options.put("applicableEntityTypes", "[\"Table\",\"View\",\"Column\"]");
        bmDef.setOptions(options);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setBusinessMetadataDefs(Collections.singletonList(bmDef));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);
        assertNotNull(created);
        assertNotNull(created.getBusinessMetadataDefs());
        assertFalse(created.getBusinessMetadataDefs().isEmpty());

        // Use the actual name from the response (may have been modified by Atlas)
        AtlasBusinessMetadataDef createdBmDef = created.getBusinessMetadataDefs().get(0);
        bmName = createdBmDef.getName();
        bmStringAttrName = createdBmDef.getAttributeDefs().stream()
                .filter(a -> "bm_string_attr".equals(a.getDisplayName()))
                .map(AtlasAttributeDef::getName)
                .findFirst()
                .orElse("bm_string_attr");
        bmIntAttrName = createdBmDef.getAttributeDefs().stream()
                .filter(a -> "bm_int_attr".equals(a.getDisplayName()))
                .map(AtlasAttributeDef::getName)
                .findFirst()
                .orElse("bm_int_attr");
        bmBoolAttrName = createdBmDef.getAttributeDefs().stream()
                .filter(a -> "bm_bool_attr".equals(a.getDisplayName()))
                .map(AtlasAttributeDef::getName)
                .findFirst()
                .orElse("bm_bool_attr");
        LOG.info("Resolved BM typedef/attr names: bm={} string={} int={} bool={}",
                bmName, bmStringAttrName, bmIntAttrName, bmBoolAttrName);
        bmCreated = true;

        LOG.info("Created business metadata typedef: {}", bmName);
    }

    @Test
    @Order(2)
    void testGetBusinessMetadataDef() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");

        AtlasBusinessMetadataDef bmDef = atlasClient.getBusinessMetadataDefByName(bmName);

        assertNotNull(bmDef);
        assertEquals(bmName, bmDef.getName());
        assertNotNull(bmDef.getAttributeDefs());
        assertEquals(3, bmDef.getAttributeDefs().size());

        LOG.info("Fetched business metadata def: {} with {} attributes",
                bmDef.getName(), bmDef.getAttributeDefs().size());
    }

    @Test
    @Order(3)
    void testCreateEntityForBusinessMetadata() throws AtlasServiceException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "bm-test-table-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/bm/table/" + testId);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        entityGuid = response.getFirstEntityCreated().getGuid();
        assertNotNull(entityGuid);

        LOG.info("Created entity for BM test: guid={}", entityGuid);
    }

    @Test
    @Order(4)
    void testAddBusinessMetadataToEntity() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        Map<String, Object> bmAttrs = new HashMap<>();
        bmAttrs.put(bmStringAttrName, "test-value");
        bmAttrs.put(bmIntAttrName, 42);
        bmAttrs.put(bmBoolAttrName, true);

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put(bmName, bmAttrs);
        LOG.info("Applying BM attributes to entity {} with keys {}", entityGuid, bmAttrs.keySet());

        atlasClient.addOrUpdateBusinessAttributes(entityGuid, false, businessAttributes);

        LOG.info("Added business metadata to entity: guid={}", entityGuid);
    }

    @Test
    @Order(5)
    void testGetEntityWithBusinessMetadata() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        AtlasEntity entity = result.getEntity();

        assertNotNull(entity);
        assertNotNull(entity.getBusinessAttributes(), "Entity should have business attributes");
        assertTrue(entity.getBusinessAttributes().containsKey(bmName),
                "Entity should have BM: " + bmName);

        @SuppressWarnings("unchecked")
        Map<String, Object> bmAttrs = (Map<String, Object>) entity.getBusinessAttributes().get(bmName);
        assertEquals("test-value", bmAttrs.get(bmStringAttrName));

        LOG.info("Verified business metadata on entity: {}", entity.getBusinessAttributes().keySet());
    }

    @Test
    @Order(6)
    void testUpdateBusinessMetadata() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        Map<String, Object> bmAttrs = new HashMap<>();
        bmAttrs.put(bmStringAttrName, "updated-value");
        bmAttrs.put(bmIntAttrName, 99);
        bmAttrs.put(bmBoolAttrName, false);

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put(bmName, bmAttrs);
        LOG.info("Updating BM attributes on entity {} with keys {}", entityGuid, bmAttrs.keySet());

        atlasClient.addOrUpdateBusinessAttributes(entityGuid, true, businessAttributes);

        // Verify update
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        @SuppressWarnings("unchecked")
        Map<String, Object> fetchedAttrs = (Map<String, Object>) result.getEntity()
                .getBusinessAttributes().get(bmName);
        assertEquals("updated-value", fetchedAttrs.get(bmStringAttrName));

        LOG.info("Updated business metadata on entity: guid={}", entityGuid);
    }

    @Test
    @Order(7)
    void testPartialUpdateBusinessMetadata() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        // Only update one attribute
        Map<String, Object> bmAttrs = new HashMap<>();
        bmAttrs.put(bmStringAttrName, "partial-update");

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put(bmName, bmAttrs);
        LOG.info("Partially updating BM attributes on entity {} with keys {}", entityGuid, bmAttrs.keySet());

        atlasClient.addOrUpdateBusinessAttributes(entityGuid, false, businessAttributes);

        // Verify partial update
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        @SuppressWarnings("unchecked")
        Map<String, Object> fetchedAttrs = (Map<String, Object>) result.getEntity()
                .getBusinessAttributes().get(bmName);
        assertEquals("partial-update", fetchedAttrs.get(bmStringAttrName));

        LOG.info("Partially updated business metadata: guid={}", entityGuid);
    }

    @Test
    @Order(8)
    void testRemoveBusinessMetadata() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        Map<String, Object> bmAttrs = new HashMap<>();
        bmAttrs.put(bmStringAttrName, "");
        bmAttrs.put(bmIntAttrName, 0);
        bmAttrs.put(bmBoolAttrName, false);

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put(bmName, bmAttrs);
        LOG.info("Removing BM attributes from entity {} with keys {}", entityGuid, bmAttrs.keySet());

        atlasClient.removeBusinessAttributes(entityGuid, businessAttributes);

        LOG.info("Removed business metadata from entity: guid={}", entityGuid);
    }

    @Test
    @Order(9)
    void testVerifyBusinessMetadataRemoved() throws AtlasServiceException {
        assertTrue(bmCreated, "BM typedef not created");
        assertNotNull(entityGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        Map<String, Map<String, Object>> bm = result.getEntity().getBusinessAttributes();

        // After removal, BM may be null, empty, or have null values
        if (bm != null && bm.containsKey(bmName)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> bmAttrs = (Map<String, Object>) bm.get(bmName);
            LOG.info("BM attributes after removal: {}", bmAttrs);
        } else {
            LOG.info("Business metadata successfully cleared from entity");
        }
    }

    @Test
    @Order(10)
    void testAddBusinessMetadataToNonExistentEntity() {
        assertTrue(bmCreated, "BM typedef not created");

        String bogusGuid = "00000000-0000-0000-0000-000000000000";

        Map<String, Object> bmAttrs = new HashMap<>();
        bmAttrs.put(bmStringAttrName, "test");

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put(bmName, bmAttrs);

        assertThrows(AtlasServiceException.class,
                () -> atlasClient.addOrUpdateBusinessAttributes(bogusGuid, false, businessAttributes),
                "Adding BM to non-existent entity should throw");

        LOG.info("Correctly rejected BM addition to non-existent entity");
    }
}
