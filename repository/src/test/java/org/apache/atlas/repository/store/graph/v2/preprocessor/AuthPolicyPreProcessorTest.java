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
package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for AuthPolicyPreProcessor duplicate policy name validation.
 */
public class AuthPolicyPreProcessorTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private EntityMutationContext context;

    @Mock
    private EntityDiscoveryService discoveryService;

    @Mock
    private IndexAliasStore aliasStore;

    @Mock
    private AtlasVertex mockVertex;

    private AuthPolicyPreProcessor preProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get().setUser("testUser", null);

        // Create preprocessor with mocked dependencies
        // Mocked methods automatically return default values (null, false, 0, etc.)
        preProcessor = new AuthPolicyPreProcessor(graph, typeRegistry, entityRetriever, discoveryService, aliasStore);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Helper to create a Persona entity
     */
    private AtlasEntity createPersonaEntity(String name, String qualifiedName) {
        AtlasEntity persona = new AtlasEntity();
        persona.setTypeName(PERSONA_ENTITY_TYPE);
        persona.setAttribute(NAME, name);
        persona.setAttribute(QUALIFIED_NAME, qualifiedName);
        persona.setAttribute(ATTR_PERSONA_USERS, new ArrayList<>());
        persona.setAttribute(ATTR_PERSONA_GROUPS, new ArrayList<>());
        persona.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true); // Required boolean attribute
        persona.setGuid("persona-guid-123");
        persona.setStatus(AtlasEntity.Status.ACTIVE);
        return persona;
    }

    /**
     * Helper to create a Purpose entity
     */
    private AtlasEntity createPurposeEntity(String name, String qualifiedName) {
        AtlasEntity purpose = new AtlasEntity();
        purpose.setTypeName(PURPOSE_ENTITY_TYPE);
        purpose.setAttribute(NAME, name);
        purpose.setAttribute(QUALIFIED_NAME, qualifiedName);
        purpose.setAttribute(ATTR_PURPOSE_CLASSIFICATIONS, Collections.singletonList("PII"));
        purpose.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true); // Required boolean attribute
        purpose.setGuid("purpose-guid-456");
        purpose.setStatus(AtlasEntity.Status.ACTIVE);
        return purpose;
    }

    /**
     * Helper to create a Policy entity
     */
    private AtlasEntity createPolicyEntity(String name, String category, AtlasEntity parent) {
        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setAttribute(NAME, name);
        policy.setAttribute(ATTR_POLICY_CATEGORY, category);

        // Add required attributes for validation
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, "atlas");
        policy.setAttribute(ATTR_POLICY_TYPE, "allow");
        // Use valid glossary action by default (will be overridden if needed)
        policy.setAttribute(ATTR_POLICY_ACTIONS, Collections.singletonList("persona-glossary-read"));

        if (parent != null) {
            AtlasObjectId parentRef = new AtlasObjectId(parent.getGuid(), parent.getTypeName());
            policy.setRelationshipAttribute(REL_ATTR_ACCESS_CONTROL, parentRef);
        }

        return policy;
    }

    // =====================================================================================
    // Test Group 1: Duplicate Policy Name Validation for Persona Policies
    // =====================================================================================

    @Test
    public void testDuplicatePolicyName_PersonaPolicy_ThrowsException() throws Exception {
        // Setup
        AtlasEntity persona = createPersonaEntity("TestPersona", "default/persona123");
        AtlasEntity policy = createPolicyEntity("duplicate-policy", POLICY_CATEGORY_PERSONA, persona);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_GLOSSARY);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:default/glossary/test-glossary"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM);
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, "persona");

        // Mock entity retriever to return the persona
        AtlasEntityWithExtInfo personaWithExt = new AtlasEntityWithExtInfo(persona);
        personaWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(personaWithExt);

        // Mock discovery service to return existing policy (simulating duplicate)
        List<AtlasVertex> existingPolicies = Collections.singletonList(mockVertex);
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class))).thenReturn(existingPolicies);

        // Execute & Verify
        try {
            preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown AtlasBaseException for duplicate policy name");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Policy with name 'duplicate-policy' already exists"));
            assertTrue(e.getMessage().contains("TestPersona"));
        }

        // Verify discovery service was called
        verify(discoveryService).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testUniquePolicyName_PersonaPolicy_Success() throws Exception {
        // Setup
        AtlasEntity persona = createPersonaEntity("TestPersona", "default/persona123");
        AtlasEntity policy = createPolicyEntity("unique-policy", POLICY_CATEGORY_PERSONA, persona);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_GLOSSARY);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:default/glossary/test-glossary"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM);

        // Mock entity retriever to return the persona
        AtlasEntityWithExtInfo personaWithExt = new AtlasEntityWithExtInfo(persona);
        personaWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(personaWithExt);

        // Mock discovery service to return empty list (no duplicates)
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class))).thenReturn(Collections.emptyList());

        try {
            // Execute - should not throw exception
            preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);

            // Verify validation was called
            verify(discoveryService).directVerticesIndexSearch(any(IndexSearchParams.class));

            // Verify qualifiedName was set
            assertNotNull(policy.getAttribute(QUALIFIED_NAME));
            assertTrue(((String) policy.getAttribute(QUALIFIED_NAME)).startsWith("default/persona123/"));
        } catch (Exception e) {
            // Print the actual error for debugging
            e.printStackTrace();
            fail("Test failed with exception: " + e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testDuplicateValidation_VerifyESQueryStructure() throws Exception {
        // Setup
        AtlasEntity persona = createPersonaEntity("TestPersona", "default/persona123");
        AtlasEntity policy = createPolicyEntity("test-policy", POLICY_CATEGORY_PERSONA, persona);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_GLOSSARY);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:default/glossary/test-glossary"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM);

        AtlasEntityWithExtInfo personaWithExt = new AtlasEntityWithExtInfo(persona);
        personaWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(personaWithExt);
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class))).thenReturn(Collections.emptyList());

        ArgumentCaptor<IndexSearchParams> paramsCaptor = ArgumentCaptor.forClass(IndexSearchParams.class);

        // Execute
        preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);

        // Verify the ES query structure
        verify(discoveryService).directVerticesIndexSearch(paramsCaptor.capture());

        IndexSearchParams capturedParams = paramsCaptor.getValue();
        Map<String, Object> dsl = capturedParams.getDsl();

        assertNotNull(dsl, "DSL should not be null");
        assertEquals(dsl.get("size"), 1, "Query size should be 1");

        Map<String, Object> query = (Map<String, Object>) dsl.get("query");
        assertNotNull(query, "Query should not be null");

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        assertNotNull(bool, "Bool clause should not be null");

        List<Map<String, Object>> filters = (List<Map<String, Object>>) bool.get("filter");
        assertEquals(filters.size(), 5, "Should have 5 filter clauses");

        // Verify filter clauses contain expected terms
        boolean hasStateFilter = filters.stream().anyMatch(f -> f.containsKey("term") &&
            ((Map<String, Object>) f.get("term")).containsKey("__state"));
        boolean hasTypeFilter = filters.stream().anyMatch(f -> f.containsKey("term") &&
            ((Map<String, Object>) f.get("term")).containsKey("__typeName.keyword"));
        boolean hasCategoryFilter = filters.stream().anyMatch(f -> f.containsKey("term") &&
            ((Map<String, Object>) f.get("term")).containsKey("policyCategory"));
        boolean hasNameFilter = filters.stream().anyMatch(f -> f.containsKey("term") &&
            ((Map<String, Object>) f.get("term")).containsKey("name.keyword"));
        boolean hasPrefixFilter = filters.stream().anyMatch(f -> f.containsKey("prefix"));

        assertTrue(hasStateFilter, "Should have __state filter");
        assertTrue(hasTypeFilter, "Should have __typeName filter");
        assertTrue(hasCategoryFilter, "Should have policyCategory filter");
        assertTrue(hasNameFilter, "Should have name filter");
        assertTrue(hasPrefixFilter, "Should have qualifiedName prefix filter");

        // Verify the policyCategory filter value is "persona"
        Map<String, Object> categoryFilter = filters.stream()
            .filter(f -> f.containsKey("term") && ((Map<String, Object>) f.get("term")).containsKey("policyCategory"))
            .findFirst()
            .orElse(null);
        assertNotNull(categoryFilter);
        Map<String, Object> categoryTerm = (Map<String, Object>) categoryFilter.get("term");
        assertEquals(categoryTerm.get("policyCategory"), "persona", "Should filter for persona category");
    }

    @Test
    public void testNullPolicyName_SkipsValidation() throws Exception {
        // Setup
        AtlasEntity persona = createPersonaEntity("TestPersona", "default/persona123");
        AtlasEntity policy = createPolicyEntity(null, POLICY_CATEGORY_PERSONA, persona);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_GLOSSARY);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:default/glossary/test-glossary"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM);

        AtlasEntityWithExtInfo personaWithExt = new AtlasEntityWithExtInfo(persona);
        personaWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(personaWithExt);

        // Execute - should not throw exception
        preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);

        // Verify validation was NOT called (name is null)
        verify(discoveryService, never()).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testEmptyPolicyName_SkipsValidation() throws Exception {
        // Setup
        AtlasEntity persona = createPersonaEntity("TestPersona", "default/persona123");
        AtlasEntity policy = createPolicyEntity("", POLICY_CATEGORY_PERSONA, persona);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_GLOSSARY);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:default/glossary/test-glossary"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM);

        AtlasEntityWithExtInfo personaWithExt = new AtlasEntityWithExtInfo(persona);
        personaWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(personaWithExt);

        // Execute - should not throw exception
        preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);

        // Verify validation was NOT called (name is empty)
        verify(discoveryService, never()).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    // =====================================================================================
    // Test Group 2: Purpose Policies - NOT Validated
    // =====================================================================================

    @Test
    public void testDuplicatePolicyName_PurposePolicy_NotValidated() throws Exception {
        // Setup
        AtlasEntity purpose = createPurposeEntity("TestPurpose", "default/purpose456");

        AtlasEntity policy = createPolicyEntity("any-name", POLICY_CATEGORY_PURPOSE, purpose);
        // Purpose policies don't need policyResources initially - they are set from purpose tags
        // Set purpose-specific attributes
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_METADATA);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Collections.singletonList("entity-read"));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PURPOSE);

        // Mock entity retriever to return the purpose
        AtlasEntityWithExtInfo purposeWithExt = new AtlasEntityWithExtInfo(purpose);
        purposeWithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class))).thenReturn(purposeWithExt);

        // Execute - should not throw exception
        preProcessor.processAttributes(policy, context, EntityMutations.EntityOperation.CREATE);

        // Verify validation was NOT called for Purpose policy
        verify(discoveryService, never()).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    // =====================================================================================
    // Test Group 3: Different Personas Allow Same Policy Name
    // =====================================================================================

    @Test
    public void testSamePolicyName_DifferentPersonas_BothAllowed() throws Exception {
        // Setup persona 1
        AtlasEntity persona1 = createPersonaEntity("Persona1", "default/persona123");
        AtlasEntity policy1 = createPolicyEntity("my-policy", POLICY_CATEGORY_PERSONA, persona1);
        policy1.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_DOMAIN);
        policy1.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:*"));

        AtlasEntityWithExtInfo persona1WithExt = new AtlasEntityWithExtInfo(persona1);
        persona1WithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());

        // Setup persona 2
        AtlasEntity persona2 = createPersonaEntity("Persona2", "default/persona456");
        AtlasEntity policy2 = createPolicyEntity("my-policy", POLICY_CATEGORY_PERSONA, persona2);
        policy2.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_DOMAIN);
        policy2.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList("entity:*"));

        AtlasEntityWithExtInfo persona2WithExt = new AtlasEntityWithExtInfo(persona2);
        persona2WithExt.getEntity().setRelationshipAttribute(REL_ATTR_POLICIES, new ArrayList<>());

        // Mock retriever to return different personas
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class)))
            .thenReturn(persona1WithExt)
            .thenReturn(persona2WithExt);

        // Mock discovery to return empty (no duplicates within each persona's scope)
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        // Execute both - should not throw exceptions
        preProcessor.processAttributes(policy1, context, EntityMutations.EntityOperation.CREATE);
        preProcessor.processAttributes(policy2, context, EntityMutations.EntityOperation.CREATE);

        // Verify both policies were created successfully
        assertNotNull(policy1.getAttribute(QUALIFIED_NAME));
        assertNotNull(policy2.getAttribute(QUALIFIED_NAME));

        // Verify the qualified names are different (different personas)
        assertNotEquals(policy1.getAttribute(QUALIFIED_NAME), policy2.getAttribute(QUALIFIED_NAME));

        // Verify validation was called twice (once for each policy)
        verify(discoveryService, times(2)).directVerticesIndexSearch(any(IndexSearchParams.class));
    }
}
