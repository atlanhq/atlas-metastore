package org.apache.atlas.repository.store.aliasstore;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.apache.atlas.type.Constants.GLOSSARY_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ESAliasStoreTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private EntityGraphRetriever entityRetriever;

    private ESAliasStore esAliasStore;
    private MockedStatic<AccessControlUtils> mockedAccessControlUtils;

    @BeforeAll
    public static void setupClass() {
        // Set system property for persona policy asset limit
        System.setProperty("atlas.persona.policy.asset.max.limit", "1000");
    }

    @BeforeEach
    public void setUp() throws AtlasBaseException {
        esAliasStore = new ESAliasStore(graph, entityRetriever);
        
        // Mock static methods
        mockedAccessControlUtils = mockStatic(AccessControlUtils.class);
        
        // Mock connection qualified name extraction
        mockedAccessControlUtils.when(() -> getConnectionQualifiedNameFromPolicyAssets(any(EntityGraphRetriever.class), anyList()))
            .thenReturn("default_connection");
        
        // Mock policy type check
        mockedAccessControlUtils.when(() -> getIsAllowPolicy(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                return POLICY_TYPE_ALLOW.equals(policy.getAttribute(ATTR_POLICY_TYPE));
            });
        
        // Mock policy actions
        mockedAccessControlUtils.when(() -> getPolicyActions(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                return (List<String>) policy.getAttribute(ATTR_POLICY_ACTIONS);
            });
        
        // Mock policy assets extraction
        mockedAccessControlUtils.when(() -> getPolicyAssets(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                List<String> resources = (List<String>) policy.getAttribute(ATTR_POLICY_RESOURCES);
                if (resources == null) return Collections.emptyList();
                
                List<String> assets = new ArrayList<>();
                for (String resource : resources) {
                    if (resource.startsWith(RESOURCES_ENTITY)) {
                        assets.add(resource.substring(RESOURCES_ENTITY.length()));
                    }
                }
                return assets;
            });
        
        // Mock policy resources
        mockedAccessControlUtils.when(() -> getPolicyResources(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                return (List<String>) policy.getAttribute(ATTR_POLICY_RESOURCES);
            });
        
        // Mock filtered policy resources
        mockedAccessControlUtils.when(() -> getFilteredPolicyResources(anyList(), anyString()))
            .thenAnswer(invocation -> {
                List<String> resources = invocation.getArgument(0);
                String prefix = invocation.getArgument(1);
                List<String> filtered = new ArrayList<>();
                for (String resource : resources) {
                    if (resource.startsWith(prefix)) {
                        filtered.add(resource.substring(prefix.length()));
                    }
                }
                return filtered;
            });
        
        // Mock policy subcategory
        mockedAccessControlUtils.when(() -> getPolicySubCategory(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                return policy.getAttribute(ATTR_POLICY_SUB_CATEGORY);
            });
        
        // Mock policy connection QN
        mockedAccessControlUtils.when(() -> getPolicyConnectionQN(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                return policy.getAttribute("policyConnectionQN");
            });

        // Let's add some debug logging to help track what's happening
        mockedAccessControlUtils.when(() -> getIsAllowPolicy(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                boolean result = POLICY_TYPE_ALLOW.equals(policy.getAttribute(ATTR_POLICY_TYPE));
                System.out.println("getIsAllowPolicy called with type: " + policy.getAttribute(ATTR_POLICY_TYPE) + ", returning: " + result);
                return result;
            });

        mockedAccessControlUtils.when(() -> getPolicySubCategory(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                String result = (String) policy.getAttribute(ATTR_POLICY_SUB_CATEGORY);
                System.out.println("getPolicySubCategory called, returning: " + result);
                return result;
            });

        mockedAccessControlUtils.when(() -> getPolicyAssets(any(AtlasEntity.class)))
            .thenAnswer(invocation -> {
                AtlasEntity policy = invocation.getArgument(0);
                List<String> resources = (List<String>) policy.getAttribute(ATTR_POLICY_RESOURCES);
                List<String> result = new ArrayList<>();
                if (resources != null) {
                    for (String resource : resources) {
                        if (resource.startsWith(RESOURCES_ENTITY)) {
                            result.add(resource.substring(RESOURCES_ENTITY.length()));
                        }
                    }
                }
                System.out.println("getPolicyAssets called, returning: " + result);
                return result;
            });
    }

    @AfterEach
    public void tearDown() {
        if (mockedAccessControlUtils != null) {
            mockedAccessControlUtils.close();
        }
    }

    @Test
    public void testPersonaPolicyToESDslClauses_MetadataPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, "atlas");
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_METADATA);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_METADATA));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "test.db.table1",
            RESOURCES_ENTITY + "test.db.table2"
        ));

        policies.add(policy);

        // Print policy state before execution
        System.out.println("Policy before execution:");
        System.out.println("Type: " + policy.getAttribute(ATTR_POLICY_TYPE));
        System.out.println("SubCategory: " + policy.getAttribute(ATTR_POLICY_SUB_CATEGORY));
        System.out.println("Resources: " + policy.getAttribute(ATTR_POLICY_RESOURCES));

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Print results
        System.out.println("After execution:");
        System.out.println("allowClauseList size: " + allowClauseList.size());
        System.out.println("denyClauseList size: " + denyClauseList.size());

        // Verify
        assertEquals(1, allowClauseList.size(), "allowClauseList should have one entry");
        assertTrue(allowClauseList.get(0).containsKey("terms"), "allowClauseList should contain terms");
        Map<String, Object> terms = (Map<String, Object>) allowClauseList.get(0).get("terms");
        List<String> qualifiedNames = (List<String>) terms.get(QUALIFIED_NAME);
        assertTrue(qualifiedNames.contains("test.db.table1"), "Should contain first table");
        assertTrue(qualifiedNames.contains("test.db.table2"), "Should contain second table");
        assertTrue(qualifiedNames.contains("default_connection"), "Should contain connection");
        assertTrue(denyClauseList.isEmpty(), "denyClauseList should be empty");
    }

    @Test
    public void testPersonaPolicyToESDslClauses_GlossaryPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_GLOSSARY));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "glossary1",
            RESOURCES_ENTITY + "glossary2"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(2, allowClauseList.size());
        Map<String, Object> qualifiedNameTerms = (Map<String, Object>) allowClauseList.get(0).get("terms");
        Map<String, Object> glossaryTerms = (Map<String, Object>) allowClauseList.get(1).get("terms");
        
        List<String> qualifiedNames = (List<String>) qualifiedNameTerms.get(QUALIFIED_NAME);
        List<String> glossaryNames = (List<String>) glossaryTerms.get(GLOSSARY_PROPERTY_KEY);
        
        assertTrue(qualifiedNames.containsAll(Arrays.asList("glossary1", "glossary2")));
        assertTrue(glossaryNames.containsAll(Arrays.asList("glossary1", "glossary2")));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_DomainPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_DOMAIN));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "default/domain/finance/domain/trading"
        ));

        policies.add(policy);

        // Print policy state before execution
        System.out.println("Domain Policy before execution:");
        System.out.println("Type: " + policy.getAttribute(ATTR_POLICY_TYPE));
        System.out.println("Actions: " + policy.getAttribute(ATTR_POLICY_ACTIONS));
        System.out.println("Resources: " + policy.getAttribute(ATTR_POLICY_RESOURCES));

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Print results
        System.out.println("After execution:");
        System.out.println("allowClauseList: " + allowClauseList);
        System.out.println("denyClauseList: " + denyClauseList);

        // Verify
        assertTrue(allowClauseList.size() >= 2, "Should have at least 2 clauses");
        
        // Find the terms clause
        Map<String, Object> termsClause = null;
        Map<String, Object> hierarchyClause = null;
        
        for (Map<String, Object> clause : allowClauseList) {
            if (clause.containsKey("terms")) {
                termsClause = clause;
            } else if (clause.containsKey("term")) {
                hierarchyClause = clause;
            }
        }
        
        assertNotNull(termsClause, "Should have a terms clause");
        assertNotNull(hierarchyClause, "Should have a hierarchy clause");
        
        // Verify terms clause
        Map<String, Object> terms = (Map<String, Object>) termsClause.get("terms");
        List<String> qualifiedNames = (List<String>) terms.get(QUALIFIED_NAME);
        assertTrue(qualifiedNames.contains("default/domain/finance/domain/trading"), "Should contain the full path");
        assertTrue(qualifiedNames.contains("default/domain/finance"), "Should contain parent domain");
        
        // Verify hierarchy clause
        Map<String, Object> term = (Map<String, Object>) hierarchyClause.get("term");
        assertEquals("default/domain/finance/domain/trading", term.get(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY), 
            "Should have correct hierarchy path");
        
        assertTrue(denyClauseList.isEmpty(), "denyClauseList should be empty");
    }

    @Test
    public void testPersonaPolicyToESDslClauses_AllDomainPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_DOMAIN));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "*/super"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(1, allowClauseList.size());
        Map<String, Object> wildcard = (Map<String, Object>) allowClauseList.get(0).get("wildcard");
        assertEquals("default/domain/*/super*", wildcard.get(QUALIFIED_NAME));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_SubDomainPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_SUB_DOMAIN));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "finance"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(1, allowClauseList.size());
        Map<String, Object> boolQuery = (Map<String, Object>) allowClauseList.get(0).get("bool");
        List<Map<String, Object>> mustClauses = (List<Map<String, Object>>) boolQuery.get("must");
        assertEquals(2, mustClauses.size());
        
        Map<String, Object> hierarchyTerm = (Map<String, Object>) mustClauses.get(0).get("term");
        assertEquals("finance/domain", hierarchyTerm.get(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY));
        
        Map<String, Object> typeTerm = (Map<String, Object>) mustClauses.get(1).get("term");
        assertEquals("DataDomain", typeTerm.get("__typeName.keyword"));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_ProductPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_PRODUCT));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "finance"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(1, allowClauseList.size());
        Map<String, Object> boolQuery = (Map<String, Object>) allowClauseList.get(0).get("bool");
        List<Map<String, Object>> mustClauses = (List<Map<String, Object>>) boolQuery.get("must");
        assertEquals(2, mustClauses.size());
        
        Map<String, Object> hierarchyTerm = (Map<String, Object>) mustClauses.get(0).get("term");
        assertEquals("finance/product", hierarchyTerm.get(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY));
        
        Map<String, Object> typeTerm = (Map<String, Object>) mustClauses.get(1).get("term");
        assertEquals("DataProduct", typeTerm.get("__typeName.keyword"));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_AIPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_AI_APP));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY_TYPE + "AIApplication",
            RESOURCES_ENTITY_TYPE + "AIModel"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(1, allowClauseList.size());
        Map<String, Object> boolQuery = (Map<String, Object>) allowClauseList.get(0).get("bool");
        List<Map<String, Object>> mustClauses = (List<Map<String, Object>>) boolQuery.get("must");
        assertEquals(1, mustClauses.size());
        
        Map<String, Object> typeTerms = (Map<String, Object>) mustClauses.get(0).get("terms");
        List<String> types = (List<String>) typeTerms.get("__typeName.keyword");
        assertTrue(types.containsAll(Arrays.asList("AIApplication", "AIModel")));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_DenyPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_DENY);
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, "atlas");
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_METADATA);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_METADATA));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "test.db.table1",
            RESOURCES_ENTITY + "test.db.table2"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertTrue(allowClauseList.isEmpty());
        assertEquals(1, denyClauseList.size());
        Map<String, Object> terms = (Map<String, Object>) denyClauseList.get(0).get("terms");
        List<String> qualifiedNames = (List<String>) terms.get(QUALIFIED_NAME);
        assertTrue(qualifiedNames.containsAll(Arrays.asList("test.db.table1", "test.db.table2", "default_connection")));
    }

    @Test
    public void testPersonaPolicyToESDslClauses_InactivePolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.DELETED);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_METADATA));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList(
            RESOURCES_ENTITY + "test.db.table1"
        ));

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertTrue(allowClauseList.isEmpty());
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_ABACPolicy() throws AtlasBaseException {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, POLICY_SERVICE_NAME_ABAC);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_METADATA));
        policy.setAttribute(ATTR_POLICY_FILTER_CRITERIA, "{\"entity\":{\"condition\":\"AND\",\"criterion\":[{\"attributeName\":\"name\",\"operator\":\"eq\",\"attributeValue\":\"test\"}]}}");

        policies.add(policy);

        // Execute
        esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);

        // Verify
        assertEquals(1, allowClauseList.size());
        assertTrue(allowClauseList.get(0).containsKey("bool"));
        assertTrue(denyClauseList.isEmpty());
    }

    @Test
    public void testPersonaPolicyToESDslClauses_ExceedAssetLimit() {
        // Setup
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();
        List<AtlasEntity> policies = new ArrayList<>();

        AtlasEntity policy = new AtlasEntity();
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, "atlas");
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_METADATA);
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList(ACCESS_READ_PERSONA_METADATA));
        
        // Create a list of 1001 assets (exceeding the default limit of 1000)
        List<String> assets = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            assets.add(RESOURCES_ENTITY + "test.db.table" + i);
        }
        policy.setAttribute(ATTR_POLICY_RESOURCES, assets);

        policies.add(policy);

        // Execute - should throw AtlasBaseException
        assertThrows(AtlasBaseException.class, () -> 
            esAliasStore.personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList)
        );
    }
} 