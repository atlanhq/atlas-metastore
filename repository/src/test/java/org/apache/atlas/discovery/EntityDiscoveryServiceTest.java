package org.apache.atlas.discovery;

import junit.framework.TestCase;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.discovery.SearchParams;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PRODUCT;

/**
 * Unit tests for EntityDiscoveryService
 * - optimizeQueryIfApplicable
 * - addClassificationNamesToSourceFields
 */
public class EntityDiscoveryServiceTest extends TestCase {
    
    /**
     * Test that optimizeQueryIfApplicable does NOT optimize the query 
     * when enableFullRestriction is true.
     * 
     * This test actually invokes the method and verifies that when ABAC full 
     * restriction is enabled, the query remains unchanged (early return behavior).
     */
    public void testOptimizeQueryIfApplicable_WithFullRestrictionEnabled() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();
        
        // Set up a query DSL that would normally be optimized
        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
        dsl.put("query", query);
        
        searchParams.setDsl(dsl);
        searchParams.setEnableFullRestriction(true);
        
        // Store original query for comparison
        String queryBeforeOptimization = searchParams.getQuery();
        assertNotNull("Query should not be null", queryBeforeOptimization);
        
        // Act - invoke the actual method through our testable service
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.optimizeQueryIfApplicable(searchParams, CLIENT_ORIGIN_PRODUCT);
        
        // Assert - query should remain unchanged due to early return
        String queryAfterOptimization = searchParams.getQuery();
        assertEquals("Query MUST NOT be optimized when enableFullRestriction is true", 
                     queryBeforeOptimization, 
                     queryAfterOptimization);
        assertTrue("Query should contain original structure", 
                   queryAfterOptimization.contains("match_all"));
    }
    
    /**
     * Test with a different query structure to ensure the check works 
     * with more complex queries.
     */
    public void testOptimizeQueryIfApplicable_FullRestrictionOverridesClientOrigin() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();
        
        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", Map.of("term", Map.of("name", "test")));
        dsl.put("query", Map.of("bool", boolQuery));
        
        searchParams.setDsl(dsl);
        searchParams.setEnableFullRestriction(true);
        
        String queryBeforeOptimization = searchParams.getQuery();
        
        // Act - even with PRODUCT origin (which normally triggers optimization),
        // full restriction should prevent it
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.optimizeQueryIfApplicable(searchParams, CLIENT_ORIGIN_PRODUCT);
        
        // Assert
        String queryAfterOptimization = searchParams.getQuery();
        assertEquals("Full restriction should prevent optimization regardless of client origin", 
                     queryBeforeOptimization, 
                     queryAfterOptimization);
    }
    
    // ==================== Tests for addClassificationNamesToSourceFields ====================

    /**
     * Test that _source fields are added when includeClassificationNames=true
     * and excludeClassifications=true (optimization case).
     */
    public void testAddClassificationNamesToSourceFields_AddsSourceWhenConditionsMet() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();

        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
        dsl.put("query", query);

        searchParams.setDsl(dsl);
        searchParams.setIncludeClassificationNames(true);
        searchParams.setExcludeClassifications(true);

        String queryBefore = searchParams.getQuery();
        assertFalse("Query should not contain _source before optimization",
                queryBefore.contains("\"_source\""));

        // Act
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.addClassificationNamesToSourceFields(searchParams);

        // Assert
        String queryAfter = searchParams.getQuery();
        assertTrue("Query should contain _source after optimization",
                queryAfter.contains("\"_source\""));
        assertTrue("Query should contain __traitNames in _source includes",
                queryAfter.contains("__traitNames"));
        assertTrue("Query should contain __propagatedTraitNames in _source includes",
                queryAfter.contains("__propagatedTraitNames"));
    }

    /**
     * Test that _source fields are NOT added when includeClassificationNames=false.
     */
    public void testAddClassificationNamesToSourceFields_NoChangeWhenIncludeClassificationNamesFalse() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("query", Map.of("match_all", new HashMap<>()));

        searchParams.setDsl(dsl);
        searchParams.setIncludeClassificationNames(false);
        searchParams.setExcludeClassifications(true);

        String queryBefore = searchParams.getQuery();

        // Act
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.addClassificationNamesToSourceFields(searchParams);

        // Assert
        String queryAfter = searchParams.getQuery();
        assertEquals("Query should remain unchanged when includeClassificationNames=false",
                queryBefore, queryAfter);
        assertFalse("Query should not contain _source",
                queryAfter.contains("\"_source\""));
    }

    /**
     * Test that _source fields are NOT added when excludeClassifications=false.
     * This is because we need full classification objects, so we still need Cassandra.
     */
    public void testAddClassificationNamesToSourceFields_NoChangeWhenExcludeClassificationsFalse() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("query", Map.of("match_all", new HashMap<>()));

        searchParams.setDsl(dsl);
        searchParams.setIncludeClassificationNames(true);
        searchParams.setExcludeClassifications(false); // Need full classification objects

        String queryBefore = searchParams.getQuery();

        // Act
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.addClassificationNamesToSourceFields(searchParams);

        // Assert
        String queryAfter = searchParams.getQuery();
        assertEquals("Query should remain unchanged when excludeClassifications=false",
                queryBefore, queryAfter);
    }

    /**
     * Test that _source fields are NOT added when both flags are false.
     */
    public void testAddClassificationNamesToSourceFields_NoChangeWhenBothFlagsFalse() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("query", Map.of("match_all", new HashMap<>()));

        searchParams.setDsl(dsl);
        searchParams.setIncludeClassificationNames(false);
        searchParams.setExcludeClassifications(false);

        String queryBefore = searchParams.getQuery();

        // Act
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.addClassificationNamesToSourceFields(searchParams);

        // Assert
        String queryAfter = searchParams.getQuery();
        assertEquals("Query should remain unchanged when both flags are false",
                queryBefore, queryAfter);
    }

    /**
     * Test with a complex query structure to ensure _source is added correctly.
     */
    public void testAddClassificationNamesToSourceFields_ComplexQuery() {
        // Arrange
        IndexSearchParams searchParams = new IndexSearchParams();

        Map<String, Object> dsl = new HashMap<>();
        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", Map.of("term", Map.of("__typeName", "Table")));
        boolQuery.put("filter", Map.of("term", Map.of("__state", "ACTIVE")));
        dsl.put("query", Map.of("bool", boolQuery));
        dsl.put("size", 25);
        dsl.put("from", 0);

        searchParams.setDsl(dsl);
        searchParams.setIncludeClassificationNames(true);
        searchParams.setExcludeClassifications(true);

        // Act
        TestableEntityDiscoveryService service = new TestableEntityDiscoveryService();
        service.addClassificationNamesToSourceFields(searchParams);

        // Assert
        String queryAfter = searchParams.getQuery();
        assertTrue("Query should contain _source",
                queryAfter.contains("\"_source\""));
        assertTrue("Query should contain __traitNames",
                queryAfter.contains("__traitNames"));
        assertTrue("Query should preserve original query structure",
                queryAfter.contains("__typeName"));
        assertTrue("Query should preserve size",
                queryAfter.contains("\"size\""));
    }

    /**
     * Testable subclass that uses the special testing constructor.
     * This allows us to test protected methods without full dependency initialization.
     */
    private static class TestableEntityDiscoveryService extends EntityDiscoveryService {

        public TestableEntityDiscoveryService() {
            // Use the special testing constructor that skips complex initialization
            super(true);
        }

        // Expose the protected method for testing
        @Override
        protected void optimizeQueryIfApplicable(org.apache.atlas.model.discovery.SearchParams searchParams,
                                                  String clientOrigin) {
            super.optimizeQueryIfApplicable(searchParams, clientOrigin);
        }

        // Expose the new method for testing
        public void addClassificationNamesToSourceFields(SearchParams searchParams) {
            super.addClassificationNamesToSourceFields(searchParams);
        }
    }
}

