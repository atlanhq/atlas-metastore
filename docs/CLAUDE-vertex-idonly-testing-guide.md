# Vertex ID-Only Implementation Testing Guide

This document provides testing strategies based on the actual implementation in the feature branch, focusing on practical test patterns and real-world scenarios.

## Table of Contents
1. [Testing Overview](#testing-overview)
2. [Unit Testing Patterns](#unit-testing-patterns)
3. [Integration Testing Approach](#integration-testing-approach)
4. [Key Test Scenarios](#key-test-scenarios)
5. [Testing Best Practices](#testing-best-practices)
6. [Test Data Management](#test-data-management)

## Testing Overview

The ID-only implementation requires testing at multiple levels to ensure proper functionality when vertex properties are offloaded to Cassandra.

### Core Testing Areas
1. **Property Storage**: Verify correct routing between JanusGraph and Cassandra
2. **Type Conversions**: Ensure numeric values are handled correctly
3. **Feature Toggle**: Test behavior with ID-only mode enabled/disabled
4. **Bulk Operations**: Validate performance and correctness at scale
5. **Error Handling**: Ensure graceful handling of failures

## Unit Testing Patterns

### Testing DynamicVertex

The `DynamicVertex` class is central to the implementation and requires thorough testing:

```java
public class DynamicVertexTest {
    
    @Test
    public void testPropertyStorage() {
        DynamicVertex vertex = new DynamicVertex();
        
        // Test basic property operations
        vertex.setProperty("name", "test-entity");
        assertEquals("test-entity", vertex.getProperty("name", String.class));
        
        // Test property exists check
        assertTrue(vertex.hasProperty("name"));
        assertFalse(vertex.hasProperty("nonexistent"));
    }
    
    @Test
    public void testNumericStringConversion() {
        DynamicVertex vertex = new DynamicVertex();
        
        // Store numbers as strings (Elasticsearch compatibility)
        vertex.setProperty("count", "42");
        
        // Test conversions to different numeric types
        assertEquals(Long.valueOf(42), vertex.getProperty("count", Long.class));
        assertEquals(Integer.valueOf(42), vertex.getProperty("count", Integer.class));
        
        // Test decimal conversions
        vertex.setProperty("score", "3.14");
        assertEquals(Float.valueOf(3.14f), vertex.getProperty("score", Float.class));
        assertEquals(Double.valueOf(3.14), vertex.getProperty("score", Double.class));
    }
    
    @Test
    public void testCollectionHandling() {
        DynamicVertex vertex = new DynamicVertex();
        
        // Test Set properties
        vertex.addSetProperty("tags", "tag1");
        vertex.addSetProperty("tags", "tag2");
        vertex.addSetProperty("tags", "tag1"); // Duplicate - should not be added
        
        Set<Object> tags = (Set<Object>) vertex.getProperty("tags", Set.class);
        assertEquals(2, tags.size());
        assertTrue(tags.contains("tag1"));
        assertTrue(tags.contains("tag2"));
        
        // Test List properties
        vertex.addListProperty("events", "event1");
        vertex.addListProperty("events", "event2");
        vertex.addListProperty("events", "event1"); // Duplicate allowed in lists
        
        List<Object> events = (List<Object>) vertex.getProperty("events", List.class);
        assertEquals(3, events.size());
    }
    
    @Test
    public void testMapPropertyConversion() {
        DynamicVertex vertex = new DynamicVertex();
        
        // Test JSON string to Map conversion
        String jsonMap = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        vertex.setProperty("metadata", jsonMap);
        
        Map<String, Object> metadata = vertex.getProperty("metadata", Map.class);
        assertEquals("value1", metadata.get("key1"));
        assertEquals("value2", metadata.get("key2"));
    }
    
    @Test(expected = ClassCastException.class)
    public void testInvalidConversion() {
        DynamicVertex vertex = new DynamicVertex();
        vertex.setProperty("text", "not-a-number");
        
        // Should throw ClassCastException
        vertex.getProperty("text", Long.class);
    }
}
```

### Testing AtlasJanusVertex with ID-Only Mode

```java
public class AtlasJanusVertexTest {
    
    @Mock
    private AtlasJanusGraph graph;
    
    @Mock
    private Vertex janusVertex;
    
    private AtlasJanusVertex atlasVertex;
    
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        atlasVertex = new AtlasJanusVertex(graph, janusVertex);
        atlasVertex.setDynamicVertex(new DynamicVertex());
    }
    
    @Test
    public void testPropertyRoutingWithIdOnlyMode() {
        // Enable ID-only mode
        RequestContext.get().setIdOnlyGraphEnabled(true);
        
        // Add a regular property
        atlasVertex.addProperty("customAttribute", "value");
        
        // Verify it's stored in DynamicVertex, not JanusGraph
        assertEquals("value", atlasVertex.getDynamicVertex()
            .getProperty("customAttribute", String.class));
        verify(janusVertex, never()).property(any(), any(), any());
    }
    
    @Test
    public void testCorePropertyStorageInBothStores() {
        RequestContext.get().setIdOnlyGraphEnabled(true);
        
        // Add a core property
        atlasVertex.addProperty("__guid", "12345");
        
        // Verify it's stored in both places
        assertEquals("12345", atlasVertex.getDynamicVertex()
            .getProperty("__guid", String.class));
        verify(janusVertex).property("__guid", "12345");
    }
    
    @Test
    public void testLegacyModeWithIdOnlyDisabled() {
        // Disable ID-only mode
        RequestContext.get().setIdOnlyGraphEnabled(false);
        
        // Add property
        atlasVertex.addProperty("attribute", "value");
        
        // Verify it's only stored in JanusGraph
        verify(janusVertex).property(VertexProperty.Cardinality.set, "attribute", "value");
        assertNull(atlasVertex.getDynamicVertex()
            .getProperty("attribute", String.class));
    }
}
```

### Testing CassandraVertexDataRepository

```java
public class CassandraVertexDataRepositoryTest {
    
    @Mock
    private CqlSession session;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private BoundStatement boundStatement;
    
    private CassandraVertexDataRepository repository;
    
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        repository = new CassandraVertexDataRepository(session);
    }
    
    @Test
    public void testBucketCalculation() {
        // Test bucket distribution
        Set<Integer> buckets = new HashSet<>();
        
        for (int i = 1000000; i < 1001000; i++) {
            int bucket = repository.calculateBucket(String.valueOf(i));
            assertTrue(bucket >= 0 && bucket < 32);
            buckets.add(bucket);
        }
        
        // Verify reasonable distribution
        assertTrue(buckets.size() > 20); // Should use most buckets
    }
    
    @Test
    public void testBatchInsert() throws AtlasBaseException {
        Map<String, String> vertices = new HashMap<>();
        vertices.put("1001", "{\"name\":\"entity1\"}");
        vertices.put("1002", "{\"name\":\"entity2\"}");
        
        when(session.execute(anyString())).thenReturn(mock(ResultSet.class));
        
        repository.insertVertices(vertices);
        
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        verify(session).execute(queryCaptor.capture());
        
        String query = queryCaptor.getValue();
        assertTrue(query.contains("BEGIN BATCH"));
        assertTrue(query.contains("INSERT into"));
        assertTrue(query.contains("APPLY BATCH"));
    }
    
    @Test
    public void testLargeBatchHandling() throws AtlasBaseException {
        // Create more than MAX_IN_CLAUSE_ITEMS vertices
        List<String> vertexIds = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            vertexIds.add(String.valueOf(1000000 + i));
        }
        
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        when(boundStatement.setInt(anyInt(), anyInt())).thenReturn(boundStatement);
        when(boundStatement.setString(anyInt(), anyString())).thenReturn(boundStatement);
        when(boundStatement.setConsistencyLevel(any())).thenReturn(boundStatement);
        when(session.execute(any(BoundStatement.class)))
            .thenReturn(mock(ResultSet.class));
        
        Map<String, DynamicVertex> results = repository.fetchVerticesDirectly(vertexIds);
        
        // Verify multiple queries were prepared (batching)
        verify(session, atLeast(2)).prepare(anyString());
    }
}
```

## Integration Testing Approach

### Testing with RequestContext

```java
@IntegrationTest
public class VertexOffloadingIntegrationTest {
    
    @Test
    public void testEndToEndPropertyStorage() {
        // Setup
        RequestContext.get().setIdOnlyGraphEnabled(true);
        
        // Create vertex through graph
        AtlasVertex vertex = graph.addVertex();
        vertex.setProperty("__guid", "test-123");
        vertex.setProperty("name", "Integration Test");
        vertex.setProperty("count", 42L);
        
        // Verify DynamicVertex populated
        DynamicVertex dynamicVertex = ((AtlasJanusVertex) vertex).getDynamicVertex();
        assertNotNull(dynamicVertex);
        assertEquals("Integration Test", dynamicVertex.getProperty("name", String.class));
        assertEquals(Long.valueOf(42), dynamicVertex.getProperty("count", Long.class));
        
        // Verify core properties in graph
        assertEquals("test-123", vertex.getProperty("__guid", String.class));
    }
    
    @Test
    public void testFeatureToggleBehavior() {
        // Test with feature disabled
        RequestContext.get().setIdOnlyGraphEnabled(false);
        AtlasVertex v1 = graph.addVertex();
        v1.setProperty("prop1", "value1");
        
        // Enable feature
        RequestContext.get().setIdOnlyGraphEnabled(true);
        AtlasVertex v2 = graph.addVertex();
        v2.setProperty("prop2", "value2");
        
        // Verify different storage behavior
        assertNull(((AtlasJanusVertex) v1).getDynamicVertex()
            .getProperty("prop1", String.class));
        assertEquals("value2", ((AtlasJanusVertex) v2).getDynamicVertex()
            .getProperty("prop2", String.class));
    }
}
```

## Key Test Scenarios

### 1. Property Type Handling

Test all supported property types:

```java
@Test
public void testAllPropertyTypes() {
    DynamicVertex vertex = new DynamicVertex();
    
    // Primitive types
    vertex.setProperty("stringProp", "text");
    vertex.setProperty("longProp", "123456789");
    vertex.setProperty("intProp", "42");
    vertex.setProperty("floatProp", "3.14");
    vertex.setProperty("doubleProp", "2.71828");
    vertex.setProperty("booleanProp", true);
    
    // Collection types
    vertex.addSetProperty("setProp", "item1");
    vertex.addListProperty("listProp", "item1");
    
    // Complex types
    vertex.setProperty("mapProp", "{\"nested\":\"value\"}");
    
    // Verify conversions
    assertEquals("text", vertex.getProperty("stringProp", String.class));
    assertEquals(Long.valueOf(123456789), vertex.getProperty("longProp", Long.class));
    assertEquals(Integer.valueOf(42), vertex.getProperty("intProp", Integer.class));
    assertEquals(Float.valueOf(3.14f), vertex.getProperty("floatProp", Float.class));
    assertEquals(Double.valueOf(2.71828), vertex.getProperty("doubleProp", Double.class));
    assertEquals(true, vertex.getProperty("booleanProp", Boolean.class));
}
```

### 2. Bulk Operations Performance

```java
@Test
public void testBulkOperationPerformance() throws AtlasBaseException {
    int entityCount = 1000;
    Map<String, String> serializedVertices = new HashMap<>();
    
    // Generate test data
    for (int i = 0; i < entityCount; i++) {
        String vertexId = String.valueOf(1000000 + i);
        String jsonData = String.format(
            "{\"name\":\"Entity-%d\",\"index\":%d,\"timestamp\":%d}",
            i, i, System.currentTimeMillis()
        );
        serializedVertices.put(vertexId, jsonData);
    }
    
    // Measure insert performance
    long startTime = System.currentTimeMillis();
    repository.insertVertices(serializedVertices);
    long insertTime = System.currentTimeMillis() - startTime;
    
    // Verify performance meets requirements
    assertTrue("Bulk insert took " + insertTime + "ms", insertTime < 5000);
    
    // Measure fetch performance
    List<String> vertexIds = new ArrayList<>(serializedVertices.keySet());
    startTime = System.currentTimeMillis();
    Map<String, DynamicVertex> fetched = repository.fetchVerticesDirectly(vertexIds);
    long fetchTime = System.currentTimeMillis() - startTime;
    
    assertTrue("Bulk fetch took " + fetchTime + "ms", fetchTime < 2000);
    assertEquals(entityCount, fetched.size());
}
```

### 3. Error Handling and Recovery

```java
@Test
public void testErrorHandling() {
    // Test handling of invalid vertex IDs
    try {
        repository.calculateBucket("non-numeric-id");
        fail("Should throw exception for non-numeric ID");
    } catch (NumberFormatException expected) {
        // Expected
    }
    
    // Test handling of malformed JSON
    String malformedJson = "{\"incomplete\": ";
    DynamicVertex vertex = serializer.deserialize(malformedJson);
    assertNull(vertex); // Should return null, not throw
    
    // Test Cassandra connection failure
    when(session.execute(anyString()))
        .thenThrow(new NoNodeAvailableException());
    
    try {
        repository.insertVertices(Collections.singletonMap("123", "{}"));
        fail("Should throw AtlasBaseException");
    } catch (AtlasBaseException expected) {
        assertTrue(expected.getMessage().contains("Failed to insert"));
    }
}
```

## Testing Best Practices

### 1. Use RequestContext Properly

Always set up and clean up RequestContext in tests:

```java
@Before
public void setupRequestContext() {
    RequestContext.clear();
    RequestContext.get().setUser("test-user");
}

@After
public void cleanupRequestContext() {
    RequestContext.clear();
}
```

### 2. Test Both Modes

Always test with ID-only mode both enabled and disabled:

```java
@Test
public void testFeatureCompatibility() {
    // Test with feature off
    testScenarioWithFeature(false);
    
    // Test with feature on
    testScenarioWithFeature(true);
}

private void testScenarioWithFeature(boolean enabled) {
    RequestContext.get().setIdOnlyGraphEnabled(enabled);
    // ... test logic
}
```

### 3. Verify Property Routing

Ensure properties are stored in the correct location:

```java
private void verifyPropertyStorage(AtlasVertex vertex, 
                                 String propertyName, 
                                 Object value,
                                 boolean expectInDynamicVertex) {
    if (expectInDynamicVertex) {
        DynamicVertex dv = ((AtlasJanusVertex) vertex).getDynamicVertex();
        assertEquals(value, dv.getProperty(propertyName, value.getClass()));
    } else {
        assertEquals(value, vertex.getProperty(propertyName, value.getClass()));
    }
}
```

### 4. Test Data Consistency

Verify data remains consistent across operations:

```java
@Test
public void testDataConsistency() {
    String guid = "test-guid-123";
    
    // Create vertex with properties
    AtlasVertex vertex = createTestVertex(guid);
    
    // Serialize to Cassandra format
    DynamicVertex dynamicVertex = ((AtlasJanusVertex) vertex).getDynamicVertex();
    String serialized = serializer.serialize(dynamicVertex);
    
    // Deserialize and verify
    DynamicVertex deserialized = serializer.deserialize(serialized);
    
    // Verify all properties preserved
    for (String key : dynamicVertex.getPropertyKeys()) {
        Object original = dynamicVertex.getProperty(key, Object.class);
        Object restored = deserialized.getProperty(key, Object.class);
        assertEquals("Property mismatch: " + key, original, restored);
    }
}
```

## Test Data Management

### Creating Test Vertices

```java
public class TestDataFactory {
    
    public static AtlasVertex createTestVertex(AtlasGraph graph, String guid) {
        RequestContext.get().setIdOnlyGraphEnabled(true);
        
        AtlasVertex vertex = graph.addVertex();
        vertex.setProperty("__guid", guid);
        vertex.setProperty("__typeName", "TestType");
        vertex.setProperty("name", "Test-" + guid);
        vertex.setProperty("created", System.currentTimeMillis());
        vertex.setProperty("version", 1);
        
        // Add collections
        vertex.addSetProperty("tags", "tag1");
        vertex.addSetProperty("tags", "tag2");
        vertex.addListProperty("history", "created");
        vertex.addListProperty("history", "modified");
        
        return vertex;
    }
    
    public static Map<String, String> generateBulkVertexData(int count) {
        Map<String, String> data = new HashMap<>();
        
        for (int i = 0; i < count; i++) {
            String vertexId = String.valueOf(1000000 + i);
            DynamicVertex vertex = new DynamicVertex();
            
            vertex.setProperty("__guid", "guid-" + i);
            vertex.setProperty("__typeName", "BulkTestType");
            vertex.setProperty("name", "Bulk-Entity-" + i);
            vertex.setProperty("index", String.valueOf(i));
            vertex.setProperty("timestamp", String.valueOf(System.currentTimeMillis()));
            
            data.put(vertexId, new JacksonVertexSerializer().serialize(vertex));
        }
        
        return data;
    }
}
```

### Cleaning Test Data

```java
@After
public void cleanupTestData() throws AtlasBaseException {
    if (testVertexIds != null && !testVertexIds.isEmpty()) {
        // Clean up Cassandra data
        repository.dropVertices(testVertexIds);
        
        // Clean up graph data
        for (String id : testVertexIds) {
            AtlasVertex vertex = graph.getVertex(id);
            if (vertex != null) {
                graph.removeVertex(vertex);
            }
        }
        
        testVertexIds.clear();
    }
}
```

## Conclusion

Testing the ID-only implementation requires a comprehensive approach covering unit tests for individual components, integration tests for end-to-end flows, and performance tests for scalability. Focus on verifying correct property routing, type conversions, and feature toggle behavior while ensuring data consistency and proper error handling.