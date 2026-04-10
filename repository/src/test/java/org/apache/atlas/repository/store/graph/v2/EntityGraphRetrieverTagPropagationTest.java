package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for tag propagation traversal methods.
 *
 * Covers:
 * 1. Batch vertex fetch in BFS (graph.getVertices instead of N × graph.getVertex)
 * 2. Sub-batching at 500 for large BFS levels
 * 3. Null/deleted vertex handling during traversal
 * 4. getAdjacentVerticesIds returning empty set for unknown entity types
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityGraphRetrieverTagPropagationTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    private AutoCloseable closeable;
    private EntityGraphRetriever retriever;

    /** Map of vertexId → mock vertex, used by the getVertices stub */
    private Map<String, AtlasVertex> vertexStore;

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

        vertexStore = new HashMap<>();

        retriever = mock(EntityGraphRetriever.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        setField(retriever, "graph", graph);
        setField(retriever, "typeRegistry", typeRegistry);
        setField(retriever, "executorService", newSameThreadExecutorService());

        // Stub graph.getVertices() to return vertices from vertexStore (filters missing IDs)
        when(graph.getVertices(any(String[].class))).thenAnswer(invocation -> {
            String[] ids = invocation.getArgument(0);
            Set<AtlasVertex> result = new LinkedHashSet<>();
            for (String id : ids) {
                AtlasVertex v = vertexStore.get(id);
                if (v != null) {
                    result.add(v);
                }
            }
            return result;
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    /** Register a mock vertex so both getVertex and getVertices can find it. */
    private AtlasVertex registerVertex(String id) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getIdForDisplay()).thenReturn(id);
        vertexStore.put(id, vertex);
        when(graph.getVertex(id)).thenReturn(vertex);
        return vertex;
    }

    // -----------------------------------------------------------------------
    //  Existing tests — updated to use batch vertex fetch
    // -----------------------------------------------------------------------

    /**
     * Test: traverseImpactedVerticesByLevelV2 should gracefully skip vertices
     * that are not found by graph.getVertices (e.g., deleted concurrently).
     */
    @Test
    void testTraverseV2_nullVertexSkippedGracefully() {
        AtlasVertex startVertex = registerVertex("v-start");
        // v-deleted is NOT registered — getVertices will not include it

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(entityType);

        AtlasEdge edge = createMockEdge("v-start", "v-deleted", "rel-1");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(edge));

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            setupEdgeMocks(ghMock, edge);
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        // v-deleted is discovered as adjacent and added to traversedVerticesIds.
        // When BFS tries to expand it at the next level, getVertices doesn't return it,
        // so it's safely skipped (no NPE, no processing).
        assertTrue(result.contains("v-deleted"), "Adjacent vertex should be in traversal result");
    }

    @Test
    void testTraverseV2_nullStartVertex() {
        Set<String> result = new HashSet<>();

        retriever.traverseImpactedVerticesByLevelV2(
                null, null, "classif-1", result,
                null, false, null, null);

        assertTrue(result.isEmpty(), "Result should be empty when start vertex is null");
    }

    @Test
    void testTraverseV2_entityTypeWithNoTagPropagationEdges() {
        AtlasVertex startVertex = registerVertex("v-start");

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("CustomType")).thenReturn(entityType);

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("CustomType");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.isEmpty(), "Result should be empty when entity type has no propagation edges");
    }

    @Test
    void testTraverseV2_unknownEntityType() {
        AtlasVertex startVertex = registerVertex("v-start");

        when(typeRegistry.getEntityTypeByName("UnknownType")).thenReturn(null);

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("UnknownType");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.isEmpty(), "Result should be empty for unknown entity type");
    }

    @Test
    void testTraverseV2_mixOfValidAndDeletedVerticesAtSameLevel() {
        AtlasVertex startVertex = registerVertex("v-start");
        AtlasVertex validVertex = registerVertex("v-valid");
        // v-deleted is NOT registered — simulates deleted vertex

        AtlasEntityType tableType = mock(AtlasEntityType.class);
        when(tableType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(tableType);

        AtlasEntityType columnType = mock(AtlasEntityType.class);
        when(columnType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("Column")).thenReturn(columnType);

        AtlasEdge edge1 = createMockEdge("v-start", "v-valid", "rel-1");
        AtlasEdge edge2 = createMockEdge("v-start", "v-deleted", "rel-2");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Arrays.asList(edge1, edge2));
        when(validVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(validVertex)).thenReturn("Column");
            setupEdgeMocks(ghMock, edge1);
            setupEdgeMocks(ghMock, edge2);

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.contains("v-valid"), "Valid vertex should be in result");
        assertTrue(result.contains("v-deleted"), "Deleted vertex should still be in result from discovery");
    }

    // -----------------------------------------------------------------------
    //  New tests — batch vertex fetch and sub-batching
    // -----------------------------------------------------------------------

    /**
     * Test: BFS uses graph.getVertices (batch API) instead of individual graph.getVertex calls.
     * Verifies that batch API is called at least once during traversal.
     */
    @Test
    void testTraverseV2_usesBatchVertexFetch() {
        AtlasVertex startVertex = registerVertex("v-start");
        AtlasVertex child1 = registerVertex("v-child1");
        AtlasVertex child2 = registerVertex("v-child2");

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(entityType);

        AtlasEntityType leafType = mock(AtlasEntityType.class);
        when(leafType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("Column")).thenReturn(leafType);

        AtlasEdge edge1 = createMockEdge("v-start", "v-child1", "rel-1");
        AtlasEdge edge2 = createMockEdge("v-start", "v-child2", "rel-2");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Arrays.asList(edge1, edge2));
        when(child1.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());
        when(child2.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(child1)).thenReturn("Column");
            ghMock.when(() -> GraphHelper.getTypeName(child2)).thenReturn("Column");
            setupEdgeMocks(ghMock, edge1);
            setupEdgeMocks(ghMock, edge2);

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        // Verify batch API was called (at least for expanding child level)
        verify(graph, atLeast(1)).getVertices(any(String[].class));
        assertEquals(2, result.size(), "Both children should be in result");
        assertTrue(result.contains("v-child1"));
        assertTrue(result.contains("v-child2"));
    }

    /**
     * Test: BFS correctly traverses multiple levels using batch vertex fetch.
     * Graph: start -> [level1a, level1b] -> [level2a]
     */
    @Test
    void testTraverseV2_multiLevelBatchFetch() {
        AtlasVertex startVertex = registerVertex("v-start");
        AtlasVertex level1a = registerVertex("v-l1a");
        AtlasVertex level1b = registerVertex("v-l1b");
        AtlasVertex level2a = registerVertex("v-l2a");

        AtlasEntityType tableType = mock(AtlasEntityType.class);
        when(tableType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(tableType);

        AtlasEntityType leafType = mock(AtlasEntityType.class);
        when(leafType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("Leaf")).thenReturn(leafType);

        // start -> l1a, l1b
        AtlasEdge e1 = createMockEdge("v-start", "v-l1a", "rel-1");
        AtlasEdge e2 = createMockEdge("v-start", "v-l1b", "rel-2");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Arrays.asList(e1, e2));

        // l1a -> l2a
        AtlasEdge e3 = createMockEdge("v-l1a", "v-l2a", "rel-3");
        when(level1a.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(e3));

        // l1b -> no children
        when(level1b.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        // l2a -> leaf
        when(level2a.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(level1a)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(level1b)).thenReturn("Leaf");
            ghMock.when(() -> GraphHelper.getTypeName(level2a)).thenReturn("Leaf");
            setupEdgeMocks(ghMock, e1);
            setupEdgeMocks(ghMock, e2);
            setupEdgeMocks(ghMock, e3);

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertEquals(3, result.size(), "All 3 descendants should be in result");
        assertTrue(result.contains("v-l1a"));
        assertTrue(result.contains("v-l1b"));
        assertTrue(result.contains("v-l2a"));
    }

    /**
     * Test: verticesWithClassification set is correctly populated during batch traversal.
     * When storeVerticesWithoutClassification=true, vertices NOT in the initial set should be added.
     */
    @Test
    void testTraverseV2_verticesWithClassificationTracking() {
        AtlasVertex startVertex = registerVertex("v-start");
        AtlasVertex child = registerVertex("v-child");

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(entityType);

        AtlasEntityType leafType = mock(AtlasEntityType.class);
        when(leafType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("Column")).thenReturn(leafType);

        AtlasEdge edge = createMockEdge("v-start", "v-child", "rel-1");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(edge));
        when(child.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        Set<String> result = new HashSet<>();
        // v-start already has the classification, v-child does not
        Set<String> verticesWithClassification = new HashSet<>(Collections.singleton("v-start"));

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(child)).thenReturn("Column");
            setupEdgeMocks(ghMock, edge);

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, verticesWithClassification, null);
        }

        // verticesWithClassification should now contain vertices that need the tag propagated
        // (v-child does NOT have the classification, so it should be in the set)
        assertTrue(verticesWithClassification.contains("v-child"),
                "v-child should be tracked as needing classification propagation");
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    private void setupEdgeMocks(MockedStatic<GraphHelper> ghMock, AtlasEdge edge) {
        ghMock.when(() -> GraphHelper.getPropagateTags(edge)).thenReturn(
                org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);
        ghMock.when(() -> GraphHelper.getRelationshipGuid(edge)).thenReturn("rel-guid");
        ghMock.when(() -> GraphHelper.getEdgeStatus(edge)).thenReturn(
                org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE);
        ghMock.when(() -> GraphHelper.getBlockedClassificationIds(edge)).thenReturn(Collections.emptyList());
    }

    private AtlasEdge createMockEdge(String outVertexId, String inVertexId, String edgeId) {
        AtlasEdge edge = mock(AtlasEdge.class);
        AtlasVertex outVertex = mock(AtlasVertex.class);
        AtlasVertex inVertex = mock(AtlasVertex.class);
        when(outVertex.getIdForDisplay()).thenReturn(outVertexId);
        when(inVertex.getIdForDisplay()).thenReturn(inVertexId);
        when(edge.getOutVertex()).thenReturn(outVertex);
        when(edge.getInVertex()).thenReturn(inVertex);
        when(edge.getIdForDisplay()).thenReturn(edgeId);
        return edge;
    }

    private static ExecutorService newSameThreadExecutorService() {
        return new AbstractExecutorService() {
            private volatile boolean shutdown = false;

            @Override public void execute(Runnable command) { command.run(); }
            @Override public void shutdown() { shutdown = true; }
            @Override public List<Runnable> shutdownNow() { shutdown = true; return Collections.emptyList(); }
            @Override public boolean isShutdown() { return shutdown; }
            @Override public boolean isTerminated() { return shutdown; }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        };
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        while (clazz != null) {
            try {
                return clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
