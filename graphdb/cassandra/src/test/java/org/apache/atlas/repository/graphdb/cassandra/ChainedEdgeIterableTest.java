package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for ChainedEdgeIterable.
 * Covers: single label, multiple labels, empty labels, mixed empty/non-empty labels.
 */
public class ChainedEdgeIterableTest {

    private CassandraGraph graph;

    @BeforeMethod
    public void setUp() {
        CqlSession mockSession = mock(CqlSession.class);
        PreparedStatement mockPrepared = mock(PreparedStatement.class);
        BoundStatement mockBound = mock(BoundStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(mockSession.prepare(anyString())).thenReturn(mockPrepared);
        when(mockPrepared.bind(any())).thenReturn(mockBound);
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(null);

        graph = new CassandraGraph(mockSession);
    }

    // ======================== Single Label ========================

    @Test
    public void testSingleLabel() {
        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{"knows"},
            label -> wrapEdges(List.of(
                edge("e1", "v1", "v2", label),
                edge("e2", "v1", "v3", label)
            ))
        );

        List<String> ids = collectEdgeIds(iterable);
        assertEquals(ids.size(), 2);
        assertEquals(ids.get(0), "e1");
        assertEquals(ids.get(1), "e2");
    }

    // ======================== Multiple Labels ========================

    @Test
    public void testMultipleLabels() {
        Map<String, List<CassandraEdge>> edgesByLabel = new LinkedHashMap<>();
        edgesByLabel.put("knows", List.of(edge("e1", "v1", "v2", "knows")));
        edgesByLabel.put("likes", List.of(edge("e2", "v1", "v3", "likes"), edge("e3", "v1", "v4", "likes")));
        edgesByLabel.put("owns", List.of(edge("e4", "v1", "v5", "owns")));

        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{"knows", "likes", "owns"},
            label -> wrapEdges(edgesByLabel.getOrDefault(label, Collections.emptyList()))
        );

        List<String> ids = collectEdgeIds(iterable);
        assertEquals(ids.size(), 4);
        assertEquals(ids.get(0), "e1"); // from "knows"
        assertEquals(ids.get(1), "e2"); // from "likes"
        assertEquals(ids.get(2), "e3"); // from "likes"
        assertEquals(ids.get(3), "e4"); // from "owns"
    }

    // ======================== Empty Labels Array ========================

    @Test
    public void testEmptyLabelsArray() {
        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{},
            label -> { throw new IllegalStateException("Should not be called"); }
        );

        List<String> ids = collectEdgeIds(iterable);
        assertTrue(ids.isEmpty());
    }

    // ======================== Label With No Edges ========================

    @Test
    public void testLabelWithNoEdges() {
        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{"empty1", "hasEdges", "empty2"},
            label -> {
                if ("hasEdges".equals(label)) {
                    return wrapEdges(List.of(edge("e1", "v1", "v2", label)));
                }
                return wrapEdges(Collections.emptyList());
            }
        );

        List<String> ids = collectEdgeIds(iterable);
        assertEquals(ids.size(), 1);
        assertEquals(ids.get(0), "e1");
    }

    // ======================== All Labels Empty ========================

    @Test
    public void testAllLabelsEmpty() {
        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{"a", "b", "c"},
            label -> wrapEdges(Collections.emptyList())
        );

        List<String> ids = collectEdgeIds(iterable);
        assertTrue(ids.isEmpty());
    }

    // ======================== Re-iterable (iterator() called multiple times) ========================

    @Test
    public void testMultipleIteratorCalls() {
        ChainedEdgeIterable iterable = new ChainedEdgeIterable(
            new String[]{"knows"},
            label -> wrapEdges(List.of(edge("e1", "v1", "v2", label)))
        );

        // First iteration
        List<String> ids1 = collectEdgeIds(iterable);
        assertEquals(ids1.size(), 1);

        // Second iteration — ChainedEdgeIterable does NOT enforce single-use
        // because it creates fresh per-label iterables each time
        List<String> ids2 = collectEdgeIds(iterable);
        assertEquals(ids2.size(), 1);
    }

    // ======================== Helpers ========================

    private CassandraEdge edge(String id, String outV, String inV, String label) {
        return new CassandraEdge(id, outV, inV, label, graph);
    }

    @SuppressWarnings("unchecked")
    private Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> wrapEdges(List<CassandraEdge> edges) {
        List<AtlasEdge<CassandraVertex, CassandraEdge>> wrapped = new ArrayList<>();
        for (CassandraEdge e : edges) {
            wrapped.add((AtlasEdge) e);
        }
        return wrapped;
    }

    private List<String> collectEdgeIds(Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> iterable) {
        List<String> ids = new ArrayList<>();
        for (AtlasEdge<CassandraVertex, CassandraEdge> edge : iterable) {
            ids.add(((CassandraEdge) edge).getIdString());
        }
        return ids;
    }
}
