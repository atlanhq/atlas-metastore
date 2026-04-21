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
 * Tests for MergedEdgeIterable.
 * Covers: buffer-only, persisted-only, mixed, duplicates, removals, empty, single-use.
 */
public class MergedEdgeIterableTest {

    private CassandraGraph graph;
    private TransactionBuffer buffer;

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
        buffer = new TransactionBuffer();
    }

    // ======================== Empty ========================

    @Test
    public void testBothEmpty() {
        List<CassandraEdge> buffered = Collections.emptyList();
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(Collections.emptyList());

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertTrue(ids.isEmpty());
    }

    // ======================== Buffer Only ========================

    @Test
    public void testBufferOnlyEdges() {
        List<CassandraEdge> buffered = List.of(
            edge("e1", "v1", "v2", "knows"),
            edge("e2", "v1", "v3", "likes")
        );
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(Collections.emptyList());

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertEquals(ids.size(), 2);
        assertEquals(ids.get(0), "e1");
        assertEquals(ids.get(1), "e2");
    }

    // ======================== Persisted Only ========================

    @Test
    public void testPersistedOnlyEdges() {
        List<CassandraEdge> buffered = Collections.emptyList();
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(List.of(
            edge("e3", "v1", "v4", "owns"),
            edge("e4", "v1", "v5", "owns")
        ));

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertEquals(ids.size(), 2);
        assertEquals(ids.get(0), "e3");
        assertEquals(ids.get(1), "e4");
    }

    // ======================== Mixed — Buffered First Then Persisted ========================

    @Test
    public void testMixedBufferedThenPersisted() {
        List<CassandraEdge> buffered = List.of(edge("e1", "v1", "v2", "knows"));
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(List.of(
            edge("e2", "v1", "v3", "likes")
        ));

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertEquals(ids.size(), 2);
        assertEquals(ids.get(0), "e1"); // buffered first
        assertEquals(ids.get(1), "e2"); // then persisted
    }

    // ======================== Duplicate Override ========================

    @Test
    public void testBufferedEdgeOverridesPersisted() {
        CassandraEdge bufferedEdge = edge("e1", "v1", "v2", "knows");
        CassandraEdge persistedEdge = edge("e1", "v1", "v2", "knows"); // same ID

        List<CassandraEdge> buffered = List.of(bufferedEdge);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(List.of(persistedEdge));

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        // Only 1 edge — the persisted duplicate is skipped
        assertEquals(ids.size(), 1);
        assertEquals(ids.get(0), "e1");
    }

    // ======================== Removed Edges Filtered ========================

    @Test
    public void testRemovedEdgesFilteredFromPersisted() {
        CassandraEdge removedEdge = edge("e1", "v1", "v2", "knows");
        buffer.removeEdge(removedEdge);

        List<CassandraEdge> buffered = Collections.emptyList();
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(List.of(
            edge("e1", "v1", "v2", "knows"),  // should be filtered
            edge("e2", "v1", "v3", "likes")   // should pass
        ));

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertEquals(ids.size(), 1);
        assertEquals(ids.get(0), "e2");
    }

    // ======================== Complex Scenario ========================

    @Test
    public void testComplexMerge() {
        CassandraEdge removedEdge = edge("e4", "v1", "v5", "has");
        buffer.removeEdge(removedEdge);

        List<CassandraEdge> buffered = List.of(
            edge("e1", "v1", "v2", "knows"),
            edge("e3", "v1", "v4", "owns")
        );
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(List.of(
            edge("e2", "v1", "v3", "likes"),
            edge("e3", "v1", "v4", "owns"),  // duplicate of buffered — skipped
            edge("e4", "v1", "v5", "has")    // removed — skipped
        ));

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);
        List<String> ids = collectEdgeIds(merged);

        assertEquals(ids.size(), 3);
        assertEquals(ids.get(0), "e1"); // buffered
        assertEquals(ids.get(1), "e3"); // buffered (overrides persisted)
        assertEquals(ids.get(2), "e2"); // persisted (not duplicate, not removed)
    }

    // ======================== Re-Iterability ========================

    @Test
    public void testMultipleIteratorCallsAllowed() {
        List<CassandraEdge> buffered = List.of(edge("e1", "v1", "v2", "knows"));
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persisted = wrapEdges(Collections.emptyList());

        MergedEdgeIterable merged = new MergedEdgeIterable(buffered, persisted, buffer);

        // first iteration
        List<String> ids1 = collectEdgeIds(merged);
        assertEquals(ids1.size(), 1);
        assertEquals(ids1.get(0), "e1");

        // second iteration — should work, not throw
        List<String> ids2 = collectEdgeIds(merged);
        assertEquals(ids2.size(), 1);
        assertEquals(ids2.get(0), "e1");
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
