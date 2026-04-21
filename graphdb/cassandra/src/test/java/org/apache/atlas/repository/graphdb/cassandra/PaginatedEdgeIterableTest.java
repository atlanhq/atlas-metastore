package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for PaginatedEdgeIterable.
 * Covers: empty result set, single page, multi-page (simulated via row iterator),
 * row-to-edge mapping for OUT and IN directions, and single-use enforcement.
 */
public class PaginatedEdgeIterableTest {

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

    // ======================== Empty ResultSet ========================

    @Test
    public void testEmptyResultSet() {
        ResultSet rs = mockResultSet(Collections.emptyList());
        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);

        Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iter = iterable.iterator();
        assertFalse(iter.hasNext());
    }

    // ======================== Single Row — OUT direction ========================

    @Test
    public void testSingleRowOutDirection() {
        Row row = mockRow("e1", "label1", "v2", "{}", "ACTIVE");
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);
        Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iter = iterable.iterator();

        assertTrue(iter.hasNext());
        CassandraEdge edge = (CassandraEdge) iter.next();
        assertEquals(edge.getIdString(), "e1");
        assertEquals(edge.getLabel(), "label1");
        assertEquals(edge.getOutVertexId(), "v1");
        assertEquals(edge.getInVertexId(), "v2");

        assertFalse(iter.hasNext());
    }

    // ======================== Single Row — IN direction ========================

    @Test
    public void testSingleRowInDirection() {
        Row row = mockRow("e2", "label2", "v3", "{}", "ACTIVE");
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", false, graph);
        Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iter = iterable.iterator();

        assertTrue(iter.hasNext());
        CassandraEdge edge = (CassandraEdge) iter.next();
        assertEquals(edge.getOutVertexId(), "v3");
        assertEquals(edge.getInVertexId(), "v1");

        assertFalse(iter.hasNext());
    }

    // ======================== Multiple Rows ========================

    @Test
    public void testMultipleRows() {
        List<Row> rows = List.of(
            mockRow("e1", "labelA", "v2", "{}", "ACTIVE"),
            mockRow("e2", "labelA", "v3", "{}", "ACTIVE"),
            mockRow("e3", "labelB", "v4", "{}", "DELETED")
        );
        ResultSet rs = mockResultSet(rows);

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);

        List<String> edgeIds = new ArrayList<>();
        for (AtlasEdge<CassandraVertex, CassandraEdge> edge : iterable) {
            edgeIds.add(((CassandraEdge) edge).getIdString());
        }

        assertEquals(edgeIds.size(), 3);
        assertEquals(edgeIds.get(0), "e1");
        assertEquals(edgeIds.get(1), "e2");
        assertEquals(edgeIds.get(2), "e3");
    }

    // ======================== State Property Mapping ========================

    @Test
    public void testStatePropertyMapping() {
        Row row = mockRow("e1", "label1", "v2", "{}", "DELETED");
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);
        CassandraEdge edge = (CassandraEdge) iterable.iterator().next();

        assertEquals(edge.getProperty("__state", String.class), "DELETED");
    }

    @Test
    public void testNullStateDefaultsToActive() {
        Row row = mockRow("e1", "label1", "v2", "{}", null);
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);
        CassandraEdge edge = (CassandraEdge) iterable.iterator().next();

        assertEquals(edge.getProperty("__state", String.class), "ACTIVE");
    }

    // ======================== Re-Iterability ========================

    @Test
    public void testMultipleIteratorCallsAllowed() {
        Row row = mockRow("e1", "label1", "v2", "{}", "ACTIVE");
        // Supplier returns a fresh ResultSet each time
        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(
                () -> mockResultSet(List.of(row)), "v1", true, graph);

        // first iteration
        Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iter1 = iterable.iterator();
        assertTrue(iter1.hasNext());
        assertEquals(((CassandraEdge) iter1.next()).getIdString(), "e1");

        // second iteration — should work, not throw
        Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iter2 = iterable.iterator();
        assertTrue(iter2.hasNext());
        assertEquals(((CassandraEdge) iter2.next()).getIdString(), "e1");
    }

    // ======================== Properties Parsing ========================

    @Test
    public void testPropertiesParsedFromJson() {
        Row row = mockRow("e1", "label1", "v2", "{\"__typeName\":\"myRelType\",\"foo\":\"bar\"}", "ACTIVE");
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);
        CassandraEdge edge = (CassandraEdge) iterable.iterator().next();

        assertEquals(edge.getProperty("__typeName", String.class), "myRelType");
        assertEquals(edge.getProperty("foo", String.class), "bar");
    }

    @Test
    public void testNullPropertiesHandled() {
        Row row = mockRow("e1", "label1", "v2", null, "ACTIVE");
        ResultSet rs = mockResultSet(List.of(row));

        PaginatedEdgeIterable iterable = new PaginatedEdgeIterable(() -> rs, "v1", true, graph);
        CassandraEdge edge = (CassandraEdge) iterable.iterator().next();

        assertNotNull(edge);
        assertEquals(edge.getIdString(), "e1");
    }

    // ======================== Helpers ========================

    private Row mockRow(String edgeId, String label, String otherVertexId,
                        String propsJson, String state) {
        Row row = mock(Row.class);
        when(row.getString("edge_id")).thenReturn(edgeId);
        when(row.getString("edge_label")).thenReturn(label);
        when(row.getString("in_vertex_id")).thenReturn(otherVertexId);
        when(row.getString("out_vertex_id")).thenReturn(otherVertexId);
        when(row.getString("properties")).thenReturn(propsJson);
        when(row.getString("state")).thenReturn(state);
        return row;
    }

    private ResultSet mockResultSet(List<Row> rows) {
        ResultSet rs = mock(ResultSet.class);
        when(rs.iterator()).thenReturn(rows.iterator());
        return rs;
    }
}
