package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraIndexQuery.
 * Tests construction and null-client fallback paths.
 * (ES integration tests require a running ES instance and are separate.)
 */
public class CassandraIndexQueryTest {

    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== Construction ========================

    @Test
    public void testConstructWithQueryString() {
        // ES clients will be null (AtlasElasticsearchDatabase not initialized in test)
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{\"query\":{\"match_all\":{}}}", 0);
        assertNotNull(query);
    }

    @Test
    public void testConstructWithOffset() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 10);
        assertNotNull(query);
    }

    @Test
    public void testConstructWithNullSourceBuilder() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        assertNotNull(query);
    }

    // ======================== Vertices with null ES client ========================

    @Test
    public void testVerticesReturnsEmptyWhenNoESClient() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result = query.vertices();
        assertFalse(result.hasNext());
    }

    @Test
    public void testVerticesWithOffsetAndLimitReturnsEmpty() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result = query.vertices(0, 10);
        assertFalse(result.hasNext());
    }

    @Test
    public void testVerticesWithSortReturnsEmpty() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result =
                query.vertices(0, 10, "name", AtlasIndexQuery.SortOrder.ASC);
        assertFalse(result.hasNext());
    }

    // ======================== vertexTotals ========================

    @Test
    public void testVertexTotalsDefaultMinusOne() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        assertEquals(query.vertexTotals().longValue(), -1L);
    }

    // ======================== directIndexQuery when ES unreachable ========================
    // AtlasElasticsearchDatabase initializes a real REST client from atlas-application.properties,
    // so lowLevelRestClient is non-null. Calls fail with AtlasBaseException since no ES is running.

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testDirectIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.directIndexQuery("{\"query\":{\"match_all\":{}}}");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testDirectEsIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.directEsIndexQuery("{\"query\":{\"match_all\":{}}}");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testCountIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.countIndexQuery("{\"query\":{\"match_all\":{}}}");
    }
}
