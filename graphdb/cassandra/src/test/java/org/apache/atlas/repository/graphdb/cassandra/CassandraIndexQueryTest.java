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

    // ======================== ES doc ID decoding ========================

    @Test
    public void testDecodeDocIdBase62() {
        // JanusGraph encodes vertex ID 348 → base-62 string "5u"
        // 5*62 + 56 = 310 + 56... wait, let me compute: 'u' is index 30 in base-62
        // Actually, let's just verify the round-trip: encode a long, then decode it
        // JanusGraph LongEncoding: 256 → "48" (4*62 + 8 = 256)
        assertEquals(CassandraIndexQuery.decodeDocId("48"), "256");
    }

    @Test
    public void testDecodeDocIdSmallNumbers() {
        // Single digit base-62: '0'=0, '1'=1, 'a'=10, 'z'=35, 'A'=36, 'Z'=61
        assertEquals(CassandraIndexQuery.decodeDocId("0"), "0");
        assertEquals(CassandraIndexQuery.decodeDocId("1"), "1");
        assertEquals(CassandraIndexQuery.decodeDocId("a"), "10");
        assertEquals(CassandraIndexQuery.decodeDocId("z"), "35");
        assertEquals(CassandraIndexQuery.decodeDocId("A"), "36");
        assertEquals(CassandraIndexQuery.decodeDocId("Z"), "61");
    }

    @Test
    public void testDecodeDocIdMultiDigit() {
        // "10" in base-62 = 1*62 + 0 = 62
        assertEquals(CassandraIndexQuery.decodeDocId("10"), "62");
        // "11" in base-62 = 1*62 + 1 = 63
        assertEquals(CassandraIndexQuery.decodeDocId("11"), "63");
    }

    @Test
    public void testDecodeDocIdUuidPassesThrough() {
        // UUID strings contain hyphens — should pass through unchanged
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        assertEquals(CassandraIndexQuery.decodeDocId(uuid), uuid);
    }

    @Test
    public void testDecodeDocIdNullAndEmpty() {
        assertNull(CassandraIndexQuery.decodeDocId(null));
        assertEquals(CassandraIndexQuery.decodeDocId(""), "");
    }

    @Test
    public void testDecodeDocIdInvalidCharPassesThrough() {
        // Characters not in base-62 (like special chars) cause pass-through
        assertEquals(CassandraIndexQuery.decodeDocId("abc!def"), "abc!def");
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
