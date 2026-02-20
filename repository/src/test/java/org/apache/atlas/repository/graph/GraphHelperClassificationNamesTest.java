package org.apache.atlas.repository.graph;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for GraphHelper.getClassificationNamesFromVertex() and getPropagatedClassificationNamesFromVertex().
 * These methods read classification names directly from JanusGraph vertex properties,
 * avoiding Cassandra queries entirely.
 */
class GraphHelperClassificationNamesTest {

    @Test
    void testGetClassificationNamesFromVertex() {
        AtlasVertex vertex = mockVertexWithProperty(CLASSIFICATION_NAMES_KEY, "|PII|Confidential|Internal|");

        List<String> names = GraphHelper.getClassificationNamesFromVertex(vertex);

        assertEquals(3, names.size());
        assertEquals("PII", names.get(0));
        assertEquals("Confidential", names.get(1));
        assertEquals("Internal", names.get(2));
    }

    @Test
    void testGetClassificationNamesFromVertexNull() {
        AtlasVertex vertex = mockVertexWithProperty(CLASSIFICATION_NAMES_KEY, null);

        List<String> names = GraphHelper.getClassificationNamesFromVertex(vertex);

        assertNotNull(names);
        assertTrue(names.isEmpty());
    }

    @Test
    void testGetTraitNamesV2DirectOnly() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(CLASSIFICATION_NAMES_KEY, String.class)).thenReturn("|PII|Confidential|");
        when(vertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class)).thenReturn(null);

        List<String> names = GraphHelper.getTraitNamesV2(vertex, false);

        assertEquals(2, names.size());
        assertEquals("PII", names.get(0));
        assertEquals("Confidential", names.get(1));
    }

    @Test
    void testGetTraitNamesV2PropagatedOnly() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(CLASSIFICATION_NAMES_KEY, String.class)).thenReturn(null);
        when(vertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class)).thenReturn("|Sensitive|");

        List<String> names = GraphHelper.getTraitNamesV2(vertex, true);

        assertEquals(1, names.size());
        assertEquals("Sensitive", names.get(0));
    }

    @Test
    void testGetTraitNamesV2AllCombined() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(CLASSIFICATION_NAMES_KEY, String.class)).thenReturn("|PII|");
        when(vertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class)).thenReturn("|Sensitive|");

        List<String> names = GraphHelper.getTraitNamesV2(vertex, null);

        assertEquals(2, names.size());
        assertTrue(names.contains("PII"));
        assertTrue(names.contains("Sensitive"));
    }

    private AtlasVertex mockVertexWithProperty(String propertyKey, String value) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(propertyKey, String.class)).thenReturn(value);
        return vertex;
    }
}
