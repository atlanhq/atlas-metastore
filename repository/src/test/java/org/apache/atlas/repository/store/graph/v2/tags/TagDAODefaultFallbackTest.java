package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the DEFAULT fallback implementation of TagDAO.getAllClassificationsForVertices().
 * This default method is used when a TagDAO implementation does not override the batch method.
 *
 * NOTE: In production, TagDAOCassandraImpl overrides this with async parallel Cassandra queries.
 * These tests validate the sequential fallback behavior only.
 */
class TagDAODefaultFallbackTest {

    /**
     * Test: default method delegates to getAllClassificationsForVertex for each vertex.
     */
    @Test
    void testDefaultDelegatesToPerVertexCall() throws AtlasBaseException {
        TagDAO dao = createStubDAO(vertexId -> {
            if ("v1".equals(vertexId)) return Arrays.asList(new AtlasClassification("TAG_A"));
            if ("v2".equals(vertexId)) return Collections.emptyList();
            return Collections.emptyList();
        });

        Map<String, List<AtlasClassification>> result = dao.getAllClassificationsForVertices(Arrays.asList("v1", "v2"));

        assertEquals(2, result.size());
        assertEquals(1, result.get("v1").size());
        assertEquals("TAG_A", result.get("v1").get(0).getTypeName());
        assertTrue(result.get("v2").isEmpty());
    }

    /**
     * Test: default method returns empty map for null input.
     */
    @Test
    void testDefaultReturnsEmptyForNull() throws AtlasBaseException {
        TagDAO dao = createStubDAO(vertexId -> Collections.emptyList());

        Map<String, List<AtlasClassification>> result = dao.getAllClassificationsForVertices(null);

        assertTrue(result.isEmpty());
    }

    /**
     * Test: default method returns empty map for empty input.
     */
    @Test
    void testDefaultReturnsEmptyForEmptyCollection() throws AtlasBaseException {
        TagDAO dao = createStubDAO(vertexId -> {
            fail("Should not be called for empty input");
            return null;
        });

        Map<String, List<AtlasClassification>> result = dao.getAllClassificationsForVertices(Collections.emptyList());

        assertTrue(result.isEmpty());
    }

    /**
     * Test: default method propagates exception from per-vertex call.
     */
    @Test
    void testDefaultPropagatesException() {
        TagDAO dao = createStubDAO(vertexId -> {
            throw new AtlasBaseException("Cassandra timeout");
        });

        assertThrows(AtlasBaseException.class, () ->
                dao.getAllClassificationsForVertices(Arrays.asList("v1")));
    }

    /**
     * Test: default method handles duplicate vertex IDs (last write wins in map).
     */
    @Test
    void testDefaultHandlesDuplicateVertexIds() throws AtlasBaseException {
        int[] callCount = {0};
        TagDAO dao = createStubDAO(vertexId -> {
            callCount[0]++;
            return Arrays.asList(new AtlasClassification("TAG_" + callCount[0]));
        });

        Map<String, List<AtlasClassification>> result = dao.getAllClassificationsForVertices(Arrays.asList("v1", "v1"));

        // Map has 1 entry (duplicate key), second call overwrites first
        assertEquals(1, result.size());
        assertNotNull(result.get("v1"));
        assertEquals(2, callCount[0], "getAllClassificationsForVertex should be called twice even for duplicate IDs");
    }

    // =================== getClassificationNamesForVertex default tests ===================

    /**
     * Test: default getClassificationNamesForVertex with null (all) delegates to getAllClassificationsForVertex
     * and extracts type names.
     */
    @Test
    void testDefaultGetClassificationNamesForVertexAll() throws AtlasBaseException {
        TagDAO dao = createStubDAOWithDirectAndPropagated(
                Arrays.asList(new AtlasClassification("DIRECT_TAG")),
                Arrays.asList(new AtlasClassification("PROPAGATED_TAG")),
                Arrays.asList(new AtlasClassification("DIRECT_TAG"), new AtlasClassification("PROPAGATED_TAG"))
        );

        List<String> names = dao.getClassificationNamesForVertex("v1", null);
        assertEquals(2, names.size());
        assertTrue(names.contains("DIRECT_TAG"));
        assertTrue(names.contains("PROPAGATED_TAG"));
    }

    /**
     * Test: default getClassificationNamesForVertex with false returns only direct tag names.
     */
    @Test
    void testDefaultGetClassificationNamesForVertexDirectOnly() throws AtlasBaseException {
        TagDAO dao = createStubDAOWithDirectAndPropagated(
                Arrays.asList(new AtlasClassification("DIRECT_TAG")),
                Arrays.asList(new AtlasClassification("PROPAGATED_TAG")),
                Arrays.asList(new AtlasClassification("DIRECT_TAG"), new AtlasClassification("PROPAGATED_TAG"))
        );

        List<String> names = dao.getClassificationNamesForVertex("v1", false);
        assertEquals(1, names.size());
        assertEquals("DIRECT_TAG", names.get(0));
    }

    /**
     * Test: default getClassificationNamesForVertex with true returns only propagated tag names.
     */
    @Test
    void testDefaultGetClassificationNamesForVertexPropagatedOnly() throws AtlasBaseException {
        TagDAO dao = createStubDAOWithDirectAndPropagated(
                Arrays.asList(new AtlasClassification("DIRECT_TAG")),
                Arrays.asList(new AtlasClassification("PROPAGATED_TAG")),
                Arrays.asList(new AtlasClassification("DIRECT_TAG"), new AtlasClassification("PROPAGATED_TAG"))
        );

        List<String> names = dao.getClassificationNamesForVertex("v1", true);
        assertEquals(1, names.size());
        assertEquals("PROPAGATED_TAG", names.get(0));
    }

    // =================== Helper ===================

    @FunctionalInterface
    interface VertexClassificationProvider {
        List<AtlasClassification> get(String vertexId) throws AtlasBaseException;
    }

    /**
     * Creates a minimal TagDAO stub that only implements getAllClassificationsForVertex.
     * All other methods throw UnsupportedOperationException.
     * This lets us test the default method in isolation.
     */
    private TagDAO createStubDAO(VertexClassificationProvider provider) {
        return new TagDAO() {
            @Override
            public List<AtlasClassification> getAllClassificationsForVertex(String vertexId) throws AtlasBaseException {
                return provider.get(vertexId);
            }

            @Override public List<AtlasClassification> getAllDirectClassificationsForVertex(String vertexId) { throw new UnsupportedOperationException(); }
            @Override public List<Tag> getAllTagsByVertexId(String vertexId) { throw new UnsupportedOperationException(); }
            @Override public AtlasClassification findDirectDeletedTagByVertexIdAndTagTypeName(String v, String t) { throw new UnsupportedOperationException(); }
            @Override public Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String v, String t, boolean d) { throw new UnsupportedOperationException(); }
            @Override public PaginatedTagResult getPropagationsForAttachmentBatch(String s, String t, String p) { throw new UnsupportedOperationException(); }
            @Override public List<AtlasClassification> findByVertexIdAndPropagated(String v) { throw new UnsupportedOperationException(); }
            @Override public AtlasClassification findDirectTagByVertexIdAndTagTypeName(String v, String t, boolean d) { throw new UnsupportedOperationException(); }
            @Override public void putPropagatedTags(String s, String t, Set<String> p, Map<String, Map<String, Object>> m, AtlasClassification tag) { throw new UnsupportedOperationException(); }
            @Override public void putDirectTag(String a, String t, AtlasClassification tag, Map<String, Object> m) { throw new UnsupportedOperationException(); }
            @Override public void deleteDirectTag(String s, AtlasClassification t) { throw new UnsupportedOperationException(); }
            @Override public void deleteTags(List<Tag> t) { throw new UnsupportedOperationException(); }
            @Override public List<AtlasClassification> getPropagationsForAttachment(String v, String s) { throw new UnsupportedOperationException(); }
            @Override public PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String s, String t, String p, int ps) { throw new UnsupportedOperationException(); }
            @Override public PaginatedVertexIdResult getVertexIdFromTagsByIdTableWithPagination(String p, int ps) { throw new UnsupportedOperationException(); }
        };
    }

    /**
     * Creates a TagDAO stub that supports getAllClassificationsForVertex, getAllDirectClassificationsForVertex,
     * and findByVertexIdAndPropagated (needed for getClassificationNamesForVertex default method).
     */
    private TagDAO createStubDAOWithDirectAndPropagated(
            List<AtlasClassification> directTags,
            List<AtlasClassification> propagatedTags,
            List<AtlasClassification> allTags) {
        return new TagDAO() {
            @Override public List<AtlasClassification> getAllClassificationsForVertex(String vertexId) { return allTags; }
            @Override public List<AtlasClassification> getAllDirectClassificationsForVertex(String vertexId) { return directTags; }
            @Override public List<AtlasClassification> findByVertexIdAndPropagated(String vertexId) { return propagatedTags; }

            @Override public List<Tag> getAllTagsByVertexId(String vertexId) { throw new UnsupportedOperationException(); }
            @Override public AtlasClassification findDirectDeletedTagByVertexIdAndTagTypeName(String v, String t) { throw new UnsupportedOperationException(); }
            @Override public Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String v, String t, boolean d) { throw new UnsupportedOperationException(); }
            @Override public PaginatedTagResult getPropagationsForAttachmentBatch(String s, String t, String p) { throw new UnsupportedOperationException(); }
            @Override public AtlasClassification findDirectTagByVertexIdAndTagTypeName(String v, String t, boolean d) { throw new UnsupportedOperationException(); }
            @Override public void putPropagatedTags(String s, String t, Set<String> p, Map<String, Map<String, Object>> m, AtlasClassification tag) { throw new UnsupportedOperationException(); }
            @Override public void putDirectTag(String a, String t, AtlasClassification tag, Map<String, Object> m) { throw new UnsupportedOperationException(); }
            @Override public void deleteDirectTag(String s, AtlasClassification t) { throw new UnsupportedOperationException(); }
            @Override public void deleteTags(List<Tag> t) { throw new UnsupportedOperationException(); }
            @Override public List<AtlasClassification> getPropagationsForAttachment(String v, String s) { throw new UnsupportedOperationException(); }
            @Override public PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String s, String t, String p, int ps) { throw new UnsupportedOperationException(); }
            @Override public PaginatedVertexIdResult getVertexIdFromTagsByIdTableWithPagination(String p, int ps) { throw new UnsupportedOperationException(); }
        };
    }
}
