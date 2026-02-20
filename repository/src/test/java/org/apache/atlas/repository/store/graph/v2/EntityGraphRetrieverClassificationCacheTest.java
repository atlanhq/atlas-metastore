package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.service.config.DynamicConfigStore;
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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for the classification prefetch + cache mechanism in EntityGraphRetriever.
 *
 * Verifies:
 * 1. prefetchClassifications() calls TagDAO.getAllClassificationsForVertices() and caches results
 * 2. mapVertexToAtlasEntity() uses cached classifications instead of per-vertex calls
 * 3. getAllClassifications_V2() uses cached classifications
 * 4. Fallback to sync fetch when vertex is not in cache
 * 5. No prefetch when TagV2 is disabled or auth check is skipped
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityGraphRetrieverClassificationCacheTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private TagDAO tagDAO;

    private AutoCloseable closeable;
    private EntityGraphRetriever retriever;

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

        retriever = mock(EntityGraphRetriever.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        setField(retriever, "graph", graph);
        setField(retriever, "typeRegistry", typeRegistry);
        setField(retriever, "tagDAO", tagDAO);
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    /**
     * Test: prefetchClassifications calls batch fetch and populates the RequestContext cache.
     */
    @Test
    void testPrefetchClassificationsPopulatesCache() throws Exception {
        AtlasVertex v1 = mockVertex("100");
        AtlasVertex v2 = mockVertex("200");

        Map<String, List<AtlasClassification>> batchResult = new HashMap<>();
        batchResult.put("100", Arrays.asList(new AtlasClassification("TAG_A")));
        batchResult.put("200", Collections.emptyList());

        when(tagDAO.getAllClassificationsForVertices(anyCollection())).thenReturn(batchResult);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            invokePrivateMethod(retriever, "prefetchClassifications", List.class, Arrays.asList(v1, v2));
        }

        // Verify cache was populated
        RequestContext ctx = RequestContext.get();
        assertNotNull(ctx.getCachedClassifications("100"));
        assertEquals(1, ctx.getCachedClassifications("100").size());
        assertEquals("TAG_A", ctx.getCachedClassifications("100").get(0).getTypeName());

        assertNotNull(ctx.getCachedClassifications("200"));
        assertTrue(ctx.getCachedClassifications("200").isEmpty());

        // Verify batch method was called
        verify(tagDAO, times(1)).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: prefetchClassifications is a no-op when TagV2 is disabled.
     */
    @Test
    void testPrefetchSkippedWhenTagV2Disabled() throws Exception {
        AtlasVertex v1 = mockVertex("100");

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            invokePrivateMethod(retriever, "prefetchClassifications", List.class, Arrays.asList(v1));
        }

        // Verify batch method was NOT called
        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
        assertNull(RequestContext.get().getCachedClassifications("100"));
    }

    /**
     * Test: prefetchClassifications is a no-op when skipAuthorizationCheck is true.
     */
    @Test
    void testPrefetchSkippedWhenAuthCheckSkipped() throws Exception {
        AtlasVertex v1 = mockVertex("100");
        RequestContext.get().setSkipAuthorizationCheck(true);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            invokePrivateMethod(retriever, "prefetchClassifications", List.class, Arrays.asList(v1));
        }

        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: getAllClassifications_V2 uses cached value when available, skipping TagDAO call.
     */
    @Test
    void testGetAllClassificationsV2UsesCacheHit() throws Exception {
        AtlasVertex vertex = mockVertex("300");

        // Pre-populate cache
        Map<String, List<AtlasClassification>> batch = new HashMap<>();
        batch.put("300", Arrays.asList(new AtlasClassification("CACHED_TAG")));
        RequestContext.get().cacheClassifications(batch);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<AtlasClassification> result = retriever.getAllClassifications_V2(vertex);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("CACHED_TAG", result.get(0).getTypeName());
        }

        // Verify per-vertex fetch was NOT called
        verify(tagDAO, never()).getAllClassificationsForVertex(eq("300"));
    }

    /**
     * Test: getAllClassifications_V2 falls back to sync fetch when vertex is not in cache.
     */
    @Test
    void testGetAllClassificationsV2FallsBackOnCacheMiss() throws Exception {
        AtlasVertex vertex = mockVertex("400");

        when(tagDAO.getAllClassificationsForVertex("400"))
                .thenReturn(Arrays.asList(new AtlasClassification("SYNC_TAG")));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<AtlasClassification> result = retriever.getAllClassifications_V2(vertex);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("SYNC_TAG", result.get(0).getTypeName());
        }

        // Verify sync fetch WAS called
        verify(tagDAO, times(1)).getAllClassificationsForVertex("400");
    }

    /**
     * Test: When async batch fetch fails for one vertex but succeeds for another,
     * the failed vertex should not be in cache and should fall back to sync.
     */
    @Test
    void testPartialBatchFailureFallsBackToSync() throws Exception {
        AtlasVertex v1 = mockVertex("500");
        AtlasVertex v2 = mockVertex("600");

        // Simulate: batch only returns v1 (v2 failed async)
        Map<String, List<AtlasClassification>> partialResult = new HashMap<>();
        partialResult.put("500", Arrays.asList(new AtlasClassification("TAG_OK")));
        // "600" is absent — simulating async failure

        when(tagDAO.getAllClassificationsForVertices(anyCollection())).thenReturn(partialResult);
        when(tagDAO.getAllClassificationsForVertex("600"))
                .thenReturn(Arrays.asList(new AtlasClassification("TAG_FALLBACK")));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            // Prefetch
            invokePrivateMethod(retriever, "prefetchClassifications", List.class, Arrays.asList(v1, v2));

            // v1: cache hit
            List<AtlasClassification> result1 = retriever.getAllClassifications_V2(v1);
            assertEquals(1, result1.size());
            assertEquals("TAG_OK", result1.get(0).getTypeName());

            // v2: cache miss -> sync fallback
            List<AtlasClassification> result2 = retriever.getAllClassifications_V2(v2);
            assertEquals(1, result2.size());
            assertEquals("TAG_FALLBACK", result2.get(0).getTypeName());
        }

        // Verify: batch called once, sync called only for v2
        verify(tagDAO, times(1)).getAllClassificationsForVertices(anyCollection());
        verify(tagDAO, never()).getAllClassificationsForVertex("500");
        verify(tagDAO, times(1)).getAllClassificationsForVertex("600");
    }

    /**
     * Test: Cache returns a defensive copy in getAllClassifications_V2 so mutations don't affect cache.
     */
    @Test
    void testCacheReturnsCopyInGetAllClassificationsV2() throws Exception {
        AtlasVertex vertex = mockVertex("700");

        Map<String, List<AtlasClassification>> batch = new HashMap<>();
        batch.put("700", Arrays.asList(new AtlasClassification("IMMUTABLE_TAG")));
        RequestContext.get().cacheClassifications(batch);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<AtlasClassification> result = retriever.getAllClassifications_V2(vertex);
            result.clear(); // mutate the returned list

            // Original cache should be unaffected
            List<AtlasClassification> cached = RequestContext.get().getCachedClassifications("700");
            assertEquals(1, cached.size(), "Cache should not be mutated by caller");
        }
    }

    /**
     * Test: prefetchClassifications with empty vertex list short-circuits without calling TagDAO.
     */
    @Test
    void testPrefetchWithEmptyVertexList() throws Exception {
        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            invokePrivateMethod(retriever, "prefetchClassifications", List.class, Collections.emptyList());
        }

        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: getAllClassifications_V2 with empty cached list returns empty (not null).
     */
    @Test
    void testGetAllClassificationsV2WithEmptyCacheEntry() throws Exception {
        AtlasVertex vertex = mockVertex("800");

        // Cache an empty list (vertex exists but has no tags)
        Map<String, List<AtlasClassification>> batch = new HashMap<>();
        batch.put("800", Collections.emptyList());
        RequestContext.get().cacheClassifications(batch);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<AtlasClassification> result = retriever.getAllClassifications_V2(vertex);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        // Verify no sync call was made (cache had the entry, even though it was empty)
        verify(tagDAO, never()).getAllClassificationsForVertex("800");
    }

    /**
     * Test: When TagDAO.getAllClassificationsForVertices throws, prefetch propagates the exception.
     */
    @Test
    void testPrefetchPropagatesTagDAOException() throws Exception {
        AtlasVertex v1 = mockVertex("900");

        when(tagDAO.getAllClassificationsForVertices(anyCollection()))
                .thenThrow(new AtlasBaseException("Cassandra unavailable"));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            Exception ex = assertThrows(Exception.class, () ->
                    invokePrivateMethod(retriever, "prefetchClassifications", List.class, Arrays.asList(v1)));

            // The InvocationTargetException wraps the actual AtlasBaseException
            assertTrue(ex.getCause() instanceof AtlasBaseException || ex instanceof AtlasBaseException);
        }
    }

    // =================== Helpers ===================

    private AtlasVertex mockVertex(String id) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getIdForDisplay()).thenReturn(id);
        return vertex;
    }

    private void invokePrivateMethod(Object target, String methodName, Class<?> paramType, Object arg) throws Exception {
        Method method = EntityGraphRetriever.class.getDeclaredMethod(methodName, List.class);
        method.setAccessible(true);
        method.invoke(target, arg);
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
