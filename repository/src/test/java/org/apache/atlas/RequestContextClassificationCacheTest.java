package org.apache.atlas;

import org.apache.atlas.model.instance.AtlasClassification;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RequestContext classification cache functionality.
 */
public class RequestContextClassificationCacheTest {

    private RequestContext requestContext;

    @BeforeEach
    public void setUp() {
        requestContext = RequestContext.get();
        requestContext.clearCache();
    }

    @AfterEach
    public void tearDown() {
        RequestContext.clear();
        requestContext = null;
    }

    @Test
    public void testCacheIsInitiallyEmpty() {
        assertNull(requestContext.getCachedClassifications("vertex1"));
    }

    @Test
    public void testCacheClassificationsAndRetrieve() {
        Map<String, List<AtlasClassification>> batch = new HashMap<>();
        batch.put("vertex1", Arrays.asList(new AtlasClassification("TAG_A")));
        batch.put("vertex2", Collections.emptyList());

        requestContext.cacheClassifications(batch);

        assertNotNull(requestContext.getCachedClassifications("vertex1"));
        assertEquals(1, requestContext.getCachedClassifications("vertex1").size());
        assertEquals("TAG_A", requestContext.getCachedClassifications("vertex1").get(0).getTypeName());

        assertNotNull(requestContext.getCachedClassifications("vertex2"));
        assertTrue(requestContext.getCachedClassifications("vertex2").isEmpty());

        assertNull(requestContext.getCachedClassifications("vertex3"));
    }

    @Test
    public void testClearCacheRemovesClassifications() {
        Map<String, List<AtlasClassification>> batch = new HashMap<>();
        batch.put("vertex1", Arrays.asList(new AtlasClassification("TAG_A")));
        requestContext.cacheClassifications(batch);

        assertNotNull(requestContext.getCachedClassifications("vertex1"));

        requestContext.clearCache();

        assertNull(requestContext.getCachedClassifications("vertex1"));
    }

    @Test
    public void testCacheClassificationsWithNullInput() {
        requestContext.cacheClassifications(null);
        assertNull(requestContext.getCachedClassifications("vertex1"));
    }

    @Test
    public void testCacheClassificationsWithEmptyInput() {
        requestContext.cacheClassifications(new HashMap<>());
        assertNull(requestContext.getCachedClassifications("vertex1"));
    }

    @Test
    public void testCacheClassificationsMergesMultipleBatches() {
        Map<String, List<AtlasClassification>> batch1 = new HashMap<>();
        batch1.put("vertex1", Arrays.asList(new AtlasClassification("TAG_A")));
        requestContext.cacheClassifications(batch1);

        Map<String, List<AtlasClassification>> batch2 = new HashMap<>();
        batch2.put("vertex2", Arrays.asList(new AtlasClassification("TAG_B")));
        requestContext.cacheClassifications(batch2);

        assertNotNull(requestContext.getCachedClassifications("vertex1"));
        assertNotNull(requestContext.getCachedClassifications("vertex2"));
    }

    @Test
    public void testCacheOverwritesExistingKey() {
        Map<String, List<AtlasClassification>> batch1 = new HashMap<>();
        batch1.put("vertex1", Arrays.asList(new AtlasClassification("TAG_OLD")));
        requestContext.cacheClassifications(batch1);

        Map<String, List<AtlasClassification>> batch2 = new HashMap<>();
        batch2.put("vertex1", Arrays.asList(new AtlasClassification("TAG_NEW")));
        requestContext.cacheClassifications(batch2);

        List<AtlasClassification> cached = requestContext.getCachedClassifications("vertex1");
        assertEquals(1, cached.size());
        assertEquals("TAG_NEW", cached.get(0).getTypeName(), "Second batch should overwrite first for same key");
    }

    @Test
    public void testClearCacheFollowedByCacheWorks() {
        Map<String, List<AtlasClassification>> batch1 = new HashMap<>();
        batch1.put("vertex1", Arrays.asList(new AtlasClassification("TAG_A")));
        requestContext.cacheClassifications(batch1);
        requestContext.clearCache();

        // Re-cache after clear
        Map<String, List<AtlasClassification>> batch2 = new HashMap<>();
        batch2.put("vertex2", Arrays.asList(new AtlasClassification("TAG_B")));
        requestContext.cacheClassifications(batch2);

        assertNull(requestContext.getCachedClassifications("vertex1"), "vertex1 should be gone after clear");
        assertNotNull(requestContext.getCachedClassifications("vertex2"), "vertex2 should be cached after re-cache");
    }
}
