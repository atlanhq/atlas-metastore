package org.apache.atlas.repository.graphdb.cassandra;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests for PropertyCompression: gzip+base64 compression and dual-read decompression
 * used to handle oversized __AtlasAuditEntry vertices.
 */
public class PropertyCompressionTest {

    @Test
    public void testShouldCompressAuditEntry() {
        assertTrue(PropertyCompression.shouldCompress("__AtlasAuditEntry"));
    }

    @Test
    public void testShouldNotCompressRegularTypes() {
        assertFalse(PropertyCompression.shouldCompress("Table"));
        assertFalse(PropertyCompression.shouldCompress("Column"));
        assertFalse(PropertyCompression.shouldCompress("AtlasGlossaryTerm"));
        assertFalse(PropertyCompression.shouldCompress(null));
        assertFalse(PropertyCompression.shouldCompress(""));
    }

    @Test
    public void testCompressDecompressRoundTrip() {
        String json = "{\"__typeName\":\"__AtlasAuditEntry\",\"result\":\"guid1,guid2,guid3\"}";
        String compressed = PropertyCompression.compress(json);

        // Compressed output should NOT start with '{' (it's base64)
        assertNotEquals(compressed.charAt(0), '{');
        // Compressed output should be different from input
        assertNotEquals(compressed, json);

        // Decompress should return original
        String decompressed = PropertyCompression.decompressIfNeeded(compressed);
        assertEquals(decompressed, json);
    }

    @Test
    public void testDecompressIfNeededPassthroughForPlainJson() {
        String json = "{\"__typeName\":\"Table\",\"qualifiedName\":\"db.schema.table\"}";
        String result = PropertyCompression.decompressIfNeeded(json);
        assertSame(result, json); // should return exact same reference (fast path)
    }

    @Test
    public void testDecompressIfNeededPassthroughForJsonArray() {
        String json = "[\"value1\",\"value2\"]";
        String result = PropertyCompression.decompressIfNeeded(json);
        assertSame(result, json);
    }

    @Test
    public void testDecompressIfNeededNullAndEmpty() {
        assertNull(PropertyCompression.decompressIfNeeded(null));
        assertEquals(PropertyCompression.decompressIfNeeded(""), "");
    }

    @Test
    public void testCompressLargePayload() {
        // Simulate a large result field with comma-separated GUIDs
        StringBuilder sb = new StringBuilder("{\"result\":\"");
        for (int i = 0; i < 10000; i++) {
            if (i > 0) sb.append(',');
            sb.append("aaaaaaaa-bbbb-cccc-dddd-eeeeeeee").append(String.format("%04d", i));
        }
        sb.append("\"}");
        String largeJson = sb.toString();

        String compressed = PropertyCompression.compress(largeJson);

        // Compression should significantly reduce size for repetitive UUID data
        assertTrue(compressed.length() < largeJson.length(),
                "Compressed (" + compressed.length() + ") should be smaller than original (" + largeJson.length() + ")");

        // Roundtrip
        String decompressed = PropertyCompression.decompressIfNeeded(compressed);
        assertEquals(decompressed, largeJson);
    }

    @Test
    public void testDecompressInvalidBase64ReturnsAsIs() {
        // A string that doesn't start with { or [ but is not valid base64+gzip
        // should be returned as-is (defensive fallback)
        String weird = "not-valid-base64-gzip!!!";
        String result = PropertyCompression.decompressIfNeeded(weird);
        assertEquals(result, weird);
    }
}
