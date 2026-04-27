package org.apache.atlas.repository.graphdb.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compresses and decompresses vertex properties JSON for types that can exceed
 * Cassandra's max_mutation_size_in_bytes (32 MiB default).
 *
 * Currently targets {@code __AtlasAuditEntry} vertices, whose {@code result} field
 * stores comma-separated GUIDs of all entities affected by admin operations (PURGE, IMPORT)
 * and can reach 84 MiB.
 *
 * <b>Write path:</b> For vertices where the type matches, properties are compressed:
 * {@code JSON string -> gzip -> base64 encode -> store as TEXT}
 *
 * <b>Read path:</b> For all vertices, the stored text is inspected. If it starts with
 * '{' or '[' it's already plain JSON and returned as-is. Otherwise it's assumed to be
 * compressed: {@code TEXT -> base64 decode -> gzip decompress -> JSON string}.
 *
 * This is backward compatible: already-migrated tenants have plain JSON (passthrough),
 * new migrations write compressed (dual-read handles it). No schema changes needed.
 */
public final class PropertyCompression {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyCompression.class);

    private static final String AUDIT_ENTRY_TYPE = "__AtlasAuditEntry";

    private PropertyCompression() {}

    /**
     * Returns true if this type's properties should be compressed before writing to Cassandra.
     */
    public static boolean shouldCompress(String typeName) {
        return AUDIT_ENTRY_TYPE.equals(typeName);
    }

    /**
     * Gzip-compress and base64-encode a JSON string.
     *
     * @param json the raw JSON string
     * @return base64-encoded gzip bytes
     */
    public static String compress(String json) {
        try {
            byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(jsonBytes.length / 4);
            try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(jsonBytes);
            }
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            // GZIPOutputStream on a ByteArrayOutputStream should never throw IOException
            // in practice, but if it does, return the original JSON so the write can proceed.
            LOG.warn("PropertyCompression.compress failed, returning uncompressed JSON (length={}): {}",
                     json.length(), e.getMessage());
            return json;
        }
    }

    /**
     * If the text is compressed (base64-encoded gzip), decompress it. If it's already
     * plain JSON, return it as-is.
     *
     * Detection: valid JSON objects start with '{', arrays with '['. Base64-encoded gzip
     * starts with 'H4sI' (the base64 encoding of the gzip magic bytes 0x1f 0x8b 0x08)
     * and will never start with '{' or '['.
     *
     * @param text the stored properties text (may be plain JSON or compressed)
     * @return the plain JSON string
     */
    public static String decompressIfNeeded(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        char first = text.charAt(0);
        if (first == '{' || first == '[') {
            // Already plain JSON — fast path (vast majority of vertices)
            return text;
        }

        // Assume compressed: base64 decode -> gzip decompress
        try {
            byte[] compressed = Base64.getDecoder().decode(text);
            try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length * 4);
                byte[] buffer = new byte[8192];
                int len;
                while ((len = gzip.read(buffer)) != -1) {
                    baos.write(buffer, 0, len);
                }
                return baos.toString(StandardCharsets.UTF_8.name());
            }
        } catch (Exception e) {
            // Not valid base64+gzip — return as-is (defensive: should not happen in practice)
            LOG.warn("PropertyCompression.decompressIfNeeded failed, returning text as-is (length={}): {}",
                     text.length(), e.getMessage());
            return text;
        }
    }
}
