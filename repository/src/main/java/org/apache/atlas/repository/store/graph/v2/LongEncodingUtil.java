package org.apache.atlas.repository.store.graph.v2;

/**
 * Standalone replacement for org.janusgraph.util.encoding.LongEncoding.
 * Encodes/decodes long values to/from compact string representation
 * compatible with JanusGraph's Elasticsearch document ID format.
 *
 * The encoding uses a base-62 like character set to represent long values compactly.
 * This is a direct reimplementation of JanusGraph's LongEncoding to remove
 * the JanusGraph dependency while maintaining backward compatibility with
 * existing Elasticsearch document IDs.
 */
public final class LongEncodingUtil {

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int    BASE         = BASE_SYMBOLS.length();

    private LongEncodingUtil() {
        // utility class
    }

    /**
     * Encode a long value to a compact string representation.
     * Matches JanusGraph's LongEncoding.encode() output exactly.
     */
    public static String encode(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non-negative value: " + value);
        }
        if (value == 0) {
            return String.valueOf(BASE_SYMBOLS.charAt(0));
        }

        StringBuilder sb = new StringBuilder();
        while (value > 0) {
            sb.append(BASE_SYMBOLS.charAt((int) (value % BASE)));
            value = value / BASE;
        }
        return sb.reverse().toString();
    }

    /**
     * Decode a string back to a long value.
     * Matches JanusGraph's LongEncoding.decode() output exactly.
     */
    public static long decode(String encoded) {
        long value = 0;
        for (int i = 0; i < encoded.length(); i++) {
            int digit = BASE_SYMBOLS.indexOf(encoded.charAt(i));
            if (digit < 0) {
                throw new IllegalArgumentException("Invalid character in encoded string: " + encoded.charAt(i));
            }
            value = value * BASE + digit;
        }
        return value;
    }

    /**
     * Compute the ES document ID from a vertex ID string.
     * For JanusGraph numeric vertex IDs: encodes the long value using base-62.
     * For non-numeric vertex IDs (e.g., UUID strings from Cassandra graph backend):
     * returns the vertex ID as-is (it already serves as the ES doc ID).
     */
    public static String vertexIdToDocId(String vertexId) {
        try {
            return encode(Long.parseLong(vertexId));
        } catch (NumberFormatException e) {
            // Non-numeric vertex ID (e.g., UUID from Cassandra graph backend)
            return vertexId;
        }
    }
}
