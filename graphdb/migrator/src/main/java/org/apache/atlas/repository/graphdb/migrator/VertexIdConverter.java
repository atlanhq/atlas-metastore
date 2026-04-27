package org.apache.atlas.repository.graphdb.migrator;

/**
 * Standalone utility to convert JanusGraph edgestore hex partition keys
 * (as reported in old Mixpanel analysis) to real JanusGraph vertex IDs.
 *
 * ZERO external dependencies — runs with just the JDK.
 *
 * Usage:
 *   javac VertexIdConverter.java
 *   java VertexIdConverter e000000009e57200 c800000008127480 f8000000088e2b00
 *
 * Or from the built project:
 *   java -cp graphdb/migrator/target/classes \
 *     org.apache.atlas.repository.graphdb.migrator.VertexIdConverter \
 *     e000000009e57200 c800000008127480
 *
 * How it works:
 *   JanusGraph encodes vertex IDs in the edgestore partition key as an 8-byte
 *   big-endian long with the top N bits used for virtual partitioning:
 *
 *     | partition (N bits) | base vertex ID (64-N bits) |
 *
 *   The real vertex ID = (baseId << N) | partition
 *
 *   For standard Atlas/JanusGraph CQL deployments:
 *     cluster.max-partitions = 32 → partitionBits = 5
 */
public class VertexIdConverter {

    /** Default partition bits for JanusGraph CQL (cluster.max-partitions=32) */
    private static final int DEFAULT_PARTITION_BITS = 5;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: VertexIdConverter [--partition-bits N] <hex1> [hex2] ...");
            System.err.println();
            System.err.println("Converts JanusGraph edgestore hex partition keys to vertex IDs.");
            System.err.println("Default partition bits: " + DEFAULT_PARTITION_BITS +
                               " (cluster.max-partitions=32)");
            System.err.println();
            System.err.println("Examples:");
            System.err.println("  VertexIdConverter e000000009e57200 c800000008127480");
            System.err.println("  VertexIdConverter --partition-bits 0 e000000009e57200");
            System.exit(1);
        }

        int partitionBits = DEFAULT_PARTITION_BITS;
        int startIdx = 0;

        // Parse optional --partition-bits flag
        if ("--partition-bits".equals(args[0]) && args.length > 2) {
            partitionBits = Integer.parseInt(args[1]);
            startIdx = 2;
        }

        System.out.println();
        System.out.printf("Partition bits: %d (cluster.max-partitions=%d)%n",
                          partitionBits, 1 << partitionBits);
        System.out.println();
        System.out.println("hex_partition_key          -> janus_vertex_id");
        System.out.println("----------------------------------------------");

        for (int i = startIdx; i < args.length; i++) {
            String hex = args[i].trim().toLowerCase();
            try {
                long vertexId = hexKeyToVertexId(hex, partitionBits);
                System.out.printf("%-26s -> %d%n", hex, vertexId);
            } catch (Exception e) {
                System.out.printf("%-26s -> ERROR: %s%n", hex, e.getMessage());
            }
        }
        System.out.println();
    }

    /**
     * Convert a hex partition key to a JanusGraph vertex ID.
     *
     * Replicates JanusGraph IDManager.getKeyID() logic:
     *   - Read 8 bytes as big-endian long
     *   - Extract top N bits as partition
     *   - Remaining bits are the base ID
     *   - Vertex ID = (baseId << N) | partition
     */
    public static long hexKeyToVertexId(String hex, int partitionBits) {
        if (hex.length() != 16) {
            throw new IllegalArgumentException(
                "Expected 16 hex chars (8 bytes), got " + hex.length());
        }

        // Parse hex to long (big-endian)
        long value = Long.parseUnsignedLong(hex, 16);

        if (partitionBits == 0) {
            return value;
        }

        // Extract partition from top N bits
        long partition = value >>> (64 - partitionBits);

        // Extract base ID (clear top N bits)
        long baseId = (value << partitionBits) >>> partitionBits;

        // Reconstruct vertex ID
        return (baseId << partitionBits) | partition;
    }
}
