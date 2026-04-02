package org.apache.atlas.repository.graphdb.migrator;

/**
 * Marker interface for items that can be placed on the scanner→writer queue.
 *
 * Implementations:
 *   - {@link DecodedVertex}: a full vertex with properties, indexes, and (possibly chunked) edges
 *   - {@link EdgeChunk}: a batch of additional edges for a super vertex that was split during scanning
 *
 * Super vertex chunking:
 *   When the scanner encounters a vertex with more edges than {@code migration.super.vertex.edge.chunk.size},
 *   it emits a DecodedVertex (properties + first N edges) followed by one or more EdgeChunk objects
 *   (subsequent edge batches). Each chunk can be processed independently by different writer threads,
 *   preventing thread monopolization and bounding memory usage.
 */
public interface QueueItem {

    /** The JanusGraph vertex ID this item belongs to. */
    long getJgVertexId();
}
