package org.apache.atlas.repository.graphdb.migrator;

import java.util.List;

/**
 * A chunk of edges from a super vertex that was split during scanning.
 *
 * The scanner produces these when a vertex has more edges than the configured chunk size.
 * Each EdgeChunk carries a pre-resolved vertex ID so the writer can insert edges
 * without needing to look up the vertex again.
 *
 * Writer behavior: only edge table INSERTs (edges_out, edges_in, edges_by_id, edge_index).
 * No vertex row, no vertex indexes — those are handled by the DecodedVertex for this vertex.
 */
public class EdgeChunk implements QueueItem {

    private final long jgVertexId;
    private final String resolvedVertexId;
    private final List<DecodedEdge> edges;
    private final int chunkIndex;

    public EdgeChunk(long jgVertexId, String resolvedVertexId, List<DecodedEdge> edges, int chunkIndex) {
        this.jgVertexId      = jgVertexId;
        this.resolvedVertexId = resolvedVertexId;
        this.edges           = edges;
        this.chunkIndex      = chunkIndex;
    }

    @Override
    public long getJgVertexId() {
        return jgVertexId;
    }

    /** Pre-computed vertex ID for the target schema (avoids re-resolution in writer). */
    public String getResolvedVertexId() {
        return resolvedVertexId;
    }

    public List<DecodedEdge> getEdges() {
        return edges;
    }

    /** 0-based index of this chunk (chunk 0 is in the DecodedVertex, chunk 1+ are EdgeChunks). */
    public int getChunkIndex() {
        return chunkIndex;
    }

    @Override
    public String toString() {
        return "EdgeChunk{jgId=" + jgVertexId +
               ", chunk=" + chunkIndex +
               ", edges=" + edges.size() + "}";
    }
}
