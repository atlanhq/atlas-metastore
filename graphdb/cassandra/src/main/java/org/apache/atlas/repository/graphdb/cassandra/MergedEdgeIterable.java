package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;

import java.util.*;

/**
 * Lazily merges transaction-buffered edges with persisted (Cassandra) edges.
 *
 * <p>Merge strategy:
 * <ol>
 *   <li>Yield all buffered edges first (snapshot, always small — typically 0-100).</li>
 *   <li>Lazily yield persisted edges, skipping any whose ID is in the buffered set
 *       (duplicate override) or that have been removed in the current transaction.</li>
 * </ol>
 *
 * <p>Memory overhead: O(bufferSize) for the buffered-edge ID set. Persisted edges
 * are streamed lazily, so total memory is O(pageSize + bufferSize).
 *
 * <p>This is a <b>single-use</b> iterable because the underlying persisted iterable
 * ({@link PaginatedEdgeIterable}) is single-use.
 */
public class MergedEdgeIterable implements Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> {

    private final List<CassandraEdge>                                       bufferedEdges;
    private final Iterable<AtlasEdge<CassandraVertex, CassandraEdge>>       persistedEdges;
    private final TransactionBuffer                                         txBuffer;
    private volatile boolean consumed = false;

    /**
     * @param bufferedEdges  snapshot of edges from the transaction buffer (already filtered)
     * @param persistedEdges lazy iterable of edges from Cassandra
     * @param txBuffer       the transaction buffer, used for {@code isEdgeRemoved()} checks
     */
    public MergedEdgeIterable(List<CassandraEdge> bufferedEdges,
                              Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> persistedEdges,
                              TransactionBuffer txBuffer) {
        this.bufferedEdges  = bufferedEdges;
        this.persistedEdges = persistedEdges;
        this.txBuffer       = txBuffer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iterator() {
        if (consumed) {
            throw new IllegalStateException(
                "MergedEdgeIterable has already been consumed. " +
                "Each instance can only be iterated once.");
        }
        consumed = true;
        return (Iterator) new MergedEdgeIterator();
    }

    private class MergedEdgeIterator implements Iterator<CassandraEdge> {
        // Pre-collect buffered edge IDs for O(1) duplicate detection
        private final Set<String> bufferedEdgeIds;
        private final Iterator<CassandraEdge> bufferedIterator;
        private final Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> persistedIterator;
        private boolean inBufferedPhase = true;
        private CassandraEdge nextEdge = null;

        MergedEdgeIterator() {
            this.bufferedEdgeIds = new HashSet<>(bufferedEdges.size());
            for (CassandraEdge e : bufferedEdges) {
                bufferedEdgeIds.add(e.getIdString());
            }
            this.bufferedIterator  = bufferedEdges.iterator();
            this.persistedIterator = persistedEdges.iterator();
        }

        @Override
        public boolean hasNext() {
            if (nextEdge != null) {
                return true;
            }
            nextEdge = advance();
            return nextEdge != null;
        }

        @Override
        public CassandraEdge next() {
            if (nextEdge == null) {
                nextEdge = advance();
            }
            if (nextEdge == null) {
                throw new NoSuchElementException();
            }
            CassandraEdge result = nextEdge;
            nextEdge = null;
            return result;
        }

        private CassandraEdge advance() {
            // Phase 1: yield buffered edges
            if (inBufferedPhase) {
                if (bufferedIterator.hasNext()) {
                    return bufferedIterator.next();
                }
                inBufferedPhase = false;
            }

            // Phase 2: yield persisted edges, skipping duplicates and removed
            while (persistedIterator.hasNext()) {
                CassandraEdge edge = (CassandraEdge) persistedIterator.next();
                String edgeId = edge.getIdString();

                // Skip if this edge exists in buffer (buffered version takes precedence)
                if (bufferedEdgeIds.contains(edgeId)) {
                    continue;
                }

                // Skip if this edge was removed in the current transaction
                if (txBuffer.isEdgeRemoved(edgeId)) {
                    continue;
                }

                return edge;
            }

            return null;
        }
    }
}
