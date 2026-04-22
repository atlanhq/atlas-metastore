package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * Lazy {@link Iterable} that wraps a source edge iterable and yields only
 * elements matching a predicate. Memory usage is O(1) beyond the source
 * iterable — no intermediate collection is created.
 *
 * <p>Uses the same lookahead pattern as {@link PaginatedEdgeIterable}.
 */
public class FilteredEdgeIterable implements Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> {

    private final Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> source;
    private final Predicate<AtlasEdge<CassandraVertex, CassandraEdge>> predicate;

    public FilteredEdgeIterable(Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> source,
                                Predicate<AtlasEdge<CassandraVertex, CassandraEdge>> predicate) {
        this.source    = source;
        this.predicate = predicate;
    }

    @Override
    public Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iterator() {
        return new FilteredIterator(source.iterator());
    }

    private class FilteredIterator implements Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> {
        private final Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> delegate;
        private AtlasEdge<CassandraVertex, CassandraEdge> nextEdge;

        FilteredIterator(Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> delegate) {
            this.delegate = delegate;
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
        public AtlasEdge<CassandraVertex, CassandraEdge> next() {
            if (nextEdge == null) {
                nextEdge = advance();
            }
            if (nextEdge == null) {
                throw new NoSuchElementException();
            }
            AtlasEdge<CassandraVertex, CassandraEdge> result = nextEdge;
            nextEdge = null;
            return result;
        }

        private AtlasEdge<CassandraVertex, CassandraEdge> advance() {
            while (delegate.hasNext()) {
                AtlasEdge<CassandraVertex, CassandraEdge> candidate = delegate.next();
                if (predicate.test(candidate)) {
                    return candidate;
                }
            }
            return null;
        }
    }
}
