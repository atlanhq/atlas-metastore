package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Lazily chains multiple per-label edge iterables into a single iterable.
 * Instead of eagerly collecting all edges from all labels into a List,
 * this iterates through each label's edges lazily, moving to the next
 * label only when the current one is exhausted.
 *
 * <p>Memory usage: O(pageSize) — only one label's page is active at a time.
 */
public class ChainedEdgeIterable implements Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> {

    private final String[] labels;
    private final Function<String, Iterable<AtlasEdge<CassandraVertex, CassandraEdge>>> labelToEdges;

    /**
     * @param labels       the edge labels to iterate over
     * @param labelToEdges function that returns an edge iterable for a given label
     */
    public ChainedEdgeIterable(String[] labels,
                               Function<String, Iterable<AtlasEdge<CassandraVertex, CassandraEdge>>> labelToEdges) {
        this.labels       = labels;
        this.labelToEdges = labelToEdges;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> iterator() {
        return new ChainedIterator();
    }

    private class ChainedIterator implements Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> {
        private int labelIndex = 0;
        private Iterator<AtlasEdge<CassandraVertex, CassandraEdge>> currentIterator = null;

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentIterator != null && currentIterator.hasNext()) {
                    return true;
                }
                if (labelIndex >= labels.length) {
                    return false;
                }
                currentIterator = labelToEdges.apply(labels[labelIndex]).iterator();
                labelIndex++;
            }
        }

        @Override
        public AtlasEdge<CassandraVertex, CassandraEdge> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentIterator.next();
        }
    }
}
