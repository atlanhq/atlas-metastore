package org.apache.atlas.repository.graphdb.cassandra;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Lazy {@link Iterable} that wraps a source iterable and maps each element
 * through a function. Null results from the mapping function are skipped
 * (filter-map pattern), which handles cases like edge→vertex mapping where
 * the adjacent vertex may not exist.
 *
 * @param <S> source element type
 * @param <T> target element type
 */
public class MappedIterable<S, T> implements Iterable<T> {

    private final Iterable<S>    source;
    private final Function<S, T> mapper;

    public MappedIterable(Iterable<S> source, Function<S, T> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public Iterator<T> iterator() {
        return new MappedIterator(source.iterator());
    }

    private class MappedIterator implements Iterator<T> {
        private final Iterator<S> delegate;
        private T nextItem;

        MappedIterator(Iterator<S> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (nextItem != null) {
                return true;
            }
            nextItem = advance();
            return nextItem != null;
        }

        @Override
        public T next() {
            if (nextItem == null) {
                nextItem = advance();
            }
            if (nextItem == null) {
                throw new NoSuchElementException();
            }
            T result = nextItem;
            nextItem = null;
            return result;
        }

        private T advance() {
            while (delegate.hasNext()) {
                T mapped = mapper.apply(delegate.next());
                if (mapped != null) {
                    return mapped;
                }
            }
            return null;
        }
    }
}
