package org.apache.atlas.repository.graphdb.cassandra;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lazy {@link Iterable} that wraps a source iterable and stops yielding
 * after {@code limit} elements. The source iterator is <b>not</b> consumed
 * beyond the limit.
 *
 * @param <T> element type
 */
public class LimitedIterable<T> implements Iterable<T> {

    private final Iterable<T> source;
    private final int         limit;

    public LimitedIterable(Iterable<T> source, int limit) {
        this.source = source;
        this.limit  = limit;
    }

    @Override
    public Iterator<T> iterator() {
        return new LimitedIterator(source.iterator());
    }

    private class LimitedIterator implements Iterator<T> {
        private final Iterator<T> delegate;
        private int yielded;

        LimitedIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return yielded < limit && delegate.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            yielded++;
            return delegate.next();
        }
    }
}
