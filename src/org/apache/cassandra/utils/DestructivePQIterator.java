package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.PriorityQueue;

public class DestructivePQIterator<T> implements Iterator<T> {
    private PriorityQueue<T> pq;

    public DestructivePQIterator(PriorityQueue<T> pq) {
        this.pq = pq;
    }

    public boolean hasNext() {
        return pq.size() > 0;
    }

    public T next() {
        return pq.poll();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

