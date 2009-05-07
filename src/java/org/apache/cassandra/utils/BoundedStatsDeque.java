package org.apache.cassandra.utils;

import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * not threadsafe.  caller is responsible for any locking necessary.
 */
public class BoundedStatsDeque extends AbstractStatsDeque
{
    private final int size;
    protected final ArrayDeque<Double> deque;

    public BoundedStatsDeque(int size)
    {
        this.size = size;
        deque = new ArrayDeque<Double>(size);
    }

    public Iterator<Double> iterator()
    {
        return deque.iterator();
    }

    public int size()
    {
        return deque.size();
    }

    public void clear()
    {
        deque.clear();
    }

    public void add(double o)
    {
        if (size == deque.size())
        {
            deque.remove();
        }
        deque.add(o);
    }
}
