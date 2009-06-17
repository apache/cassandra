package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** threadsafe. */
public class TimedStatsDeque extends AbstractStatsDeque
{
    private final ArrayDeque<Tuple> deque;
    private final long period;

    public TimedStatsDeque(long period)
    {
        this.period = period;
        deque = new ArrayDeque<Tuple>();
    }

    private void purge()
    {
        long now = System.currentTimeMillis();
        while (!deque.isEmpty() && deque.peek().timestamp < now - period)
        {
            deque.remove();
        }
    }

    public synchronized Iterator<Double> iterator()
    {
        purge();
        // I expect this method to be called relatively infrequently so inefficiency is ok.
        // (this has the side benefit of making iteration threadsafe w/o having to use LinkedBlockingDeque.)
        List<Double> L = new ArrayList<Double>(deque.size());
        for (Tuple t : deque)
        {
            L.add(t.value);
        }
        return L.iterator();
    }

    public synchronized int size()
    {
        purge();
        return deque.size();
    }

    public synchronized void add(double o)
    {
        purge();
        deque.add(new Tuple(o, System.currentTimeMillis()));
    }

    public synchronized void clear()
    {
        deque.clear();
    }
}

class Tuple
{
    public final double value;
    public final long timestamp;

    public Tuple(double value, long timestamp)
    {
        this.value = value;
        this.timestamp = timestamp;
    }
}