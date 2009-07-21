package org.apache.cassandra.utils;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

/**
 * reduces equal values from the source iterator to a single (optionally transformed) instance.
 */
public abstract class ReducingIterator<T> extends AbstractIterator<T> implements Iterator<T>, Iterable<T>
{
    protected Iterator<T> source;
    protected T last;

    public ReducingIterator(Iterator<T> source)
    {
        this.source = source;
    }

    /** combine this object with the previous ones.  intermediate state is up to your implementation. */
    public abstract void reduce(T current);

    /** return the last object computed by reduce */
    protected abstract T getReduced();

    /** override this if the keys you want to base the reduce on are not the same as the object itself (but can be generated from it) */
    protected boolean isEqual(T o1, T o2)
    {
        return o1.equals(o2);
    }

    protected T computeNext()
    {
        if (last == null && !source.hasNext())
            return endOfData();

        boolean keyChanged = false;
        while (!keyChanged)
        {
            if (last != null)
                reduce(last);
            if (!source.hasNext())
            {
                last = null;
                break;
            }
            T current = source.next();
            if (last != null && !isEqual(current, last))
                keyChanged = true;
            last = current;
        }
        return getReduced();
    }

    public Iterator<T> iterator()
    {
        return this;
    }
}
