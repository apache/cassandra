package org.apache.cassandra.db.columniterator;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;

import java.io.IOException;


/*
 * The goal of this encapsulating OnDiskAtomIterator is to delay the use of
 * the filter until columns are actually queried.
 * The reason for that is get_paged_slice because it change the start of
 * the filter after having seen the first row, and so we must not use the
 * filter before the row data is actually queried. However, mergeIterator
 * needs to "fetch" a row in advance. But all it needs is the key and so
 * this IColumnIterator make sure getKey() can be called without triggering
 * the use of the filter itself.
 */
public class LazyColumnIterator extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final DecoratedKey key;
    private final IColumnIteratorFactory subIteratorFactory;

    private OnDiskAtomIterator subIterator;

    public LazyColumnIterator(DecoratedKey key, IColumnIteratorFactory subIteratorFactory)
    {
        this.key = key;
        this.subIteratorFactory = subIteratorFactory;
    }

    private OnDiskAtomIterator getSubIterator()
    {
        if (subIterator == null)
            subIterator = subIteratorFactory.create();
        return subIterator;
    }

    protected OnDiskAtom computeNext()
    {
        getSubIterator();
        return subIterator.hasNext() ? subIterator.next() : endOfData();
    }

    public ColumnFamily getColumnFamily()
    {
        return getSubIterator().getColumnFamily();
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public void close() throws IOException
    {
        if (subIterator != null)
            subIterator.close();
    }
}
