package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.commons.collections.IteratorUtils;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.config.DatabaseDescriptor;

public class SliceQueryFilter extends QueryFilter
{
    public final byte[] start, finish;
    public final boolean reversed;
    public final int count;

    public SliceQueryFilter(String key, QueryPath columnParent, byte[] start, byte[] finish, boolean reversed, int count)
    {
        super(key, columnParent);
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable, AbstractType comparator)
    {
        return memtable.getSliceIterator(this, comparator);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableSliceIterator(sstable, key, start, reversed);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (reversed)
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = new ReverseListIterator(columnsAsList);
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // iterate until we get to the "real" start column
        Comparator<byte[]> comparator = reversed ? superColumn.getComparator().getReverseComparator() : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), start) >= 0)
            {
                subcolumns = IteratorUtils.chainedIterator(IteratorUtils.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above.  either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    @Override
    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        return reversed ? new ReverseComparator(super.getColumnComparator(comparator)) : super.getColumnComparator(comparator);
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType comparator = container.getComparator();

        while (reducedColumns.hasNext())
        {
            IColumn column = reducedColumns.next();
            if (liveColumns >= count)
                break;
            if (finish.length > 0
                && ((!reversed && comparator.compare(column.name(), finish) > 0))
                    || (reversed && comparator.compare(column.name(), finish) < 0))
                break;

            if (!column.isMarkedForDelete())
                liveColumns++;

            if (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore)
                container.addColumn(column);
        }
    }
}
