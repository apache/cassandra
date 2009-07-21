package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Arrays;

import org.apache.commons.collections.comparators.ReverseComparator;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

public class SliceQueryFilter extends QueryFilter
{
    public final byte[] start, finish;
    public final boolean isAscending;
    public final int count;

    public SliceQueryFilter(String key, QueryPath columnParent, byte[] start, byte[] finish, boolean ascending, int count)
    {
        super(key, columnParent);
        this.start = start;
        this.finish = finish;
        isAscending = ascending;
        this.count = count;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable, AbstractType comparator)
    {
        return memtable.getSliceIterator(this, comparator);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable, AbstractType comparator) throws IOException
    {
        return new SSTableSliceIterator(sstable.getFilename(), key, getColumnFamilyName(), comparator, start, isAscending);
    }

    public void filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        int liveColumns = 0;

        for (IColumn column : superColumn.getSubColumns())
        {
            if ((start.length > 0 && superColumn.getComparator().compare(column.name(), start) < 0)
                || (finish.length > 0 && superColumn.getComparator().compare(column.name(), finish) > 0)
                || (column.isMarkedForDelete() && column.getLocalDeletionTime() <= gcBefore)
                || liveColumns > count)
            {
                superColumn.remove(column.name());
            }
            else if (!column.isMarkedForDelete())
            {
                liveColumns++;
            }
        }
    }

    @Override
    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        return isAscending ? super.getColumnComparator(comparator) : new ReverseComparator(super.getColumnComparator(comparator));
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType comparator = returnCF.getComparator();

        for (IColumn column : reducedColumns)
        {
            if (liveColumns >= count)
                break;
            if (finish.length > 0
                && ((isAscending && comparator.compare(column.name(), finish) > 0))
                    || (!isAscending && comparator.compare(column.name(), finish) < 0))
                break;

            if (!column.isMarkedForDelete())
                liveColumns++;

            if (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore)
                returnCF.addColumn(column);
        }
    }
}
