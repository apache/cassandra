package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.collections.comparators.ReverseComparator;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.*;

public class SliceQueryFilter extends QueryFilter
{
    public final String start, finish;
    public final boolean isAscending;
    public final int count;

    public SliceQueryFilter(String key, QueryPath columnParent, String start, String finish, boolean ascending, int count)
    {
        super(key, columnParent);
        this.start = start;
        this.finish = finish;
        isAscending = ascending;
        this.count = count;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable)
    {
        return memtable.getSliceIterator(this);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableSliceIterator(sstable.getFilename(), key, getColumnFamilyName(), start, isAscending);
    }

    public void filterSuperColumn(SuperColumn superColumn)
    {
        // TODO write this after CASSANDRA-240 is done
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<IColumn> getColumnComparator()
    {
        Comparator<IColumn> comparator = super.getColumnComparator();
        return isAscending ? comparator : new ReverseComparator(comparator);
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns)
    {
        int liveColumns = 0;

        for (IColumn column : reducedColumns)
        {
            if (liveColumns >= count)
                break;
            if (!finish.isEmpty()
                && ((isAscending && column.name().compareTo(finish) > 0))
                    || (!isAscending && column.name().compareTo(finish) < 0))
                break;
            if (!column.isMarkedForDelete())
                liveColumns++;

            returnCF.addColumn(column);
        }
    }
}
