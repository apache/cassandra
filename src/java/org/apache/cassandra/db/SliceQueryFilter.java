package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.collections.comparators.ReverseComparator;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;

public class SliceQueryFilter extends QueryFilter
{
    public final String start, finish;
    public final boolean isAscending;
    public final int offset, count;

    public SliceQueryFilter(String key, String columnFamilyColumn, String start, String finish, boolean ascending, int offset, int count)
    {
        super(key, columnFamilyColumn);
        this.start = start;
        this.finish = finish;
        isAscending = ascending;
        this.offset = offset;
        this.count = count;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable)
    {
        return memtable.getColumnIterator(key, columnFamilyColumn, isAscending, start);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableColumnIterator(sstable.getFilename(), key, columnFamilyColumn, start, isAscending);
    }

    @Override
    protected Comparator<IColumn> getColumnComparator()
    {
        Comparator<IColumn> comparator = super.getColumnComparator();
        return isAscending ? comparator : new ReverseComparator(comparator);
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns)
    {
        int liveColumns = 0;
        int limit = offset + count;

        for (IColumn column : reducedColumns)
        {
            if (liveColumns >= limit)
                break;
            if (!finish.isEmpty()
                && ((isAscending && column.name().compareTo(finish) > 0))
                    || (!isAscending && column.name().compareTo(finish) < 0))
                break;
            if (!column.isMarkedForDelete())
                liveColumns++;

            if (liveColumns > offset)
                returnCF.addColumn(column);
        }
    }
}
