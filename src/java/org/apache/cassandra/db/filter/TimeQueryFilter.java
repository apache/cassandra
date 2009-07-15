package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;

public class TimeQueryFilter extends QueryFilter
{
    public final long since;

    public TimeQueryFilter(String key, QueryPath columnParent, long since)
    {
        super(key, columnParent);
        this.since = since;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable)
    {
        return memtable.getTimeIterator(this);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableTimeIterator(sstable.getFilename(), key, getColumnFamilyName(), since);
    }

    @Override
    public Comparator<IColumn> getColumnComparator()
    {
        return ColumnComparatorFactory.timestampComparator_;
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns)
    {
        for (IColumn column : reducedColumns)
        {
            returnCF.addColumn(column);
        }
    }

    public void filterSuperColumn(SuperColumn superColumn)
    {
        for (IColumn column : superColumn.getSubColumns())
        {
            if (column.timestamp() < since)
            {
                superColumn.remove(column.name());
            }
        }
    }
}
