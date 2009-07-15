package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.Arrays;
import java.util.TreeSet;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.filter.ColumnIterator;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;

public class NamesQueryFilter extends QueryFilter
{
    public final SortedSet<String> columns;

    public NamesQueryFilter(String key, QueryPath columnParent, SortedSet<String> columns)
    {
        super(key, columnParent);
        this.columns = columns;
    }

    public NamesQueryFilter(String key, QueryPath columnParent, String column)
    {
        this(key, columnParent, new TreeSet<String>(Arrays.asList(column)));
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable)
    {
        return memtable.getNamesIterator(this);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableNamesIterator(sstable.getFilename(), key, getColumnFamilyName(), columns);
    }

    public void filterSuperColumn(SuperColumn superColumn)
    {
        for (IColumn column : superColumn.getSubColumns())
        {
            if (!columns.contains(column.name()))
            {
                superColumn.remove(column.name());
            }
        }
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns)
    {
        for (IColumn column : reducedColumns)
        {
            returnCF.addColumn(column);
        }
    }
}
