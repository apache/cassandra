package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.Comparator;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.AbstractType;

public class NamesQueryFilter extends QueryFilter
{
    public final SortedSet<byte[]> columns;

    public NamesQueryFilter(String key, QueryPath columnParent, SortedSet<byte[]> columns)
    {
        super(key, columnParent);
        this.columns = columns;
    }

    public NamesQueryFilter(String key, QueryPath columnParent, byte[] column)
    {
        this(key, columnParent, getSingleColumnSet(column));
    }

    private static TreeSet<byte[]> getSingleColumnSet(byte[] column)
    {
        Comparator<byte[]> singleColumnComparator = new Comparator<byte[]>()
        {
            public int compare(byte[] o1, byte[] o2)
            {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };
        TreeSet<byte[]> set = new TreeSet<byte[]>(singleColumnComparator);
        set.add(column);
        return set;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable, AbstractType comparator)
    {
        return memtable.getNamesIterator(this);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable, AbstractType comparator) throws IOException
    {
        return new SSTableNamesIterator(sstable.getFilename(), key, getColumnFamilyName(), columns);
    }

    public void filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        for (IColumn column : superColumn.getSubColumns())
        {
            if (!columns.contains(column.name()))
            {
                superColumn.remove(column.name());
            }
        }
    }

    public void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns, int gcBefore)
    {
        for (IColumn column : reducedColumns)
        {
            if (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore)
                returnCF.addColumn(column);
        }
    }
}
