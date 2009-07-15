package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.*;

public abstract class QueryFilter
{
    public final String key;
    public final QueryPath path;

    protected QueryFilter(String key, QueryPath path)
    {
        this.key = key;
        this.path = path;
    }

    /**
     * returns an iterator that returns columns from the given memtable
     * matching the Filter criteria in sorted order.
     */
    public abstract ColumnIterator getMemColumnIterator(Memtable memtable);

    /**
     * returns an iterator that returns columns from the given SSTable
     * matching the Filter criteria in sorted order.
     */
    public abstract ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException;

    /**
     * collects columns from reducedColumns into returnCF.  Termination is determined
     * by the filter code, which should have some limit on the number of columns
     * to avoid running out of memory on large rows.
     */
    public abstract void collectColumns(ColumnFamily returnCF, ReducingIterator<IColumn> reducedColumns);

    /**
     * subcolumns of a supercolumn are unindexed, so to pick out parts of those we operate in-memory.
     * @param superColumn
     */
    public abstract void filterSuperColumn(SuperColumn superColumn);

    public Comparator<IColumn> getColumnComparator()
    {
        return new Comparator<IColumn>()
        {
            public int compare(IColumn c1, IColumn c2)
            {
                return c1.name().compareTo(c2.name());
            }
        };
    }
    
    public void collectColumns(final ColumnFamily returnCF, Iterator collatedColumns)
    {
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        ReducingIterator<IColumn> reduced = new ReducingIterator<IColumn>(collatedColumns)
        {
            ColumnFamily curCF = returnCF.cloneMeShallow();

            protected Object getKey(IColumn o)
            {
                return o == null ? null : o.name();
            }

            public void reduce(IColumn current)
            {
                curCF.addColumn(current);
            }

            protected IColumn getReduced()
            {
                IColumn c = curCF.getAllColumns().first();
                curCF.clear();
                return c;
            }
        };

        collectColumns(returnCF, reduced);
    }

    public String getColumnFamilyName()
    {
        return path.columnFamilyName;
    }
}
