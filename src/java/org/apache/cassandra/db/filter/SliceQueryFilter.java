package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.collections.iterators.ReverseListIterator;

import org.apache.cassandra.io.SSTableReader;
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
        return new SSTableSliceIterator(sstable.getFilename(), key, comparator, start, isAscending);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (isAscending)
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }
        else
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = new ReverseListIterator(columnsAsList);
        }
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    @Override
    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        return isAscending ? super.getColumnComparator(comparator) : new ReverseComparator(super.getColumnComparator(comparator));
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
                && ((isAscending && comparator.compare(column.name(), finish) > 0))
                    || (!isAscending && comparator.compare(column.name(), finish) < 0))
                break;

            if (!column.isMarkedForDelete())
                liveColumns++;

            if (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore)
                container.addColumn(column);
        }
    }
}
