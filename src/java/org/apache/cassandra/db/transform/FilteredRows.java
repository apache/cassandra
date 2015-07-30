package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public final class FilteredRows extends BaseRows<Row, BaseRowIterator<?>> implements RowIterator
{
    FilteredRows(RowIterator input)
    {
        super(input);
    }

    FilteredRows(UnfilteredRowIterator input, Filter filter)
    {
        super(input);
        add(filter);
    }

    FilteredRows(Filter filter, UnfilteredRows input)
    {
        super(input);
        add(filter);
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && !hasNext();
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator, transforming it into a RowIterator.
     */
    public static RowIterator filter(UnfilteredRowIterator iterator, int nowInSecs)
    {
        return new Filter(false, nowInSecs).applyToPartition(iterator);
    }
}
