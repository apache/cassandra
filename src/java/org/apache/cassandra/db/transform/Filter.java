package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.rows.*;

final class Filter extends Transformation
{
    private final boolean filterEmpty; // generally maps to !isForThrift, but also false for direct row filtration
    private final int nowInSec;
    public Filter(boolean filterEmpty, int nowInSec)
    {
        this.filterEmpty = filterEmpty;
        this.nowInSec = nowInSec;
    }

    public RowIterator applyToPartition(BaseRowIterator iterator)
    {
        RowIterator filtered = iterator instanceof UnfilteredRows
                               ? new FilteredRows(this, (UnfilteredRows) iterator)
                               : new FilteredRows((UnfilteredRowIterator) iterator, this);

        if (filterEmpty && closeIfEmpty(filtered))
            return null;

        return filtered;
    }

    public Row applyToStatic(Row row)
    {
        if (row.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        row = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    public Row applyToRow(Row row)
    {
        return row.purge(DeletionPurger.PURGE_ALL, nowInSec);
    }

    public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        return null;
    }

    private static boolean closeIfEmpty(BaseRowIterator<?> iter)
    {
        if (iter.isEmpty())
        {
            iter.close();
            return true;
        }
        return false;
    }
}
