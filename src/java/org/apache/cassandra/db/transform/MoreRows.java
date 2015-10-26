package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import static org.apache.cassandra.db.transform.Transformation.add;
import static org.apache.cassandra.db.transform.Transformation.mutable;

/**
 * An interface for providing new row contents for a partition.
 *
 * The new contents are produced as a normal arbitrary RowIterator or UnfilteredRowIterator (as appropriate),
 * with matching staticRow, partitionKey and partitionLevelDeletion.
 *
 * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
 * new contents as the new source.
 *
 * If the new source is itself a product of any transformations, the two transforming iterators are merged
 * so that control flow always occurs at the outermost point
 */
public interface MoreRows<I extends BaseRowIterator<?>> extends MoreContents<I>
{

    public static UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more)
    {
        return add(mutable(iterator), more);
    }

    public static RowIterator extend(RowIterator iterator, MoreRows<? super RowIterator> more)
    {
        return add(mutable(iterator), more);
    }

}

