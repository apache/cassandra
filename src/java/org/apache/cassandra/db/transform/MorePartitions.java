package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;

import static org.apache.cassandra.db.transform.Transformation.add;
import static org.apache.cassandra.db.transform.Transformation.mutable;

/**
 * An interface for providing new partitions for a partitions iterator.
 *
 * The new contents are produced as a normal arbitrary PartitionIterator or UnfilteredPartitionIterator (as appropriate)
 *
 * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
 * new contents as the new source.
 *
 * If the new source is itself a product of any transformations, the two transforming iterators are merged
 * so that control flow always occurs at the outermost point
 */
public interface MorePartitions<I extends BasePartitionIterator<?>> extends MoreContents<I>
{

    public static UnfilteredPartitionIterator extend(UnfilteredPartitionIterator iterator, MorePartitions<? super UnfilteredPartitionIterator> more)
    {
        return add(mutable(iterator), more);
    }

    public static PartitionIterator extend(PartitionIterator iterator, MorePartitions<? super PartitionIterator> more)
    {
        return add(mutable(iterator), more);
    }

}

