package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

public final class FilteredPartitions extends BasePartitions<RowIterator, BasePartitionIterator<?>> implements PartitionIterator
{
    // wrap basic iterator for transformation
    FilteredPartitions(PartitionIterator input)
    {
        super(input);
    }

    // wrap basic unfiltered iterator for transformation, applying filter as first transformation
    FilteredPartitions(UnfilteredPartitionIterator input, Filter filter)
    {
        super(input);
        add(filter);
    }

    // copy from an UnfilteredPartitions, applying a filter to convert it
    FilteredPartitions(Filter filter, UnfilteredPartitions copyFrom)
    {
        super(copyFrom);
        add(filter);
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator's iterators, transforming it into a PartitionIterator.
     */
    public static PartitionIterator filter(UnfilteredPartitionIterator iterator, int nowInSecs)
    {
        Filter filter = new Filter(!iterator.isForThrift(), nowInSecs);
        if (iterator instanceof UnfilteredPartitions)
            return new FilteredPartitions(filter, (UnfilteredPartitions) iterator);
        return new FilteredPartitions(iterator, filter);
    }
}
