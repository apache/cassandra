package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

/**
 * We have a single common superclass for all Transformations to make implementation efficient.
 * we have a shared stack for all transformations, and can share the same transformation across partition and row
 * iterators, reducing garbage. Internal code is also simplified by always having a basic no-op implementation to invoke.
 *
 * Only the necessary methods need be overridden. Early termination is provided by invoking the method's stop or stopInPartition
 * methods, rather than having their own abstract method to invoke, as this is both more efficient and simpler to reason about.
 */
public abstract class Transformation<I extends BaseRowIterator<?>>
{
    // internal methods for StoppableTransformation only
    void attachTo(BasePartitions partitions) { }
    void attachTo(BaseRows rows) { }

    /**
     * Run on the close of any (logical) partitions iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    protected void onClose() { }

    /**
     * Run on the close of any (logical) rows iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    protected void onPartitionClose() { }

    /**
     * Applied to any rows iterator (partition) we encounter in a partitions iterator
     */
    protected I applyToPartition(I partition)
    {
        return partition;
    }

    /**
     * Applied to any row we encounter in a rows iterator
     */
    protected Row applyToRow(Row row)
    {
        return row;
    }

    /**
     * Applied to any RTM we encounter in a rows/unfiltered iterator
     */
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        return marker;
    }

    /**
     * Applied to the partition key of any rows/unfiltered iterator we are applied to
     */
    protected DecoratedKey applyToPartitionKey(DecoratedKey key) { return key; }

    /**
     * Applied to the static row of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    protected Row applyToStatic(Row row)
    {
        return row;
    }

    /**
     * Applied to the partition-level deletion of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return deletionTime;
    }


    //******************************************************
    //          Static Application Methods
    //******************************************************


    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Transformation<? super UnfilteredRowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static PartitionIterator apply(PartitionIterator iterator, Transformation<? super RowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static RowIterator apply(RowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }

    static UnfilteredPartitions mutable(UnfilteredPartitionIterator iterator)
    {
        return iterator instanceof UnfilteredPartitions
               ? (UnfilteredPartitions) iterator
               : new UnfilteredPartitions(iterator);
    }
    static FilteredPartitions mutable(PartitionIterator iterator)
    {
        return iterator instanceof FilteredPartitions
               ? (FilteredPartitions) iterator
               : new FilteredPartitions(iterator);
    }
    static UnfilteredRows mutable(UnfilteredRowIterator iterator)
    {
        return iterator instanceof UnfilteredRows
               ? (UnfilteredRows) iterator
               : new UnfilteredRows(iterator);
    }
    static FilteredRows mutable(RowIterator iterator)
    {
        return iterator instanceof FilteredRows
               ? (FilteredRows) iterator
               : new FilteredRows(iterator);
    }

    static <E extends BaseIterator> E add(E to, Transformation add)
    {
        to.add(add);
        return to;
    }
    static <E extends BaseIterator> E add(E to, MoreContents add)
    {
        to.add(add);
        return to;
    }

}
