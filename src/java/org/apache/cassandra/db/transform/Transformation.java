/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
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

    /**
     * Applied to the {@code PartitionColumns} of any rows iterator.
     *
     * NOTE: same remark than for applyToDeletion: it is only applied to the first iterator in a sequence of iterators
     * filled by MoreContents.
     */
    protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns)
    {
        return columns;
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

    /**
     * Even though this method is sumilar to `mutable`, it supresses the optimisation of avoiding creating an additional
     * wrapping interator object (which both creates an extra object and grows the call stack during the iteration), it
     * should be used with caution.
     *
     * It is useful in cases when the input has to be checked for more contents rather than directly checking if it
     * is stopped. For example, when concatenating two iterators (pseudocode):
     *
     *    iter1 = [row(1), row(2), row(3)]
     *    iter2 = [row(4), row(5), row(6)]
     *
     *    UnfilteredRowIterators.concat(DataLimits.cqlLimits(1).filter(iter1), DataLimits.cqlLimits(1).filter(iter1))
     *
     * Which should yield two rows: [row(1), row(4)].
     *
     * Using stacked transformations instead of wrapping would result into returning a single row, since the first
     * iterator will signal the iterator is stopped.
     */
    static UnfilteredRows wrapIterator(UnfilteredRowIterator iterator, RegularAndStaticColumns columns)
    {
        return new UnfilteredRows(iterator, columns);
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
