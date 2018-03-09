/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

import com.google.common.annotations.VisibleForTesting;

/**
 * A utility class to split the given {@link#UnfilteredRowIterator} into smaller chunks each
 * having at most {@link #throttle} + 1 unfiltereds.
 *
 * Only the first output contains partition level info: {@link UnfilteredRowIterator#partitionLevelDeletion}
 * and {@link UnfilteredRowIterator#staticRow}.
 *
 * Besides splitting, this iterator will also ensure each chunk does not finish with an open tombstone marker,
 * by closing any opened tombstone markers and re-opening on the next chunk.
 *
 * The lifecycle of outputed {{@link UnfilteredRowIterator} only last till next call to {@link #next()}.
 *
 * A subsequent {@link #next} call will exhaust the previously returned iterator before computing the next,
 * effectively skipping unfiltereds up to the throttle size.
 *
 * Closing this iterator will close the underlying iterator.
 *
 */
public class ThrottledUnfilteredIterator extends AbstractIterator<UnfilteredRowIterator> implements CloseableIterator<UnfilteredRowIterator>
{
    private final UnfilteredRowIterator origin;
    private final int throttle;

    // internal mutable state
    private UnfilteredRowIterator throttledItr;

    // extra unfiltereds from previous iteration
    private Iterator<Unfiltered> overflowed = Collections.emptyIterator();

    @VisibleForTesting
    ThrottledUnfilteredIterator(UnfilteredRowIterator origin, int throttle)
    {
        assert origin != null;
        assert throttle > 1 : "Throttle size must be higher than 1 to properly support open and close tombstone boundaries.";
        this.origin = origin;
        this.throttle = throttle;
        this.throttledItr = null;
    }

    @Override
    protected UnfilteredRowIterator computeNext()
    {
        // exhaust previous throttled iterator
        while (throttledItr != null && throttledItr.hasNext())
            throttledItr.next();

        if (!origin.hasNext())
            return endOfData();

        throttledItr = new WrappingUnfilteredRowIterator(origin)
        {
            private int count = 0;
            private boolean isFirst = throttledItr == null;

            // current batch's openMarker. if it's generated in previous batch,
            // it must be consumed as first element of current batch
            private RangeTombstoneMarker openMarker;

            // current batch's closeMarker.
            // it must be consumed as last element of current batch
            private RangeTombstoneMarker closeMarker = null;

            @Override
            public boolean hasNext()
            {
                return (withinLimit() && wrapped.hasNext()) || closeMarker != null;
            }

            @Override
            public Unfiltered next()
            {
                if (closeMarker != null)
                {
                    assert count == throttle;
                    Unfiltered toReturn = closeMarker;
                    closeMarker = null;
                    return toReturn;
                }

                Unfiltered next;
                assert withinLimit();
                // in the beginning of the batch, there might be remaining unfiltereds from previous iteration
                if (overflowed.hasNext())
                    next = overflowed.next();
                else
                    next = wrapped.next();
                recordNext(next);
                return next;
            }

            private void recordNext(Unfiltered unfiltered)
            {
                count++;
                if (unfiltered.isRangeTombstoneMarker())
                    updateMarker((RangeTombstoneMarker) unfiltered);
                // when reach throttle with a remaining openMarker, we need to create corresponding closeMarker.
                if (count == throttle && openMarker != null)
                {
                    assert wrapped.hasNext();
                    closeOpenMarker(wrapped.next());
                }
            }

            private boolean withinLimit()
            {
                return count < throttle;
            }

            private void updateMarker(RangeTombstoneMarker marker)
            {
                openMarker = marker.isOpen(isReverseOrder()) ? marker : null;
            }

            /**
             * There 3 cases for next, 1. if it's boundaryMarker, we split it as closeMarker for current batch, next
             * openMarker for next batch 2. if it's boundMakrer, it must be closeMarker. 3. if it's Row, create
             * corresponding closeMarker for current batch, and create next openMarker for next batch including current
             * Row.
             */
            private void closeOpenMarker(Unfiltered next)
            {
                assert openMarker != null;

                if (next.isRangeTombstoneMarker())
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
                    // if it's boundary, create closeMarker for current batch and openMarker for next batch
                    if (marker.isBoundary())
                    {
                        RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                        closeMarker = boundary.createCorrespondingCloseMarker(isReverseOrder());
                        overflowed = Collections.singleton((Unfiltered)boundary.createCorrespondingOpenMarker(isReverseOrder())).iterator();
                    }
                    else
                    {
                        // if it's bound, it must be closeMarker.
                        assert marker.isClose(isReverseOrder());
                        updateMarker(marker);
                        closeMarker = marker;
                    }
                }
                else
                {
                    // it's Row, need to create closeMarker for current batch and openMarker for next batch
                    DeletionTime openDeletion = openMarker.openDeletionTime(isReverseOrder());
                    ByteBuffer[] buffers = next.clustering().getRawValues();
                    closeMarker = RangeTombstoneBoundMarker.exclusiveClose(isReverseOrder(), buffers, openDeletion);

                    // for next batch
                    overflowed = Arrays.asList(RangeTombstoneBoundMarker.inclusiveOpen(isReverseOrder(),
                                                                                       buffers,
                                                                                       openDeletion), next).iterator();
                }
            }

            @Override
            public DeletionTime partitionLevelDeletion()
            {
                return isFirst ? wrapped.partitionLevelDeletion() : DeletionTime.LIVE;
            }

            @Override
            public Row staticRow()
            {
                return isFirst ? wrapped.staticRow() : Rows.EMPTY_STATIC_ROW;
            }

            @Override
            public void close()
            {
                // no op
            }
        };
        return throttledItr;
    }

    public void close()
    {
        if (origin != null)
            origin.close();
    }

    /**
     * Splits a {@link UnfilteredPartitionIterator} in {@link UnfilteredRowIterator} batches with size no higher
     * than <b>maxBatchSize</b>
     */
    public static CloseableIterator<UnfilteredRowIterator> throttle(UnfilteredPartitionIterator partitionIterator, int maxBatchSize)
    {
        return new AbstractIterator<UnfilteredRowIterator>()
        {
            ThrottledUnfilteredIterator current = null;

            protected UnfilteredRowIterator computeNext()
            {
                if (current != null && !current.hasNext())
                {
                    current.close();
                    current = null;
                }

                if (current == null && partitionIterator.hasNext())
                {
                    current = new ThrottledUnfilteredIterator(partitionIterator.next(), maxBatchSize);
                    assert current.hasNext() : "UnfilteredPartitionIterator should not contain empty partitions";
                }

                if (current != null && current.hasNext())
                    return current.next();

                return endOfData();
            }

            public void close()
            {
                if (current != null)
                    current.close();
            }
        };
    }
}
