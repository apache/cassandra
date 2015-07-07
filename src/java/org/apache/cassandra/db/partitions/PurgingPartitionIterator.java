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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

public abstract class PurgingPartitionIterator extends WrappingUnfilteredPartitionIterator
{
    private final DeletionPurger purger;
    private final int gcBefore;

    private UnfilteredRowIterator next;

    public PurgingPartitionIterator(UnfilteredPartitionIterator iterator, int gcBefore, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones)
    {
        super(iterator);
        this.gcBefore = gcBefore;
        this.purger = new DeletionPurger()
        {
            public boolean shouldPurge(long timestamp, int localDeletionTime)
            {
                if (onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                    return false;

                return timestamp < getMaxPurgeableTimestamp() && localDeletionTime < gcBefore;
            }
        };
    }

    protected abstract long getMaxPurgeableTimestamp();

    // Called at the beginning of each new partition
    protected void onNewPartition(DecoratedKey partitionKey)
    {
    }

    // Called for each partition that had only purged infos and are empty post-purge.
    protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey)
    {
    }

    // Called for every unfiltered. Meant for CompactionIterator to update progress
    protected void updateProgress()
    {
    }

    @Override
    public boolean hasNext()
    {
        while (next == null && super.hasNext())
        {
            UnfilteredRowIterator iterator = super.next();
            onNewPartition(iterator.partitionKey());

            UnfilteredRowIterator purged = purge(iterator);
            if (isForThrift() || !purged.isEmpty())
            {
                next = purged;
                return true;
            }

            onEmptyPartitionPostPurge(purged.partitionKey());
        }
        return next != null;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        UnfilteredRowIterator toReturn = next;
        next = null;
        return toReturn;
    }

    private UnfilteredRowIterator purge(final UnfilteredRowIterator iter)
    {
        return new AlteringUnfilteredRowIterator(iter)
        {
            @Override
            public DeletionTime partitionLevelDeletion()
            {
                DeletionTime dt = iter.partitionLevelDeletion();
                return purger.shouldPurge(dt) ? DeletionTime.LIVE : dt;
            }

            @Override
            public Row computeNextStatic(Row row)
            {
                return row.purge(purger, gcBefore);
            }

            @Override
            public Row computeNext(Row row)
            {
                return row.purge(purger, gcBefore);
            }

            @Override
            public RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
            {
                boolean reversed = isReverseOrder();
                if (marker.isBoundary())
                {
                    // We can only skip the whole marker if both deletion time are purgeable.
                    // If only one of them is, filterTombstoneMarker will deal with it.
                    RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
                    boolean shouldPurgeClose = purger.shouldPurge(boundary.closeDeletionTime(reversed));
                    boolean shouldPurgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));

                    if (shouldPurgeClose)
                    {
                        if (shouldPurgeOpen)
                            return null;

                        return boundary.createCorrespondingOpenMarker(reversed);
                    }

                    return shouldPurgeOpen
                         ? boundary.createCorrespondingCloseMarker(reversed)
                         : marker;
                }
                else
                {
                    return purger.shouldPurge(((RangeTombstoneBoundMarker)marker).deletionTime()) ? null : marker;
                }
            }

            @Override
            public Unfiltered next()
            {
                Unfiltered next = super.next();
                updateProgress();
                return next;
            }
        };
    }
};
