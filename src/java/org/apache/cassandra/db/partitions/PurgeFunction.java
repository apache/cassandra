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

import java.util.function.LongPredicate;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<UnfilteredRowIterator>
{
    private final DeletionPurger purger;
    private final int nowInSec;

    private final boolean enforceStrictLiveness;
    private boolean isReverseOrder;

    private boolean ignoreGcGraceSeconds;

    public PurgeFunction(int nowInSec, int gcBefore, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones,
                         boolean enforceStrictLiveness)
    {
        this.nowInSec = nowInSec;
        this.purger = (timestamp, localDeletionTime) ->
                      !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                      && (localDeletionTime < gcBefore || ignoreGcGraceSeconds)
                      && getPurgeEvaluator().test(timestamp);
        this.enforceStrictLiveness = enforceStrictLiveness;
    }

    protected abstract LongPredicate getPurgeEvaluator();

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

    // Called at the beginning of each new partition
    // Return true if the current partitionKey ignores the gc_grace_seconds during compaction.
    protected boolean shouldIgnoreGcGrace()
    {
        return false;
    }

    protected void setReverseOrder(boolean isReverseOrder)
    {
        this.isReverseOrder = isReverseOrder;
    }

    @Override
    @SuppressWarnings("resource")
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        onNewPartition(partition.partitionKey());

        ignoreGcGraceSeconds = shouldIgnoreGcGrace();

        setReverseOrder(partition.isReverseOrder());
        UnfilteredRowIterator purged = Transformation.apply(partition, this);
        if (purged.isEmpty())
        {
            onEmptyPartitionPostPurge(purged.partitionKey());
            purged.close();
            return null;
        }

        return purged;
    }

    @Override
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    @Override
    protected Row applyToStatic(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec, enforceStrictLiveness);
    }

    @Override
    protected Row applyToRow(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec, enforceStrictLiveness);
    }

    @Override
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        updateProgress();
        boolean reversed = isReverseOrder;
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
}
