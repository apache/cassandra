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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

public abstract class TombstonePurgingPartitionIterator extends FilteringPartitionIterator
{
    private final int gcBefore;

    public TombstonePurgingPartitionIterator(UnfilteredPartitionIterator iterator, int gcBefore)
    {
        super(iterator);
        this.gcBefore = gcBefore;
    }

    protected abstract long getMaxPurgeableTimestamp();

    protected FilteringRow makeRowFilter()
    {
        return new FilteringRow()
        {
            @Override
            protected boolean include(LivenessInfo info)
            {
                return !info.hasLocalDeletionTime() || !info.isPurgeable(getMaxPurgeableTimestamp(), gcBefore);
            }

            @Override
            protected boolean include(DeletionTime dt)
            {
                return includeDelTime(dt);
            }

            @Override
            protected boolean include(ColumnDefinition c, DeletionTime dt)
            {
                return includeDelTime(dt);
            }
        };
    }

    private boolean includeDelTime(DeletionTime dt)
    {
        return dt.isLive() || !dt.isPurgeable(getMaxPurgeableTimestamp(), gcBefore);
    }

    @Override
    protected boolean includePartitionDeletion(DeletionTime dt)
    {
        return includeDelTime(dt);
    }

    @Override
    protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        if (marker.isBoundary())
        {
            // We can only skip the whole marker if both deletion time are purgeable.
            // If only one of them is, filterTombstoneMarker will deal with it.
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
            return includeDelTime(boundary.endDeletionTime()) || includeDelTime(boundary.startDeletionTime());
        }
        else
        {
            return includeDelTime(((RangeTombstoneBoundMarker)marker).deletionTime());
        }
    }

    @Override
    protected RangeTombstoneMarker filterRangeTombstoneMarker(RangeTombstoneMarker marker, boolean reversed)
    {
        if (!marker.isBoundary())
            return marker;

        // Note that we know this is called after includeRangeTombstoneMarker. So if one of the deletion time is
        // purgeable, we know the other one isn't.
        RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
        if (!(includeDelTime(boundary.closeDeletionTime(reversed))))
            return boundary.createCorrespondingCloseBound(reversed);
        else if (!(includeDelTime(boundary.openDeletionTime(reversed))))
            return boundary.createCorrespondingOpenBound(reversed);
        return boundary;
    }

};
