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

package org.apache.cassandra.io.sstable.compaction;

import java.util.function.LongPredicate;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A wrapping cursor that applies tombstone purging, counterpart to
 * {@link org.apache.cassandra.db.compaction.CompactionIterator.Purger}. Purging is the process of removing tombstones
 * that do not need to be preserved, defined at minimum as:
 * - there is no data in sstables not taking part in this compaction that may be covered by the tombstone, and
 * - the gc_grace period, during which we protect tombstones to ensure that they are propagated to other replicas, has
 *   expired, and
 * - we are compacting repaired sstables or the CFS does not request that only repaired tombstones are purged.
 * Additionally, the purger converts expiring cells that have gone beyond their time-to-live to tombstones (deleting
 * their data), and collects said tombstones if they are purgeable.
 *
 * Note that this may end up creating empty rows (i.e. headers with no deletion/timestamp and no cells) -- typically
 * a SkipEmptyDataCursor would be required to apply on top of the result.
 */
public class PurgeCursor implements SSTableCursor, DeletionPurger
{
    private final SSTableCursor wrapped;
    private final CompactionController controller;
    private final int nowInSec;
    private final int gcBefore;
    private final boolean purgeTombstones;
    private LongPredicate purgeEvaluator;

    private DeletionTime partitionLevelDeletion;
    private DeletionTime activeRangeDeletion = DeletionTime.LIVE;
    private DeletionTime rowLevelDeletion;
    private DeletionTime complexColumnDeletion;
    private LivenessInfo clusteringKeyLivenessInfo;
    private ClusteringPrefix<?> clusteringKey;
    private Cell<?> cell;

    public PurgeCursor(SSTableCursor wrapped, CompactionController controller, int nowInSec)
    {
        this.gcBefore = controller.gcBefore;
        this.purgeTombstones = controller.compactingRepaired(); // this is also true if !cfs.onlyPurgeRepairedTombstones
        this.wrapped = wrapped;
        this.controller = controller;
        this.nowInSec = nowInSec;
    }

    public boolean shouldPurge(long timestamp, int localDeletionTime)
    {
        return purgeTombstones
               && localDeletionTime < gcBefore
               && getPurgeEvaluator().test(timestamp);
    }

    /*
     * Evaluates whether a tombstone with the given deletion timestamp can be purged. This is the minimum
     * timestamp for any sstable containing `currentKey` outside of the set of sstables involved in this compaction.
     * This is computed lazily on demand as we only need this if there is tombstones and this a bit expensive
     * (see #8914).
     */
    protected LongPredicate getPurgeEvaluator()
    {
        if (purgeEvaluator == null)
            purgeEvaluator = controller.getPurgeEvaluator(partitionKey());

        return purgeEvaluator;
    }

    public Type advance()
    {
        if (wrapped.type() == Type.RANGE_TOMBSTONE)
            activeRangeDeletion = rowLevelDeletion;

        while (true)
        {
            Type type = wrapped.advance();
            switch (type)
            {
                case EXHAUSTED:
                    return type;
                case PARTITION:
                    purgeEvaluator = null;
                    partitionLevelDeletion = maybePurge(wrapped.partitionLevelDeletion());
                    assert activeRangeDeletion == DeletionTime.LIVE;
                    return type;
                case RANGE_TOMBSTONE:
                    rowLevelDeletion = maybePurge(wrapped.rowLevelDeletion());
                    clusteringKey = maybePurge(wrapped.clusteringKey(), activeRangeDeletion, rowLevelDeletion);
                    if (clusteringKey != null)
                        return type;
                    else
                        break;  // no bound remained, move on to next item
                case ROW:
                    clusteringKey = wrapped.clusteringKey();
                    rowLevelDeletion = maybePurge(wrapped.rowLevelDeletion());
                    clusteringKeyLivenessInfo = maybePurge(wrapped.clusteringKeyLivenessInfo(), nowInSec);
                    return type;
                case COMPLEX_COLUMN:
                    this.complexColumnDeletion = maybePurge(wrapped.complexColumnDeletion());
                    return type;
                case SIMPLE_COLUMN:
                case COMPLEX_COLUMN_CELL:
                    // This also applies cells' time-to-live, converting expired cells to tombstones.
                    cell = wrapped.cell().purge(this, nowInSec);
                    if (cell != null)
                        return type;
                    break;  // otherwise, skip this cell
                default:
                    throw new AssertionError();
            }
        }
    }

    private DeletionTime maybePurge(DeletionTime deletionTime)
    {
        return shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    private LivenessInfo maybePurge(LivenessInfo liveness, int nowInSec)
    {
        return shouldPurge(liveness, nowInSec) ? LivenessInfo.EMPTY : liveness;
    }

    private ClusteringPrefix<?> maybePurge(ClusteringPrefix<?> clusteringKey, DeletionTime deletionBefore, DeletionTime deletionAfter)
    {
        // We pass only the current deletion to the purger. This may mean close bounds' deletion time is
        // already purged.
        if (deletionBefore.isLive() && clusteringKey.kind().isEnd())
        {
            // we need to strip the closing part of the tombstone
            // if only a close bound, or the new deletion is also purged, do not return
            if (clusteringKey.kind().isBound() || deletionAfter.isLive())
                return null;

            return ClusteringBound.create(clusteringKey.kind().openBoundOfBoundary(false), clusteringKey);
        }
        else if (clusteringKey.kind().isStart() && deletionAfter.isLive())
        {
            // we need to strip the opening part of the tombstone
            if (clusteringKey.kind().isBound())
                return null;  // only an open bound whose time is now purged. Do not return.
            assert !deletionBefore.isLive();  // If ending was also deleted, we would have gone through the path above.
            return ClusteringBound.create(clusteringKey.kind().closeBoundOfBoundary(false), clusteringKey);
        }
        else    // Nothing is dropped
            return clusteringKey;
    }

    public Type type()
    {
        return wrapped.type();
    }

    public DecoratedKey partitionKey()
    {
        return wrapped.partitionKey();
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public ClusteringPrefix clusteringKey()
    {
        return clusteringKey;
    }

    public LivenessInfo clusteringKeyLivenessInfo()
    {
        return clusteringKeyLivenessInfo;
    }

    public DeletionTime rowLevelDeletion()
    {
        return rowLevelDeletion;
    }

    public DeletionTime activeRangeDeletion()
    {
        return activeRangeDeletion;
    }

    public DeletionTime complexColumnDeletion()
    {
        return complexColumnDeletion;
    }

    public ColumnMetadata column()
    {
        return wrapped.column();
    }

    public Cell cell()
    {
        return cell;
    }

    public long bytesProcessed()
    {
        return wrapped.bytesProcessed();
    }

    public long bytesTotal()
    {
        return wrapped.bytesTotal();
    }

    public void close()
    {
        wrapped.close();
    }
}
