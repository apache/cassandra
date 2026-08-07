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
package org.apache.cassandra.replication;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2LongHashMap;

import accord.utils.Invariants;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.service.StorageService;

/**
 * Tracks how many unrepaired sstables of tracked tables reference each mutation journal segment.
 *
 * <p>A sstable references a segment iff the sstable's {@code StatsMetadata.commitLogIntervals} —
 * which for tracked tables stores mutation journal positions, not commit log positions — covers
 * any position within that segment. The granularity is coarse: the referenced range is the union
 * of the memtable lower/upper bounds for the sstable's lineage. This matches existing commit log
 * retention semantics; it can include a segment with no actual data from this sstable, which only
 * defers (never incorrectly enables) segment dropping.
 *
 * <p>Used by {@link MutationJournal} to decide when a static segment may be dropped: a segment may be
 * dropped only once it has no unrepaired references and {@code !needsReplay}. This guarantees the
 * journal can rebuild any unrepaired sstable from the journal if minority writes need to be filtered
 * out (CASSANDRA-21407).
 */
public class SegmentReferenceTracker implements INotificationConsumer
{
    private static final long NO_REF = 0L;

    // Guards both refsBySegment and trackedSstables to keep transitions atomic across notifications.
    private final ReentrantLock lock = new ReentrantLock();

    private final Long2LongHashMap refsBySegment = new Long2LongHashMap(NO_REF);

    // Sstables we currently hold refs for (i.e. those that were unrepaired at the time we observed them).
    // Required so SSTableRepairStatusChanged can transition an sstable in/out without the notification
    // having to carry the previous repair state.
    private final Set<SSTableReader> trackedSstables = new HashSet<>();

    // Invoked (outside the lock) whenever a notification drives at least one segment's reference count to zero,
    // so the journal can attempt to drop the now-unreferenced segment(s). This is one of the two events that make
    // a segment droppable (the other being its needsReplay flag being cleared) — CASSANDRA-21406.
    private final Runnable onSegmentsUnreferenced;

    // Resolves the local host id. Injectable for testing; may return null very early in startup (before
    // ClusterMetadata is ready), in which case no sstable is treated as locally originated.
    private final Supplier<UUID> localHostIdSupplier;

    public SegmentReferenceTracker(Runnable onSegmentsUnreferenced)
    {
        this(onSegmentsUnreferenced, StorageService.instance::getLocalHostUUID);
    }

    @VisibleForTesting
    SegmentReferenceTracker(Runnable onSegmentsUnreferenced, Supplier<UUID> localHostIdSupplier)
    {
        this.onSegmentsUnreferenced = onSegmentsUnreferenced;
        this.localHostIdSupplier = localHostIdSupplier;
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
            onAdded(((SSTableAddedNotification) notification).added);
        else if (notification instanceof InitialSSTableAddedNotification)
            onAdded(((InitialSSTableAddedNotification) notification).added);
        else if (notification instanceof SSTableListChangedNotification)
            onListChanged((SSTableListChangedNotification) notification);
        else if (notification instanceof SSTableRepairStatusChanged)
            onRepairStatusChanged(((SSTableRepairStatusChanged) notification).sstables);

        // Other lifecycle notifications are deliberately not handled because the actual sstable-lifecycle
        // effect is delivered by SSTableListChangedNotification:
        //   - SSTableDeletingNotification: fires when the on-disk files are scheduled for deletion, after
        //     the sstable has already left the live view via SSTableListChangedNotification. Handling it
        //     here would double-decrement.
        //   - TruncationNotification: truncate calls notifyTruncated for higher-level concerns (snapshots,
        //     truncatedAt persistence), then discardSSTables -> Tracker.dropSSTables -> notifySSTablesChanged
        //     which fires SSTableListChangedNotification(removed, empty) covering the refcount release.
        //   - TableDroppedNotification: drop table fires notifyDropped for MBean/snapshot cleanup, then
        //     CFS.invalidate(..., dropData=true) -> data.dropSSTables() which again fires
        //     SSTableListChangedNotification(removed, empty) covering the refcount release.
    }

    /**
     * Whether any unrepaired sstable references the given segment id.
     */
    boolean isReferenced(long segmentId)
    {
        lock.lock();
        try
        {
            return refsBySegment.get(segmentId) > NO_REF;
        }
        finally
        {
            lock.unlock();
        }
    }

    private void onAdded(Iterable<SSTableReader> added)
    {
        lock.lock();
        try
        {
            for (SSTableReader sstable : added)
                acquireIfTracked(sstable);
        }
        finally
        {
            lock.unlock();
        }
    }

    private void onListChanged(SSTableListChangedNotification notification)
    {
        boolean anyReleased = false;
        lock.lock();
        try
        {
            // Process additions before removals so refcounts are never observed briefly empty
            // between a compaction's input drop and output add when both span the same segment.
            for (SSTableReader sstable : notification.added)
                acquireIfTracked(sstable);
            for (SSTableReader sstable : notification.removed)
                anyReleased |= releaseIfTracked(sstable);
        }
        finally
        {
            lock.unlock();
        }
        if (anyReleased)
            onSegmentsUnreferenced.run();
    }

    private void onRepairStatusChanged(Collection<SSTableReader> changed)
    {
        boolean anyReleased = false;
        lock.lock();
        try
        {
            for (SSTableReader sstable : changed)
            {
                if (shouldTrack(sstable))
                    acquireIfTracked(sstable);
                else
                    anyReleased |= releaseIfTracked(sstable);
            }
        }
        finally
        {
            lock.unlock();
        }
        if (anyReleased)
            onSegmentsUnreferenced.run();
    }

    /**
     * A sstable is tracked while it is locally originated, unrepaired, and carries tracked mutations (non-empty
     * coordinatorLogOffsets). Repaired sstables have been reconciled and no longer need the journal to rebuild;
     * sstables with empty coordinatorLogOffsets (untracked or pre-migration data) reference the commit log rather
     * than the mutation journal; and sstables streamed from another host reference that host's journal segments,
     * not ours — none of these must be counted (CASSANDRA-21406). Segment ref tracking is purely local.
     */
    private boolean shouldTrack(SSTableReader sstable)
    {
        return isLocallyOriginated(sstable)
               && !sstable.isRepaired()
               && !sstable.getCoordinatorLogOffsets().isEmpty();
    }

    private boolean isLocallyOriginated(SSTableReader sstable)
    {
        UUID originatingHostId = sstable.getSSTableMetadata().originatingHostId;
        UUID localHostId = localHostIdSupplier.get();
        // Matches CommitLogReplayer / MetadataCollector: a null originating (or not-yet-known local) host id is
        // treated as not locally originated, and therefore not tracked.
        return originatingHostId != null && originatingHostId.equals(localHostId);
    }

    private void acquireIfTracked(SSTableReader sstable)
    {
        if (shouldTrack(sstable) && trackedSstables.add(sstable))
            forEachSegment(sstable, this::incrementRef);
    }

    /**
     * @return true if releasing this sstable drove at least one segment's reference count to zero.
     */
    private boolean releaseIfTracked(SSTableReader sstable)
    {
        if (!trackedSstables.remove(sstable))
            return false;
        boolean[] anyReachedZero = { false };
        forEachSegment(sstable, segmentId -> anyReachedZero[0] |= decrementRef(segmentId));
        return anyReachedZero[0];
    }

    private void incrementRef(long segmentId)
    {
        refsBySegment.compute(segmentId, (k, currentValue) -> currentValue + 1);
    }

    /**
     * @return true if the segment's reference count reached zero (i.e. it is no longer referenced).
     */
    private boolean decrementRef(long segmentId)
    {
        long remaining = refsBySegment.compute(segmentId, (k, prev) -> {
            Invariants.require(prev > NO_REF, "Refcount underflow for segment %d", segmentId);
            return prev - 1;
        });
        return remaining == NO_REF;
    }

    private static void forEachSegment(SSTableReader sstable, LongConsumer consumer)
    {
        IntervalSet<CommitLogPosition> intervals = sstable.getSSTableMetadata().commitLogIntervals;
        if (intervals.isEmpty())
            return;

        // IntervalSet guarantees starts and ends are returned in matching order.
        Iterator<CommitLogPosition> startIt = intervals.starts().iterator();
        Iterator<CommitLogPosition> endIt = intervals.ends().iterator();
        while (startIt.hasNext())
        {
            CommitLogPosition start = startIt.next();
            CommitLogPosition end = endIt.next();
            for (long s = start.segmentId; s <= end.segmentId; s++)
                consumer.accept(s);
        }
    }

    @VisibleForTesting
    long referenceCountForTesting(long segmentId)
    {
        lock.lock();
        try
        {
            return refsBySegment.get(segmentId);
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    int trackedSstableCountForTesting()
    {
        lock.lock();
        try
        {
            return trackedSstables.size();
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    void incrementRefForTesting(long segmentId)
    {
        lock.lock();
        try
        {
            incrementRef(segmentId);
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    void decrementRefForTesting(long segmentId)
    {
        lock.lock();
        try
        {
            decrementRef(segmentId);
        }
        finally
        {
            lock.unlock();
        }
    }
}
