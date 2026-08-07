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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2ObjectHashMap;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.StorageService;

/**
 * Tracks, for each mutation journal segment, the set of local unrepaired sstables of tracked tables that
 * reference it.
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
 *
 * <p>A per-segment <em>set</em> of referrers (rather than a bare refcount) is kept so that individual
 * referrers can be located and dropped from a segment's set — needed to reason about keyspaces migrating
 * to and from tracked replication (CASSANDRA-21406) and to surface which sstables hold a segment.
 */
public class SegmentReferenceTracker implements INotificationConsumer
{
    // Guards both referrersBySegment and trackedSstables to keep transitions atomic across notifications.
    private final ReentrantLock lock = new ReentrantLock();

    // segment id -> set of local unrepaired sstables referencing it. A segment with no entry is unreferenced;
    // a set is removed as soon as it becomes empty, so isReferenced is a simple containsKey.
    private final Long2ObjectHashMap<Set<SSTableReader>> referrersBySegment = new Long2ObjectHashMap<>();

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
     * @param segmentId the identifier for the segment
     * @return whether any unrepaired local sstable references the given segment id
     */
    boolean isReferenced(long segmentId)
    {
        lock.lock();
        try
        {
            return referrersBySegment.containsKey(segmentId);
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
     * Release every reference currently held for the given sstables, dropping each from every segment's referrer
     * set. Used when a table migrates away from tracked replication (CASSANDRA-21406): such sstables are never
     * promoted to repaired (their reconcile→repaired path is gated on the table still being tracked), so their
     * journal-segment references would otherwise be pinned forever. Idempotent: sstables that aren't currently
     * tracked are ignored.
     */
    public void evict(Iterable<SSTableReader> sstables)
    {
        boolean anyReleased = false;
        lock.lock();
        try
        {
            for (SSTableReader sstable : sstables)
                anyReleased |= releaseIfTracked(sstable);
        }
        finally
        {
            lock.unlock();
        }
        if (anyReleased)
            onSegmentsUnreferenced.run();
    }

    /**
     * A sstable is tracked while it belongs to a still-tracked table, is locally originated, unrepaired, and
     * carries tracked mutations (non-empty coordinatorLogOffsets). Repaired sstables have been reconciled and no
     * longer need the journal to rebuild; sstables with empty coordinatorLogOffsets (untracked or pre-migration
     * data) reference the commit log rather than the mutation journal; sstables streamed from another host
     * reference that host's journal segments, not ours; and sstables of a table that has migrated away from
     * tracked will never be promoted to repaired, so counting them (or a compaction output that inherits their
     * offsets) would pin their segments forever — none of these must be counted (CASSANDRA-21406). Segment ref
     * tracking is purely local.
     */
    private boolean shouldTrack(SSTableReader sstable)
    {
        return isLocallyOriginated(sstable)
               && !sstable.isRepaired()
               && !sstable.getCoordinatorLogOffsets().isEmpty()
               && isTrackedTable(sstable);
    }

    private static boolean isTrackedTable(SSTableReader sstable)
    {
        ReplicationType replicationType = sstable.metadata().replicationType();
        return replicationType != null && replicationType.isTracked();
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
            forEachSegment(sstable, segmentId ->
                                    referrersBySegment.computeIfAbsent(segmentId, k -> new HashSet<>()).add(sstable));
    }

    /**
     * @return true if releasing this sstable emptied at least one segment's referrer set (i.e. that segment is
     * no longer referenced).
     */
    private boolean releaseIfTracked(SSTableReader sstable)
    {
        if (!trackedSstables.remove(sstable))
            return false;
        boolean[] anyEmptied = { false };
        forEachSegment(sstable, segmentId -> {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers != null && referrers.remove(sstable) && referrers.isEmpty())
            {
                referrersBySegment.remove(segmentId);
                anyEmptied[0] = true;
            }
        });
        return anyEmptied[0];
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

    /**
     * Number of unrepaired local sstables currently holding the given segment (for diagnostics / the vtable).
     */
    int referenceCount(long segmentId)
    {
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            return referrers == null ? 0 : referrers.size();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Sorted base filenames of the sstables currently holding the given segment (for diagnostics / the vtable).
     */
    public List<String> referrerDescriptors(long segmentId)
    {
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers == null || referrers.isEmpty())
                return Collections.emptyList();
            List<String> names = new ArrayList<>(referrers.size());
            for (SSTableReader sstable : referrers)
                names.add(sstable.descriptor.baseFile().name());
            Collections.sort(names);
            return names;
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    long referenceCountForTesting(long segmentId)
    {
        return referenceCount(segmentId);
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
    void addReferenceForTesting(long segmentId, SSTableReader referrer)
    {
        lock.lock();
        try
        {
            referrersBySegment.computeIfAbsent(segmentId, k -> new HashSet<>()).add(referrer);
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    void removeReferenceForTesting(long segmentId, SSTableReader referrer)
    {
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers != null && referrers.remove(referrer) && referrers.isEmpty())
                referrersBySegment.remove(segmentId);
        }
        finally
        {
            lock.unlock();
        }
    }
}
