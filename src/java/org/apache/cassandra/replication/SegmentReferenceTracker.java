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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.LongHashSet.LongIterator;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.journal.KeyStats;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.ReplicationType;

/**
 * Tracks, for each mutation journal segment, whether any unreconciled sstables could reference mutation ids it contains
 *
 * <p>An sstable references a segment if its {@code StatsMetadata.coordinatorLogOffsets} overlap with the segment's
 * recorded offset ranges for any coordinator log.
 *
 * <p>Used by {@link MutationJournal} to decide when a static segment can be dropped: a segment can be
 * dropped only when it has no unrepaired references and {@code !needsReplay}. This guarantees the journal can rebuild
 * any unrepaired sstable from the journal if minority writes need to be filtered out (CASSANDRA-21407).
 *
 * <p>A per-segment <em>set</em> of referrers (rather than a bare refcount) is kept so that individual
 * referrers can be located and dropped from a segment's set — needed to reason about keyspaces migrating
 * to and from tracked replication (CASSANDRA-21406) and to surface which sstables hold a segment.
 *
 */
public class SegmentReferenceTracker implements INotificationConsumer
{
    private final ReentrantLock lock = new ReentrantLock();

    // Index mapping each segment id to the set of unrepaired SSTables that overlap it.
    // Used by the compactor to quickly determine if a segment can be dropped.
    private final Long2ObjectHashMap<Set<SSTableReader>> referrersBySegment = new Long2ObjectHashMap<>();

    // Reverse index mapping each tracked SSTable to the journal segment ids it references.
    // Used to remove references across all impacted segments when an SSTable is deleted or repaired.
    private final Map<SSTableReader, LongHashSet> segmentsBySSTable = new HashMap<>();

    private final Runnable onSegmentsUnreferenced;

    private final Supplier<UUID> localHostIdSupplier;

    private final Supplier<Iterable<Segment<ShortMutationId, Mutation>>> allSegmentsSupplier;

    public SegmentReferenceTracker(Runnable onSegmentsUnreferenced,
                                   Supplier<UUID> localHostIdSupplier,
                                   Supplier<Iterable<Segment<ShortMutationId, Mutation>>> allSegmentsSupplier)
    {
        this.onSegmentsUnreferenced = onSegmentsUnreferenced;
        this.localHostIdSupplier = localHostIdSupplier;
        this.allSegmentsSupplier = allSegmentsSupplier;
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
    }

    /**
     * @param segment the journal segment
     * @return whether any unrepaired local sstable references the given segment
     */
    public boolean isReferenced(Segment<ShortMutationId, Mutation> segment)
    {
        return segment != null && isReferenced(segment.id());
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
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers == null)
                return false;
            if (referrers.isEmpty())
            {
                referrersBySegment.remove(segmentId);
                return false;
            }
            return true;
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
     * Release every reference currently held for the given sstables.
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
     * @return an immutable snapshot of the currently tracked SSTables
     */
    public Set<SSTableReader> trackedSSTables()
    {
        lock.lock();
        try
        {
            if (segmentsBySSTable.isEmpty())
                return Set.of();
            segmentsBySSTable.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
            if (segmentsBySSTable.isEmpty())
                return Set.of();
            return Set.copyOf(segmentsBySSTable.keySet());
        }
        finally
        {
            lock.unlock();
        }
    }

    boolean shouldTrack(SSTableReader sstable)
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
        return originatingHostId != null && originatingHostId.equals(localHostId);
    }

    private void acquireIfTracked(SSTableReader sstable)
    {
        if (!shouldTrack(sstable) || segmentsBySSTable.containsKey(sstable))
            return;

        LongHashSet segmentIds = new LongHashSet();
        if (allSegmentsSupplier != null)
        {
            Iterable<Segment<ShortMutationId, Mutation>> allSegments = allSegmentsSupplier.get();
            if (allSegments != null)
            {
                for (Segment<ShortMutationId, Mutation> segment : allSegments)
                {
                    KeyStats<ShortMutationId> keyStats = segment.keyStats();
                    if (keyStats instanceof MutationJournal.OffsetRanges)
                    {
                        MutationJournal.OffsetRanges segRanges = (MutationJournal.OffsetRanges) keyStats;
                        if (segRanges.overlaps(sstable.getCoordinatorLogOffsets()))
                        {
                            segmentIds.add(segment.id());
                            referrersBySegment.computeIfAbsent(segment.id(), k -> new HashSet<>()).add(sstable);
                        }
                    }
                }
            }
        }
        if (!segmentIds.isEmpty())
            segmentsBySSTable.put(sstable, segmentIds);
    }

    private boolean releaseIfTracked(SSTableReader sstable)
    {
        LongHashSet segmentIds = segmentsBySSTable.remove(sstable);
        if (segmentIds == null || segmentIds.isEmpty())
            return false;

        boolean anyEmptied = false;
        LongIterator idIterator = segmentIds.iterator();
        while (idIterator.hasNext())
        {
            long segmentId = idIterator.nextValue();
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers != null && referrers.remove(sstable) && referrers.isEmpty())
            {
                referrersBySegment.remove(segmentId);
                anyEmptied = true;
            }
        }
        return anyEmptied;
    }

    /**
     * Number of unrepaired local sstables currently holding the given segment (for diagnostics / the vtable).
     */
    public int referenceCount(Segment<ShortMutationId, Mutation> segment)
    {
        return segment != null ? referenceCount(segment.id()) : 0;
    }

    public int referenceCount(long segmentId)
    {
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers == null)
                return 0;
            if (referrers.isEmpty())
            {
                referrersBySegment.remove(segmentId);
                return 0;
            }
            return referrers.size();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Sorted base filenames of the sstables currently holding the given segment (for diagnostics / the vtable).
     */
    public List<String> referrerDescriptors(Segment<ShortMutationId, Mutation> segment)
    {
        return segment != null ? referrerDescriptors(segment.id()) : Collections.emptyList();
    }

    public List<String> referrerDescriptors(long segmentId)
    {
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers == null || referrers.isEmpty())
            {
                if (referrers != null)
                    referrersBySegment.remove(segmentId);
                return Collections.emptyList();
            }
            List<String> names = new ArrayList<>(referrers.size());
            for (SSTableReader sstable : referrers)
                names.add(sstable.descriptor != null ? sstable.descriptor.baseFile().name() : sstable.getFilename());
            Collections.sort(names);
            return names;
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    long referenceCountForTesting(Segment<ShortMutationId, Mutation> segment)
    {
        return referenceCount(segment);
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
            segmentsBySSTable.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
            return segmentsBySSTable.size();
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
            segmentsBySSTable.computeIfAbsent(referrer, k -> new LongHashSet()).add(segmentId);
        }
        finally
        {
            lock.unlock();
        }
    }

    @VisibleForTesting
    void removeReferenceForTesting(long segmentId, SSTableReader referrer)
    {
        boolean removed = false;
        lock.lock();
        try
        {
            Set<SSTableReader> referrers = referrersBySegment.get(segmentId);
            if (referrers != null && referrers.remove(referrer))
            {
                if (referrers.isEmpty())
                    referrersBySegment.remove(segmentId);
                removed = true;
            }
            LongHashSet segmentIds = segmentsBySSTable.get(referrer);
            if (segmentIds != null)
            {
                segmentIds.remove(segmentId);
                if (segmentIds.isEmpty())
                    segmentsBySSTable.remove(referrer);
            }
        }
        finally
        {
            lock.unlock();
        }
        if (removed)
            onSegmentsUnreferenced.run();
    }
}
