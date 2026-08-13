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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.journal.ActiveSegment;
import org.apache.cassandra.journal.Component;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.JournalReadError;
import org.apache.cassandra.journal.JournalWriteError;
import org.apache.cassandra.journal.KeyStats;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static org.apache.cassandra.replication.MutationTrackingService.DISABLED_MESSAGE;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

// TODO (required): handle table truncations
public class MutationJournal
{
    private static final Logger logger = LoggerFactory.getLogger(MutationJournal.class);

    // opaque / immutable list of segments that we should clear the needs-replay flag on
    public static class PendingClearReplay
    {
        private final ImmutableSet<Long> segments;

        public PendingClearReplay(ImmutableSet<Long> segments)
        {
            this.segments = segments;
        }
    }

    private static final MutationJournal instance = DatabaseDescriptor.getMutationTrackingEnabled() ? new MutationJournal() : null;

    private final Journal<ShortMutationId, Mutation> journal;
    private final Map<Long, SegmentStateTracker> segmentStateTrackers;
    private final SegmentReferenceTracker segmentReferenceTracker;

    // Static segments awaiting durable cleanup of their needsReplay=false metadata.
    private final Set<Long> pendingClearReplay = ConcurrentHashMap.newKeySet();

    // Most of the time during write, we will notify last known segment, so we optimistically cache last segment tracker,
    // without imposing any visibility guarantees. If we do not see the right segment in this field, we will look it up
    // in NBHM.
    private SegmentStateTracker lastSegmentTracker;

    public Iterable<Segment<ShortMutationId, Mutation>> getAllSegments()
    {
        return journal.getAllSegments();
    }

    public static class Snapshot implements AutoCloseable
    {
        private final Journal.Snapshot<ShortMutationId, Mutation> wrapped;

        public Snapshot(Journal.Snapshot<ShortMutationId, Mutation> wrapped)
        {
            this.wrapped = wrapped;
        }

        public void readAll(RecordConsumer<ShortMutationId> consumer)
        {
            wrapped.readAll(consumer);
        }

        @Override
        public void close()
        {
            wrapped.close();
        }
    }

    private MutationJournal()
    {
        this(new File(DatabaseDescriptor.getMutationTrackingJournalDirectory()), new JournalParams());
    }

    public static void start()
    {
        if (instance == null)
            return;

        instance.startInternal();
    }

    public static void shutdown()
    {
        if (instance == null)
            return;

        instance.shutdownBlocking();
    }

    public static MutationJournal instance()
    {
        if (instance == null)
            throw new IllegalStateException(DISABLED_MESSAGE);

        return instance;
    }

    @VisibleForTesting
    MutationJournal(File directory, Params params)
    {
        journal =
            new Journal<>("MutationJournal",
                          directory,
                          params,
                          MutationIdSupport.INSTANCE,
                          MutationSerializer.INSTANCE,
                          OffsetRangesFactory.INSTANCE,
                          SegmentCompactor.noop(),
                          new OpOrder())
                          {
                              // TODO (expected): a cleaner way to override it; pass a Callbacks object with sanctioned callbacks?
                              @Override
                              protected void closeActiveSegmentAndOpenAsStatic(ActiveSegment<ShortMutationId, Mutation> activeSegment, Runnable onDone)
                              {
                                  super.closeActiveSegmentAndOpenAsStatic(activeSegment, () -> {
                                      maybeCleanupStaticSegment(Invariants.nonNull(getSegment(activeSegment.id())));
                                      if (onDone != null) onDone.run();
                                  });
                              }
                          };
        // When a segment loses its last unrepaired-sstable reference, attempt to drop it. This (together with a
        // segment's needsReplay being cleared) is what triggers journal truncation now that it is event-driven
        // rather than performed every LogStatePersister tick (CASSANDRA-21406).
        segmentReferenceTracker = new SegmentReferenceTracker(
            () -> MutationTrackingService.instance().scheduleSegmentDropAttempt());
        segmentStateTrackers = new NonBlockingHashMapLong<>();
    }

    public CommitLogPosition getCurrentPosition()
    {
        return journal.currentActiveSegment().currentPosition();
    }

    // If all Memtables associated with given segment were flushed by the time we have closed active segment
    // and opened it as static, the segment is eligible to be marked as not needing replay. The actual durable
    // recording of needsReplay=false is deferred — we record the segment in pendingClearReplay and let the
    // LogStatePersister drain the queue after it has written witnessed offsets to system.coordinator_logs.
    //
    // A null tracker means the segment holds no full-replica (memtable-backed) data — it is either empty or holds
    // only witnessed-only mutations, which are never applied to a memtable and so never flushed. Such a segment
    // has nothing to replay for local durability, so it is immediately eligible; any remaining retention is
    // governed by reconciliation coverage in dropSegments, not by needsReplay.
    //
    // See the comment in LogStatePersister or CASSANDRA-21443 for an explanation of why we do this
    private void maybeCleanupStaticSegment(Segment<ShortMutationId, Mutation> segment)
    {
        Invariants.require(segment.isStatic());
        SegmentStateTracker tracker = segmentStateTrackers.get(segment.id());
        if (tracker == null || tracker.removeCleanFromDirty())
            pendingClearReplay.add(segment.id());
    }

    /**
     * Snapshot the current set of segments awaiting clearing of their needs replay flag.
     */
    public PendingClearReplay snapshotPendingClearReplay()
    {
        return new PendingClearReplay(ImmutableSet.copyOf(pendingClearReplay));
    }

    /**
     * Mark the given PendingClearReplay as not needing replay
     *
     * See the comment in LogStatePersister or CASSANDRA-21443 for an explanation of why we do this
     */
    public void drainCleanup(PendingClearReplay toDrain)
    {
        boolean anyCleared = false;
        for (long segId : toDrain.segments)
        {
            List<Segment<ShortMutationId, Mutation>> found = journal.getSegments(segId, segId);
            if (found.isEmpty())
            {
                // segment was dropped between enqueue and drain — nothing to persist.
                pendingClearReplay.remove(segId);
                continue;
            }
            Segment<ShortMutationId, Mutation> segment = found.get(0);
            try
            {
                segment.metadata().clearNeedsReplay();
                segment.persistMetadata();
                pendingClearReplay.remove(segId);
                anyCleared = true;
            }
            catch (Throwable t)
            {
                logger.warn("Deferred cleanup failed for segment {}; will retry next persister tick", segId, t);
                // leave in live queue
            }
        }

        // Clearing needsReplay is one of the two events that can make a segment droppable (the other being its
        // last unrepaired-sstable reference being released), so attempt a drop now that we've cleared some
        // (CASSANDRA-21406). No-op if the executor has been shut down (e.g. during the final flush at shutdown,
        // where the caller drops synchronously instead). Guarded on isEnabled() so a standalone journal in unit
        // tests (no running service) doesn't reach for the service singleton.
        if (anyCleared && MutationTrackingService.isEnabled())
            MutationTrackingService.instance().scheduleSegmentDropAttempt();
    }

    @VisibleForTesting
    public Set<Long> pendingCleanupForTesting()
    {
        return pendingClearReplay;
    }

    public static int pendingClearReplaySize()
    {
        if (instance == null)
            return 0;
        return instance.pendingClearReplay.size();
    }

    void startInternal()
    {
        journal.start();
    }

    void shutdownBlocking()
    {
        journal.shutdown();
    }

    @VisibleForTesting
    public RecordPointer write(ShortMutationId id, Mutation mutation)
    {
        return write(id, mutation, true);
    }

    /**
     * Append a mutation to the journal.
     *
     * @param id          the short mutation id
     * @param mutation    the mutation to be applied to the journal
     * @param fullReplica whether this node is a full replica for the mutation's token. Only full-replica
     *                    writes mark the segment dirty: a witnessed-only mutation is journaled
     *                    (and witnessed for reconciliation) but never applied to a memtable, so marking
     *                    it dirty would pin the segment's needsReplay forever preventing it from flushing.
     *                    A witness-only segment is instead retained until its offsets are durably reconciled
     *                    (see dropSegments).
     * @return the record pointer to the journal
     */
    public RecordPointer write(ShortMutationId id, Mutation mutation, boolean fullReplica)
    {
        // TODO (required): why are we using blocking write here? We can/should wait for completion on `close` of WriteContext.
        RecordPointer ptr = journal.blockingWrite(id, mutation);

        // IMPORTANT: there should be no way for mutation to be applied to memtable before we mark it as dirty here,
        // since this will introduce a race between marking as dirty and marking as clean.
        if (fullReplica)
        {
            for (TableId tableId : mutation.getTableIds())
            {
                SegmentStateTracker tracker = lastSegmentTracker;
                if (tracker == null || tracker.segmentId() != ptr.segmentId)
                {
                    tracker = segmentStateTrackers.computeIfAbsent(ptr.segmentId, SegmentStateTracker::new);
                    lastSegmentTracker = tracker;
                }

                tracker.markDirty(tableId, ptr);
            }
        }

        return ptr;
    }

    /**
     * Called by post-flush callback, Memtable is fully flushed to SSTable.
     */
    public void notifyFlushed(TableId tableId, CommitLogPosition lowerBound, CommitLogPosition upperBound)
    {
        for (Segment<ShortMutationId, Mutation> segment : journal.getSegments(lowerBound.segmentId, upperBound.segmentId))
        {
            SegmentStateTracker tracker = segmentStateTrackers.get(segment.id());
            // upper flush bound can be first position in an empty active segment
            if (tracker == null)
                continue;

            segmentStateTrackers.get(segment.id()).markClean(tableId, lowerBound, upperBound);

            // We can only safely mark static segments as non-replayable. Active segment can still be written to,
            // so we only persist this metadata on flush.
            if (segment.isStatic())
                maybeCleanupStaticSegment(segment);
        }
    }

    @Nullable
    public Mutation read(ShortMutationId id)
    {
        return journal.readLast(id);
    }

    boolean read(ShortMutationId id, RecordConsumer<ShortMutationId> consumer)
    {
        return journal.readLast(id, consumer);
    }

    public MutationJournal.Snapshot snapshot()
    {
        return new MutationJournal.Snapshot(journal.snapshot(s -> true));
    }

    @VisibleForTesting
    public void advanceSegment()
    {
        journal.advanceSegment();
    }

    /**
     * @return record pointer of the last mutation with the provided id, or null if not found
     */
    @Nullable
    RecordPointer lookUp(ShortMutationId id)
    {
        return journal.lookUpLast(id);
    }

    int sizeOfRecord(RecordPointer pointer)
    {
        return journal.sizeOfRecord(pointer);
    }

    boolean read(RecordPointer pointer, RecordConsumer<ShortMutationId> consumer)
    {
        return journal.read(pointer, consumer);
    }

    public void readAll(Iterable<ShortMutationId> ids, Collection<Mutation> into)
    {
        for (ShortMutationId id : ids)
        {
            Mutation mutation = read(id);
            Preconditions.checkState(mutation != null);
            into.add(mutation);
        }
    }

    public void replayStaticSegments()
    {
        replay(new DeserializedRecordConsumer<>(MutationSerializer.INSTANCE)
        {
            @Override
            protected void accept(long segmentId, int position, ShortMutationId key, Mutation value)
            {
                if (Schema.instance.getKeyspaceMetadata(value.getKeyspaceName()) == null)
                    return;
                // TODO: if (commitLogReplayer.pointInTimeExceeded(mutation))
                final Keyspace keyspace = Keyspace.open(value.getKeyspaceName());

                // Witnessed-only mutations are still replayed (so their offsets are re-witnessed on startup) but must
                // not mark the segment dirty — they are never applied to a memtable, so a dirty mark would pin the
                // segment's needsReplay forever. Witness status is per keyspace+token, hence uniform across this
                // mutation's tables.
                final boolean isFullReplica = keyspace.isFullReplicaFor(value.key().getToken(), ClusterMetadata.current());

                Mutation.PartitionUpdateCollector newPUCollector = null;
                // TODO (required): replayFilter
                for (Map.Entry<TableId, PartitionUpdate> e : value.modifications().entrySet())
                {
                    PartitionUpdate update = e.getValue();
                    update.validate();
                    if (Schema.instance.getTableMetadata(update.metadata().id) == null)
                        continue; // dropped
                    TableId tableId = e.getKey();

                    // Start segment state tracking (full-replica, memtable-backed data only; see comment above)
                    if (isFullReplica)
                        segmentStateTrackers.computeIfAbsent(segmentId, SegmentStateTracker::new)
                                            .markDirty(tableId, segmentId, position);
                    // TODO (required): shouldReplay
                    if (newPUCollector == null)
                        newPUCollector = new Mutation.PartitionUpdateCollector(value.id(), value.getKeyspaceName(), value.key());
                    newPUCollector.add(update);
                    // TODO (required): replayedCount
                }
                if (newPUCollector != null)
                {
                    assert !newPUCollector.isEmpty();
                    keyspace.applyForReplay(newPUCollector.build());
                }
            }
        }, getAvailableProcessors());
    }

    @VisibleForTesting
    public void replay(DeserializedRecordConsumer<ShortMutationId, Mutation> replayOne, int parallelism)
    {
        try (Journal<ShortMutationId, Mutation>.SegmentKeyIterator iter =
                     journal.staticSegmentKeyIterator(s -> s.isStatic()
                                                        && s.metadata().totalCount() > 0
                                                        && s.metadata().needsReplay()))
        {
            final Semaphore replayParallelism = Semaphore.newSemaphore(parallelism);
            final AtomicBoolean abort = new AtomicBoolean();

            while (iter.hasNext() && !abort.get())
            {
                Journal.KeyRefs<ShortMutationId> v = iter.next();
                v = v; // Make sure it can not be used in async lambda by accident
                ShortMutationId key = v.key();
                long lastSegment = v.lastSegment();
                // TODO: respect SystemKeyspace.getTruncatedPosition(cfs.metadata.id);
                replayParallelism.acquireThrowUncheckedOnInterrupt(1);
                Stage.MUTATION.submit(() -> journal.readLast(key, lastSegment, replayOne))
                              .addCallback((BiConsumer<Object, Throwable>) (o, fail) ->
                              {
                                  if (fail != null && !journal.handleError("Could not replay mutation " + key, fail))
                                      abort.set(true);
                                  replayParallelism.release(1);
                              });
            }

            // Wait for all mutations to be applied before returning
            replayParallelism.acquireThrowUncheckedOnInterrupt(parallelism);
        }
    }

    /**
     * Drop every static segment that is safe to reclaim, i.e. one that:
     * <ol>
     *   <li>(F) does not need replay — every full-replica memtable holding its data has been flushed; and</li>
     *   <li>(R) is not referenced by any unrepaired local sstable — otherwise the journal may still be needed to
     *       rebuild that sstable with minority writes filtered out; and</li>
     *   <li>(W) is fully covered by the given durably-reconciled offsets, so any witnessed-only mutations it
     *       carries (which never produce an sstable) have been durably reconciled across peers (CASSANDRA-21406).</li>
     * </ol>
     * For a full replica (F)+(R) dominate ((W) is implied, since an sstable becomes repaired — and thus stops
     * referencing the segment — only once reconciled); for a witness (R) is trivially satisfied (no sstables) and
     * (W) is the real gate, restoring the pre-reference-tracking reconciliation-based drop condition.
     *
     * <p>Synchronized so the several event-driven callers cannot both select and then discard the same segment,
     * which would over-release its reference.
     */
    synchronized int dropSegments(Log2OffsetsMap<?> durablyReconciled)
    {
        return journal.dropStaticSegments(segment -> {
            return !segment.metadata().needsReplay()
                   && !segmentReferenceTracker.isReferenced(segment.id())
                   && ((StaticOffsetRanges) segment.keyStats()).isFullyCovered(durablyReconciled);
        });
    }

    /**
     * Listener tracking how many unrepaired sstables of tracked tables reference each static segment.
     * Subscribed by {@link org.apache.cassandra.db.ColumnFamilyStore} on init for every tracked CFS.
     */
    public SegmentReferenceTracker segmentReferenceTracker()
    {
        return segmentReferenceTracker;
    }

    public void readAll(RecordConsumer<ShortMutationId> consumer)
    {
        journal.readAll(consumer);
    }

    static class JournalParams implements Params
    {
        @Override
        public int segmentSize()
        {
            return DatabaseDescriptor.getCommitLogSegmentSize();
        }

        @Override
        public FailurePolicy failurePolicy()
        {
            return FailurePolicy.STOP;
        }

        @Override
        public FlushMode flushMode()
        {
            Config.CommitLogSync mode = DatabaseDescriptor.getCommitLogSync();
            switch (DatabaseDescriptor.getCommitLogSync())
            {
                case batch:
                    return FlushMode.BATCH;
                case periodic:
                    return FlushMode.PERIODIC;
                case group:
                    return FlushMode.GROUP;
                default:
                    throw new IllegalStateException("Unhandled flush mode: " + mode);
            }
        }

        @Override
        public int compactMaxSegments()
        {
            return 0;
        }

        @Override
        public ReplayMode replayMode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enableCompaction()
        {
            return false;
        }

        @Override
        public long compactionPeriod(TimeUnit units)
        {
            return 0;
        }

        @Override
        public long flushPeriod(TimeUnit units)
        {
            return units.convert(DatabaseDescriptor.getCommitLogSyncPeriod(), TimeUnit.MILLISECONDS);
        }

        @Override
        public long periodicBlockPeriod(TimeUnit units)
        {
            return units.convert(DatabaseDescriptor.getPeriodicCommitLogSyncBlock(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int userVersion()
        {
            return MessagingService.current_version;
        }
    }

    static class MutationIdSupport implements KeySupport<ShortMutationId>
    {
        static final MutationIdSupport INSTANCE = new MutationIdSupport();

        static final int LOG_ID_OFFSET = 0;
        static final int OFFSET_OFFSET = LOG_ID_OFFSET + TypeSizes.LONG_SIZE;

        @Override
        public int serializedSize(int userVersion)
        {
            return TypeSizes.LONG_SIZE  // logId
                 + TypeSizes.INT_SIZE; // offset
        }

        @Override
        public void serialize(ShortMutationId id, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(id.logId());
            out.writeInt(id.offset());
        }

        @Override
        public void serialize(ShortMutationId id, ByteBuffer out, int userVersion) throws IOException
        {
            out.putLong(id.logId());
            out.putInt(id.offset());
        }

        @Override
        public ShortMutationId deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            long logId = in.readLong();
            int offset = in.readInt();
            return new ShortMutationId(logId, offset);
        }

        @Override
        public ShortMutationId deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            long logId = buffer.getLong(position + LOG_ID_OFFSET);
            int offset = buffer.getInt(position + OFFSET_OFFSET);
            return new ShortMutationId(logId, offset);
        }

        @Override
        public ShortMutationId deserialize(ByteBuffer buffer, int userVersion)
        {
            long logId = buffer.getLong();
            int offset = buffer.getInt();
            return new ShortMutationId(logId, offset);
        }

        @Override
        public void updateChecksum(CRC32 crc, ShortMutationId id, int userVersion)
        {
            Crc.updateWithLong(crc, id.logId());
            Crc.updateWithInt(crc, id.offset());
        }

        @Override
        public int compareWithKeyAt(ShortMutationId id, ByteBuffer buffer, int position, int userVersion)
        {
            int cmp = Long.compare(id.logId(), buffer.getLong(position + LOG_ID_OFFSET));
            return cmp != 0 ? cmp : Integer.compare(id.offset(), buffer.getInt(position + OFFSET_OFFSET));
        }

        @Override
        public int compare(ShortMutationId id1, ShortMutationId id2)
        {
            int cmp = Long.compare(id1.logId(), id2.logId());
            return cmp != 0 ? cmp : Integer.compare(id1.offset(), id2.offset());
        }
    }

    public static class MutationSerializer implements ValueSerializer<ShortMutationId, Mutation>
    {
        public static MutationSerializer INSTANCE = new MutationSerializer();

        @Override
        public void serialize(ShortMutationId id, Mutation mutation, DataOutputPlus out, int userVersion) throws IOException
        {
            Invariants.require(id.hostId != Integer.MIN_VALUE);
            Mutation.serializer.serialize(mutation, out, userVersion);
        }

        @Override
        public Mutation deserialize(ShortMutationId id, DataInputPlus in, int userVersion) throws IOException
        {
            Invariants.require(id.hostId != Integer.MIN_VALUE);
            return Mutation.serializer.deserialize(in, userVersion);
        }
    }

    /*
     * KeyStats component to track per log min and max offset in a segment
     */

    static abstract class OffsetRanges implements KeyStats<ShortMutationId>
    {
        @Override
        public abstract boolean mayContain(ShortMutationId id);

        protected static boolean mayContain(long range, ShortMutationId id)
        {
            return id.offset() >= minOffset(range) && id.offset() <= maxOffset(range);
        }

        protected static int minOffset(long range)
        {
            return (int) (range >>> 32);
        }

        protected static int maxOffset(long range)
        {
            return (int) range;
        }

        protected static long range(int minOffset, int maxOffset)
        {
            return ((long) minOffset << 32) | (maxOffset & 0xFFFFFFFFL);
        }

        abstract Map<Long, Long> asMap();

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder(getClass().getSimpleName());
            builder.append('{');
            for (Map.Entry<Long, Long> entry : asMap().entrySet())
            {
                CoordinatorLogId logId = new CoordinatorLogId(entry.getKey());
                long range = entry.getValue();
                int min = minOffset(range);
                int max = maxOffset(range);
                builder.append(logId)
                       .append("->")
                       .append('[')
                       .append(min)
                       .append(", ")
                       .append(max)
                       .append(']')
                       .append(',');
            }
            return builder.append('}').toString();
        }
    }

    // TODO (consider): an off-heap version
    static class ActiveOffsetRanges extends OffsetRanges implements KeyStats.Active<ShortMutationId>
    {
        private final NonBlockingHashMapLong<Long> ranges;

        ActiveOffsetRanges()
        {
            ranges = new NonBlockingHashMapLong<>();
        }

        @Override
        protected Map<Long, Long> asMap()
        {
            return ranges;
        }

        @Override
        public boolean mayContain(ShortMutationId id)
        {
            Long range = ranges.get(id.logId());
            return range != null && mayContain(range, id);
        }

        @Override
        @SuppressWarnings("WrapperTypeMayBePrimitive")
        public void update(ShortMutationId id)
        {
            Long prev, next;
            do
            {
                prev = ranges.get(id.logId());
                int min = prev == null ? id.offset() : Math.min(minOffset(prev), id.offset());
                int max = prev == null ? id.offset() : Math.max(maxOffset(prev), id.offset());
                next = range(min, max);
            }
            while (!compareAndSet(id.logId(), prev, next));
        }

        // NonBlockingHashMapLong doesn't expose putIfMatch() directly, so we need to have this logic
        private boolean compareAndSet(long logId, Long prevValue, Long nextValue)
        {
            return prevValue == null
                 ? ranges.putIfAbsent(logId, nextValue) == null
                 : ranges.replace(logId, prevValue, nextValue);
        }

        @Override
        public void persist(Descriptor descriptor)
        {
            File tmpFile = descriptor.tmpFileFor(Component.KEYSTATS);
            try (FileOutputStreamPlus out = new FileOutputStreamPlus(tmpFile))
            {
                write(out);

                out.flush();
                out.sync();
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, tmpFile, e);
            }
            tmpFile.move(descriptor.fileFor(Component.KEYSTATS));
        }

        private void write(DataOutputPlus out) throws IOException
        {
            CRC32 crc = Crc.crc32();
            int count = ranges.size();
            out.writeInt(count);
            Crc.updateWithInt(crc, count);
            out.writeInt((int) crc.getValue());
            for (Map.Entry<Long, Long> entry : ranges.entrySet())
            {
                long logId = entry.getKey();
                long range = entry.getValue();
                out.writeLong(logId);
                out.writeLong(range);
                Crc.updateWithLong(crc, logId);
                Crc.updateWithLong(crc, range);
            }
            out.writeInt((int) crc.getValue());
        }
    }

    static class StaticOffsetRanges extends OffsetRanges implements KeyStats.Static<ShortMutationId>
    {
        private static final long NO_VALUE = Long.MIN_VALUE;

        private final Long2LongHashMap ranges;

        StaticOffsetRanges(Long2LongHashMap ranges)
        {
            this.ranges = ranges;
        }

        @Override
        protected Map<Long, Long> asMap()
        {
            return ranges;
        }

        @Override
        public boolean mayContain(ShortMutationId id)
        {
            long range = ranges.get(id.logId());
            return range != NO_VALUE && mayContain(range, id);
        }

        static StaticOffsetRanges read(DataInputPlus in) throws IOException
        {
            CRC32 crc = Crc.crc32();
            int count = in.readInt();
            Crc.updateWithInt(crc, count);
            Crc.validate(crc, in.readInt());
            Long2LongHashMap ranges = new Long2LongHashMap(count, 0.65f, NO_VALUE, false);
            for (int i = 0; i < count; i++)
            {
                long logId = in.readLong();
                long range = in.readLong();
                Crc.updateWithLong(crc, logId);
                Crc.updateWithLong(crc, range);
                ranges.put(logId, range);
            }
            Crc.validate(crc, in.readInt());
            return new StaticOffsetRanges(ranges);
        }

        /**
         * @return whether every key range in this segment is fully covered by the given (durably reconciled)
         * offsets — i.e. every mutation id the segment holds has been durably reconciled across peers. Used to
         * decide when a segment holding witnessed-only data may be dropped.
         */
        @SuppressWarnings("unchecked")
        boolean isFullyCovered(Log2OffsetsMap<?> durablyReconciled)
        {
            Long2ObjectHashMap<Offsets> reconciledMap = ((Log2OffsetsMap<Offsets>) durablyReconciled).asMap();
            for (Long2LongHashMap.EntryIterator iter = ranges.entrySet().iterator(); iter.hasNext();)
            {
                iter.next();

                long logId = iter.getLongKey();
                long range = iter.getLongValue();
                int min = minOffset(range);
                int max = maxOffset(range);

                Offsets offsets = reconciledMap.get(logId);
                if (offsets == null
                    || !offsets.containsRange(min, max))
                    return false;
            }
            return true;
        }
    }

    static final class OffsetRangesFactory implements KeyStats.Factory<ShortMutationId>
    {
        static final OffsetRangesFactory INSTANCE = new OffsetRangesFactory();

        @Override
        public ActiveOffsetRanges create()
        {
            return new ActiveOffsetRanges();
        }

        @Override
        public StaticOffsetRanges load(Descriptor descriptor)
        {
            File file = descriptor.fileFor(Component.KEYSTATS);
            try (FileInputStreamPlus in = new FileInputStreamPlus(file))
            {
                return StaticOffsetRanges.read(in);
            }
            catch (IOException e)
            {
                throw new JournalReadError(descriptor, file, e);
            }
        }

        @Override
        public ActiveOffsetRanges rebuild(Descriptor descriptor, KeySupport<ShortMutationId> keySupport, int fsyncedLimit)
        {
            ActiveOffsetRanges active = create();
            try (StaticSegment.SequentialReader<ShortMutationId> reader = StaticSegment.sequentialReader(descriptor, keySupport, fsyncedLimit))
            {
                while (reader.advance())
                    active.update(reader.key());
            }
            return active;
        }
    }

    /*
     * Test helpers
     */

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
    }

    @VisibleForTesting
    void clearNeedsReplayForTesting()
    {
        journal.clearNeedsReplayForTesting();
    }

    @VisibleForTesting
    public int countStaticSegmentsForTesting()
    {
        return journal.countStaticSegmentsForTesting();
    }

    /**
     *  Lets tests wait for reconciliation to converge without having to first release the sstable references
     *  that gate (R).
     * @return the number of static segments not yet fully covered by the given durably-reconciled offsets.
     */
    @VisibleForTesting
    public int countStaticSegmentsPendingReconciliationForTesting(Log2OffsetsMap<?> durablyReconciled)
    {
        return journal.countStaticSegmentsForTesting(
        segment -> !((StaticOffsetRanges) segment.keyStats()).isFullyCovered(durablyReconciled));
    }

    public long getDiskSpaceUsed()
    {
        return journal.getDiskSpaceUsed();
    }
}
