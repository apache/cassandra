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

import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.cassandra.journal.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.zip.Checksum;

import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

// TODO (required): handle table truncations
public class MutationJournal
{
    public static final MutationJournal instance = new MutationJournal();
    private static final Logger log = LoggerFactory.getLogger(MutationJournal.class);

    private final Journal<ShortMutationId, Mutation> journal;
    private final Map<Long, SegmentStateTracker> segmentStateTrackers;

    // Most of the time during write, we will notify last known segment, so we optimistically cache last segment tracker,
    // without imposing any visibility guarantees. If we do not see the right segment in this field, we will look it up
    // in NBHM.
    private SegmentStateTracker lastSegmentTracker;

    private MutationJournal()
    {
        this(new File(DatabaseDescriptor.getCommitLogLocation()), new JournalParams());
    }

    @VisibleForTesting
    MutationJournal(File directory, Params params)
    {
        journal = new Journal<>("MutationJournal", directory, params, MutationIdSupport.INSTANCE, MutationSerializer.INSTANCE, SegmentCompactor.noop()) {
            @Override
            protected void closeActiveSegmentAndOpenAsStatic(ActiveSegment<ShortMutationId, Mutation> activeSegment, Runnable onDone)
            {
                super.closeActiveSegmentAndOpenAsStatic(activeSegment,
                                                        () -> {
                                                            maybeCleanupStaticSegment(Invariants.nonNull(getSegment(activeSegment.id())));
                                                            if (onDone != null) onDone.run();
                                                        });
            }
        };
        segmentStateTrackers = new NonBlockingHashMapLong<>();
    }

    public CommitLogPosition getCurrentPosition()
    {
        return journal.currentActiveSegment().currentPosition();
    }

    // If all Memtables associated with given segment were flushed by the time we have closed active segment
    // and opened it as static, mark its metadata to indicate it does not need replay. It may happen that we
    // crash before persisting this metadata, in which case we will unnecessarily replay the segment, which
    // has no correctness implications.
    private void maybeCleanupStaticSegment(Segment<ShortMutationId, Mutation> segment)
    {
        Invariants.require(segment.isStatic());
        SegmentStateTracker tracker = segmentStateTrackers.get(segment.id());
        if (tracker != null && tracker.removeCleanFromDirty())
        {
            segment.metadata().clearNeedsReplay();
            segment.persistMetadata();
        }
    }

    public void start()
    {
        journal.start();
    }

    public void shutdownBlocking()
    {
        journal.shutdown();
    }

    public RecordPointer write(ShortMutationId id, Mutation mutation)
    {
        // TODO (required): why are we using blocking write here? We can/should wait for completion on `close` of WriteContext.
        RecordPointer ptr = journal.blockingWrite(id, mutation);

        // IMPORTANT: there should be no way for mutation to be applied to memtable before we mark it as dirty here,
        // since this will introduce a race between marking as dirty and marking as clean.
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

        return ptr;
    }

    /**
     * Called by post-flush callback, Memtable is fully flushed to SSTable.
     */
    public void notifyFlushed(TableId tableId, CommitLogPosition lowerBound, CommitLogPosition upperBound)
    {
        for (Segment<ShortMutationId, Mutation> segment : journal.getSegments(lowerBound.segmentId, upperBound.segmentId))
        {
            Invariants.nonNull(segmentStateTrackers.get(segment.id())).markClean(tableId, lowerBound, upperBound);

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

                Mutation.PartitionUpdateCollector newPUCollector = null;
                // TODO (required): replayFilter
                for (Map.Entry<TableId, PartitionUpdate> e : value.modifications().entrySet())
                {
                    PartitionUpdate update = e.getValue();
                    update.validate();
                    if (Schema.instance.getTableMetadata(update.metadata().id) == null)
                        continue; // dropped
                    TableId tableId = e.getKey();

                    // Start segment state tracking
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
                    keyspace.apply(newPUCollector.build(), false, true, false);
                }
            }
        }, getAvailableProcessors());
    }

    @VisibleForTesting
    public void replay(DeserializedRecordConsumer<ShortMutationId, Mutation> replayOne, int parallelism)
    {
        try (Journal<ShortMutationId, Mutation>.StaticSegmentKeyIterator iter =
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
                              .addCallback(new BiConsumer<Object, Throwable>()
                {
                    @Override
                    public void accept(Object o, Throwable fail)
                    {
                        if (fail != null && !journal.handleError("Could not replay mutation " + key, fail))
                            abort.set(true);
                        replayParallelism.release(1);
                    }
                });
            }

            // Wait for all mutations to be applied before returning
            replayParallelism.acquireThrowUncheckedOnInterrupt(parallelism);
        }
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
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
        public void updateChecksum(Checksum crc, ShortMutationId id, int userVersion)
        {
            FBUtilities.updateChecksumLong(crc, id.logId());
            FBUtilities.updateChecksumInt(crc, id.offset());
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
}
