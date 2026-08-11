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
package org.apache.cassandra.service.accord.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2LongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.CommandChange;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.MinimalCommand;
import accord.local.MinimalCommand.MinimalWithDeps;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.PersistentField;

import org.apache.cassandra.config.AccordConfig.JournalConfig;
import org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Compactor;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.Segments;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.journal.Merger.KeepFirst;
import org.apache.cassandra.service.accord.journal.RangeSearcher.NoopJournalRangeSearcher;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static accord.api.Journal.Load.MINIMAL;
import static accord.api.Journal.Load.MINIMAL_WITH_DEPS;
import static accord.local.Cleanup.Input.FULL;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.journal_sai;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccord;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordJournalDirectory;
import static org.apache.cassandra.service.accord.JournalKey.Type.COMMAND_DIFF;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.safeStopMarker;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.startMarker;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.writeMarker;
import static org.apache.cassandra.service.accord.journal.TopologyRecord.newTopology;

public class AccordJournal implements accord.api.Journal, RangeSearcher.Supplier
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    @VisibleForTesting
    protected final Journal<JournalKey, Object> segments;
    protected final ColumnFamilyStore table;
    @VisibleForTesting
    protected final @Nullable RangeSearchManager rangeSearch;
    protected final OpOrder readOrder;
    private final Params params;

    public AccordJournal(Params params)
    {
        this(params, new File(getAccordJournalDirectory()), Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
    }

    @VisibleForTesting
    public AccordJournal(Params params, File directory, ColumnFamilyStore table)
    {
        Version userVersion = Version.fromVersion(params.userVersion());
        this.rangeSearch = RangeSearchManager.ifEnabled(table);
        this.table = table;
        this.readOrder = table.readOrdering;
        this.params = params;
        // initialise journal last because we call a self method to initialise its compactor
        this.segments = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT,
                                      new ValueSerializer.Unsupported<>(),
                                      compactor(table, userVersion),
                                      table.readOrdering);
    }

    @Override
    public void open(Node node)
    {
        segments.open();
    }

    public void start(Node node)
    {
        if (rangeSearch != null)
            rangeSearch.start();

        long maxTableDescriptor = maxTableDescriptor();
        segments.start(maxTableDescriptor);
    }

    public Descriptor stop()
    {
        return segments.stop();
    }

    public void close()
    {
        segments.close();
    }

    public boolean awaitTerminationUntil(long deadlineNanos) throws InterruptedException
    {
        try
        {
            segments.awaitTerminationUntil(deadlineNanos);
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @Override
    public void saveCommand(int commandStoreId, CommandUpdate update, @Nullable Runnable onFlush)
    {
        CommandChangeWriter change = CommandChangeWriter.make(update.before, update.after);
        if (change == null)
        {
            if (onFlush != null)
                onFlush.run();
            return;
        }

        JournalKey key = new JournalKey(update.txnId, COMMAND_DIFF, commandStoreId);
        RecordPointer pointer = segments.asyncWrite(key, change);
        if (rangeSearch != null)
            onFlush = merge(onFlush, rangeSearch.maybeIndex(key, pointer, change));
        if (onFlush != null)
            segments.onDurable(pointer, onFlush);
    }

    <T> void append(JournalKey key, T write, Runnable onFlush)
    {
        RecordPointer pointer = appendInternal(key, write);
        if (onFlush != null)
            segments.onDurable(pointer, onFlush);
    }

    private <T> RecordPointer appendInternal(JournalKey key, T write)
    {
        MergeSerializer<T, ?, ?> serializer = (MergeSerializer<T, ?, ?>) key.type.serializer;
        return segments.asyncWrite(key, (out, userVersion) -> serializer.serialize(key, write, out, Version.fromVersion(userVersion)));
    }


    public long maxTableDescriptor()
    {
        return table.getTracker().getView().liveSSTables()
                    .stream()
                    .filter(sst -> sst.getSSTableMetadata().totalRows > 0)
                    .map(sst -> LongType.instance.compose(sst.getSSTableMetadata().coveredClustering.end().bufferAt(0)))
                    .max(Long::compare).orElse(0L);
    }

    public long maxDescriptor()
    {
        return Math.max(segments.maxDescriptor(), maxTableDescriptor());
    }

    public Params configuration()
    {
        return params;
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = load(commandStoreId, txnId);
        builder.maybeCleanup(true, FULL, redundantBefore, durableBefore);
        return builder.construct(redundantBefore);
    }

    @Override
    public List<DebugEntry> debugCommand(int commandStoreId, TxnId txnId)
    {
        JournalKey key = new JournalKey(txnId, COMMAND_DIFF, commandStoreId);
        List<DebugEntry> result = new ArrayList<>();
        readAll(key, (long segment, int position, JournalKey k, ByteBuffer buffer, int userVersion) -> {
            CommandChanges builder = new CommandChanges(txnId);
            new RecordConsumerAdapter<>(builder::deserializeNext).accept(segment, position, k, buffer, userVersion);
            result.add(new DebugEntry(segment, position, builder));
        });
        return result;
    }

    @Override
    public MinimalCommand loadMinimal(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = CommandChanges.cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimal();
    }

    @Override
    public MinimalWithDeps loadMinimalWithDeps(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = CommandChanges.cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL_WITH_DEPS), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimalWithDeps();
    }

    private CommandChanges loadDiffs(int commandStoreId, TxnId txnId, Load load)
    {
        JournalKey key = new JournalKey(txnId, COMMAND_DIFF, commandStoreId);
        CommandChanges builder = new CommandChanges(txnId, load);
        readAll(key, builder::deserializeNext);
        return builder;
    }

    @VisibleForTesting
    public CommandChanges load(int commandStoreId, TxnId txnId)
    {
        return loadDiffs(commandStoreId, txnId, Load.ALL);
    }

    @Override
    public List<TopologyUpdate> loadTopologies()
    {
        List<accord.api.Journal.TopologyUpdate> images = new ArrayList<>();
        try (CloseableIterator<accord.api.Journal.TopologyUpdate> iter = new CloseableIterator<>()
        {
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter = keyIterator(TopologyRecord.journalKey(0L),
                                                                                    TopologyRecord.journalKey(Timestamp.MAX_EPOCH),
                                                                                    true, 0);
            TopologyRecord.TopologyImage prev = null;

            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public accord.api.Journal.TopologyUpdate next()
            {
                Journal.KeyRefs<JournalKey> ref = iter.next();
                MergeSerializers.TopologyMerger reader = readAll(ref.key());
                if (reader.read().kind() == TopologyRecord.Kind.Repeat)
                {
                    if (prev == null)
                    {
                        logger.error("Encountered TopologyImage Repeat record for epoch {}, but no prior image record was found", ref.key().id.epoch());
                        return null;
                    }
                    prev = reader.read().asImage(Invariants.nonNull(prev.update()));
                }
                else prev = reader.read();

                return new accord.api.Journal.TopologyUpdate(prev.update().commandStores,
                                                             prev.update().global,
                                                             prev.update().previouslyOwned);
            }

            @Override
            public void close()
            {
                iter.close();
            }
        })
        {
            accord.api.Journal.TopologyUpdate prev = null;
            while (iter.hasNext())
            {
                accord.api.Journal.TopologyUpdate next = iter.next();
                if (next == null)
                    continue;

                Invariants.require(prev == null || next.global.epoch() > prev.global.epoch());
                // Due to partial compaction, we can clean up only some of the old epochs, creating gaps. We skip these epochs here.
                if (prev != null && next.global.epoch() > prev.global.epoch() + 1)
                    images.clear();

                images.add(next);
                prev = next;
            }
        }
        return images;
    }

    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        append(TopologyRecord.journalKey(topologyUpdate.global.epoch()),
               newTopology(topologyUpdate),
               onFlush);
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        KeepFirst<RedundantBefore> accumulator = readLast(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, commandStoreId));
        return accumulator.get();
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        KeepFirst<NavigableMap<TxnId, Ranges>> accumulator = readLast(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, commandStoreId));
        return accumulator.get();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        KeepFirst<NavigableMap<Timestamp, Ranges>> accumulator = readLast(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, commandStoreId));
        return accumulator.get();
    }

    @Override
    public CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId)
    {
        KeepFirst<RangesForEpoch> accumulator = readLast(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, commandStoreId));
        return accumulator.get();
    }

    @Override
    public Ranges loadPermanentlyUnsafeToRead(int commandStoreId)
    {
        KeepFirst<Ranges> accumulator = readLast(new JournalKey(TxnId.NONE, JournalKey.Type.PERMANENTLY_UNSAFE_TO_READ, commandStoreId));
        return accumulator.get();
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return new DurableBeforePersister(this);
    }

    @Override
    public void saveStoreState(int commandStoreId, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        RecordPointer pointer = null;
        if (fieldUpdates.newRedundantBefore != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, commandStoreId), fieldUpdates.newRedundantBefore);
        if (fieldUpdates.newBootstrapBeganAt != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, commandStoreId), fieldUpdates.newBootstrapBeganAt);
        if (fieldUpdates.newSafeToRead != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, commandStoreId), fieldUpdates.newSafeToRead);
        if (fieldUpdates.newRangesForEpoch != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, commandStoreId), fieldUpdates.newRangesForEpoch);
        if (fieldUpdates.newPermanentlyUnsafeToRead != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.PERMANENTLY_UNSAFE_TO_READ, commandStoreId), fieldUpdates.newPermanentlyUnsafeToRead);

        if (onFlush == null)
            return;

        if (pointer != null)
            segments.onDurable(pointer, onFlush);
        else
            onFlush.run();
    }

    public <BUILDER extends Merger> BUILDER readAll(JournalKey key)
    {
        Invariants.require(segments.isReadable());
        BUILDER builder = (BUILDER) key.type.serializer.mergerFor();
        builder.reset(key);
        // TODO (expected): this can be further improved to avoid allocating lambdas
        MergeSerializer<?, ? super BUILDER, ? extends BUILDER> serializer = (MergeSerializer<?, ? super BUILDER, ? extends BUILDER>) key.type.serializer;
        // TODO (expected): for those where we store an image, read only the first entry we find in DESC order
        readAll(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion));
        return builder;
    }

    public <BUILDER extends Merger> BUILDER readLast(JournalKey key)
    {
        Invariants.require(segments.isReadable());
        BUILDER builder = (BUILDER) key.type.serializer.mergerFor();
        builder.reset(key);
        // TODO (expected): this can be further improved to avoid allocating lambdas
        MergeSerializer<?, ? super BUILDER, ? extends BUILDER> serializer = (MergeSerializer<?, ? super BUILDER, ? extends BUILDER>) key.type.serializer;
        readLast(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion));
        return builder;
    }

    public void forEachEntry(JournalKey key, Reader reader)
    {
        readAll(key, reader);
    }

    public interface Reader
    {
        void read(DataInputPlus input, Version userVersion) throws IOException;

        default void read(ByteBuffer buffer, Version userVersion)
        {
            try (DataInputBuffer in = new DataInputBuffer(buffer, false))
            {
                read(in, userVersion);
            }
            catch (IOException e)
            {
                // can only throw if serializer is buggy or bytes got corrupted
                throw new UncheckedIOException(e);
            }
        }
    }

    static class RecordConsumerAdapter<K> implements RecordConsumer<K>
    {
        protected final Reader reader;
        private long prevSegment = Long.MAX_VALUE;
        private long prevPosition = Long.MAX_VALUE;

        RecordConsumerAdapter(Reader reader)
        {
            this.reader = reader;
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, int userVersion)
        {
            Invariants.require(segment <= prevSegment,
                               "Records should always be iterated over in a reverse order, but segment %d was seen after %d while reading %s", segment, prevSegment, key);
            Invariants.require(segment != prevSegment || position < prevPosition,
                               "Records should always be iterated over in a reverse order, but position %d was seen after %d for segment %d while reading %s", position, prevPosition, segment, key);
            reader.read(buffer, Version.fromVersion(userVersion));
            prevSegment = segment;
            prevPosition = position;
        }
    }

    /**
     * Perform a read from Journal table, followed by the reads from all journal segments.
     * <p>
     * When reading from journal segments, skip descriptors that were read from the table.
     */
    public void readAll(JournalKey key, Reader reader)
    {
        readAll(key, new RecordConsumerAdapter<>(reader));
    }

    public void readAll(JournalKey key, RecordConsumer<JournalKey> reader)
    {
        try (OpOrder.Group readOrder = table.readOrdering.start())
        {
            // SELECT segments first, to avoid missing segments due to races compacting segment->sstable
            Segments<JournalKey, Object> segments = this.segments.segments();
            try (TableRecordIterator table = TableRecordIterator.all(this.table, key, readOrder))
            {
                boolean hasTableData = table.advance();
                long minSegment = hasTableData ? table.segment : Long.MIN_VALUE;
                // First, read all journal entries newer than anything flushed into sstables
                Journal.readAll(key, (segment, position, key1, buffer, userVersion) -> {
                    if (segment > minSegment)
                        reader.accept(segment, position, key1, buffer, userVersion);
                }, readOrder, segments);

                // Then, read SSTables
                while (hasTableData)
                {
                    reader.accept(table.segment, table.offset, key, table.value, table.userVersion);
                    hasTableData = table.advance();
                }
            }
        }
    }

    public void readLast(JournalKey key, Reader reader)
    {
        readLast(key, new RecordConsumerAdapter<>(reader));
    }

    public void readLast(JournalKey key, RecordConsumer<JournalKey> reader)
    {
        try (OpOrder.Group readOrder = table.readOrdering.start())
        {
            Segments<JournalKey, Object> segments = this.segments.segments();
            try (TableRecordIterator table = TableRecordIterator.all(this.table, key, readOrder))
            {
                boolean hasTableData = table.advance();
                long minSegment = hasTableData ? table.segment : Long.MIN_VALUE;

                class JournalReader implements RecordConsumer<JournalKey>
                {
                    boolean read;
                    @Override
                    public void accept(long segment, int position, JournalKey key, ByteBuffer buffer, int userVersion)
                    {
                        if (segment > minSegment)
                        {
                            reader.accept(segment, position, key, buffer, userVersion);
                            read = true;
                        }
                    }
                }

                // First, read all journal entries newer than anything flushed into sstables
                JournalReader journalReader = new JournalReader();
                Journal.readLast(key, journalReader, readOrder, segments);

                // Then, read SSTables, if we haven't found a record already
                if (hasTableData && !journalReader.read)
                    reader.accept(table.segment, table.offset, key, table.value, table.userVersion);
            }
        }
    }

    public CloseableIterator<Journal.KeyRefs<JournalKey>> keyIterator(@Nullable JournalKey min, @Nullable JournalKey max, boolean includeActive, long minSegment)
    {
        // the readOrder is taken only to ensure we get a consistent snapshot of both segments and sstables;
        // we take references to the segments and sstables directly, and do not need to manage the readOrder for the lifetime of the iterator
        try (OpOrder.Group readOrder = this.readOrder.start())
        {
            return new TableAndSegmentKeyIterator<>(segments, table, min, max, includeActive, minSegment);
        }
    }

    public void forEach(Consumer<JournalKey> consumer, boolean includeActive, long minSegment)
    {
        forEach(consumer, null, null, includeActive, minSegment);
    }

    public void forEach(Consumer<JournalKey> consumer, @Nullable JournalKey min, @Nullable JournalKey max, boolean includeActive, long minSegment)
    {
        try (CloseableIterator<Journal.KeyRefs<JournalKey>> iter = keyIterator(min, max, includeActive, minSegment))
        {
            while (iter.hasNext())
            {
                Journal.KeyRefs<JournalKey> ref = iter.next();
                consumer.accept(ref.key());
            }
        }
    }

    public Compactor<JournalKey, Object> compactor()
    {
        return segments.compactor();
    }

    protected org.apache.cassandra.journal.SegmentCompactor<JournalKey, Object> compactor(ColumnFamilyStore cfs, Version userVersion)
    {
        if (rangeSearch == null)
        {
            Invariants.require(getAccord().range_index_mode != journal_sai, "range_index_mode is journal_sai, but the storage attached index was not found on initialisation");
            return new SegmentCompactor<>(userVersion, cfs);
        }

        return rangeSearch.compactor(cfs, userVersion);
    }

    public void forceCompaction()
    {
        table.forceMajorCompaction();
    }

    @Override
    public void purge(CommandStores commandStores, EpochSupplier minEpoch)
    {
        segments.closeCurrentSegmentForTestingIfNonEmpty();
        segments.runCompactorForTesting();
        forceCompaction();
    }

    public void replay(CommandStore commandStore, ReplayMode replayMode, long minSegmentId)
    {
        Long2LongHashMap minSegments = new Long2LongHashMap(0);
        minSegments.put(commandStore.id(), minSegmentId);
        Replay.replay(this, replayMode, new CommandStore[] {commandStore }, minSegments);
    }

    @Override
    public boolean replay(CommandStores commandStores, Object param)
    {
        ReplayMode mode = params instanceof JournalConfig ? ((JournalConfig)params).replay
                                                          : getAccord().journal.replay;
        return Replay.replay(this, mode, commandStores.all(), param);
    }

    @Override
    public RangeSearcher rangeSearcher()
    {
        if (rangeSearch == null)
            return NoopJournalRangeSearcher.instance;
        return rangeSearch.rangeSearcher();
    }

    public void writeStartMarker()
    {
        writeMarker(startMarker(), segments.peekSegmentId(), -1L);
    }

    public void writeSafeStopMarker(long lastUniqueTimeStamp)
    {
        segments.fsync();
        writeMarker(safeStopMarker(), segments.peekSegmentId(), lastUniqueTimeStamp);
    }

    private static Runnable merge(Runnable first, Runnable second)
    {
        if (first == null) return second;
        if (second == null) return first;
        return () ->
        {
            try { first.run(); }
            finally { second.run(); }
        };
    }

    public static class DebugEntry implements Supplier<CommandChange.Builder>
    {
        public final long segment;
        public final int position;
        public final CommandChanges builder;

        public DebugEntry(long segment, int position, CommandChanges builder)
        {
            this.segment = segment;
            this.position = position;
            this.builder = builder;
        }

        @Override
        public CommandChange.Builder get()
        {
            return builder;
        }
    }

    @VisibleForTesting
    public Journal<JournalKey, Object> unsafeGetJournal()
    {
        return segments;
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        segments.closeCurrentSegmentForTestingIfNonEmpty();
    }

    public void sanityCheck(int commandStoreId, RedundantBefore redundantBefore, Command orig)
    {
        CommandChanges builder = load(commandStoreId, orig.txnId());
        builder.forceResult(orig.result());
        // We can only use strict equality if we supply result.
        Command reconstructed = builder.construct(redundantBefore);
        Invariants.require(orig.equals(reconstructed),
                           '\n' +
                           "Original:      %s\n" +
                           "Reconstructed: %s\n" +
                           "Diffs:         %s", orig, reconstructed, builder);
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        segments.truncateForTesting();
        if (rangeSearch != null)
            rangeSearch.safeNotify(SegmentRangeSearcher::truncateForTesting);
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        segments.runCompactorForTesting();
    }

    @VisibleForTesting
    public int inMemorySize()
    {
        return segments.currentActiveSegment().index().size();
    }
}