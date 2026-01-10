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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
import org.apache.cassandra.io.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.AbstractReplayer;
import accord.impl.CommandChange;
import accord.impl.CommandChange.Field;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.primitives.PartialDeps;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.journal.Compactor;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.Segments;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.service.accord.AccordJournalSerializers.KeepFirst;
import org.apache.cassandra.service.accord.JournalKey.JournalKeySupport;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static accord.api.Journal.Load.ALL;
import static accord.api.Journal.Load.MINIMAL;
import static accord.api.Journal.Load.MINIMAL_WITH_DEPS;
import static accord.impl.CommandChange.Field.CLEANUP;
import static accord.impl.CommandChange.anyFieldChanged;
import static accord.impl.CommandChange.describeFlags;
import static accord.impl.CommandChange.getFlags;
import static accord.impl.CommandChange.isChanged;
import static accord.impl.CommandChange.isNull;
import static accord.impl.CommandChange.nextSetField;
import static accord.impl.CommandChange.toIterableNonNullFields;
import static accord.impl.CommandChange.toIterableSetFields;
import static accord.impl.CommandChange.unsetIterable;
import static accord.impl.CommandChange.validateFlags;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordJournalDirectory;
import static org.apache.cassandra.service.accord.AccordJournalSerializers.DurableBeforeAccumulator;
import static org.apache.cassandra.service.accord.JournalKey.SUPPORT;
import static org.apache.cassandra.service.accord.JournalKey.Type.COMMAND_DIFF;
import static org.apache.cassandra.service.accord.journal.AccordTopologyUpdate.TopologyAccumulator;
import static org.apache.cassandra.service.accord.journal.AccordTopologyUpdate.Kind;
import static org.apache.cassandra.service.accord.journal.AccordTopologyUpdate.TopologyImage;
import static org.apache.cassandra.service.accord.journal.AccordTopologyUpdate.newTopology;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public class AccordJournal implements accord.api.Journal, JournalRangeSearcher.Supplier
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);
    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[JournalKeySupport.TOTAL_SIZE]);

    @VisibleForTesting
    protected final Journal<JournalKey, Object> journal;
    @VisibleForTesting
    protected final AccordJournalTable<Object> table;
    protected final OpOrder readOrder;
    private final Params params;

    public AccordJournal(Params params)
    {
        this(params, new File(getAccordJournalDirectory()), Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
    }

    @VisibleForTesting
    public AccordJournal(Params params, File directory, ColumnFamilyStore cfs)
    {
        Version userVersion = Version.fromVersion(params.userVersion());
        this.journal = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT,
                                     new ValueSerializer.Unsupported<>(),
                                     compactor(cfs, userVersion),
                                     cfs.readOrdering);
        this.table = new AccordJournalTable<>(cfs);
        this.readOrder = table.cfs.readOrdering;
        this.params = params;
    }

    protected SegmentCompactor<JournalKey, Object> compactor(ColumnFamilyStore cfs, Version userVersion)
    {
        return new AccordSegmentCompactor<>(userVersion, cfs) {
            @Nullable
            @Override
            public Collection<StaticSegment<JournalKey, Object>> compact(Collection<StaticSegment<JournalKey, Object>> staticSegments)
            {
                if (table == null)
                    throw new IllegalStateException("Unsafe access to AccordJournal during <init>; journalTable was touched before it was published");
                Collection<StaticSegment<JournalKey, Object>> result = super.compact(staticSegments);
                table.safeNotify(index -> index.remove(staticSegments));
                return result;
            }
        };
    }

    @VisibleForTesting
    public int inMemorySize()
    {
        return journal.currentActiveSegment().index().size();
    }

    @Override
    public void open(Node node)
    {
        journal.open();
    }

    public void start(Node node)
    {
        // start table first to scrub directories before compactor starts
        table.start();
        long maxTableDescriptor = table.maxDescriptor();
        journal.start(maxTableDescriptor);
    }

    public long maxDescriptor()
    {
        return Math.max(journal.maxDescriptor(), table.maxDescriptor());
    }

    public Params configuration()
    {
        return params;
    }

    public Compactor<JournalKey, Object> compactor()
    {
        return journal.compactor();
    }

    public Descriptor stop()
    {
        return journal.stop();
    }

    public void close()
    {
        journal.close();
    }

    public boolean awaitTerminationUntil(long deadlineNanos) throws InterruptedException
    {
        try
        {
            journal.awaitTerminationUntil(deadlineNanos);
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = load(commandStoreId, txnId);
        builder.maybeCleanup(true, FULL, redundantBefore, durableBefore);
        return builder.construct(redundantBefore);
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

    // applies cleanup and returns null if no command should be returned
    public static CommandChanges cleanupAndFilter(CommandChanges builder, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (builder.isEmpty())
            return null;

        Cleanup cleanup = builder.shouldCleanup(FULL, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case VESTIGIAL:
            case EXPUNGE:
            case ERASE:
                return null;
        }
        Invariants.require(builder.saveStatus() != null, "No saveSatus loaded, but next was called and cleanup was not: %s", builder);
        return builder;
    }

    @Override
    public Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimal();
    }

    @Override
    public Command.MinimalWithDeps loadMinimalWithDeps(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChanges builder = cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL_WITH_DEPS), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimalWithDeps();
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
    public void saveCommand(int commandStoreId, CommandUpdate update, @Nullable Runnable onFlush)
    {
        CommandChangeWriter diff = CommandChangeWriter.make(update.before, update.after);
        if (diff == null)
        {
            if (onFlush != null)
                onFlush.run();
            return;
        }

        JournalKey key = new JournalKey(update.txnId, COMMAND_DIFF, commandStoreId);
        RecordPointer pointer = journal.asyncWrite(key, diff);
        if (table.shouldIndex(key)
            && diff.hasParticipants()
            && diff.after.route() != null)
            journal.onDurable(pointer, () ->
                                       table.safeNotify(index ->
                                                               index.update(pointer.segment, key.commandStoreId, key.id, diff.after.route())));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    @Override
    public List<TopologyUpdate> replayTopologies()
    {
        List<TopologyUpdate> images = new ArrayList<>();
        try (CloseableIterator<TopologyUpdate> iter = new CloseableIterator<>()
        {
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter = keyIterator(topologyUpdateKey(0L),
                                                                                    topologyUpdateKey(Timestamp.MAX_EPOCH),
                                                                                    true, 0);
            TopologyImage prev = null;

            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public TopologyUpdate next()
            {
                Journal.KeyRefs<JournalKey> ref = iter.next();
                TopologyAccumulator reader = readAll(ref.key());
                if (reader.read().kind() == Kind.Repeat)
                {
                    if (prev == null)
                    {
                        logger.error("Encountered TopologyImage Repeat record for epoch {}, but no prior image record was found", ref.key().id.epoch());
                        return null;
                    }
                    prev = reader.read().asImage(Invariants.nonNull(prev.getUpdate()));
                }
                else prev = reader.read();

                return new TopologyUpdate(prev.getUpdate().commandStores,
                                          prev.getUpdate().global);
            }

            @Override
            public void close()
            {
                iter.close();
            }
        })
        {
            TopologyUpdate prev = null;
            while (iter.hasNext())
            {
                TopologyUpdate next = iter.next();
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
        RecordPointer pointer = appendInternal(topologyUpdateKey(topologyUpdate.global.epoch()),
                                               newTopology(topologyUpdate));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    private static JournalKey topologyUpdateKey(long epoch)
    {
        return new JournalKey(TxnId.fromValues(epoch, 0L, Node.Id.NONE),
                              JournalKey.Type.TOPOLOGY_UPDATE, Integer.MAX_VALUE);
    }

    private static final JournalKey DURABLE_BEFORE_KEY = new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0);

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return new PersistentField.Persister<>()
        {
            @Override
            public AsyncResult<?> persist(DurableBefore addValue, DurableBefore newValue)
            {
                AsyncResult.Settable<Void> result = AsyncResults.settable();
                RecordPointer pointer = appendInternal(DURABLE_BEFORE_KEY, addValue);
                // TODO (required): what happens on failure?
                journal.onDurable(pointer, () -> result.setSuccess(null));
                return result;
            }

            @Override
            public DurableBefore load()
            {
                DurableBeforeAccumulator accumulator = readAll(DURABLE_BEFORE_KEY);
                return accumulator.get();
            }
        };
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

        if (onFlush == null)
            return;

        if (pointer != null)
            journal.onDurable(pointer, onFlush);
        else
            onFlush.run();
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

    public <BUILDER extends AccordJournalSerializers.Builder> BUILDER readAll(JournalKey key)
    {
        Invariants.require(journal.isReadable());
        BUILDER builder = (BUILDER) key.type.serializer.builderFor();
        builder.reset(key);
        // TODO (expected): this can be further improved to avoid allocating lambdas
        AccordJournalSerializers.MergeSerializer<?, ? super BUILDER, ? extends BUILDER> serializer = (AccordJournalSerializers.MergeSerializer<?, ? super BUILDER, ? extends BUILDER>) key.type.serializer;
        // TODO (expected): for those where we store an image, read only the first entry we find in DESC order
        readAll(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion));
        return builder;
    }

    public <BUILDER extends AccordJournalSerializers.Builder> BUILDER readLast(JournalKey key)
    {
        Invariants.require(journal.isReadable());
        BUILDER builder = (BUILDER) key.type.serializer.builderFor();
        builder.reset(key);
        // TODO (expected): this can be further improved to avoid allocating lambdas
        AccordJournalSerializers.MergeSerializer<?, ? super BUILDER, ? extends BUILDER> serializer = (AccordJournalSerializers.MergeSerializer<?, ? super BUILDER, ? extends BUILDER>) key.type.serializer;
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
        try (OpOrder.Group readOrder = table.cfs.readOrdering.start())
        {
            // SELECT segments first, to avoid missing segments due to races compacting segment->sstable
            Segments<JournalKey, Object> segments = journal.segments();
            try (AccordJournalTable.TableKeyIterator table = this.table.readAllFromTable(key, readOrder))
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
        try (OpOrder.Group readOrder = table.cfs.readOrdering.start())
        {
            Segments<JournalKey, Object> segments = journal.segments();
            try (AccordJournalTable.TableKeyIterator table = this.table.readAllFromTable(key, readOrder))
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


    @SuppressWarnings("resource") // Auto-closeable iterator will release related resources
    public CloseableIterator<Journal.KeyRefs<JournalKey>> keyIterator(@Nullable JournalKey min, @Nullable JournalKey max, boolean includeActive, long minSegment)
    {
        try (OpOrder.Group readOrder = this.readOrder.start())
        {
            return new JournalAndTableKeyIterator<>(journal, table, min, max, includeActive, minSegment);
        }
    }

    private static class JournalAndTableKeyIterator<V> extends AbstractIterator<Journal.KeyRefs<JournalKey>> implements CloseableIterator<Journal.KeyRefs<JournalKey>>
    {
        final Journal<JournalKey, V>.SegmentKeyIterator journalIterator;
        final AccordJournalTable.TableIterator tableIterator;

        private JournalAndTableKeyIterator(Journal<JournalKey, V> journal, AccordJournalTable<?> table, JournalKey min, JournalKey max, boolean includeActive, long minSegment)
        {
            // We must initialise journal reader first, else we may race with segment->table compaction and miss some data
            // that is, the following sequence could happen:
            //  - Select sstables to read
            //  - Segments compacted; segments removed and sstables added
            //  - Segment iterator created
            // TODO (expected): segments should be sstables on creation
            this.journalIterator = journal.segmentKeyIterator(min, max, segment -> segment.id() >= minSegment && (includeActive || segment.isStatic()));
            this.tableIterator = table.keyIterator(min, max, minSegment);
        }

        JournalKey prevFromTable = null;
        JournalKey prevFromJournal = null;

        @Override
        protected Journal.KeyRefs<JournalKey> computeNext()
        {
            JournalKey tableKey = tableIterator.hasNext() ? tableIterator.peek() : null;
            JournalKey journalKey = journalIterator.hasNext() ? journalIterator.peek().key() : null;

            if (journalKey != null)
            {
                Invariants.require(prevFromJournal == null || SUPPORT.compare(journalKey, prevFromJournal) >= 0, // == for case where we have not consumed previous on prev iteration
                                   "Incorrect sort order in journal segments: %s should strictly follow %s", journalKey, prevFromJournal);
                prevFromJournal = journalKey;
            }
            else
            {
                prevFromJournal = null;
            }

            if (tableKey != null)
            {
                Invariants.require(prevFromTable == null || SUPPORT.compare(tableKey, prevFromTable) >= 0, // == for case where we have not consumed previous on prev iteration
                                   "Incorrect sort order in journal table: %s should strictly follow %s", tableKey, prevFromTable);
                prevFromTable = tableKey;
            }
            else
            {
                prevFromTable = null;
            }

            if (tableKey == null)
                return journalKey == null ? endOfData() : journalIterator.next();

            if (journalKey == null)
                return new Journal.KeyRefs<>(tableIterator.next());

            int cmp = SUPPORT.compare(tableKey, journalKey);
            if (cmp == 0)
            {
                tableIterator.next();
                return journalIterator.next();
            }

            return cmp < 0 ? new Journal.KeyRefs<>(tableIterator.next()) : journalIterator.next();
        }

        public void close()
        {
            tableIterator.close();
            journalIterator.close();
        }
    }

    private <T> RecordPointer appendInternal(JournalKey key, T write)
    {
        AccordJournalSerializers.MergeSerializer<T, ?, ?> serializer = (AccordJournalSerializers.MergeSerializer<T, ?, ?>) key.type.serializer;
        return journal.asyncWrite(key, (out, userVersion) -> serializer.serialize(key, write, out, Version.fromVersion(userVersion)));
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
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
        journal.truncateForTesting();
        table.safeNotify(JournalSegmentRangeSearcher::truncateForTesting);
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        journal.runCompactorForTesting();
    }

    @Override
    public void purge(CommandStores commandStores, EpochSupplier minEpoch)
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        journal.runCompactorForTesting();
        table.forceCompaction();
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

    @Override
    public boolean replay(CommandStores commandStores, Object param)
    {
        Invariants.require(param == null || param.getClass() == Long2LongHashMap.class, "Param should be null or a map of commandStoreId->minSegmentId");
        final Long2LongHashMap minSegments = param == null ? new Long2LongHashMap(0L) : (Long2LongHashMap) param;

        // TODO (expected): make the parallelisms configurable
        // Replay is performed in parallel, where at most X commands can be in flight, across at most Y commands stores.
        // That is, you can limit replay parallelism to 1 command store at a time, but load multiple commands within that data store,
        // _or_ have multiple commands being loaded accross multiple data stores.
        final Semaphore commandParallelism = Semaphore.newSemaphore(getAvailableProcessors());
        final int commandStoreParallelism = Math.max(Math.max(1, Math.min(getAvailableProcessors(), 4)), getAvailableProcessors() / 4);
        final AtomicBoolean abort = new AtomicBoolean();
        final IntArrayList activeCommandStoreIds = new IntArrayList();
        final ReplayQueue pendingCommandStores = new ReplayQueue(commandStores.all());

        class ReplayStream implements Closeable
        {
            final CommandStore commandStore;
            final AbstractReplayer replayer;
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter;
            JournalKey prev;

            public ReplayStream(CommandStore commandStore, long minSegment)
            {
                this.commandStore = commandStore;
                this.replayer = (AbstractReplayer) commandStore.replayer();
                // Keys in the index are sorted by command store id, so index iteration will be sequential
                this.iter = keyIterator(new JournalKey(replayer.minReplay.withoutNonIdentityFlags(), COMMAND_DIFF, commandStore.id()), new JournalKey(TxnId.MAX.withoutNonIdentityFlags(), COMMAND_DIFF, commandStore.id()), false, minSegment);
                logger.info("Beginning replay of {} with min={}, {}", commandStore, replayer.minReplay,
                            replayer.redundantBefore.map(b -> b == null ? null : b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE), TxnId[]::new));
            }

            boolean replay()
            {
                logger.info("Beginning replay of {} with min={}, {}", commandStore, replayer.minReplay,
                            replayer.redundantBefore.map(b -> b == null ? null : b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE), TxnId[]::new));

                JournalKey key;
                long[] segments;
                while (true)
                {
                    if (!iter.hasNext())
                    {
                        logger.info("Completed replay of {}", commandStore);
                        return false;
                    }

                    Journal.KeyRefs<JournalKey> ref = iter.next();
                    if (ref.key().type != COMMAND_DIFF)
                        continue;

                    key = ref.key();
                    segments = table.shouldIndex(key) ? ref.copyOfSegments() : null;
                    break;
                }

                TxnId txnId = key.id;
                Invariants.require(prev == null ||
                                   key.commandStoreId != prev.commandStoreId ||
                                   key.id.compareTo(prev.id) != 0,
                                   "duplicate key detected %s == %s", key, prev);
                prev = key;
                commandParallelism.acquireThrowUncheckedOnInterrupt(1);
                replayer.replay(txnId)
                        .map(route -> {
                          if (segments != null && route != null)
                          {
                              for (long segment : segments)
                                  table.safeNotify(index -> index.update(segment, key.commandStoreId, txnId, (Route<?>) route));
                          }
                          return null;
                      }).begin((success, fail) -> {
                          commandParallelism.release(1);
                          if (fail != null && !journal.handleError("Could not replay command " + txnId, fail))
                              abort.set(true);
                      });

                return true;
            }

            @Override
            public void close()
            {
                iter.close();
            }
        }

        // Replay streams by command store id, can hold at most commandStoreParallelism items
        final Int2ObjectHashMap<ReplayStream> replayStreams = new Int2ObjectHashMap<>();
        try
        {
            // index of the store we're currently pulling from in the activeCommandStoreIds collection
            int cur = 0;
            while (!abort.get())
            {
                if (cur == activeCommandStoreIds.size())
                {
                    if (activeCommandStoreIds.size() < commandStoreParallelism && !pendingCommandStores.isEmpty())
                    {
                        CommandStore next = pendingCommandStores.next();
                        int id = next.id();
                        activeCommandStoreIds.add(id);
                        replayStreams.put(id, new ReplayStream(next, minSegments.getOrDefault(id, 0)));
                    }
                    else if (activeCommandStoreIds.isEmpty()) break;
                    else cur = 0;
                }

                int id = activeCommandStoreIds.get(cur);
                ReplayStream replayStream = replayStreams.get(id);
                while (!replayStream.replay())
                {
                    // Replay complete for this command store; close and replace
                    replayStreams.remove(id).close();
                    if (pendingCommandStores.isEmpty())
                    {
                        // no more pending to submit; remove and continue with the next remaining (if any)
                        activeCommandStoreIds.removeAt(cur);
                        if (cur == activeCommandStoreIds.size())
                            --cur;
                        if (cur < 0)
                            break;
                        id = activeCommandStoreIds.get(cur);
                    }
                    else
                    {
                        // replace it with a pending command store, and continue processing
                        CommandStore next = pendingCommandStores.next(streamId(replayStream.commandStore));
                        id = next.id();
                        activeCommandStoreIds.set(cur, id);
                        replayStreams.put(id, new ReplayStream(next, minSegments.getOrDefault(id, 0)));
                    }

                    replayStream = replayStreams.get(id);
                }

                ++cur;
            }
            return true;
        }
        catch (Throwable t)
        {
            try { FileUtils.close(replayStreams.values()); }
            catch (Throwable t2) { t.addSuppressed(t2); }
            throw t;
        }
    }

    static class ReplayQueue
    {
        final Int2ObjectHashMap<Queue<CommandStore>> byExecutor = new Int2ObjectHashMap<>();
        final Deque<Integer> nextId = new ArrayDeque<>();

        ReplayQueue(CommandStore[] commandStores)
        {
            for (CommandStore commandStore : commandStores)
            {
                byExecutor.computeIfAbsent(streamId(commandStore), ignore -> new ArrayDeque<>())
                          .add(commandStore);
            }
            nextId.addAll(byExecutor.keySet());
        }

        boolean isEmpty()
        {
            return byExecutor.isEmpty();
        }

        CommandStore next()
        {
            while (true)
            {
                if (byExecutor.isEmpty())
                    return null;

                Integer id = nextId.poll();
                if (id == null)
                {
                    nextId.addAll(byExecutor.keySet());
                    id = nextId.poll();
                }

                Queue<CommandStore> queue = byExecutor.get(id);
                if (queue != null)
                {
                    CommandStore next = queue.poll();
                    if (queue.isEmpty())
                        byExecutor.remove(id);
                    if (next != null)
                        return next;
                }
            }
        }

        CommandStore next(int streamId)
        {
            Queue<CommandStore> queue = byExecutor.get(streamId);
            if (queue == null)
                return next();

            CommandStore next = queue.poll();
            if (queue.isEmpty())
                byExecutor.remove(streamId);

            return next;
        }
    }

    private static int streamId(CommandStore commandStore)
    {
        return commandStore instanceof AccordCommandStore ? ((AccordCommandStore) commandStore).executor().executorId() : 1;
    }

    public static @Nullable ByteBuffer asSerializedChange(Command before, Command after, Version userVersion) throws IOException
    {
        // TODO (expected): reusable buffer to build, or pre-size
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            CommandChangeWriter writer = CommandChangeWriter.make(before, after);
            if (writer == null)
                return null;

            writer.write(out, userVersion);
            return out.asNewBuffer();
        }
    }

    @VisibleForTesting
    public Journal<JournalKey, Object> unsafeGetJournal()
    {
        return journal;
    }

    @Override
    public JournalRangeSearcher rangeSearcher()
    {
        return table.rangeSearcher();
    }

    public static class CommandChangeWriter implements Journal.Writer
    {
        private final Command after;
        private final int flags;

        private CommandChangeWriter(Command after, int flags)
        {
            this.after = after;
            this.flags = flags;
        }

        public static CommandChangeWriter make(Command before, Command after)
        {
            if (before == after
                || after == null
                || after.saveStatus() == SaveStatus.Uninitialised)
                return null;

            int flags = validateFlags(getFlags(before, after));
            if (!anyFieldChanged(flags))
                return null;

            return new CommandChangeWriter(after, flags);
        }

        @Override
        public void write(DataOutputPlus out, int userVersion) throws IOException
        {
            write(out, Version.fromVersion(userVersion));
        }

        public void write(DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(after, flags, out, userVersion);
        }

        private static void serialize(Command command, int flags, DataOutputPlus out, Version userVersion) throws IOException
        {
            Invariants.require(flags != 0);
            out.writeInt(flags);

            int iterable = toIterableSetFields(flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (isNull(field, flags))
                {
                    iterable = unsetIterable(field, iterable);
                    continue;
                }

                switch (field)
                {
                    case EXECUTE_AT:
                        ExecuteAtSerializer.serialize(command.txnId(), command.executeAt(), out);
                        break;
                    case EXECUTES_AT_LEAST:
                        ExecuteAtSerializer.serialize(command.executesAtLeast(), out);
                        break;
                    case MIN_UNIQUE_HLC:
                        Invariants.require(command.waitingOn().minUniqueHlc() != 0);
                        out.writeUnsignedVInt(command.waitingOn().minUniqueHlc());
                        break;
                    case SAVE_STATUS:
                        out.writeByte(command.saveStatus().ordinal());
                        break;
                    case DURABILITY:
                        out.writeByte(command.durability().encoded());
                        break;
                    case ACCEPTED:
                        CommandSerializers.ballot.serialize(command.acceptedOrCommitted(), out);
                        break;
                    case PROMISED:
                        CommandSerializers.ballot.serialize(command.promised(), out);
                        break;
                    case PARTICIPANTS:
                        CommandSerializers.participants.serialize(command.participants(), out);
                        break;
                    case PARTIAL_TXN:
                        CommandSerializers.partialTxn.serialize(command.partialTxn(), out, userVersion);
                        break;
                    case PARTIAL_DEPS:
                        DepsSerializers.partialDepsById.serialize(command.partialDeps(), out);
                        break;
                    case WAITING_ON:
                        Command.WaitingOn waitingOn = command.waitingOn();
                        WaitingOnSerializer.serializeBitSetsOnly(command.txnId(), waitingOn, out);
                        break;
                    case WRITES:
                        CommandSerializers.writes.serialize(command.writes(), out, userVersion);
                        break;
                    case RESULT:
                        ResultSerializers.result.serialize(command.result(), out);
                        break;
                    case CLEANUP:
                        Cleanup cleanup;
                        switch (command.saveStatus())
                        {
                            default: throw new UnhandledEnum(command.saveStatus());
                            case Erased: cleanup = Cleanup.ERASE; break;
                            case Invalidated: cleanup = Cleanup.INVALIDATE; break;
                        }
                        out.writeByte(cleanup.ordinal());
                        break;
                }

                iterable = unsetIterable(field, iterable);
            }
        }

        private boolean hasField(Field fields)
        {
            return !isNull(fields, flags);
        }

        public boolean hasParticipants()
        {
            return hasField(Field.PARTICIPANTS);
        }

        @Override
        public String toString()
        {
            return after.saveStatus() + " " + describeFlags(flags);
        }
    }

    public static class CommandChanges extends CommandChange.Builder implements AccordJournalSerializers.Builder
    {
        private final boolean deserializeDeps;

        public CommandChanges()
        {
            this(Load.ALL);
        }

        public CommandChanges(Load load)
        {
            this(null, load);
        }

        public CommandChanges(TxnId txnId)
        {
            this(txnId, Load.ALL);
        }

        public CommandChanges(TxnId txnId, Load load)
        {
            super(txnId, load);
            deserializeDeps = load == ALL;
        }

        @Override
        public PartialDeps partialDeps()
        {
            if (partialDeps instanceof ByteBuffer)
            {
                try
                {
                    partialDeps = DepsSerializers.partialDepsById.deserialize((ByteBuffer) partialDeps);
                }
                catch (IOException e)
                {
                    throw new IllegalStateException("Failed to materialise partially deserialised deps", e);
                }
            }
            return (PartialDeps) partialDeps;
        }

        public void reset(JournalKey key)
        {
            reset(key.id);
        }

        public ByteBuffer asByteBuffer(Version userVersion) throws IOException
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                serialize(out, userVersion);
                return out.asNewBuffer();
            }
        }

        public void serialize(DataOutputPlus out, Version userVersion) throws IOException
        {
            Invariants.require(mask == 0);
            Invariants.require(flags != 0);

            int flags = validateFlags(this.flags);
            serialize(flags, out, userVersion);
        }

        private void serialize(int flags, DataOutputPlus out, Version userVersion) throws IOException
        {
            Invariants.require(flags != 0);
            out.writeInt(flags);

            int iterable = toIterableNonNullFields(flags);
            for (Field field = nextSetField(iterable) ; field != null; iterable = unsetIterable(field, iterable), field = nextSetField(iterable))
            {
                switch (field)
                {
                    default: throw new UnhandledEnum(field);
                    case CLEANUP:
                        out.writeByte(cleanup.ordinal());
                        break;
                    case EXECUTE_AT:
                        Invariants.require(txnId != null, "%s", this);
                        Invariants.require(executeAt != null, "%s", this);
                        ExecuteAtSerializer.serialize(txnId, executeAt, out);
                        break;
                    case EXECUTES_AT_LEAST:
                        Invariants.require(executesAtLeast != null);
                        ExecuteAtSerializer.serialize(executesAtLeast, out);
                        break;
                    case MIN_UNIQUE_HLC:
                        Invariants.require(minUniqueHlc != 0, "%s", this);
                        out.writeUnsignedVInt(minUniqueHlc);
                        break;
                    case SAVE_STATUS:
                        Invariants.require(saveStatus != null, "%s", this);
                        out.writeByte(saveStatus.ordinal());
                        break;
                    case DURABILITY:
                        Invariants.require(durability != null, "%s", this);
                        out.writeByte(durability.encoded());
                        break;
                    case ACCEPTED:
                        Invariants.require(acceptedOrCommitted != null, "%s", this);
                        CommandSerializers.ballot.serialize(acceptedOrCommitted, out);
                        break;
                    case PROMISED:
                        Invariants.require(promised != null, "%s", this);
                        CommandSerializers.ballot.serialize(promised, out);
                        break;
                    case PARTICIPANTS:
                        Invariants.require(participants != null, "%s", this);
                        CommandSerializers.participants.serialize(participants, out);
                        break;
                    case PARTIAL_TXN:
                        Invariants.require(partialTxn != null, "%s", this);
                        CommandSerializers.partialTxn.serialize(partialTxn, out, userVersion);
                        break;
                    case PARTIAL_DEPS:
                        Invariants.require(partialDeps != null, "%s", this);
                        if (partialDeps instanceof ByteBuffer) out.write(((ByteBuffer) partialDeps).duplicate());
                        else DepsSerializers.partialDepsById.serialize((PartialDeps) partialDeps, out);
                        break;
                    case WAITING_ON:
                        Invariants.require(waitingOn != null, "%s", this);
                        ((WaitingOnSerializer.WaitingOnBitSetsAndLength)waitingOn).reserialize(out);
                        break;
                    case WRITES:
                        Invariants.require(writes != null, "%s", this);
                        CommandSerializers.writes.serialize(writes, out, userVersion);
                        break;
                    case RESULT:
                        Invariants.require(result != null, "%s", this);
                        ResultSerializers.result.serialize(result, out);
                        break;
                }
            }
        }

        public void deserializeNext(ByteBuffer buffer, Version userVersion)
        {
            try (DataInputBuffer in = new DataInputBuffer(buffer, false))
            {
                deserializeNext(in, userVersion);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        public void deserializeNext(DataInputPlus in, Version userVersion) throws IOException
        {
            Invariants.require(txnId != null);
            int readFlags = in.readInt();
            Invariants.require(readFlags != 0);
            hasUpdate = true;
            count++;

            // batch-apply any new nulls
            setNulls(false, readFlags);
            // iterator sets low 16 bits; low readFlag bits are nulls, so masking with ~readFlags restricts to non-null changed fields
            int iterable = toIterableSetFields(readFlags) & ~readFlags;
            for (Field field = nextSetField(iterable) ; field != null; field = nextSetField(iterable = unsetIterable(field, iterable)))
            {
                // Since we are iterating in reverse order, we skip the fields that were
                // set by entries written later (i.e. already read ones).
                if (isChanged(field, flags | mask) && field != CLEANUP)
                    skip(txnId, field, in, userVersion);
                else
                    deserialize(field, in, userVersion);
            }

            // upper 16 bits are changed flags, lower are nulls; by masking upper by ~lower we restrict to only non-null changed fields
            this.flags |= readFlags & (~readFlags << 16);
        }

        private void deserialize(Field field, DataInputPlus in, Version userVersion) throws IOException
        {
            switch (field)
            {
                case EXECUTE_AT:
                    executeAt = ExecuteAtSerializer.deserialize(txnId, in);
                    break;
                case EXECUTES_AT_LEAST:
                    executesAtLeast = ExecuteAtSerializer.deserialize(in);
                    break;
                case MIN_UNIQUE_HLC:
                    minUniqueHlc = in.readUnsignedVInt();
                    break;
                case SAVE_STATUS:
                    saveStatus = SaveStatus.values()[in.readByte()];
                    break;
                case DURABILITY:
                    durability = Durability.forEncoded(in.readUnsignedByte());
                    break;
                case ACCEPTED:
                    acceptedOrCommitted = CommandSerializers.ballot.deserialize(in);
                    break;
                case PROMISED:
                    promised = CommandSerializers.ballot.deserialize(in);
                    break;
                case PARTICIPANTS:
                    participants = CommandSerializers.participants.deserialize(in);
                    break;
                case PARTIAL_TXN:
                    partialTxn = CommandSerializers.partialTxn.deserialize(in, userVersion);
                    break;
                case PARTIAL_DEPS:
                    // TODO (expected): this optimisation will be easily disabled;
                    //  should either operate natively on ByteBuffer
                    //  or else use some explicit API for copying bytes while skipping
                    if (deserializeDeps || !(in instanceof DataInputBuffer))
                    {
                        partialDeps = DepsSerializers.partialDepsById.deserialize(in);
                    }
                    else
                    {
                        ByteBuffer buf = ((DataInputBuffer)in).buffer();
                        int start = buf.position();
                        DepsSerializers.partialDepsById.skip(in);
                        int end = buf.position();
                        partialDeps = buf.duplicate().position(start).limit(end);
                    }
                    break;
                case WAITING_ON:
                    waitingOn = WaitingOnSerializer.deserializeBitSets(txnId, in);
                    break;
                case WRITES:
                    writes = CommandSerializers.writes.deserialize(in, userVersion);
                    break;
                case CLEANUP:
                    Cleanup newCleanup = Cleanup.forOrdinal(in.readByte());
                    if (cleanup == null || newCleanup.compareTo(cleanup) > 0)
                        cleanup = newCleanup;
                    break;
                case RESULT:
                    result = ResultSerializers.result.deserialize(in);
                    break;
            }
        }

        private static void skip(TxnId txnId, Field field, DataInputPlus in, Version userVersion) throws IOException
        {
            switch (field)
            {
                default: throw new UnhandledEnum(field);
                case EXECUTE_AT:
                    ExecuteAtSerializer.skip(txnId, in);
                    break;
                case EXECUTES_AT_LEAST:
                    ExecuteAtSerializer.skip(in);
                    break;
                case MIN_UNIQUE_HLC:
                    in.readUnsignedVInt();
                    break;
                case SAVE_STATUS:
                case DURABILITY:
                case CLEANUP:
                    in.readByte();
                    break;
                case ACCEPTED:
                case PROMISED:
                    CommandSerializers.ballot.skip(in);
                    break;
                case PARTICIPANTS:
                    CommandSerializers.participants.skip(in);
                    break;
                case PARTIAL_TXN:
                    CommandSerializers.partialTxn.skip(in, userVersion);
                    break;
                case PARTIAL_DEPS:
                    DepsSerializers.partialDepsById.skip(in);
                    break;
                case WAITING_ON:
                    WaitingOnSerializer.skip(txnId, in);
                    break;
                case WRITES:
                    // TODO (expected): skip
                    CommandSerializers.writes.skip(in, userVersion);
                    break;
                case RESULT:
                    // TODO (expected): skip
                    ResultSerializers.result.skip(in);
                    break;
            }
        }
    }

    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), "started");
    }

    public static File stopMarker()
    {
        return new File(getAccordJournalDirectory(), "stopped");
    }

    void writeStartMarker()
    {
        writeMarker(startMarker(), journal.peekSegmentId());
    }

    void writeStopMarker()
    {
        writeMarker(stopMarker(), journal.peekSegmentId());
    }

    static void writeMarker(File file, long timestamp)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.writeBytes(Long.toString(timestamp));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    static long readStartMarker()
    {
        return readMarker(startMarker());
    }

    static long readStopMarker()
    {
        return readMarker(stopMarker());
    }

    static long readMarker(File file)
    {
        if (!file.exists())
            return -1L;

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return Long.parseLong(sb.toString());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static void trySyncJournalDirectory()
    {
        trySyncDirectory(getAccordJournalDirectory());
    }

    private static void trySyncDirectory(String path)
    {
        int fd = NativeLibrary.tryOpenDirectory(path);
        NativeLibrary.trySync(fd);
    }

    public static File saveDirectory()
    {
        return new File(getAccordJournalDirectory(), "save_state");
    }
}