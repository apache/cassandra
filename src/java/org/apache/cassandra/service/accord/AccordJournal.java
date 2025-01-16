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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.CommandChange;
import accord.impl.CommandChange.Field;
import accord.impl.ErasedSafeCommand;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Compactor;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.IdentityAccumulator;
import org.apache.cassandra.service.accord.JournalKey.JournalKeySupport;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static accord.impl.CommandChange.anyFieldChanged;
import static accord.impl.CommandChange.getFieldChanged;
import static accord.impl.CommandChange.getFieldIsNull;
import static accord.impl.CommandChange.getFlags;
import static accord.impl.CommandChange.getWaitingOn;
import static accord.impl.CommandChange.nextSetField;
import static accord.impl.CommandChange.setFieldChanged;
import static accord.impl.CommandChange.setFieldIsNull;
import static accord.impl.CommandChange.toIterableSetFields;
import static accord.impl.CommandChange.unsetIterableFields;
import static accord.impl.CommandChange.validateFlags;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.Status.Truncated;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;

public class AccordJournal implements accord.api.Journal, RangeSearcher.Supplier, Shutdownable
{
    static
    {
        // make noise early if we forget to update our version mappings
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_51, "Expected current version to be %d but given %d", MessagingService.VERSION_51, MessagingService.current_version);
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[JournalKeySupport.TOTAL_SIZE]);

    private final Journal<JournalKey, Object> journal;
    private final AccordJournalTable<JournalKey, Object> journalTable;
    private final Params params;
    private final AccordAgent agent;
    /**
     * Access to this field should only ever be handled by {@link #safeNotify(Consumer)}.  There is an assumption that
     * an error in the index should not cause the journal to crash, so {@link #safeNotify(Consumer)} exists to make sure
     * this property holds true.
     */
    @Nullable
    private final ParticipantsInMemoryIndex<JournalKey, Object> index;
    private final ColumnFamilyStore cfs;
    Node node;

    enum Status { INITIALIZED, STARTING, REPLAY, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    public AccordJournal(Params params, AccordAgent agent)
    {
        this(params, agent, new File(DatabaseDescriptor.getAccordJournalDirectory()), Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
    }

    @VisibleForTesting
    public AccordJournal(Params params, AccordAgent agent, File directory, ColumnFamilyStore cfs)
    {
        // this class relies on unsafe publishing of "this", so need to make sure the fields impacted are written to (they are final) before this leak happens
        this.cfs = cfs;
        if (cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME) != null)
        {
            this.index = new ParticipantsInMemoryIndex<>();
        }
        else
        {
            this.index = null;
        }
        this.agent = agent;
        AccordSegmentCompactor<Object> compactor = new AccordSegmentCompactor<>(params.userVersion(), cfs) {
            @Nullable
            @Override
            public Collection<StaticSegment<JournalKey, Object>> compact(Collection<StaticSegment<JournalKey, Object>> staticSegments)
            {
                Collection<StaticSegment<JournalKey, Object>> result = super.compact(staticSegments);
                if (result != null)
                    safeNotify(index -> index.onCompact(staticSegments));
                return result;
            }
        };
        this.journal = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT,
                                     // In Accord, we are using streaming serialization, i.e. Reader/Writer interfaces instead of materializing objects
                                     new ValueSerializer<>()
                                     {
                                         @Override
                                         public void serialize(JournalKey key, Object value, DataOutputPlus out, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }

                                         @Override
                                         public Object deserialize(JournalKey key, DataInputPlus in, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }
                                     },
                                     compactor);
        this.journalTable = new AccordJournalTable<>(journal, JournalKey.SUPPORT, cfs, params.userVersion());
        this.params = params;
    }

    @VisibleForTesting
    public int inMemorySize()
    {
        return journal.currentActiveSegment().index().size();
    }

    public AccordJournal start(Node node)
    {
        Invariants.checkState(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        return this;
    }

    public boolean started()
    {
        return status == Status.STARTED;
    }

    public Params configuration()
    {
        return params;
    }

    public Compactor<JournalKey, Object> compactor()
    {
        return journal.compactor();
    }

    @Override
    public boolean isTerminated()
    {
        return status == Status.TERMINATED;
    }

    @Override
    public void shutdown()
    {
        Invariants.checkState(status == Status.REPLAY || status == Status.STARTED, "%s", status);
        status = Status.TERMINATING;
        journal.shutdown();
        status = Status.TERMINATED;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Collections.singletonList(journal));
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
        Builder builder = load(commandStoreId, txnId);
        Cleanup cleanup = builder.shouldCleanup(agent, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return ErasedSafeCommand.erased(txnId, ErasedOrVestigial);
        }
        return builder.construct(redundantBefore);
    }

    @Override
    public Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, Load load, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Builder builder = loadDiffs(commandStoreId, txnId, load);
        if (builder.isEmpty())
            return null;

        Cleanup cleanup = builder.shouldCleanup(node.agent(), redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return null;
        }
        Invariants.checkState(builder.saveStatus() != null, "No saveSatus loaded, but next was called and cleanup was not: %s", builder);
        return builder.asMinimal();
    }

    @Override
    public RedundantBefore loadRedundantBefore(int store)
    {
        IdentityAccumulator<RedundantBefore> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, store), false);
        return accumulator.get();
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int store)
    {
        IdentityAccumulator<NavigableMap<TxnId, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, store), false);
        return accumulator.get();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int store)
    {
        IdentityAccumulator<NavigableMap<Timestamp, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, store), false);
        return accumulator.get();
    }

    @Override
    public CommandStores.RangesForEpoch loadRangesForEpoch(int store)
    {
        IdentityAccumulator<RangesForEpoch> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, store), false);
        return accumulator.get();
    }

    @Override
    public void saveCommand(int store, CommandUpdate update, @Nullable Runnable onFlush)
    {
        Writer diff = Writer.make(update.before, update.after);
        if (diff == null)
        {
            if (onFlush != null)
                onFlush.run();
            return;
        }

        JournalKey key = new JournalKey(update.txnId, JournalKey.Type.COMMAND_DIFF, store);
        RecordPointer pointer = journal.asyncWrite(key, diff, SENTINEL_HOSTS);
        if (index != null && index.isSupported(key))
            journal.onDurable(pointer, () -> safeNotify(index -> index.onWrite(key, diff, pointer)));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    @Override
    public Iterator<TopologyUpdate> replayTopologies()
    {
        AccordJournalValueSerializers.MapAccumulator<Long, TopologyUpdate> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.TOPOLOGY_UPDATE, 0), false);
        return accumulator.get().values().iterator();
    }

    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        RecordPointer pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.TOPOLOGY_UPDATE, 0), topologyUpdate);
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return new PersistentField.Persister<>()
        {
            @Override
            public AsyncResult<?> persist(DurableBefore addDurableBefore, DurableBefore newDurableBefore)
            {
                AsyncResult.Settable<Void> result = AsyncResults.settable();
                JournalKey key = new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0);
                RecordPointer pointer = appendInternal(key, addDurableBefore);
                // TODO (required): what happens on failure?
                journal.onDurable(pointer, () -> result.setSuccess(null));
                return result;
            }

            @Override
            public DurableBefore load()
            {
                DurableBeforeAccumulator accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0), false);
                return accumulator.get();
            }
        };
    }

    @Override
    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        RecordPointer pointer = null;
        // TODO: avoid allocating keys
        if (fieldUpdates.newRedundantBefore != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, store), fieldUpdates.newRedundantBefore);
        if (fieldUpdates.newBootstrapBeganAt != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, store), fieldUpdates.newBootstrapBeganAt);
        if (fieldUpdates.newSafeToRead != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, store), fieldUpdates.newSafeToRead);
        if (fieldUpdates.newRangesForEpoch != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, store), fieldUpdates.newRangesForEpoch);

        if (onFlush == null)
            return;

        if (pointer != null)
            journal.onDurable(pointer, onFlush);
        else
            onFlush.run();
    }

    private Builder loadDiffs(int commandStoreId, TxnId txnId, Load load)
    {
        JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStoreId);
        Builder builder = new Builder(txnId, load);
        journalTable.readAll(key, builder::deserializeNext, false);
        return builder;
    }

    @VisibleForTesting
    public Builder load(int commandStoreId, TxnId txnId)
    {
        return loadDiffs(commandStoreId, txnId, Load.ALL);
    }

    private <BUILDER> BUILDER readAll(JournalKey key, boolean asc)
    {
        BUILDER builder = (BUILDER) key.type.serializer.mergerFor(key);
        // TODO: this can be further improved to avoid allocating lambdas
        AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER> serializer = (AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER>) key.type.serializer;
        // TODO (expected): for those where we store an image, read only the first entry we find in DESC order
        journalTable.readAll(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion), asc);
        return builder;
    }

    private RecordPointer appendInternal(JournalKey key, Object write)
    {
        AccordJournalValueSerializers.FlyweightSerializer<Object, ?> serializer = (AccordJournalValueSerializers.FlyweightSerializer<Object, ?>) key.type.serializer;
        return journal.asyncWrite(key, (out, userVersion) -> serializer.serialize(key, write, out, userVersion), SENTINEL_HOSTS);
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTestingIfNonEmpty()
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
    }

    public void sanityCheck(int commandStoreId, RedundantBefore redundantBefore, Command orig)
    {
        Builder builder = load(commandStoreId, orig.txnId());
        builder.forceResult(orig.result());
        // We can only use strict equality if we supply result.
        Command reconstructed = builder.construct(redundantBefore);
        Invariants.checkState(orig.equals(reconstructed),
                              '\n' +
                              "Original:      %s\n" +
                              "Reconstructed: %s\n" +
                              "Diffs:         %s", orig, reconstructed, builder);
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        journal.truncateForTesting();
        safeNotify(ParticipantsInMemoryIndex::truncateForTesting);
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        journal.runCompactorForTesting();
    }

    @Override
    public void purge(CommandStores commandStores)
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        journal.runCompactorForTesting();
        journalTable.forceCompaction();
    }

    @Override
    public void replay(CommandStores commandStores)
    {
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        try (AccordJournalTable.KeyOrderIterator<JournalKey> iter = journalTable.readAll())
        {
            JournalKey key;
            Builder builder = new Builder();

            while ((key = iter.key()) != null)
            {
                builder.reset(key.id);
                if (key.type != JournalKey.Type.COMMAND_DIFF)
                {
                    // TODO (required): add "skip" for the key to avoid getting stuck
                    iter.readAllForKey(key, (segment, position, key1, buffer, hosts, userVersion) -> {});
                    continue;
                }

                JournalKey finalKey = key;
                iter.readAllForKey(key, (segment, position, local, buffer, hosts, userVersion) -> {
                    Invariants.checkState(finalKey.equals(local));
                    try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                    {
                        builder.deserializeNext(in, userVersion);
                        safeNotify(index -> {
                            if (!index.isSupported(finalKey)) return;
                            Route<?> route = builder.participants().route();
                            if (route != null)
                                index.update(segment, finalKey.commandStoreId, finalKey.id, route);
                        });
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                });

                if (!builder.isEmpty())
                {
                    CommandStore commandStore = commandStores.forId(key.commandStoreId);
                    Command command = builder.construct(commandStore.unsafeGetRedundantBefore());
                    Invariants.checkState(command.saveStatus() != SaveStatus.Uninitialised,
                                          "Found uninitialized command in the log: %s %s", command.toString(), builder.toString());
                    Loader loader = commandStore.loader();
                    async(loader::load, command).get();
                    if (command.saveStatus().compareTo(SaveStatus.Stable) >= 0 && !command.hasBeen(Truncated))
                        async(loader::apply, command).get();
                }
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not replay journal.", t);
        }
    }

    private AsyncPromise<?> async(BiConsumer<Command, OnDone> consumer, Command command)
    {
        AsyncPromise<?> future = new AsyncPromise<>();
        consumer.accept(command, new OnDone()
        {
            public void success()
            {
                future.setSuccess(null);
            }

            public void failure(Throwable t)
            {
                future.setFailure(t);
            }
        });
        return future;
    }

    public static @Nullable ByteBuffer asSerializedChange(Command before, Command after, int userVersion) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Writer writer = Writer.make(before, after);
            if (writer == null)
                return null;

            writer.write(out, userVersion);
            return out.asNewBuffer();
        }
    }

    @VisibleForTesting
    public void unsafeSetStarted()
    {
        status = Status.STARTED;
    }

    @Override
    public RangeSearcher rangeSearcher()
    {
        if (index == null)
            return RangeSearcher.NoopRangeSearcher.instance;
        return new Searcher(cfs, index);
    }

    void safeNotify(Consumer<ParticipantsInMemoryIndex<JournalKey, Object>> fn)
    {
        if (index == null)
            return;
        try
        {
            fn.accept(index);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Failure updating index", t);
        }
    }

    private static class Searcher implements RangeSearcher
    {
        private final ColumnFamilyStore cfs;
        private final ParticipantsInMemoryIndex<JournalKey, Object> index;
        private final Index tableIndex;

        public Searcher(ColumnFamilyStore cfs, ParticipantsInMemoryIndex<JournalKey, Object> index)
        {
            this.cfs = cfs;
            this.index = index;
            this.tableIndex = cfs.indexManager.getIndexByName("record");
            if (!cfs.indexManager.isIndexQueryable(tableIndex))
                throw new AssertionError("Journal record index is not queryable");
        }

        @Override
        public void intersects(int storeId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
        {
            try (CloseableIterator<Entry> it = search(storeId, range.start(), range.end()))
            {
                consume(it, storeId, minTxnId, maxTxnId, forEach);
            }
        }

        @Override
        public void intersects(int storeId, AccordRoutingKey key, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
        {
            try (CloseableIterator<Entry> it = search(storeId, key))
            {
                consume(it, storeId, minTxnId, maxTxnId, forEach);
            }
        }

        private void consume(Iterator<Entry> it, int storeId, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
        {
            while (it.hasNext())
            {
                Entry next = it.next();
                if (next.store_id != storeId) continue; // the index should filter out, but just in case...
                if (next.txnId.compareTo(minTxnId) >= 0 && next.txnId.compareTo(maxTxnId) < 0)
                    forEach.accept(next.txnId);
            }
        }

        private CloseableIterator<Entry> search(int store, AccordRoutingKey start, AccordRoutingKey end)
        {
            Invariants.checkArgument(start.table().equals(end.table()), "Start %s has different table than end %s", start, end);
            CloseableIterator<Entry> inMemory = toIterator(store, index.search(store, start, end));
            CloseableIterator<Entry> table = tableSearch(store, start, end);
            return merge(inMemory, table);
        }

        private CloseableIterator<Entry> tableSearch(int store, AccordRoutingKey start, AccordRoutingKey end)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(SyntheticColumn.participants.metadata, Operator.GT, OrderedRouteSerializer.serializeRoutingKey(start));
            rowFilter.add(SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serializeRoutingKey(end));
            rowFilter.add(SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));

            PartitionRangeReadCommand cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                             FBUtilities.nowInSeconds(),
                                                                             ColumnFilter.selectionBuilder()
                                                                                         .add(SyntheticColumn.store_id.metadata)
                                                                                         .add(SyntheticColumn.txn_id.metadata)
                                                                                         .build(),
                                                                             rowFilter,
                                                                             DataLimits.NONE,
                                                                             DataRange.allData(cfs.getPartitioner()));
            return process(cmd);
        }

        private CloseableIterator<Entry> search(int store, AccordRoutingKey key)
        {
            CloseableIterator<Entry> inMemory = toIterator(store, index.search(store, key));
            CloseableIterator<Entry> table = tableSearch(store, key);
            return merge(inMemory, table);
        }

        private CloseableIterator<Entry> tableSearch(int store, AccordRoutingKey key)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(SyntheticColumn.participants.metadata, Operator.GTE, OrderedRouteSerializer.serializeRoutingKey(key));
            rowFilter.add(SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serializeRoutingKey(key));
            rowFilter.add(SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));

            PartitionRangeReadCommand cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                             FBUtilities.nowInSeconds(),
                                                                             ColumnFilter.selectionBuilder()
                                                                                         .add(SyntheticColumn.store_id.metadata)
                                                                                         .add(SyntheticColumn.txn_id.metadata)
                                                                                         .build(),
                                                                             rowFilter,
                                                                             DataLimits.NONE,
                                                                             DataRange.allData(cfs.getPartitioner()));
            return process(cmd);
        }

        private CloseableIterator<Entry> process(PartitionRangeReadCommand cmd)
        {
            Index.Searcher s = tableIndex.searcherFor(cmd);
            try (ReadExecutionController controller = cmd.executionController())
            {
                UnfilteredPartitionIterator partitionIterator = s.search(controller);
                return new CloseableIterator<Entry>()
                {
                    private final Entry entry = new Entry();

                    @Override
                    public void close()
                    {
                        partitionIterator.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return partitionIterator.hasNext();
                    }

                    @Override
                    public Entry next()
                    {
                        UnfilteredRowIterator next = partitionIterator.next();
                        JournalKey partitionKeyComponents = AccordKeyspace.JournalColumns.getJournalKey(next.partitionKey());
                        entry.store_id = partitionKeyComponents.commandStoreId;
                        entry.txnId = partitionKeyComponents.id;
                        return entry;
                    }
                };
            }
        }

        private static CloseableIterator<Entry> toIterator(int store, NavigableMap<IndexRange, Set<TxnId>> journalSearch)
        {
            TreeSet<TxnId> matches = new TreeSet<>();
            journalSearch.values().forEach(s -> matches.addAll(s));
            return new CloseableIterator<>()
            {
                private final Entry entry = new Entry();
                private final Iterator<TxnId> it = matches.iterator();
                @Override
                public void close()
                {
                    matches.clear();
                }

                @Override
                public boolean hasNext()
                {
                    return it.hasNext();
                }

                @Override
                public Entry next()
                {
                    entry.store_id = store;
                    entry.txnId = it.next();
                    return entry;
                }
            };
        }

        private static CloseableIterator<Entry> merge(CloseableIterator<Entry> inMemory, CloseableIterator<Entry> disk)
        {
            return MergeIterator.get(Arrays.asList(inMemory, disk), (a, b) -> {
                Invariants.checkArgument(a.store_id == b.store_id);
                return a.txnId.compareTo(b.txnId);
            }, new MergeIterator.Reducer<Entry, Entry>()
            {
                private Entry first = null;
                @Override
                protected void onKeyChange()
                {
                    first = null;
                }

                @Override
                public void reduce(int idx, Entry current)
                {
                    if (first == null)
                        first = current;
                }

                @Override
                protected Entry getReduced()
                {
                    Invariants.checkState(first != null);
                    return first;
                }
            });
        }
    }

    /**
     * When using {@link PartitionRangeReadCommand} we need to work with {@link RowFilter} which works with columns.
     * But the index doesn't care about table based queries and needs to be queried using the fields in the index, to
     * support that this enum exists.  This enum represents the fields present in the index and can be used to apply
     * filters to the index.
     */
    public enum SyntheticColumn
    {
        participants("participants", BytesType.instance),
        store_id("store_id", Int32Type.instance),
        txn_id("txn_id", AccordKeyspace.TIMESTAMP_TYPE);

        public final ColumnMetadata metadata;

        SyntheticColumn(String name, AbstractType<?> type)
        {
            this.metadata = new ColumnMetadata("journal", "routes", new ColumnIdentifier(name, false), type, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        }
    }

    private static final class Entry
    {
        public int store_id;
        public TxnId txnId;
    }

    public static class Writer implements Journal.Writer
    {
        public final Command after;
        private final int flags;

        private Writer(Command after, int flags)
        {
            this.after = after;
            this.flags = flags;
        }

        public static Writer make(Command before, Command after)
        {
            if (before == after
                || after == null
                || after.saveStatus() == SaveStatus.Uninitialised)
                return null;

            int flags = validateFlags(getFlags(before, after));
            if (!anyFieldChanged(flags))
                return null;

            return new Writer(after, flags);
        }

        @Override
        public void write(DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(after, flags, out, userVersion);
        }

        private static void serialize(Command command, int flags, DataOutputPlus out, int userVersion) throws IOException
        {
            Invariants.checkState(flags != 0);
            out.writeInt(flags);

            int iterable = toIterableSetFields(flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (getFieldIsNull(field, flags))
                {
                    iterable = unsetIterableFields(field, iterable);
                    continue;
                }

                switch (field)
                {
                    case EXECUTE_AT:
                        CommandSerializers.timestamp.serialize(command.executeAt(), out, userVersion);
                        break;
                    case EXECUTES_AT_LEAST:
                        CommandSerializers.timestamp.serialize(command.executesAtLeast(), out, userVersion);
                        break;
                    case SAVE_STATUS:
                        out.writeShort(command.saveStatus().ordinal());
                        break;
                    case DURABILITY:
                        out.writeByte(command.durability().ordinal());
                        break;
                    case ACCEPTED:
                        CommandSerializers.ballot.serialize(command.acceptedOrCommitted(), out, userVersion);
                        break;
                    case PROMISED:
                        CommandSerializers.ballot.serialize(command.promised(), out, userVersion);
                        break;
                    case PARTICIPANTS:
                        CommandSerializers.participants.serialize(command.participants(), out, userVersion);
                        break;
                    case PARTIAL_TXN:
                        CommandSerializers.partialTxn.serialize(command.partialTxn(), out, userVersion);
                        break;
                    case PARTIAL_DEPS:
                        DepsSerializers.partialDeps.serialize(command.partialDeps(), out, userVersion);
                        break;
                    case WAITING_ON:
                        Command.WaitingOn waitingOn = getWaitingOn(command);
                        long size = WaitingOnSerializer.serializedSize(command.txnId(), waitingOn);
                        ByteBuffer serialized = WaitingOnSerializer.serialize(command.txnId(), waitingOn);
                        Invariants.checkState(serialized.remaining() == size);
                        out.writeInt((int) size);
                        out.write(serialized);
                        break;
                    case WRITES:
                        CommandSerializers.writes.serialize(command.writes(), out, userVersion);
                        break;
                    case RESULT:
                        ResultSerializers.result.serialize(command.result(), out, userVersion);
                        break;
                    case CLEANUP:
                        throw new IllegalStateException();
                }

                iterable = unsetIterableFields(field, iterable);
            }
        }

        public boolean hasField(Field fields)
        {
            return !getFieldIsNull(fields, flags);
        }
    }

    public static class Builder extends CommandChange.Builder
    {
        public Builder()
        {
            super(null, Load.ALL);
        }

        public Builder(TxnId txnId)
        {
            super(txnId, Load.ALL);
        }

        public Builder(TxnId txnId, Load load)
        {
            super(txnId, load);
        }
        public ByteBuffer asByteBuffer(RedundantBefore redundantBefore, int userVersion) throws IOException
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                serialize(out, redundantBefore, userVersion);
                return out.asNewBuffer();
            }
        }

        public Builder maybeCleanup(Cleanup cleanup)
        {
            super.maybeCleanup(cleanup);
            return this;
        }

        public void serialize(DataOutputPlus out, RedundantBefore redundantBefore, int userVersion) throws IOException
        {
            Invariants.checkState(mask == 0);
            Invariants.checkState(flags != 0);

            int flags = validateFlags(this.flags);
            Writer.serialize(construct(redundantBefore), flags, out, userVersion);
        }

        public void deserializeNext(DataInputPlus in, int userVersion) throws IOException
        {
            Invariants.checkState(txnId != null);
            int flags = in.readInt();
            Invariants.checkState(flags != 0);
            nextCalled = true;
            count++;

            int iterable = toIterableSetFields(flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (getFieldChanged(field, this.flags) || getFieldIsNull(field, mask))
                {
                    if (!getFieldIsNull(field, flags))
                        skip(field, in, userVersion);

                    iterable = unsetIterableFields(field, iterable);
                    continue;
                }
                this.flags = setFieldChanged(field, this.flags);

                if (getFieldIsNull(field, flags))
                {
                    this.flags = setFieldIsNull(field, this.flags);
                }
                else
                {
                    deserialize(field, in, userVersion);
                }

                iterable = unsetIterableFields(field, iterable);
            }
        }

        private void deserialize(Field field, DataInputPlus in, int userVersion) throws IOException
        {
            switch (field)
            {
                case EXECUTE_AT:
                    executeAt = CommandSerializers.timestamp.deserialize(in, userVersion);
                    break;
                case EXECUTES_AT_LEAST:
                    executeAtLeast = CommandSerializers.timestamp.deserialize(in, userVersion);
                    break;
                case SAVE_STATUS:
                    saveStatus = SaveStatus.values()[in.readShort()];
                    break;
                case DURABILITY:
                    durability = accord.primitives.Status.Durability.values()[in.readByte()];
                    break;
                case ACCEPTED:
                    acceptedOrCommitted = CommandSerializers.ballot.deserialize(in, userVersion);
                    break;
                case PROMISED:
                    promised = CommandSerializers.ballot.deserialize(in, userVersion);
                    break;
                case PARTICIPANTS:
                    participants = CommandSerializers.participants.deserialize(in, userVersion);
                    break;
                case PARTIAL_TXN:
                    partialTxn = CommandSerializers.partialTxn.deserialize(in, userVersion);
                    break;
                case PARTIAL_DEPS:
                    partialDeps = DepsSerializers.partialDeps.deserialize(in, userVersion);
                    break;
                case WAITING_ON:
                    int size = in.readInt();

                    byte[] waitingOnBytes = new byte[size];
                    in.readFully(waitingOnBytes);
                    ByteBuffer buffer = ByteBuffer.wrap(waitingOnBytes);
                    waitingOn = (localTxnId, deps) -> {
                        try
                        {
                            Invariants.nonNull(deps);
                            return WaitingOnSerializer.deserialize(localTxnId, deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps, buffer);
                        }
                        catch (IOException e)
                        {
                            throw Throwables.unchecked(e);
                        }
                    };
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
                    result = ResultSerializers.result.deserialize(in, userVersion);
                    break;
            }
        }

        private void skip(Field field, DataInputPlus in, int userVersion) throws IOException
        {
            switch (field)
            {
                case EXECUTE_AT:
                case EXECUTES_AT_LEAST:
                    CommandSerializers.timestamp.skip(in, userVersion);
                    break;
                case SAVE_STATUS:
                    in.readShort();
                    break;
                case DURABILITY:
                    in.readByte();
                    break;
                case ACCEPTED:
                case PROMISED:
                    CommandSerializers.ballot.skip(in, userVersion);
                    break;
                case PARTICIPANTS:
                    // TODO (expected): skip
                    CommandSerializers.participants.deserialize(in, userVersion);
                    break;
                case PARTIAL_TXN:
                    CommandSerializers.partialTxn.deserialize(in, userVersion);
                    break;
                case PARTIAL_DEPS:
                    // TODO (expected): skip
                    DepsSerializers.partialDeps.deserialize(in, userVersion);
                    break;
                case WAITING_ON:
                    int size = in.readInt();
                    in.skipBytesFully(size);
                    break;
                case WRITES:
                    // TODO (expected): skip
                    CommandSerializers.writes.deserialize(in, userVersion);
                    break;
                case CLEANUP:
                    in.readByte();
                    break;
                case RESULT:
                    // TODO (expected): skip
                    result = ResultSerializers.result.deserialize(in, userVersion);
                    break;
            }
        }
    }
}
