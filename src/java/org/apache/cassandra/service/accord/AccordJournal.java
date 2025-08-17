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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import accord.primitives.PartialTxn;
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
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.journal.Compactor;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.FlyweightImage;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.IdentityAccumulator;
import org.apache.cassandra.service.accord.JournalKey.JournalKeySupport;
import org.apache.cassandra.service.accord.journal.AccordTopologyUpdate;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ExecutorUtils;
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
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;
import static org.apache.cassandra.service.accord.JournalKey.Type.COMMAND_DIFF;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public class AccordJournal implements accord.api.Journal, RangeSearcher.Supplier, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);
    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[JournalKeySupport.TOTAL_SIZE]);

    @VisibleForTesting
    protected final Journal<JournalKey, Object> journal;
    @VisibleForTesting
    protected final AccordJournalTable<JournalKey, Object> journalTable;
    private final Params params;
    Node node;

    enum Status { INITIALIZED, STARTING, REPLAY, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    public AccordJournal(Params params)
    {
        this(params, new File(DatabaseDescriptor.getAccordJournalDirectory()), Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
    }

    @VisibleForTesting
    public AccordJournal(Params params, File directory, ColumnFamilyStore cfs)
    {
        Version userVersion = Version.fromVersion(params.userVersion());
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
                                     compactor(cfs, userVersion));
        this.journalTable = new AccordJournalTable<>(journal, JournalKey.SUPPORT, cfs, userVersion);
        this.params = params;
    }

    protected SegmentCompactor<JournalKey, Object> compactor(ColumnFamilyStore cfs, Version userVersion)
    {
        return new AccordSegmentCompactor<>(userVersion, cfs) {
            @Nullable
            @Override
            public Collection<StaticSegment<JournalKey, Object>> compact(Collection<StaticSegment<JournalKey, Object>> staticSegments)
            {
                if (journalTable == null)
                    throw new IllegalStateException("Unsafe access to AccordJournal during <init>; journalTable was touched before it was published");
                Collection<StaticSegment<JournalKey, Object>> result = super.compact(staticSegments);
                journalTable.safeNotify(index -> index.remove(staticSegments));
                return result;
            }
        };
    }

    @VisibleForTesting
    public int inMemorySize()
    {
        return journal.currentActiveSegment().index().size();
    }

    public void start(Node node)
    {
        Invariants.require(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        journalTable.start();
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
        Invariants.require(status == Status.REPLAY || status == Status.STARTED, "%s", status);
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
        builder.maybeCleanup(true, FULL, redundantBefore, durableBefore);
        return builder.construct(redundantBefore);
    }

    public static class DebugEntry implements Supplier<CommandChange.Builder>
    {
        public final long segment;
        public final int position;
        public final Builder builder;

        public DebugEntry(long segment, int position, Builder builder)
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
        journalTable.readAll(key, (long segment, int position, JournalKey k, ByteBuffer buffer, int userVersion) -> {
            Builder builder = new Builder(txnId);
            new AccordJournalTable.RecordConsumerAdapter<>(builder::deserializeNext).accept(segment, position, k, buffer, userVersion);
            result.add(new DebugEntry(segment, position, builder));
        });
        return result;
    }

    // applies cleanup and returns null if no command should be returned
    public static Builder cleanupAndFilter(Builder builder, RedundantBefore redundantBefore, DurableBefore durableBefore)
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
        Builder builder = cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimal();
    }

    @Override
    public Command.MinimalWithDeps loadMinimalWithDeps(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Builder builder = cleanupAndFilter(loadDiffs(commandStoreId, txnId, MINIMAL_WITH_DEPS), redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimalWithDeps();
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        IdentityAccumulator<RedundantBefore> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, commandStoreId));
        return accumulator.get();
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        IdentityAccumulator<NavigableMap<TxnId, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, commandStoreId));
        return accumulator.get();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        IdentityAccumulator<NavigableMap<Timestamp, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, commandStoreId));
        return accumulator.get();
    }

    @Override
    public CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId)
    {
        IdentityAccumulator<RangesForEpoch> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, commandStoreId));
        return accumulator.get();
    }

    @Override
    public void saveCommand(int commandStoreId, CommandUpdate update, @Nullable Runnable onFlush)
    {
        Writer diff = Writer.make(update.before, update.after);
        if (diff == null)
        {
            if (onFlush != null)
                onFlush.run();
            return;
        }

        JournalKey key = new JournalKey(update.txnId, COMMAND_DIFF, commandStoreId);
        RecordPointer pointer = journal.asyncWrite(key, diff);
        if (journalTable.shouldIndex(key)
            && diff.hasParticipants()
            && diff.after.route() != null)
            journal.onDurable(pointer, () ->
                                       journalTable.safeNotify(index ->
                                                               index.update(pointer.segment, key.commandStoreId, key.id, diff.after.route())));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    public void patchCommand(int commandStoreId, TxnId txnId, Cleanup cleanup, @Nullable Runnable onFlush)
    {
        Builder change = new Builder(txnId);
        change.maybeCleanup(false, cleanup);

        JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStoreId);
        RecordPointer pointer = journal.asyncWrite(key, (out, userVersion) -> change.serialize(out, Version.fromVersion(configuration().userVersion())));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
    }

    @Override
    public Iterator<AccordTopologyUpdate.ImmutableTopoloyImage> replayTopologies()
    {
        AccordTopologyUpdate.Accumulator accumulator = readAll(TopologyUpdateKey);
        return accumulator.images();
    }

    private static final JournalKey TopologyUpdateKey = new JournalKey(TxnId.NONE, JournalKey.Type.TOPOLOGY_UPDATE, 0);
    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        RecordPointer pointer = appendInternal(TopologyUpdateKey, AccordTopologyUpdate.newTopology(topologyUpdate));
        if (onFlush != null)
            journal.onDurable(pointer, onFlush);
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
        // TODO: avoid allocating keys
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

    private Builder loadDiffs(int commandStoreId, TxnId txnId, Load load)
    {
        JournalKey key = new JournalKey(txnId, COMMAND_DIFF, commandStoreId);
        Builder builder = new Builder(txnId, load);
        journalTable.readAll(key, builder::deserializeNext);
        return builder;
    }

    @VisibleForTesting
    public Builder load(int commandStoreId, TxnId txnId)
    {
        return loadDiffs(commandStoreId, txnId, Load.ALL);
    }

    public <BUILDER extends FlyweightImage> BUILDER readAll(JournalKey key)
    {
        BUILDER builder = (BUILDER) key.type.serializer.mergerFor();
        // TODO (expected): this can be further improved to avoid allocating lambdas
        AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER> serializer = (AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER>) key.type.serializer;
        // TODO (expected): for those where we store an image, read only the first entry we find in DESC order
        journalTable.readAll(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion));
        return builder;
    }

    public void forEachEntry(JournalKey key, AccordJournalTable.Reader reader)
    {
        journalTable.readAll(key, reader);
    }

    private <T> RecordPointer appendInternal(JournalKey key, T write)
    {
        AccordJournalValueSerializers.FlyweightSerializer<T, ?> serializer = (AccordJournalValueSerializers.FlyweightSerializer<T, ?>) key.type.serializer;
        return journal.asyncWrite(key, (out, userVersion) -> serializer.serialize(key, write, out, Version.fromVersion(userVersion)));
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
        journalTable.safeNotify(RouteInMemoryIndex::truncateForTesting);
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
        journalTable.forceCompaction();
    }

    public void forEach(Consumer<JournalKey> consumer)
    {
        try (CloseableIterator<Journal.KeyRefs<JournalKey>> iter = journalTable.keyIterator(null, null))
        {
            while (iter.hasNext())
            {
                Journal.KeyRefs<JournalKey> ref = iter.next();
                consumer.accept(ref.key());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void replay(CommandStores commandStores)
    {
        // TODO (expected): make the parallelisms configurable
        // Replay is performed in parallel, where at most X commands can be in flight, accross at most Y commands stores.
        // That is, you can limit replay parallelism to 1 command store at a time, but load multiple commands within that data store,
        // _or_ have multiple commands being loaded accross multiple data stores.
        final Semaphore commandParallelism = Semaphore.newSemaphore(getAvailableProcessors());
        final int commandStoreParallelism = Math.max(Math.max(1, Math.min(getAvailableProcessors(), 4)), getAvailableProcessors() / 4);
        final AtomicBoolean abort = new AtomicBoolean();
        // TODO (expected): balance work submission by AccordExecutor
        final IntArrayList activeCommandStoreIds = new IntArrayList();
        final ReplayQueue pendingCommandStores = new ReplayQueue(commandStores.all());

        class ReplayStream implements Closeable
        {
            final CommandStore commandStore;
            final Replayer replayer;
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter;
            JournalKey prev;

            public ReplayStream(CommandStore commandStore)
            {
                this.commandStore = commandStore;
                this.replayer = commandStore.replayer();
                // Keys in the index are sorted by command store id, so index iteration will be sequential
                this.iter = journalTable.keyIterator(new JournalKey(TxnId.NONE, COMMAND_DIFF, commandStore.id()), new JournalKey(TxnId.MAX.withoutNonIdentityFlags(), COMMAND_DIFF, commandStore.id()));
            }

            boolean replay()
            {
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
                    segments = journalTable.shouldIndex(key) ? ref.copyOfSegments() : null;
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
                                  journalTable.safeNotify(index -> index.update(segment, key.commandStoreId, txnId, (Route)route));
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
                        replayStreams.put(id, new ReplayStream(next));
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
                        replayStreams.put(id, new ReplayStream(next));
                    }

                    replayStream = replayStreams.get(id);
                }

                ++cur;
            }
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

    @VisibleForTesting
    public Journal<JournalKey, Object> unsafeGetJournal()
    {
        return journal;
    }

    @Override
    public RangeSearcher rangeSearcher()
    {
        return journalTable.rangeSearcher();
    }

    public static class Writer implements Journal.Writer
    {
        private final Command after;
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

    public static class Builder extends CommandChange.Builder implements FlyweightImage
    {
        private final boolean deserializeDeps;

        public Builder()
        {
            this(Load.ALL);
        }

        public Builder(Load load)
        {
            this(null, load);
        }

        public Builder(TxnId txnId)
        {
            this(txnId, Load.ALL);
        }

        public Builder(TxnId txnId, Load load)
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
                        if (partialTxn instanceof ByteBuffer) out.write(((ByteBuffer) partialTxn).duplicate());
                        else CommandSerializers.partialTxn.serialize((PartialTxn) partialTxn, out, userVersion);
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
                if (isChanged(field, flags) && field != CLEANUP)
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
                    // TODO (required): this optimisation will be easily disabled;
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
}
