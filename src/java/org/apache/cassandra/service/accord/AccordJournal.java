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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.ErasedSafeCommand;
import accord.impl.TimestampsForKey;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Compactor;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.IdentityAccumulator;
import org.apache.cassandra.service.accord.JournalKey.JournalKeySupport;
import org.apache.cassandra.utils.ExecutorUtils;

import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.Status.Truncated;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.RedundantBeforeAccumulator;

public class AccordJournal implements IJournal, Shutdownable
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
    Node node;

    enum Status { INITIALIZED, STARTING, REPLAY, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal(Params params)
    {
        File directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
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
                                     new AccordSegmentCompactor<>(params.userVersion()));
        this.journalTable = new AccordJournalTable<>(journal, JournalKey.SUPPORT, params.userVersion());
        this.params = params;
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
        SavedCommand.Builder builder = loadDiffs(commandStoreId, txnId);
        Cleanup cleanup = builder.shouldCleanup(redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return ErasedSafeCommand.erased(txnId, ErasedOrVestigial);
        }
        return builder.construct();
    }

    @Override
    public SavedCommand.MinimalCommand loadMinimal(int commandStoreId, TxnId txnId, SavedCommand.Load load, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        SavedCommand.Builder builder = loadDiffs(commandStoreId, txnId, load);
        Cleanup cleanup = builder.shouldCleanup(redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return null;
        }
        return builder.asMinimal();
    }

    @VisibleForTesting
    public RedundantBefore loadRedundantBefore(int store)
    {
        RedundantBeforeAccumulator accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, store));
        return accumulator.get();
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int store)
    {
        IdentityAccumulator<NavigableMap<TxnId, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, store));
        return accumulator.get();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int store)
    {
        IdentityAccumulator<NavigableMap<Timestamp, Ranges>> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, store));
        return accumulator.get();
    }

    @Override
    public CommandStores.RangesForEpoch.Snapshot loadRangesForEpoch(int store)
    {
        IdentityAccumulator<RangesForEpoch.Snapshot> accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, store));
        return accumulator.get();
    }

    @Override
    public void appendCommand(int store, SavedCommand.DiffWriter value, Runnable onFlush)
    {
        if (value == null || status == Status.REPLAY)
        {
            if (onFlush != null)
                onFlush.run();
            return;
        }

        // TODO: use same API for commands as for the other states?
        JournalKey key = new JournalKey(value.key(), JournalKey.Type.COMMAND_DIFF, store);
        RecordPointer pointer = journal.asyncWrite(key, value, SENTINEL_HOSTS);
        if (onFlush != null)
            journal.onFlush(pointer, onFlush);
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return new PersistentField.Persister<>()
        {
            @Override
            public AsyncResult<?> persist(DurableBefore addDurableBefore, DurableBefore newDurableBefore)
            {
                if (status == Status.REPLAY)
                    return AsyncResults.success(null);

                AsyncResult.Settable<Void> result = AsyncResults.settable();
                JournalKey key = new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0);
                RecordPointer pointer = appendInternal(key, addDurableBefore);
                // TODO (required): what happens on failure?
                journal.onFlush(pointer, () -> result.setSuccess(null));
                return result;
            }

            @Override
            public DurableBefore load()
            {
                DurableBeforeAccumulator accumulator = readAll(new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0));
                return accumulator.get();
            }
        };
    }

    @Override
    public void persistStoreState(int store, AccordSafeCommandStore.FieldUpdates fieldUpdates, Runnable onFlush)
    {
        RecordPointer pointer = null;
        // TODO: avoid allocating keys
        if (fieldUpdates.addRedundantBefore != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.REDUNDANT_BEFORE, store), fieldUpdates.addRedundantBefore);
        if (fieldUpdates.newBootstrapBeganAt != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.BOOTSTRAP_BEGAN_AT, store), fieldUpdates.newBootstrapBeganAt);
        if (fieldUpdates.newSafeToRead != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.SAFE_TO_READ, store), fieldUpdates.newSafeToRead);
        if (fieldUpdates.newRangesForEpoch != null)
            pointer = appendInternal(new JournalKey(TxnId.NONE, JournalKey.Type.RANGES_FOR_EPOCH, store), fieldUpdates.newRangesForEpoch);

        if (onFlush == null)
            return;

        if (pointer != null)
            journal.onFlush(pointer, onFlush);
        else
            onFlush.run();
    }

    @VisibleForTesting
    public SavedCommand.Builder loadDiffs(int commandStoreId, TxnId txnId, SavedCommand.Load load)
    {
        JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStoreId);
        SavedCommand.Builder builder = new SavedCommand.Builder(txnId, load);
        journalTable.readAll(key, builder::deserializeNext);
        return builder;
    }

    public SavedCommand.Builder loadDiffs(int commandStoreId, TxnId txnId)
    {
        return loadDiffs(commandStoreId, txnId, SavedCommand.Load.ALL);
    }

    private <BUILDER> BUILDER readAll(JournalKey key)
    {
        BUILDER builder = (BUILDER) key.type.serializer.mergerFor(key);
        // TODO: this can be further improved to avoid allocating lambdas
        AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER> serializer = (AccordJournalValueSerializers.FlyweightSerializer<?, BUILDER>) key.type.serializer;
        // TODO (expected): for those where we store an image, read only the first entry we find in DESC order
        journalTable.readAll(key, (in, userVersion) -> serializer.deserialize(key, builder, in, userVersion));
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

    public void sanityCheck(int commandStoreId, Command orig)
    {
        SavedCommand.Builder diffs = loadDiffs(commandStoreId, orig.txnId());
        diffs.forceResult(orig.result());
        // We can only use strict equality if we supply result.
        Command reconstructed = diffs.construct();
        Invariants.checkState(orig.equals(reconstructed),
                              '\n' +
                              "Original:      %s\n" +
                              "Reconstructed: %s\n" +
                              "Diffs:         %s", orig, reconstructed, diffs);
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        journal.truncateForTesting();
    }

    @VisibleForTesting
    public void runCompactorForTesting()
    {
        journal.runCompactorForTesting();
    }

    public void replay()
    {
        logger.info("Starting journal replay.");
        TimestampsForKey.unsafeSetReplay(true);
        CommandsForKey.disableLinearizabilityViolationsReporting();
        AccordKeyspace.truncateAllCaches();

        try (AccordJournalTable.KeyOrderIterator<JournalKey> iter = journalTable.readAll())
        {
            JournalKey key;
            SavedCommand.Builder builder = new SavedCommand.Builder();
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
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                });

                if (builder.nextCalled)
                {
                    Command command = builder.construct();
                    AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().forId(key.commandStoreId);
                    AccordCommandStore.Loader loader = commandStore.loader();
                    loader.load(command).get();
                    if (command.saveStatus().compareTo(SaveStatus.Stable) >= 0 && !command.hasBeen(Truncated))
                        loader.apply(command).get();
                }
            }

            logger.info("Waiting for command stores to quiesce.");
            ((AccordCommandStores)node.commandStores()).waitForQuiescense();
            CommandsForKey.enableLinearizabilityViolationsReporting();
            TimestampsForKey.unsafeSetReplay(false);
            logger.info("Finished journal replay.");
            status = Status.STARTED;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not replay journal.", t);
        }
    }

    // TODO: this is here temporarily; for debugging purposes
    @VisibleForTesting
    public void checkAllCommands()
    {
        try (AccordJournalTable.KeyOrderIterator<JournalKey> iter = journalTable.readAll())
        {
            IAccordService.CompactionInfo compactionInfo = AccordService.instance().getCompactionInfo();
            JournalKey key;
            SavedCommand.Builder builder = new SavedCommand.Builder();
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
                List<RecordPointer> pointers = new ArrayList<>();
                try
                {
                    iter.readAllForKey(key, (segment, position, local, buffer, hosts, userVersion) -> {
                        pointers.add(new RecordPointer(segment, position));
                        Invariants.checkState(finalKey.equals(local));
                        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                        {
                            builder.deserializeNext(in, userVersion);
                        }
                        catch (IOException e)
                        {
                            // can only throw if serializer is buggy
                            throw new RuntimeException(e);
                        }
                    });

                    Cleanup cleanup = builder.shouldCleanup(compactionInfo.redundantBefores.get(key.commandStoreId), compactionInfo.durableBefores.get(key.commandStoreId));
                    switch (cleanup)
                    {
                        case ERASE:
                        case EXPUNGE:
                        case EXPUNGE_PARTIAL:
                        case VESTIGIAL:
                            continue;
                    }
                    builder.construct();
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(String.format("Caught an exception after iterating over: %s", pointers),
                                               t);
                }
            }
        }
    }

    public void unsafeSetStarted()
    {
        status = Status.STARTED;
    }
}