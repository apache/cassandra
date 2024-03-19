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

package org.apache.cassandra.service.accord.async;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.impl.SafeCommandsForKey;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCachingState;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordSafeCommand;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;

import static accord.local.KeyHistory.COMMANDS;
import static accord.local.PreLoadContext.contextFor;
import static accord.utils.Property.qt;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.keys;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;
import static org.apache.cassandra.service.accord.async.AsyncLoader.txnIds;

public class AsyncOperationTest
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncOperationTest.class);
    private static final AtomicLong clock = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void before()
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.ACCORD_KEYSPACE_NAME, AccordKeyspace.COMMANDS));
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.ACCORD_KEYSPACE_NAME, AccordKeyspace.TIMESTAMPS_FOR_KEY));
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.ACCORD_KEYSPACE_NAME, AccordKeyspace.COMMANDS_FOR_KEY));
    }

    /**
     * Commands which were not previously on disk and were only accessed via `ifPresent`, and therefore,
     * not initialized, should not be saved at the end of the operation
     */
    @Test
    public void optionalCommandTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = AccordTestUtils.createWriteTxn((int)clock.incrementAndGet());
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        getUninterruptibly(commandStore.execute(contextFor(txnId), instance -> {
            // TODO review: This change to `ifInitialized` was done in a lot of places and it doesn't preserve this property
            // I fixed this reference to point to `ifLoadedAndInitialised` and but didn't update other places
            SafeCommand command = instance.ifLoadedAndInitialised(txnId);
            Assert.assertNull(command);
        }));

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void optionalCommandsForKeyTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Txn txn = AccordTestUtils.createWriteTxn((int)clock.incrementAndGet());
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        getUninterruptibly(commandStore.execute(contextFor(key), instance -> {
            SafeCommandsForKey cfk = ((AccordSafeCommandStore) instance).maybeCommandsForKey(key);
            Assert.assertNull(cfk);
        }));

        long nowInSeconds = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = AccordKeyspace.getCommandsForKeyRead(commandStore.id(), key, (int) nowInSeconds);
        try(ReadExecutionController controller = command.executionController();
            FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            Assert.assertFalse(partitions.hasNext());
        }
    }

    private static Command createStableAndPersist(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        Command command = AccordTestUtils.Commands.stable(txnId, createPartialTxn(0), executeAt);
        AccordSafeCommand safeCommand = new AccordSafeCommand(loaded(txnId, null));
        safeCommand.set(command);

        AccordKeyspace.getCommandMutation(commandStore, safeCommand, commandStore.nextSystemTimestampMicros()).apply();
        Commit commit =
            Commit.SerializerSupport.create(txnId,
                                            command.route().slice(AccordTestUtils.fullRange(command.partialTxn().keys())),
                                            txnId.epoch(),
                                            Commit.Kind.StableWithTxnAndDeps,
                                            Ballot.ZERO,
                                            executeAt,
                                            command.partialTxn().keys(),
                                            command.partialTxn(),
                                            command.partialDeps(),
                                            Route.castToFullRoute(command.route()),
                                            null);
        commandStore.appendToJournal(commit);

        return command;
    }

    private static Command createStableAndPersist(AccordCommandStore commandStore, TxnId txnId)
    {
        return createStableAndPersist(commandStore, txnId, txnId);
    }

    private static Command createStableUsingFastLifeCycle(AccordCommandStore commandStore, TxnId txnId)
    {
        return createStableUsingFastLifeCycle(commandStore, txnId, txnId);
    }

    private static Command createStableUsingFastLifeCycle(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        PartialTxn partialTxn = createPartialTxn(0);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Ranges ranges = AccordTestUtils.fullRange(partialTxn.keys());
        PartialRoute<?> partialRoute = route.slice(ranges);
        PartialDeps deps = PartialDeps.builder(ranges).build();

        // create and write messages to the journal for loading to succeed
        PreAccept preAccept =
            PreAccept.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), txnId.epoch(), false, txnId.epoch(), partialTxn, route);
        Commit stable =
            Commit.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), Commit.Kind.StableFastPath, Ballot.ZERO, executeAt, partialTxn.keys(), partialTxn, deps, route, null);

        commandStore.appendToJournal(preAccept);
        commandStore.appendToJournal(stable);

        try
        {
            Command command = getUninterruptibly(commandStore.submit(contextFor(txnId, partialTxn.keys(), COMMANDS), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, null);
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, null, partialTxn, executeAt, deps);
                return safe.ifInitialised(txnId).current();
            }).beginAsResult());

            // clear cache
            commandStore.executeBlocking(() -> {
                long cacheSize = commandStore.capacity();
                commandStore.setCapacity(0);
                commandStore.setCapacity(cacheSize);
                commandStore.cache().awaitSaveResults();
            });

            return command;
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Command createStableUsingSlowLifeCycle(AccordCommandStore commandStore, TxnId txnId)
    {
        return createStableUsingSlowLifeCycle(commandStore, txnId, txnId);
    }

    private static Command createStableUsingSlowLifeCycle(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        PartialTxn partialTxn = createPartialTxn(0);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Ranges ranges = AccordTestUtils.fullRange(partialTxn.keys());
        PartialRoute<?> partialRoute = route.slice(ranges);
        PartialDeps deps = PartialDeps.builder(ranges).build();

        // create and write messages to the journal for loading to succeed
        PreAccept preAccept =
            PreAccept.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), txnId.epoch(), false, txnId.epoch(), partialTxn, route);
        Accept accept =
            Accept.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), txnId.epoch(), false, Ballot.ZERO, executeAt, partialTxn.keys(), deps);
        Commit commit =
            Commit.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), Commit.Kind.Commit, Ballot.ZERO, executeAt, partialTxn.keys(), partialTxn, deps, route, null);
        Commit stable =
            Commit.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), Commit.Kind.StableSlowPath, Ballot.ZERO, executeAt, partialTxn.keys(), partialTxn, deps, route, null);

        commandStore.appendToJournal(preAccept);
        commandStore.appendToJournal(accept);
        commandStore.appendToJournal(commit);
        commandStore.appendToJournal(stable);

        try
        {
            Command command = getUninterruptibly(commandStore.submit(contextFor(txnId, partialTxn.keys(), COMMANDS), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, null);
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, partialTxn.keys(), null, executeAt, deps);
                CheckedCommands.commit(safe, SaveStatus.Committed, Ballot.ZERO, txnId, route, null, partialTxn, executeAt, deps);
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, null, partialTxn, executeAt, deps);
                return safe.ifInitialised(txnId).current();
            }).beginAsResult());

            // clear cache
            commandStore.executeBlocking(() -> {
                long cacheSize = commandStore.capacity();
                commandStore.setCapacity(0);
                commandStore.setCapacity(cacheSize);
                commandStore.cache().awaitSaveResults();
            });

            return command;
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    private static void assertFutureState(AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> cache, TxnId txnId, boolean referenceExpected, boolean expectLoadFuture, boolean expectSaveFuture)
    {
        if (cache.isReferenced(txnId) != referenceExpected)
            throw new AssertionError(referenceExpected ? "Cache reference unexpectedly not found for " + txnId
                                                       : "Unexpectedly found cache reference for " + txnId);
        cache.complete(txnId);
        if (cache.hasLoadResult(txnId) != expectLoadFuture)
            throw new AssertionError(expectLoadFuture ? "Load future unexpectedly not found for " + txnId
                                                      : "Unexpectedly found load future for " + txnId);
        if (cache.hasSaveResult(txnId) != expectSaveFuture)
            throw new AssertionError(expectSaveFuture ? "Save future unexpectedly not found for " + txnId
                                                      : "Unexpectedly found save future for " + txnId);

    }

    /**
     * save and load futures should be cleaned up as part of the operation
     */
    @Test
    public void testFutureCleanup() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        createStableAndPersist(commandStore, txnId);

        Consumer<SafeCommandStore> consumer = safeStore -> safeStore.ifInitialised(txnId).readyToExecute(safeStore);
        PreLoadContext ctx = contextFor(txnId);
        AsyncOperation<Void> operation = new AsyncOperation.ForConsumer(commandStore, ctx, consumer)
        {

            private AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> cache()
            {
                return commandStore.commandCache();
            }

            @Override
            AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
            {
                return new AsyncLoader(commandStore, txnIds(preLoadContext), preLoadContext.keys(), preLoadContext.keyHistory())
                {
                    @Override
                    void state(State state)
                    {
                        switch (state)
                        {
                            case SETUP:
                                assertFutureState(cache(), txnId, false, false, false);
                                break;
                            case FINISHED:
                                assertFutureState(cache(), txnId, true, false, false);
                                break;
                            case LOADING:
                                assertFutureState(cache(), txnId, true, true, false);
                                break;
                        }
                        super.state(state);
                    }
                };
            }
        };

        commandStore.executor().submit(operation);

        getUninterruptibly(operation);
    }

    @Test
    public void loadFail()
    {
        AtomicLong clock = new AtomicLong(0);
        // all txn use the same key; 0
        Keys keys = keys(Schema.instance.getTableMetadata("ks", "tbl"), 0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        commandStore.executeBlocking(() -> commandStore.setCapacity(0));
        Gen<TxnId> txnIdGen = rs -> txnId(1, clock.incrementAndGet(), 1);

        qt().withPure(false).withExamples(50).forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 10)).check((rs, ids) -> {
            before(); // truncate tables

            createCommand(commandStore, rs, ids);

            Map<TxnId, Boolean> failed = selectFailedTxn(rs, ids);

            assertNoReferences(commandStore, ids, keys);

            PreLoadContext ctx = contextFor(null, ids, keys, COMMANDS);

            Consumer<SafeCommandStore> consumer = Mockito.mock(Consumer.class);

            commandStore.commandCache().unsafeSetLoadFunction(txnId ->
            {
                logger.info("Attempting to load {}; expected to fail? {}", txnId, failed.get(txnId));
                if (!failed.get(txnId)) return AccordKeyspace.loadCommand(commandStore, txnId);
                throw new NullPointerException("txn_id " + txnId);
            });
            AsyncOperation<Void> o1 = new AsyncOperation.ForConsumer(commandStore, ctx, consumer);

            AssertionUtils.assertThatThrownBy(() -> getUninterruptibly(o1))
                      .hasRootCause()
                      .isInstanceOf(NullPointerException.class)
                      .hasNoSuppressedExceptions();

            Mockito.verifyNoInteractions(consumer);

            assertNoReferences(commandStore, ids, keys);
            // the first failed load causes the whole operation to fail, so some ids may still be pending
            // to make sure the next operation does not see a PENDING that will fail, wait for all loads to complete
            awaitDone(commandStore, ids, keys);

            // can we recover?
            commandStore.commandCache().unsafeSetLoadFunction(txnId -> AccordKeyspace.loadCommand(commandStore, txnId));
            AsyncOperation.ForConsumer o2 = new AsyncOperation.ForConsumer(commandStore, ctx, store -> ids.forEach(id -> store.ifInitialised(id).readyToExecute(store)));
            getUninterruptibly(o2);
        });
    }

    @Test
    public void consumerFails()
    {
        AtomicLong clock = new AtomicLong(0);
        // all txn use the same key; 0
        Keys keys = keys(Schema.instance.getTableMetadata("ks", "tbl"), 0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Gen<TxnId> txnIdGen = rs -> txnId(1, clock.incrementAndGet(), 1);

        AtomicInteger counter = new AtomicInteger();
        qt().withPure(false).withSeed(3131884991952253478L).withExamples(100).forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 10)).check((rs, ids) -> {
            logger.info("Test #{}", counter.incrementAndGet());
            before(); // truncate tables

            createCommand(commandStore, rs, ids);
            assertNoReferences(commandStore, ids, keys);

            PreLoadContext ctx = contextFor(null, ids, keys, COMMANDS);

            Consumer<SafeCommandStore> consumer = Mockito.mock(Consumer.class);
            String errorMsg = "txn_ids " + ids;
            Mockito.doThrow(new NullPointerException(errorMsg)).when(consumer).accept(Mockito.any());

            AsyncOperation<Void> operation = new AsyncOperation.ForConsumer(commandStore, ctx, consumer);

            AssertionUtils.assertThatThrownBy(() -> getUninterruptibly(operation))
                          .hasRootCause()
                          .isInstanceOf(NullPointerException.class)
                          .hasMessage(errorMsg)
                          .hasNoSuppressedExceptions();

            assertNoReferences(commandStore, ids, keys);
        });
    }

    private static void createCommand(AccordCommandStore commandStore, RandomSource rs, List<TxnId> ids)
    {
        // to simulate CommandsForKey not being found, use createCommittedAndPersist periodically as it does not update
        switch (rs.nextInt(3))
        {
            case 0: ids.forEach(id -> createStableAndPersist(commandStore, id)); break;
            case 1: ids.forEach(id -> createStableUsingFastLifeCycle(commandStore, id)); break;
            case 2: ids.forEach(id -> createStableUsingSlowLifeCycle(commandStore, id));
        }
        commandStore.unsafeClearCache();
    }

    private static Map<TxnId, Boolean> selectFailedTxn(RandomSource rs, List<TxnId> ids)
    {
        Map<TxnId, Boolean> failed = Maps.newHashMapWithExpectedSize(ids.size());
        for (TxnId id : ids)
            failed.put(id, rs.nextBoolean());
        if (failed.values().stream().allMatch(b -> b == Boolean.FALSE))
            failed.put(ids.get(0), Boolean.TRUE);
        return failed;
    }

    private static void assertNoReferences(AccordCommandStore commandStore, List<TxnId> ids, Keys keys)
    {
        AssertionError error = null;
        try
        {
            assertNoReferences(commandStore.commandCache(), ids);
        }
        catch (AssertionError e)
        {
            error = e;
        }
        try
        {
            //TODO this is due to bad typing for Instance, it doesn't use ? extends RoutableKey
            assertNoReferences(commandStore.commandsForKeyCache(), (Iterable<RoutableKey>) (Iterable<?>) keys);
        }
        catch (AssertionError e)
        {
            if (error == null) error = e;
            else error.addSuppressed(e);
        }
        if (error != null) throw error;
    }

    private static <T> void assertNoReferences(AccordStateCache.Instance<T, ?, ?> cache, Iterable<T> keys)
    {
        AssertionError error = null;
        for (T key : keys)
        {
            AccordCachingState<T, ?> node = cache.getUnsafe(key);
            if (node == null) continue;
            try
            {
                Assertions.assertThat(node.referenceCount())
                          .describedAs("Key %s found referenced in cache", key)
                          .isEqualTo(0);
            }
            catch (AssertionError e)
            {
                if (error == null)
                {
                    error = e;
                }
                else
                {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null) throw error;
    }

    private static void awaitDone(AccordCommandStore commandStore, List<TxnId> ids, Keys keys)
    {
        awaitDone(commandStore.commandCache(), ids);
        //TODO this is due to bad typing for Instance, it doesn't use ? extends RoutableKey
        awaitDone(commandStore.commandsForKeyCache(), (Iterable<RoutableKey>) (Iterable<?>) keys);
    }

    private static <T> void awaitDone(AccordStateCache.Instance<T, ?, ?> cache, Iterable<T> keys)
    {
        for (T key : keys)
        {
            AccordCachingState<T, ?> node = cache.getUnsafe(key);
            if (node == null) continue;
            Awaitility.await("For node " + node.key() + " to complete")
            .atMost(Duration.ofMinutes(1))
            .until(node::isComplete);
        }
    }
}
