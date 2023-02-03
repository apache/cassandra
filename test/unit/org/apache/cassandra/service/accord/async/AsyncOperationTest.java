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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import accord.local.Command;
import accord.local.Commands;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordSafeCommand;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;

import static accord.local.PreLoadContext.contextFor;
import static accord.utils.Property.qt;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.keys;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

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
        QueryProcessor.executeInternal("TRUNCATE system_accord.commands");
        QueryProcessor.executeInternal("TRUNCATE system_accord.commands_for_key");
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
        Txn txn = createTxn((int)clock.incrementAndGet());
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        getUninterruptibly(commandStore.execute(contextFor(txnId), instance -> {
            SafeCommand command = instance.ifPresent(txnId);
            Assert.assertNull(command);
        }));

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void optionalCommandsForKeyTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Txn txn = createTxn((int)clock.incrementAndGet());
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        getUninterruptibly(commandStore.execute(contextFor(Collections.emptyList(), Keys.of(key)), instance -> {
            SafeCommandsForKey cfk = ((AccordSafeCommandStore) instance).maybeCommandsForKey(key);
            Assert.assertNull(cfk);
        }));

        int nowInSeconds = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = AccordKeyspace.getCommandsForKeyRead(commandStore, key, nowInSeconds);
        try(ReadExecutionController controller = command.executionController();
            FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            Assert.assertFalse(partitions.hasNext());
        }
    }

    private static Command createCommittedAndPersist(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        Command command = AccordTestUtils.Commands.committed(txnId, createPartialTxn(0), executeAt);
        AccordSafeCommand safeCommand = new AccordSafeCommand(loaded(txnId, null));
        safeCommand.set(command);
        AccordKeyspace.getCommandMutation(commandStore, safeCommand, commandStore.nextSystemTimestampMicros()).apply();
        return command;
    }

    private static Command createCommittedAndPersist(AccordCommandStore commandStore, TxnId txnId)
    {
        return createCommittedAndPersist(commandStore, txnId, txnId);
    }

    private static Command createCommittedUsingLifeCycle(AccordCommandStore commandStore, TxnId txnId)
    {
        return createCommittedUsingLifeCycle(commandStore, txnId, txnId);
    }

    private static Command createCommittedUsingLifeCycle(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        PartialTxn partialTxn = createPartialTxn(0);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Ranges ranges = AccordTestUtils.fullRange(partialTxn.keys());
        PartialRoute<?> partialRoute = route.slice(ranges);
        PartialDeps deps = PartialDeps.builder(ranges).build();
        try
        {
            return getUninterruptibly(commandStore.submit(PreLoadContext.contextFor(Collections.singleton(txnId), partialTxn.keys()), safe -> {
                Commands.AcceptOutcome result = Commands.preaccept(safe, txnId, partialTxn, route, null);
                if (result != Commands.AcceptOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);

                result = Commands.accept(safe, txnId, Ballot.ZERO, partialRoute, partialTxn.keys(), null, executeAt, deps);
                if (result != Commands.AcceptOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);

                Commands.CommitOutcome commit = Commands.commit(safe, txnId, route, null, partialTxn, executeAt, deps);
                if (commit != Commands.CommitOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);

                // clear cache
                long cacheSize = commandStore.getCacheSize();
                commandStore.setCacheSize(0);
                commandStore.setCacheSize(cacheSize);

                return safe.command(txnId).current();
            }).beginAsResult());
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
        cache.cleanupLoadResult(txnId);
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

        createCommittedAndPersist(commandStore, txnId);

        Consumer<SafeCommandStore> consumer = safeStore -> safeStore.command(txnId).readyToExecute();
        PreLoadContext ctx = PreLoadContext.contextFor(singleton(txnId), Keys.EMPTY);
        AsyncOperation<Void> operation = new AsyncOperation.ForConsumer(commandStore, ctx, consumer)
        {

            private AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> cache()
            {
                return commandStore.commandCache();
            }

            @Override
            AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
            {
                return new AsyncLoader(commandStore, preLoadContext.txnIds(), (Iterable<RoutableKey>) preLoadContext.keys()) {

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

            @Override
            AsyncWriter createAsyncWriter(AccordCommandStore commandStore)
            {
                return new AsyncWriter(commandStore) {

                    @Override
                    void setState(State state)
                    {
                        switch (state)
                        {
                            case SETUP:
                                assertFutureState(cache(), txnId, true, false, false);
                                break;
                            case FINISHED:
                                assertFutureState(cache(), txnId, false, false, false);
                                break;
                            case SAVING:
                                assertFutureState(cache(), txnId, true, false, true);
                                break;

                        }
                        super.setState(state);
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
        commandStore.executeBlocking(() -> commandStore.setCacheSize(0));
        Gen<TxnId> txnIdGen = rs -> txnId(1, clock.incrementAndGet(), 1);

        qt().withPure(false).withExamples(50).forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 10)).check((rs, ids) -> {
            before(); // truncate tables

            createCommand(commandStore, rs, ids);

            Map<TxnId, Boolean> failed = selectFailedTxn(rs, ids);

            assertNoReferences(commandStore, ids, keys);

            PreLoadContext ctx = PreLoadContext.contextFor(ids, keys);

            Consumer<SafeCommandStore> consumer = Mockito.mock(Consumer.class);

            AsyncOperation<Void> o1 = new AsyncOperation.ForConsumer(commandStore, ctx, consumer)
            {
                @Override
                AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
                {
                    return new AsyncLoader(commandStore, preLoadContext.txnIds(), (Iterable<RoutableKey>) preLoadContext.keys())
                    {
                        @Override
                        Function<TxnId, Command> loadCommandFunction()
                        {
                            Function<TxnId, Command> delegate = super.loadCommandFunction();
                            return txnId -> {
                                logger.info("Attempting to load {}; expected to fail? {}", txnId, failed.get(txnId));
                                if (!failed.get(txnId)) return delegate.apply(txnId);

                                throw new NullPointerException("txn_id " + txnId);
                            };
                        }
                    };
                }
            };

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
            AsyncOperation.ForConsumer o2 = new AsyncOperation.ForConsumer(commandStore, ctx, store -> ids.forEach(id -> store.command(id).readyToExecute()));
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

            PreLoadContext ctx = PreLoadContext.contextFor(ids, keys);

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

    @Test
    public void writeFail()
    {
        AtomicLong clock = new AtomicLong(0);
        // all txn use the same key; 0
        Keys keys = keys(Schema.instance.getTableMetadata("ks", "tbl"), 0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Gen<TxnId> txnIdGen = rs -> txnId(1, clock.incrementAndGet(), 1);

        qt().withExamples(100).forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 10)).check((rs, ids) -> {
            before(); // truncate tables

            createCommand(commandStore, rs, ids);

            Map<TxnId, Boolean> failed = selectFailedTxn(rs, ids);

            assertNoReferences(commandStore, ids, keys);

            PreLoadContext ctx = PreLoadContext.contextFor(ids, keys);

            Consumer<SafeCommandStore> consumer = store -> ids.forEach(id -> store.command(id).readyToExecute());

            AsyncOperation<Void> o1 = new AsyncOperation.ForConsumer(commandStore, ctx, consumer)
            {
                @Override
                AsyncWriter createAsyncWriter(AccordCommandStore commandStore)
                {
                    return new AsyncWriter(commandStore)
                    {
                        @Override
                        protected AsyncWriter.StateMutationFunction<AccordSafeCommand> writeCommandFunction()
                        {
                            StateMutationFunction<AccordSafeCommand> delegate = super.writeCommandFunction();
                            return (store, updated, timestamp) -> {
                                if (!failed.get(updated.txnId())) return delegate.apply(store, updated, timestamp);


                                Mutation mutation = Mockito.mock(Mutation.class);
                                Mockito.doThrow(new NullPointerException("txn_id " + updated.txnId())).when(mutation).apply();
                                return mutation;
                            };
                        }
                    };
                }
            };

            Assertions.assertThatThrownBy(() -> getUninterruptibly(o1));


            assertNoReferences(commandStore, ids, keys);
            assertCanNotEvict(commandStore.commandCache(), failed.entrySet().stream()
                                                                 .filter(e -> e.getValue())
                                                                 .map(e -> e.getKey())
                                                                 .collect(Collectors.toList()));
            // first write will fail the operation, so make sure to wait for all write results
            awaitSaveResult(commandStore.cache());

            // the command should be ReadyToExecute, so move it forward and allow the save
            AsyncOperation.ForConsumer o2 = new AsyncOperation.ForConsumer(commandStore, ctx, store -> ids.forEach(id -> {
                SafeCommand command = store.command(id);
                Command current = command.current();
                Assertions.assertThat(current.status()).isEqualTo(Status.ReadyToExecute);
                Writes writes = current.partialTxn().execute(current.executeAt(), new TxnData());
                command.preapplied(current, current.txnId(), current.asCommitted().waitingOn(), writes, null);
            }));
            getUninterruptibly(o2);

            assertNoReferences(commandStore, ids, keys);
            assertCanEvict(commandStore.commandCache(), ids);
            assertCanEvict(commandStore.commandsForKeyCache(), (Iterable<RoutableKey>) (Iterable<?>) keys);
        });
    }

    private static void createCommand(AccordCommandStore commandStore, Gen.Random rs, List<TxnId> ids)
    {
        // to simulate CommandsForKey not being found, use createCommittedAndPersist periodically as it does not update
        if (rs.nextBoolean()) ids.forEach(id -> createCommittedAndPersist(commandStore, id));
        else ids.forEach(id -> createCommittedUsingLifeCycle(commandStore, id));
        commandStore.clearCache();
    }

    private static Map<TxnId, Boolean> selectFailedTxn(Gen.Random rs, List<TxnId> ids)
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
            AccordStateCache.Node<T, ?> node = cache.getUnsafe(key);
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
            AccordStateCache.Node<T, ?> node = cache.getUnsafe(key);
            if (node == null) continue;
            Awaitility.await("For node " + node.key() + " to complete")
            .atMost(Duration.ofMinutes(1))
            .until(() -> node.isComplete());
        }
    }

    private static void awaitSaveResult(AccordStateCache cache)
    {
        for (Map.Entry<Object, AsyncResult<Void>> e : cache.saveResults().entrySet())
            AsyncChains.awaitUninterruptibly(e.getValue());
    }

    private static <T> void assertCanEvict(AccordStateCache.Instance<T, ?, ?> cache, Iterable<T> keys)
    {
        for (T key : keys)
        {
            AccordStateCache.Node<T, ?> node = cache.getUnsafe(key);
            if (node == null)
                continue;
            Assert.assertTrue("Unable to evict " + node.key(), cache.canEvict(node.key()));
        }
    }

    private static <T> void assertCanNotEvict(AccordStateCache.Instance<T, ?, ?> cache, Iterable<T> keys)
    {
        List<String> errors = new ArrayList<>();
        for (T key : keys)
        {
            if (cache.getUnsafe(key) == null)
            {
                errors.add(String.format("Node %s was evicted, but should not be", key));
                continue;
            }
            if (cache.canEvict(key)) errors.add(String.format("Node %s is evictable but should not be", key));
        }
        if (!errors.isEmpty()) throw new AssertionError(String.join("\n", errors));
    }
}
