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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Route;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
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
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;
import org.apache.cassandra.service.accord.AccordExecutor.ExclusiveGlobalCaches;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;

import static accord.local.KeyHistory.SYNC;
import static accord.local.PreLoadContext.contextFor;
import static accord.utils.Property.qt;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.keys;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AccordTaskTest
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTaskTest.class);
    private static final AtomicLong clock = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void before()
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.ACCORD_KEYSPACE_NAME, AccordKeyspace.COMMANDS));
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

        getUninterruptibly(commandStore.execute(contextFor(txnId), instance -> {
            // TODO review: This change to `ifInitialized` was done in a lot of places and it doesn't preserve this property
            // I fixed this reference to point to `ifLoadedAndInitialised` and but didn't update other places
            Assert.assertNull(instance.ifInitialised(txnId));
            Assert.assertNull(instance.ifLoadedAndInitialised(txnId));
        }));

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void touchUnknownTxn() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        getUninterruptibly(commandStore.execute(contextFor(txnId), safe -> {
            StoreParticipants participants = StoreParticipants.empty(txnId);
            SafeCommand command = safe.get(txnId, participants);
            Assert.assertNotNull(command);
        }));

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void optionalCommandsForKeyTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Txn txn = AccordTestUtils.createWriteTxn((int)clock.incrementAndGet());
        TokenKey key = ((PartitionKey) Iterables.getOnlyElement(txn.keys())).toUnseekable();

        getUninterruptibly(commandStore.execute(contextFor(key), instance -> {
            SafeCommandsForKey cfk = instance.ifLoadedAndInitialised(key);
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
        appendDiffToLog(commandStore).accept(null, command);
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
        route.slice(ranges);
        PartialDeps deps = PartialDeps.builder(ranges, true).build();

        try
        {
            Command command = getUninterruptibly(commandStore.submit(contextFor(txnId, route, SYNC), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, appendDiffToLog(commandStore));
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, partialTxn, executeAt, deps, appendDiffToLog(commandStore));
                return safe.ifInitialised(txnId).current();
            }).beginAsResult());

            // clear cache
            commandStore.executeBlocking(() -> {
                try (ExclusiveGlobalCaches cache = commandStore.executor().lockCaches();)
                {
                    long cacheSize = cache.global.capacity();
                    cache.global.setCapacity(0);
                    cache.global.setCapacity(cacheSize);
                }
            });

            while (commandStore.executor().hasTasks())
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

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

    private static BiConsumer<Command, Command> appendDiffToLog(AccordCommandStore commandStore)
    {
        return (before, after) -> {
            Condition condition = Condition.newOneTimeCondition();
            commandStore.appendToLog(before, after, condition::signal);
            condition.awaitUninterruptibly();
        };
    }

    private static Command createStableUsingSlowLifeCycle(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        PartialTxn partialTxn = createPartialTxn(0);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Ranges ranges = AccordTestUtils.fullRange(partialTxn.keys());
        Route<?> partialRoute = route.slice(ranges);
        PartialDeps deps = PartialDeps.builder(ranges, true).build();

        try
        {
            Command command = getUninterruptibly(commandStore.submit(contextFor(txnId, route, SYNC), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, appendDiffToLog(commandStore));
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, executeAt, deps, appendDiffToLog(commandStore));
                CheckedCommands.commit(safe, SaveStatus.Committed, Ballot.ZERO, txnId, route, partialTxn, executeAt, deps, appendDiffToLog(commandStore));
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, partialTxn, executeAt, deps, appendDiffToLog(commandStore));
                return safe.ifInitialised(txnId).current();
            }).beginAsResult());

            // clear cache
            commandStore.executeBlocking(() -> {
                try (ExclusiveGlobalCaches cache = commandStore.executor().lockCaches();)
                {
                    long cacheSize = cache.global.capacity();
                    cache.global.setCapacity(0);
                    cache.global.setCapacity(cacheSize);
                }
            });

            while (commandStore.executor().hasTasks())
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

            return command;
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void loadFail()
    {
        AtomicLong clock = new AtomicLong(0);
        // all txn use the same key; 0
        Keys keys = keys(Schema.instance.getTableMetadata("ks", "tbl"), 0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        commandStore.executeBlocking(() -> commandStore.executor().cacheUnsafe().setCapacity(0));
        Gen<TxnId> txnIdGen = rs -> txnId(1, clock.incrementAndGet(), 1);

        qt().withSeed(3447647345436261108L).withPure(false)
            .withExamples(50)
            .forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 2))
            .check((rs, ids) -> {
            before(); // truncate tables

            Participants<RoutingKey> participants = keys.toParticipants();
            assertNoReferences(commandStore, ids, participants);
            createCommand(commandStore, rs, ids);
            awaitDone(commandStore, ids, participants);
            assertNoReferences(commandStore, ids, participants);

            PreLoadContext ctx = contextFor(ids.get(0), ids.size() == 1 ? null : ids.get(1), participants, SYNC);
            Consumer<SafeCommandStore> consumer = Mockito.mock(Consumer.class);

            Map<TxnId, Boolean> failed = selectFailedTxn(rs, ids);
            try (ExclusiveGlobalCaches caches = commandStore.executor().lockCaches())
            {
                caches.commands.unsafeSetLoadFunction((s, txnId) ->
                {
                    logger.info("Attempting to load {}; expected to fail? {}", txnId, failed.get(txnId));
                    if (!failed.get(txnId))
                        return commandStore.loadCommand(txnId);
                    throw new NullPointerException("txn_id " + txnId);
                });
            }
            AccordTask<Void> o1 = AccordTask.create(commandStore, ctx, consumer);
            AssertionUtils.assertThatThrownBy(() -> getUninterruptibly(o1.chain()))
                          .hasRootCause()
                          .isInstanceOf(NullPointerException.class)
                          .hasNoSuppressedExceptions();

            Mockito.verifyNoInteractions(consumer);

            assertNoReferences(commandStore, ids, participants);
            // the first failed load causes the whole operation to fail, so some ids may still be pending
            // to make sure the next operation does not see a PENDING that will fail, wait for all loads to complete
            awaitDone(commandStore, ids, participants);

            // can we recover?
            try (ExclusiveGlobalCaches caches = commandStore.executor().lockCaches())
            {
                caches.commands.unsafeSetLoadFunction((s, txnId) -> {
                    Command cmd = commandStore.loadCommand(txnId);
                    return cmd;
                });
            }
            AccordTask<Void> o2 = AccordTask.create(commandStore, ctx, store -> {
                ids.forEach(id -> {
                    store.ifInitialised(id).readyToExecute(store);
                });
            });
            getUninterruptibly(o2.chain());
            awaitDone(commandStore, ids, participants);
            assertNoReferences(commandStore, ids, participants);

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
        qt().withPure(false).withExamples(100).forAll(Gens.random(), Gens.lists(txnIdGen).ofSizeBetween(1, 10)).check((rs, ids) -> {
            logger.info("Test #{}", counter.incrementAndGet());
            before(); // truncate tables

            Participants<RoutingKey> participants = keys.toParticipants();
            assertNoReferences(commandStore, ids, participants);
            createCommand(commandStore, rs, ids);

            PreLoadContext ctx = contextFor(ids.get(0), ids.size() == 1 ? null : ids.get(1), participants, SYNC);

            Consumer<SafeCommandStore> consumer = Mockito.mock(Consumer.class);
            String errorMsg = "txn_ids " + ids;
            Mockito.doThrow(new NullPointerException(errorMsg)).when(consumer).accept(Mockito.any());

            AccordTask<Void> operation = AccordTask.create(commandStore, ctx, consumer);

            AssertionUtils.assertThatThrownBy(() -> getUninterruptibly(operation.chain()))
                          .hasRootCause()
                          .isInstanceOf(NullPointerException.class)
                          .hasMessage(errorMsg)
                          .hasNoSuppressedExceptions();

            assertNoReferences(commandStore, ids, participants);
        });
    }

    private static void createCommand(AccordCommandStore commandStore, RandomSource rs, List<TxnId> ids)
    {
        // to simulate CommandsForKey not being found, use createCommittedAndPersist periodically as it does not update
        switch (rs.nextInt(3))
        {
            case 0:
                logger.info("createStableAndPersist(): {}", ids);
                ids.forEach(id -> createStableAndPersist(commandStore, id));
                break;
            case 1:
                logger.info("createStableUsingFastLifeCycle(): {}", ids);
                ids.forEach(id -> createStableUsingFastLifeCycle(commandStore, id));
                break;
            case 2:
                logger.info("createStableUsingSlowLifeCycle(): {}", ids);
                ids.forEach(id -> createStableUsingSlowLifeCycle(commandStore, id));
        }
    }

    private static Map<TxnId, Boolean> selectFailedTxn(RandomSource rs, List<TxnId> ids)
    {
        ids = new ArrayList<>(ids);
        Map<TxnId, Boolean> failed = Maps.newHashMapWithExpectedSize(ids.size());
        int failedCount = Math.max(1, rs.nextInt(ids.size()));
        Collections.shuffle(ids, rs.asJdkRandom());
        for (int i = 0 ; i < failedCount ; ++i)
            failed.put(ids.get(i), true);
        for (int i = failedCount ; i < ids.size() ; ++i)
            failed.put(ids.get(i), false);
        return failed;
    }

    private static void assertNoReferences(AccordCommandStore commandStore, List<TxnId> ids, Participants<RoutingKey> keys)
    {
        AssertionError error = null;
        try (ExclusiveCaches caches = commandStore.lockCaches())
        {
            assertNoReferences(caches.commands(), ids);
        }
        catch (AssertionError e)
        {
            error = e;
        }
        try (ExclusiveCaches caches = commandStore.lockCaches())
        {
            assertNoReferences(caches.commandsForKeys(), keys);
        }
        catch (AssertionError e)
        {
            if (error == null) error = e;
            else error.addSuppressed(e);
        }
        if (error != null) throw error;
    }

    private static <T> void assertNoReferences(AccordCache.Type<T, ?, ?>.Instance cache, Iterable<T> keys)
    {
        AssertionError error = null;
        for (T key : keys)
        {
            AccordCacheEntry<T, ?> node = cache.getUnsafe(key);
            if (node == null) continue;
            try
            {
                if (node.references() > 0)
                    throw new IllegalStateException();
                Assertions.assertThat(node.references())
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

    private static void awaitDone(AccordCommandStore commandStore, List<TxnId> ids, Participants<RoutingKey> keys)
    {
        awaitDone(commandStore.cachesUnsafe().commands(), ids);
        awaitDone(commandStore.cachesUnsafe().commandsForKeys(), keys);
    }

    private static <T> void awaitDone(AccordCache.Type<T, ?, ?>.Instance cache, Iterable<T> keys)
    {
        for (T key : keys)
        {
            AccordCacheEntry<T, ?> node = cache.getUnsafe(key);
            if (node == null) continue;
            Awaitility.await("For node " + node.key() + " to complete")
            .atMost(Duration.ofMinutes(1))
            .until(node::isComplete);
        }
    }
}
