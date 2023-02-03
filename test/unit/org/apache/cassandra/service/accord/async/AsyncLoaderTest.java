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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.primitives.PartialTxn;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordLoadingState;
import org.apache.cassandra.service.accord.AccordSafeCommand;
import org.apache.cassandra.service.accord.AccordSafeCommandsForKey;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncOperation.Context;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.Commands.notWitnessed;
import static org.apache.cassandra.service.accord.AccordTestUtils.commandsForKey;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.execute;
import static org.apache.cassandra.service.accord.AccordTestUtils.loaded;
import static org.apache.cassandra.service.accord.AccordTestUtils.testLoad;
import static org.apache.cassandra.service.accord.AccordTestUtils.testableLoad;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AsyncLoaderTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    /**
     * Loading a cached resource shoudln't block
     */
    @Test
    public void cachedTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache = commandStore.commandCache();
        commandStore.executeBlocking(() -> commandStore.setCacheSize(1024));

        AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> cfkCache = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release

        AccordSafeCommand safeCommand = commandCache.reference(txnId);
        testLoad(safeCommand, notWitnessed(txnId, txn));
        commandCache.release(safeCommand);

        AccordSafeCommandsForKey safeCfk = cfkCache.reference(key);
        testLoad(safeCfk, commandsForKey(key));
        cfkCache.release(safeCfk);

        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // everything is cached, so the loader should return immediately
        commandStore.executeBlocking(() -> {
            Context context = new Context();
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertEquals(safeCommand.global(), context.commands.get(txnId).global());
            Assert.assertEquals(safeCfk.global(), context.commandsForKeys.get(key).global());
            Assert.assertTrue(result);
        });

        Assert.assertSame(safeCommand.global(), commandCache.getUnsafe(txnId));
        Assert.assertSame(safeCfk.global(), cfkCache.getUnsafe(key));
    }

    /**
     * Loading a cached resource should block
     */
    @Test
    public void loadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // create / persist
        AccordSafeCommand safeCommand = new AccordSafeCommand(loaded(txnId, null));
        safeCommand.preExecute();
        safeCommand.set(notWitnessed(txnId, txn));
        AccordKeyspace.getCommandMutation(commandStore, safeCommand, commandStore.nextSystemTimestampMicros()).apply();

        AccordSafeCommandsForKey cfk = new AccordSafeCommandsForKey(loaded(key, null));
        safeCommand.preExecute();
        cfk.set(commandsForKey(key));
        AccordKeyspace.getCommandsForKeyMutation(commandStore, cfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        Context context = new Context();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                Assert.assertTrue(context.commands.containsKey(txnId));
                Assert.assertTrue(context.commandsForKeys.containsKey(key));
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(context.commands.containsKey(txnId));
            Assert.assertTrue(context.commandsForKeys.containsKey(key));
            Assert.assertTrue(result);
        });
    }

    /**
     * Test when some resources are cached and others need to be loaded
     */
    @Test
    public void partialLoadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire /release, create / persist
        AccordSafeCommand safeCommand = commandCache.reference(txnId);
        testLoad(safeCommand, notWitnessed(txnId, txn));
        commandCache.release(safeCommand);


        AccordSafeCommandsForKey safeCfk = new AccordSafeCommandsForKey(loaded(key, null));
        safeCfk.set(commandsForKey(key));
        AccordKeyspace.getCommandsForKeyMutation(commandStore, safeCfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        Context context = new Context();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                Assert.assertTrue(context.commands.containsKey(txnId));
                Assert.assertTrue(context.commandsForKeys.containsKey(key));
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {

            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(context.commands.containsKey(txnId));
            Assert.assertTrue(context.commandsForKeys.containsKey(key));
            Assert.assertTrue(result);
        });
    }

    /**
     * If another process is loading a resource, piggyback on it's future
     */
    @Test
    public void inProgressLoadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        commandStore.executor().submit(() -> commandStore.setCacheSize(1024)).get();
        AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> cfkCache = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release
        AccordSafeCommand safeCommand = commandCache.reference(txnId);
        Assert.assertEquals(AccordLoadingState.LoadingState.UNINITIALIZED, safeCommand.loadingState());
        Runnable load = safeCommand.load(testableLoad(safeCommand.key(), notWitnessed(txnId, txn)));
        Assert.assertEquals(AccordLoadingState.LoadingState.PENDING, safeCommand.loadingState());
        Assert.assertTrue(commandCache.isReferenced(txnId));
        Assert.assertFalse(commandCache.isLoaded(txnId));

        AccordSafeCommandsForKey safeCfk = cfkCache.reference(key);
        testLoad(safeCfk, commandsForKey(key));
        cfkCache.release(safeCfk);

        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // since there's a read future associated with the txnId, we'll wait for it to load
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        Context context = new Context();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                Assert.assertTrue(context.commands.containsKey(txnId));
                Assert.assertTrue(context.commandsForKeys.containsKey(key));
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        Assert.assertFalse(cbFired.isSuccess());
        load.run();
        Assert.assertEquals(AccordLoadingState.LoadingState.LOADED, safeCommand.loadingState());
        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);
        Assert.assertTrue(cbFired.isSuccess());

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(context.commands.containsKey(txnId));
            Assert.assertTrue(context.commandsForKeys.containsKey(key));
            Assert.assertTrue(result);
        });
    }

    @Test
    public void failedLoadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        AsyncResult.Settable<Void> promise1 = AsyncResults.settable();
        AtomicReference<Consumer<AccordSafeCommand>> consumer1 = new AtomicReference<>();
        AsyncResult.Settable<Void> promise2 = AsyncResults.settable();
        AtomicReference<Consumer<AccordSafeCommand>> consumer2 = new AtomicReference<>();
        AsyncResult.Settable<Void> callback = AsyncResults.settable();
        RuntimeException failure = new RuntimeException();

        execute(commandStore, () -> {
            AtomicInteger loadCalls = new AtomicInteger();
            AsyncLoader loader = new AsyncLoader(commandStore, ImmutableList.of(txnId1, txnId2), Collections.emptyList()){

                @Override
                Function<TxnId, Command> loadCommandFunction()
                {
                    return txnId -> {
                        loadCalls.incrementAndGet();
                        if (txnId.equals(txnId1))
                        {
                            throw failure;
                        }
                        if (txnId.equals(txnId2))
                        {
                            return notWitnessed(txnId, null);
                        }
                        throw new AssertionError("Unknown txnId: " + txnId);
                    };
                }
            };

            boolean result = loader.load(new Context(), (u, t) -> {
                Assert.assertFalse(callback.isDone());
                Assert.assertNull(u);
                Assert.assertEquals(failure, t);
                callback.trySuccess(null);
            });
            Assert.assertFalse(result);
            Assert.assertEquals(2, loadCalls.get());
        });

        promise1.tryFailure(failure);
        AsyncChains.getUninterruptibly(callback);
    }
}
