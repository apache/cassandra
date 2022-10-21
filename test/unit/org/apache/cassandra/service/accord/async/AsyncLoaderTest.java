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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Status;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.execute;
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

    private static void load(AccordCommandStore commandStore, AsyncContext context, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys)
    {
        execute(commandStore, () ->
        {
            AsyncLoader loader = new AsyncLoader(commandStore, txnIds, keys);
            while (!loader.load(context, (o, t) -> Assert.assertNull(t)));
        });
    }

    private static void load(AccordCommandStore commandStore, AsyncContext context, TxnId... txnIds)
    {
        load(commandStore, context, ImmutableList.copyOf(txnIds), Collections.emptyList());
    }

    /**
     * Loading a cached resource shoudln't block
     */
    @Test
    public void cachedTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release
        AccordCommand command = commandCache.getOrCreate(txnId).initialize();
        command.txn(txn);
        commandCache.release(command);
        AccordCommandsForKey cfk = cfkCacche.getOrCreate(key).initialize();
        cfkCacche.release(cfk);

        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // everything is cached, so the loader should return immediately
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });

        Assert.assertSame(command, context.commands.get(txnId));
        Assert.assertSame(cfk, context.commandsForKey.get(key));
    }

    /**
     * Loading a cached resource should block
     */
    @Test
    public void loadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // create / persist
        AccordCommand command = new AccordCommand(commandStore, txnId).initialize();
        command.txn(txn);
        AccordKeyspace.getCommandMutation(command, commandStore.nextSystemTimestampMicros()).apply();
        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).initialize();
        AccordKeyspace.getCommandsForKeyMutation(cfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    /**
     * Test when some resources are cached and others need to be loaded
     */
    @Test
    public void partialLoadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire /release, create / persist
        AccordCommand command = commandCache.getOrCreate(txnId).initialize();
        command.txn(txn);
        commandCache.release(command);
        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).initialize();
        AccordKeyspace.getCommandsForKeyMutation(cfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    /**
     * If another process is loading a resource, piggyback on it's future
     */
    @Test
    public void inProgressLoadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release
        AccordCommand command = commandCache.getOrCreate(txnId).initialize();
        command.txn(txn);
        commandCache.release(command);
        AccordCommandsForKey cfk = cfkCacche.getOrCreate(key).initialize();
        cfkCacche.release(cfk);

        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // since there's a read future associated with the txnId, we'll wait for it to load
        AsyncPromise<Void> readFuture = new AsyncPromise<>();
        commandCache.setLoadFuture(command.txnId(), readFuture);

        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        Assert.assertFalse(cbFired.isSuccess());
        readFuture.setSuccess(null);
        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);
        Assert.assertTrue(cbFired.isSuccess());

        // then return immediately after the callback has fired
        commandStore.processBlocking(() -> {
            boolean result = loader.load(context, (o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    @Test
    public void pendingWriteOnlyApplied()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId blockApply = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId blockCommit = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) getOnlyElement(txn.keys());

        AccordCommand command = new AccordCommand(commandStore, txnId).initialize();
        command.txn(txn);
        command.executeAt(txnId);
        command.status(Status.Committed);
        AccordKeyspace.getCommandMutation(command, commandStore.nextSystemTimestampMicros()).apply();
        command.clearModifiedFlag();

        execute(commandStore, () -> {
            AccordStateCache.Instance<TxnId, AccordCommand> cache = commandStore.commandCache();
            AccordCommand.WriteOnly writeOnly1 = new AccordCommand.WriteOnly(commandStore, txnId);
            writeOnly1.blockingApplyOn.blindAdd(blockApply);
            writeOnly1.future(new AsyncPromise<>());
            cache.addWriteOnly(writeOnly1);

            AccordCommand.WriteOnly writeOnly2 = new AccordCommand.WriteOnly(commandStore, txnId);
            writeOnly2.blockingCommitOn.blindAdd(blockCommit);
            writeOnly2.future(new AsyncPromise<>());
            cache.addWriteOnly(writeOnly2);

            AsyncContext context = new AsyncContext();
            AsyncLoader loader = new AsyncLoader(commandStore, ImmutableList.of(txnId), Collections.emptyList());
            while (true)
            {
                if (loader.load(context, (o, t) -> Assert.assertNull(t)))
                    break;
            }
            AccordCommand loaded = context.commands.get(txnId);

            Assert.assertEquals(txnId, loaded.executeAt());
            Assert.assertEquals(Status.Committed, loaded.status());
            Assert.assertEquals(blockApply, Iterables.getOnlyElement(loaded.blockingApplyOn.getView()));
            Assert.assertEquals(blockCommit, Iterables.getOnlyElement(loaded.blockingCommitOn.getView()));
        });
    }

    @Test
    public void failedLoadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 0, 1);

        AsyncPromise<Void> promise1 = new AsyncPromise<>();
        AsyncPromise<Void> promise2 = new AsyncPromise<>();
        AsyncPromise<Void> callback = new AsyncPromise<>();
        RuntimeException failure = new RuntimeException();

        execute(commandStore, () -> {
            AsyncContext context = new AsyncContext();
            AtomicInteger loadCalls = new AtomicInteger();
            AsyncLoader loader = new AsyncLoader(commandStore, ImmutableList.of(txnId1, txnId2), Collections.emptyList()){
                @Override
                Function<AccordCommand, Future<?>> loadCommandFunction(Object callback)
                {
                    return cmd -> {
                        TxnId txnId = cmd.txnId();
                        loadCalls.incrementAndGet();
                        if (txnId.equals(txnId1))
                            return promise1;
                        if (txnId.equals(txnId2))
                            return promise2;
                        throw new AssertionError("Unknown txnId: " + txnId);
                    };
                }
            };

            boolean result = loader.load(context, (u, t) -> {
                Assert.assertFalse(callback.isDone());
                Assert.assertNull(u);
                Assert.assertEquals(failure, t);
                callback.trySuccess(null);
            });
            Assert.assertFalse(result);
            Assert.assertEquals(2, loadCalls.get());
        });

        promise1.tryFailure(failure);
        callback.get();
    }
}
