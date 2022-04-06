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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.*;

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
        AccordStateCache.Instance<TxnId, AccordCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys);

        // acquire / release
        AccordCommand command = commandCache.getOrCreate(txnId).loadEmpty();
        command.txn(txn);
        commandCache.release(command);
        AccordCommandsForKey cfk = cfkCacche.getOrCreate(key).loadEmpty();
        cfkCacche.release(cfk);

        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // everything is cached, so the loader should return immediately
        boolean result = loader.load(context, (o, t) -> Assert.fail());
        Assert.assertTrue(result);

        Assert.assertSame(command, context.commands.get(txnId));
        Assert.assertSame(cfk, context.commandsForKey(key));
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
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys);

        // create / persist
        AccordCommand command = new AccordCommand(commandStore, txnId).loadEmpty();
        command.txn(txn);
        AccordKeyspace.getCommandMutation(command).apply();
        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).loadEmpty();
        AccordKeyspace.getCommandsForKeyMutation(cfk).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        boolean result = loader.load(context, (o, t) -> {
            Assert.assertNull(t);
            cbFired.setSuccess(null);
        });
        Assert.assertFalse(result);

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        result = loader.load(context, (o, t) -> Assert.fail());
        Assert.assertTrue(result);
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
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys);

        // acquire /release, create / persist
        AccordCommand command = commandCache.getOrCreate(txnId).loadEmpty();
        command.txn(txn);
        commandCache.release(command);
        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).loadEmpty();
        AccordKeyspace.getCommandsForKeyMutation(cfk).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncContext context = new AsyncContext();
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        boolean result = loader.load(context, (o, t) -> {
            Assert.assertNull(t);
            cbFired.setSuccess(null);
        });
        Assert.assertFalse(result);

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        result = loader.load(context, (o, t) -> Assert.fail());
        Assert.assertTrue(result);
    }

    /**
     * If another process is loading a resource, piggyback on it's future
     */
    @Test
    public void inProgressLoadTest()
    {

    }
}
