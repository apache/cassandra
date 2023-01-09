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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStore.SafeAccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.FBUtilities;

import static accord.local.PreLoadContext.contextFor;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AsyncOperationTest
{
    private static AtomicLong clock = new AtomicLong(0);

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

        commandStore.execute(contextFor(txnId), instance -> {
            Command command = instance.ifPresent(txnId);
            Assert.assertNull(command);
        }).get();

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void optionalCommandsForKeyTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Txn txn = createTxn((int)clock.incrementAndGet());
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        commandStore.execute(contextFor(Collections.emptyList(), Keys.of(key)),instance -> {
            AccordCommandsForKey cfk = ((SafeAccordCommandStore)instance).maybeCommandsForKey(key);
            Assert.assertNull(cfk);
        }).get();

        int nowInSeconds = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = AccordKeyspace.getCommandsForKeyRead(commandStore, key, nowInSeconds);
        try(ReadExecutionController controller = command.executionController();
            FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            Assert.assertFalse(partitions.hasNext());
        }
    }

    private static AccordCommand createCommittedAndPersist(AccordCommandStore commandStore, TxnId txnId, Timestamp executeAt)
    {
        AccordCommand command = new AccordCommand(txnId).initialize();
        command.setPartialTxn(createPartialTxn(0));
        command.setExecuteAt(executeAt);
        command.setStatus(Status.Committed);
        AccordKeyspace.getCommandMutation(commandStore, command, commandStore.nextSystemTimestampMicros()).apply();
        command.clearModifiedFlag();
        return command;
    }

    private static AccordCommand createCommittedAndPersist(AccordCommandStore commandStore, TxnId txnId)
    {
        return createCommittedAndPersist(commandStore, txnId, txnId);
    }

    private static void assertFutureState(AccordStateCache.Instance<TxnId, AccordCommand> cache, TxnId txnId, boolean expectLoadFuture, boolean expectSaveFuture)
    {
        if (cache.hasLoadFuture(txnId) != expectLoadFuture)
            throw new AssertionError(expectLoadFuture ? "Load future unexpectedly not found for " + txnId
                                                      : "Unexpectedly found load future for " + txnId);
        if (cache.hasSaveFuture(txnId) != expectSaveFuture)
            throw new AssertionError(expectSaveFuture ? "Save future unexpectedly not found for " + txnId
                                                      : "Unexpectedly found save future for " + txnId);

    }

    /**
     * save and load futures should be cleaned up as part of the operation
     */
    @Test
    public void testFutureCleanup()
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        AccordCommand command = createCommittedAndPersist(commandStore, txnId);

        Consumer<SafeCommandStore> consumer = instance -> ((AccordCommand)instance.command(txnId)).setStatus(Status.PreApplied);
        AsyncOperation<Void> operation = new AsyncOperation.ForConsumer(commandStore, singleton(txnId), emptyList(), consumer)
        {

            private AccordStateCache.Instance<TxnId, AccordCommand> cache()
            {
                return commandStore.commandCache();
            }

            @Override
            AsyncLoader createAsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys)
            {
                return new AsyncLoader(commandStore, txnIds, keys) {

                    @Override
                    void state(State state)
                    {
                        switch (state)
                        {
                            case SETUP:
                            case FINISHED:
                                assertFutureState(cache(), txnId, false, false);
                                break;
                            case LOADING:
                                assertFutureState(cache(), txnId, true, false);
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
                            case FINISHED:
                                assertFutureState(cache(), txnId, false, false);
                                break;
                            case SAVING:
                                assertFutureState(cache(), txnId, false, true);

                        }
                        super.setState(state);
                    }
                };
            }
        };

        commandStore.executor().submit(operation);

        Futures.getUnchecked(operation);
    }
}
