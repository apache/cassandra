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

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.PartialCommand;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordPartialCommand;
import org.apache.cassandra.service.accord.api.AccordKey;

import static accord.local.PreLoadContext.contextFor;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.execute;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AsyncWriterTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    private static void save(AccordCommandStore commandStore, AsyncContext context)
    {
        execute(commandStore, () -> {
            AsyncWriter writer = new AsyncWriter(commandStore);
            while (true)
            {
                if (writer.save(context, (o, t) -> Assert.assertNull(t)))
                    break;
            }
        });
        context.commands.items.values().forEach(AccordCommand::clearModifiedFlag);
        context.commandsForKey.items.values().forEach(AccordCommandsForKey::clearModifiedFlag);
    }

    @Test
    public void waitingOnDenormalization() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId blockingId = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId waitingId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) getOnlyElement(txn.keys());

        AccordCommand blocking = new AccordCommand(commandStore, blockingId).initialize();
        blocking.txn(txn);
        blocking.executeAt(blockingId);
        blocking.status(Status.Committed);
        AccordKeyspace.getCommandMutation(blocking, commandStore.nextSystemTimestampMicros()).apply();
        blocking.clearModifiedFlag();

        AccordCommand waiting = new AccordCommand(commandStore, waitingId).initialize();
        waiting.txn(txn);
        waiting.executeAt(waitingId);
        waiting.status(Status.Committed);
        AccordKeyspace.getCommandMutation(waiting, commandStore.nextSystemTimestampMicros()).apply();
        waiting.clearModifiedFlag();

        AsyncContext context = new AsyncContext();
        waiting.addWaitingOnApplyIfAbsent(blocking);
        context.commands.add(waiting);
        save(commandStore, context);

        // load the blocking command and confirm the waiting command is listed as being blocked
        blocking = AccordKeyspace.loadCommand(commandStore, blockingId);
        Assert.assertTrue(blocking.blockingApplyOn.getView().contains(waitingId));

        // now change the blocking command and check it's changes are reflected in the waiting command
        context = new AsyncContext();
        blocking.status(Status.ReadyToExecute);
        context.commands.add(blocking);
        save(commandStore, context);

        waiting = AccordKeyspace.loadCommand(commandStore, waitingId);
        AccordCommand waitingFinal = waiting;
        AccordCommand blockingFinal = blocking;
        execute(commandStore, () -> {
            AsyncContext ctx = new AsyncContext();
            commandStore.setContext(ctx);
            AccordPartialCommand blockingSummary = (AccordPartialCommand) waitingFinal.firstWaitingOnApply();
            Assert.assertNotSame(blockingFinal, blockingSummary);
            Assert.assertEquals(Status.ReadyToExecute, blockingSummary.status());
            Assert.assertEquals(blockingId, blockingSummary.executeAt());
            commandStore.unsetContext(ctx);
        });
    }

    @Test
    public void commandsPerKeyDenormalization()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) getOnlyElement(txn.keys());

        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).initialize();
        AccordKeyspace.getCommandsForKeyMutation(cfk, commandStore.nextSystemTimestampMicros()).apply();
        Assert.assertTrue(cfk.uncommitted.isEmpty());
        Assert.assertTrue(cfk.committedByExecuteAt.isEmpty());
        Assert.assertTrue(cfk.committedById.isEmpty());

        AccordCommand command = new AccordCommand(commandStore, txnId).initialize();
        command.txn(txn);
        command.executeAt(executeAt);
        command.status(Status.Accepted);
        AsyncContext context = new AsyncContext();
        context.commands.add(command);
        save(commandStore, context);

        AccordCommandsForKey cfkUncommitted = AccordKeyspace.loadCommandsForKey(commandStore, key);
        execute(commandStore, () -> {
            AsyncContext ctx = new AsyncContext();
            commandStore.setContext(ctx);
            AccordPartialCommand.WithDeps summary = (AccordPartialCommand.WithDeps) getOnlyElement(cfkUncommitted.uncommitted().all().collect(Collectors.toList()));
            Assert.assertTrue(cfkUncommitted.uncommitted.map.getView().containsKey(txnId));
            Assert.assertNotSame(command, summary);
            Assert.assertEquals(Status.Accepted, summary.status());
            Assert.assertEquals(executeAt, summary.executeAt());

            Assert.assertTrue(cfkUncommitted.committedByExecuteAt.isEmpty());
            Assert.assertTrue(cfkUncommitted.committedById.isEmpty());
            commandStore.unsetContext(ctx);
        });

        // commit, summary should be moved to committed maps
        command.status(Status.Committed);
        context = new AsyncContext();
        context.commands.add(command);
        save(commandStore, context);

        AccordCommandsForKey cfkCommitted = AccordKeyspace.loadCommandsForKey(commandStore, key);
        execute(commandStore, () -> {
            AsyncContext ctx = new AsyncContext();
            commandStore.setContext(ctx);
            AccordPartialCommand.WithDeps idSummary = (AccordPartialCommand.WithDeps) getOnlyElement(cfkCommitted.committedById().all().collect(Collectors.toList()));
            AccordPartialCommand.WithDeps executeSummary = (AccordPartialCommand.WithDeps) getOnlyElement(cfkCommitted.committedByExecuteAt().all().collect(Collectors.toList()));

            Assert.assertTrue(cfkCommitted.committedById.map.getView().containsKey(txnId));
            Assert.assertTrue(cfkCommitted.committedByExecuteAt.map.getView().containsKey(executeAt));
            Assert.assertNotEquals(command, idSummary);
            // we store serialized values, so they will never be the same object as we deserialize
            Assert.assertEquals(idSummary, executeSummary);

            Assert.assertEquals(Status.Committed, idSummary.status());
            Assert.assertEquals(executeAt, idSummary.executeAt());

            Assert.assertTrue(cfkCommitted.uncommitted.isEmpty());
            commandStore.unsetContext(ctx);
        });
    }

    @Test
    public void partialCommandDenormalization()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        TxnId blockingId = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId waitingId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) getOnlyElement(txn.keys());

        {
            AccordCommand blocking = new AccordCommand(commandStore, blockingId).initialize();
            blocking.txn(txn);
            blocking.executeAt(blockingId);
            blocking.status(Status.Committed);

            AccordCommand waiting = new AccordCommand(commandStore, waitingId).initialize();
            waiting.txn(txn);
            waiting.executeAt(waitingId);
            waiting.status(Status.Committed);
            waiting.addWaitingOnApplyIfAbsent(blocking);

            blocking.addListener(waiting);

            AccordKeyspace.getCommandMutation(blocking, commandStore.nextSystemTimestampMicros()).apply();
            AccordKeyspace.getCommandMutation(waiting, commandStore.nextSystemTimestampMicros()).apply();
            blocking.clearModifiedFlag();
            waiting.clearModifiedFlag();
        }

        // confirm the blocking operation has the waiting one as a listener
        commandStore.process(contextFor(blockingId), cs -> {
            AccordCommand blocking = (AccordCommand) cs.command(blockingId);
            Assert.assertTrue(blocking.hasListenerFor(waitingId));
        });

        // remove listener from PartialCommand
        commandStore.process(contextFor(waitingId), cs -> {
            Command waiting = cs.command(waitingId);
            PartialCommand blocking = waiting.firstWaitingOnApply();
            Assert.assertNotNull(blocking);
            Assert.assertEquals(blockingId, blocking.txnId());
            blocking.removeListener(waiting);
        });

        // confirm it was propagated to the full command
        commandStore.process(contextFor(blockingId), cs -> {
            AccordCommand blocking = (AccordCommand) cs.command(blockingId);
            Assert.assertFalse(blocking.hasListenerFor(waitingId));
        });
    }
}
