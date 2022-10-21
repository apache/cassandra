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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Status;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.*;

public class AccordCommandTest
{

    private static final Node.Id ID1 = new Node.Id(1);
    private static final Node.Id ID2 = new Node.Id(2);
    private static final Node.Id ID3 = new Node.Id(3);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    private static PartitionKey key(int k)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        return new PartitionKey(metadata.id, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k)));
    }

    /**
     * disable cache and make sure correct values are coming in and out of the accord table
     */
    @Test
    public void basicCycleTest() throws ExecutionException, InterruptedException
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        commandStore.processSetup(instance -> { ((AccordCommandStore) instance).setCacheSize(0); });


        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(1);
        Key key = txn.keys().get(0);
        PreAccept preAccept = new PreAccept(txn.keys(), 1, txnId, txn, key);

        // Check preaccept
        commandStore.process(preAccept, instance -> {
            PreAccept.PreAcceptReply reply = preAccept.process(instance, key);
            Assert.assertTrue(reply.isOK());
            PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
            Assert.assertEquals(txnId, ok.witnessedAt);
            Assert.assertTrue(ok.deps.isEmpty());
        }).get();

        commandStore.process(preAccept, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(txnId, command.executeAt());
            Assert.assertEquals(Status.PreAccepted, command.status());
            Assert.assertTrue(command.savedDeps().isEmpty());

            CommandsForKey cfk = instance.commandsForKey(key(1));
            Assert.assertEquals(txnId, cfk.max());
            Assert.assertNotNull(cfk.uncommitted().get(txnId));
            Assert.assertNull(cfk.committedById().get(txnId));
            Assert.assertNull(cfk.committedByExecuteAt().get(txnId));
        }).get();

        // check accept
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 0, 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 0, 1);
        Deps.OrderedBuilder builder = Deps.orderedBuilder(false);
        builder.add(key, txnId2);
        Deps deps = builder.build();
        Accept accept = Accept.SerializerSupport.create(txn.keys(), 1, txnId, Ballot.ZERO, key, txn, executeAt, deps);

        commandStore.process(accept, instance -> {
            Accept.AcceptReply reply = accept.process(instance, key);
            Assert.assertTrue(reply.isOK());
            Accept.AcceptOk ok = (Accept.AcceptOk) reply;
            Assert.assertTrue(ok.deps.isEmpty());
        }).get();

        commandStore.process(accept, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(executeAt, command.executeAt());
            Assert.assertEquals(Status.Accepted, command.status());
            Assert.assertEquals(deps, command.savedDeps());

            CommandsForKey cfk = instance.commandsForKey(key(1));
            Assert.assertEquals(executeAt, cfk.max());
            Assert.assertNotNull(cfk.uncommitted().get(txnId));
            Assert.assertNull(cfk.committedById().get(txnId));
            Assert.assertNull(cfk.committedByExecuteAt().get(txnId));
        }).get();

        // check commit
        Commit commit = new Commit(txn.keys(), 1, txnId, txn, deps, key, executeAt, false);
        commandStore.process(commit, instance -> {
            Command command = instance.command(txnId);
            command.commit(commit.txn, key, key, commit.executeAt, commit.deps);
        }).get();

        commandStore.process(commit, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(commit.executeAt, command.executeAt());
            Assert.assertTrue(command.hasBeen(Status.Committed));
            Assert.assertEquals(commit.deps, command.savedDeps());

            CommandsForKey cfk = instance.commandsForKey(key(1));
            Assert.assertNull(cfk.uncommitted().get(txnId));
            Assert.assertNotNull(cfk.committedById().get(txnId));
            Assert.assertNotNull(cfk.committedByExecuteAt().get(commit.executeAt));
        }).get();
    }

    @Test
    public void computeDeps() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        commandStore.processSetup(instance -> { ((AccordCommandStore) instance).setCacheSize(0); });

        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(2);
        Key key = txn.keys().get(0);
        PreAccept preAccept1 = new PreAccept(txn.keys(), 1, txnId1, txn, key);

        commandStore.process(preAccept1, (Consumer<CommandStore>) cs -> preAccept1.process(cs, key)).get();

        // second preaccept should identify txnId1 as a dependency
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 0, 1);
        PreAccept preAccept2 = new PreAccept(txn.keys(), 1, txnId2, txn, key);
        commandStore.process(preAccept2, instance -> {
            PreAccept.PreAcceptReply reply = preAccept2.process(instance, key);
            Assert.assertTrue(reply.isOK());
            PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
            Assert.assertTrue(ok.deps.contains(txnId1));
        });
    }
}
