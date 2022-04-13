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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Status;
import accord.local.TxnOperation;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
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

    private static TxnOperation op(TxnId txnId, Collection<TxnId> deps, Collection<Key> keys)
    {
        return new TxnOperation()
        {
            @Override
            public TxnId txnId()
            {
                return txnId;
            }

            @Override
            public Iterable<TxnId> depsIds()
            {
                return deps;
            }

            @Override
            public Iterable<Key> keys()
            {
                return keys;
            }
        };
    }

    private static TxnOperation op(TxnId txnId)
    {
        return op(txnId, Collections.emptyList(), Collections.emptyList());
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
        PreAccept preAccept = new PreAccept(null, txnId, txn);

        // Check preaccept
        commandStore.process(preAccept, instance -> {
            PreAccept.PreAcceptReply reply = preAccept.process(instance);
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
        Dependencies deps = new Dependencies();
        deps.add(txnId2, txn);
        Accept accept = new Accept(null, Ballot.ZERO, txnId, txn, executeAt, deps);

        commandStore.process(accept, instance -> {
            Accept.AcceptReply reply = accept.process(instance);
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
        Commit commit = new Commit(null, txnId, txn, executeAt, deps, false);
        commandStore.process(commit, instance -> {
            Command command = instance.command(txnId);
            command.commit(commit.txn, commit.deps, commit.executeAt);
        }).get();

        // unseen deps txn should have been witnessed
        Timestamp executeAt2 = commandStore.process(op(txnId2), instance -> {
            Command command = instance.command(txnId2);
            Assert.assertTrue(command.hasBeen(Status.PreAccepted));
            Assert.assertTrue(command.executeAt().compareTo(executeAt) > 0);
            return command.executeAt();
        }).get();

        commandStore.process(commit, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(commit.executeAt, command.executeAt());
            Assert.assertTrue(command.hasBeen(Status.Committed));
            Assert.assertEquals(commit.deps, command.savedDeps());

            CommandsForKey cfk = instance.commandsForKey(key(1));
            Assert.assertEquals(executeAt2, cfk.max());
            Assert.assertNull(cfk.uncommitted().get(txnId));
            Assert.assertNotNull(cfk.committedById().get(txnId));
            Assert.assertNotNull(cfk.committedByExecuteAt().get(commit.executeAt));
        }).get();
    }
}
