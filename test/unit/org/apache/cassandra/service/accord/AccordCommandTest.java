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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore.SafeAccordCommandStore;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.*;

public class AccordCommandTest
{

    static final AtomicLong clock = new AtomicLong(0);
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
        return new PartitionKey(metadata.keyspace, metadata.id, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k)));
    }

    /**
     * disable cache and make sure correct values are coming in and out of the accord table
     */
    @Test
    public void basicCycleTest()
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        awaitUninterruptibly(commandStore.execute(PreLoadContext.empty(), instance -> { ((SafeAccordCommandStore) instance).commandStore().setCacheSize(0); }));


        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = createTxn(1);
        Key key = (Key)txn.keys().get(0);
        RoutingKey homeKey = key.toUnseekable();
        FullRoute<?> fullRoute = txn.keys().toRoute(homeKey);
        PartialRoute<?> route = fullRoute.slice(fullRange(txn));
        PartialTxn partialTxn = txn.slice(route.covering(), true);
        PreAccept preAccept = PreAccept.SerializerSupport.create(txnId, route, 1, 1, false, 1, partialTxn, fullRoute);

        // Check preaccept
        awaitUninterruptibly(commandStore.execute(preAccept, instance -> {
            PreAccept.PreAcceptReply reply = preAccept.apply(instance);
            Assert.assertTrue(reply.isOk());
            PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
            Assert.assertEquals(txnId, ok.witnessedAt);
            Assert.assertTrue(ok.deps.isEmpty());
        }));

        awaitUninterruptibly(commandStore.execute(preAccept, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(txnId, command.executeAt());
            Assert.assertEquals(Status.PreAccepted, command.status());
            Assert.assertTrue(command.partialDeps().isEmpty());

            AccordCommandsForKey cfk = ((SafeAccordCommandStore)instance).commandsForKey(key(1));
            Assert.assertEquals(txnId, cfk.max());
            Assert.assertNotNull((cfk.byId()).get(txnId));
            Assert.assertNotNull((cfk.byExecuteAt()).get(txnId));
        }));

        // check accept
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 1);
        PartialDeps deps;
        try (PartialDeps.Builder builder = PartialDeps.builder(route.covering()))
        {
            builder.add(key, txnId2);
            deps = builder.build();
        }
        Accept accept = Accept.SerializerSupport.create(txnId, route, 1, 1, false, Ballot.ZERO, executeAt, partialTxn.keys(), deps);

        awaitUninterruptibly(commandStore.execute(accept, instance -> {
            Accept.AcceptReply reply = accept.apply(instance);
            Assert.assertTrue(reply.isOk());
            Assert.assertTrue(reply.deps.isEmpty());
        }));

        awaitUninterruptibly(commandStore.execute(accept, instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(executeAt, command.executeAt());
            Assert.assertEquals(Status.Accepted, command.status());
            Assert.assertEquals(deps, command.partialDeps());

            AccordCommandsForKey cfk = ((SafeAccordCommandStore)instance).commandsForKey(key(1));
            Assert.assertEquals(executeAt, cfk.max());
            Assert.assertNotNull((cfk.byId()).get(txnId));
            Assert.assertNotNull((cfk.byExecuteAt()).get(txnId));
        }));

        // check commit
        Commit commit = Commit.SerializerSupport.create(txnId, route, 1, executeAt, partialTxn, deps, fullRoute, null);
        awaitUninterruptibly(commandStore.execute(commit, commit::apply));

        awaitUninterruptibly(commandStore.execute(PreLoadContext.contextFor(txnId, Keys.of(key)), instance -> {
            Command command = instance.command(txnId);
            Assert.assertEquals(commit.executeAt, command.executeAt());
            Assert.assertTrue(command.hasBeen(Status.Committed));
            Assert.assertEquals(commit.partialDeps, command.partialDeps());

            AccordCommandsForKey cfk = ((SafeAccordCommandStore)instance).commandsForKey(key(1));
            Assert.assertNotNull((cfk.byId()).get(txnId));
            Assert.assertNotNull((cfk.byExecuteAt()).get(commit.executeAt));
        }));
    }

    @Test
    public void computeDeps() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        awaitUninterruptibly(commandStore.execute(PreLoadContext.empty(), instance -> { ((SafeAccordCommandStore) instance).commandStore().setCacheSize(0); }));

        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = createTxn(2);
        Key key = (Key)txn.keys().get(0);
        RoutingKey homeKey = key.toUnseekable();
        FullRoute<?> fullRoute = txn.keys().toRoute(homeKey);
        PartialRoute<?> route = fullRoute.slice(fullRange(txn));
        PartialTxn partialTxn = txn.slice(route.covering(), true);
        PreAccept preAccept1 = PreAccept.SerializerSupport.create(txnId1, route, 1, 1, false, 1, partialTxn, fullRoute);

        awaitUninterruptibly(commandStore.execute(preAccept1, preAccept1::apply));

        // second preaccept should identify txnId1 as a dependency
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);
        PreAccept preAccept2 = PreAccept.SerializerSupport.create(txnId2, route, 1, 1, false, 1, partialTxn, fullRoute);
        awaitUninterruptibly(commandStore.execute(preAccept2, instance -> {
            PreAccept.PreAcceptReply reply = preAccept2.apply(instance);
            Assert.assertTrue(reply.isOk());
            PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
            Assert.assertTrue(ok.deps.contains(txnId1));
        }));
    }
}
