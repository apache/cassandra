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
import accord.local.cfk.CommandsForKey;
import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
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
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createWriteTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.fullRange;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

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
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
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
    public void basicCycleTest() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        getUninterruptibly(commandStore.execute(PreLoadContext.empty(), unused -> commandStore.setCapacity(0)));

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = createWriteTxn(1);
        Key key = (Key)txn.keys().get(0);
        RoutingKey homeKey = key.toUnseekable();
        FullRoute<?> fullRoute = txn.keys().toRoute(homeKey);
        PartialRoute<?> route = fullRoute.slice(fullRange(txn));
        PartialTxn partialTxn = txn.slice(route.covering(), true);
        PreAccept preAccept = PreAccept.SerializerSupport.create(txnId, route, 1, 1, false, 1, partialTxn, fullRoute);

        // Check preaccept
        getUninterruptibly(commandStore.execute(preAccept, safeStore -> {
            SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
            Command before = safeCommand.current();
            PreAccept.PreAcceptReply reply = preAccept.apply(safeStore);
            Command after = safeCommand.current();

            Assert.assertTrue(reply.isOk());
            PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
            Assert.assertEquals(txnId, ok.witnessedAt);
            Assert.assertTrue(ok.deps.isEmpty());

            AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
        }));

        getUninterruptibly(commandStore.execute(preAccept, safeStore -> {
            Command before = safeStore.ifInitialised(txnId).current();
            SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
            Assert.assertEquals(txnId, before.executeAt());
            Assert.assertEquals(Status.PreAccepted, before.status());
            Assert.assertTrue(before.partialDeps() == null || before.partialDeps().isEmpty());

            CommandsForKey cfk = safeStore.get(key(1)).current();
            Assert.assertTrue(cfk.indexOf(txnId) >= 0);
            Command after = safeCommand.current();
            AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
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

        getUninterruptibly(commandStore.execute(accept, safeStore -> {
            Command before = safeStore.ifInitialised(txnId).current();
            Accept.AcceptReply reply = accept.apply(safeStore);
            Assert.assertTrue(reply.isOk());
            Assert.assertTrue(reply.deps.isEmpty());
            Command after = safeStore.ifInitialised(txnId).current();
            AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
        }));

        getUninterruptibly(commandStore.execute(accept, safeStore -> {
            Command before = safeStore.ifInitialised(txnId).current();
            Assert.assertEquals(executeAt, before.executeAt());
            Assert.assertEquals(Status.Accepted, before.status());
            Assert.assertEquals(deps, before.partialDeps());

            CommandsForKey cfk = safeStore.get(key(1)).current();
            Assert.assertTrue(cfk.indexOf(txnId) >= 0);
            Command after = safeStore.ifInitialised(txnId).current();
            AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
        }));

        // check commit
        Commit commit = Commit.SerializerSupport.create(txnId, route, 1, Commit.Kind.StableWithTxnAndDeps, Ballot.ZERO, executeAt, partialTxn.keys(), partialTxn, deps, fullRoute, null);
        getUninterruptibly(commandStore.execute(commit, commit::apply));

        getUninterruptibly(commandStore.execute(PreLoadContext.contextFor(txnId, Keys.of(key), KeyHistory.COMMANDS), safeStore -> {
            Command before = safeStore.ifInitialised(txnId).current();
            Assert.assertEquals(commit.executeAt, before.executeAt());
            Assert.assertTrue(before.hasBeen(Status.Committed));
            Assert.assertEquals(commit.partialDeps, before.partialDeps());

            CommandsForKey cfk = safeStore.get(key(1)).current();
            Assert.assertTrue(cfk.indexOf(txnId) >= 0);
            Command after = safeStore.ifInitialised(txnId).current();
            AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
        }));
    }

    @Test
    public void computeDeps() throws Throwable
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        getUninterruptibly(commandStore.execute(PreLoadContext.empty(), unused -> commandStore.setCapacity(0)));

        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        Txn txn = createWriteTxn(2);
        Key key = (Key)txn.keys().get(0);
        RoutingKey homeKey = key.toUnseekable();
        FullRoute<?> fullRoute = txn.keys().toRoute(homeKey);
        PartialRoute<?> route = fullRoute.slice(fullRange(txn));
        PartialTxn partialTxn = txn.slice(route.covering(), true);
        PreAccept preAccept1 = PreAccept.SerializerSupport.create(txnId1, route, 1, 1, false, 1, partialTxn, fullRoute);

        getUninterruptibly(commandStore.execute(preAccept1, safeStore -> {
            persistDiff(commandStore, safeStore, txnId1, route, () -> {
                preAccept1.apply(safeStore);
            });
        }));

        // second preaccept should identify txnId1 as a dependency
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);
        PreAccept preAccept2 = PreAccept.SerializerSupport.create(txnId2, route, 1, 1, false, 1, partialTxn, fullRoute);
        getUninterruptibly(commandStore.execute(preAccept2, safeStore -> {
            persistDiff(commandStore, safeStore, txnId2, route, () -> {
                PreAccept.PreAcceptReply reply = preAccept2.apply(safeStore);
                Assert.assertTrue(reply.isOk());
                PreAccept.PreAcceptOk ok = (PreAccept.PreAcceptOk) reply;
                Assert.assertTrue(ok.deps.contains(txnId1));
            });
        }));
    }

    private static void persistDiff(AccordCommandStore commandStore, SafeCommandStore safeStore, TxnId txnId, Route<?> route, Runnable runnable)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        Command before = safeCommand.current();
        runnable.run();
        Command after = safeCommand.current();
        AccordTestUtils.appendCommandsBlocking(commandStore, before, after);
    }
}
