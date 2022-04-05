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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.Node;
import accord.local.Status;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.*;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.EQUAL;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.NOT_EXISTS;

public class AccordCommandStoreTest
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStoreTest.class);
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Test
    public void commandLoadSave() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        TokenRange range = TokenRange.fullRange(metadata.id);
        Node.Id node = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        InMemoryCommandStore.Synchronized commandStore = new InMemoryCommandStore.Synchronized(0, 1, 8,
                                                                                               node,
                                                                                               ts -> new Timestamp(1, clock.incrementAndGet(), 0, node),
                                                                                               new AccordAgent(),
                                                                                               null,
                                                                                               KeyRanges.of(range),
                                                                                               () -> topology);
        Txn depTxn = txnBuilder().withRead("SELECT * FROM ks.tbl WHERE k=0 AND c=0")
                                 .withWrite("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)")
                                 .withCondition("ks", "tbl", 0, 0, NOT_EXISTS).build();

        Dependencies dependencies = new Dependencies();
        dependencies.add(txnId(1, clock.incrementAndGet(), 0, 1), depTxn);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");

        Txn txn = txnBuilder().withRead("SELECT * FROM ks.tbl WHERE k=0 AND c=0")
                              .withWrite("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 2)")
                              .withCondition("ks", "tbl", 0, 0, "v", EQUAL, 1).build();

        TxnId oldTxnId1 = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId oldTxnId2 = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId oldTimestamp = txnId(1, clock.incrementAndGet(), 0, 1);
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        AccordCommand command = new AccordCommand(commandStore, txnId);
        command.txn(txn);
        command.promised(ballot(1, clock.incrementAndGet(), 0, 1));
        command.accepted(ballot(1, clock.incrementAndGet(), 0, 1));
        command.executeAt(timestamp(1, clock.incrementAndGet(), 0, 1));
        command.savedDeps(dependencies);
        command.status(Status.Accepted);
        command.clearWaitingOnCommit();
        command.addWaitingOnCommit(oldTxnId1, new AccordCommand(commandStore, oldTxnId1));
        command.clearWaitingOnApply();
        command.addWaitingOnApplyIfAbsent(oldTimestamp, new AccordCommand(commandStore, oldTxnId2));
        command.storedListeners.clear();
        command.addListener(new AccordCommand(commandStore, oldTxnId1));
        processCommandResult(command);

        AccordKeyspace.getCommandMutation(command).apply();
        logger.info("E: {}", command);
        Command actual = AccordKeyspace.loadCommand(commandStore, txnId);
        logger.info("A: {}", actual);

        Assert.assertEquals(command, actual);
    }

    @Test
    public void collections()
    {
        // TODO: clear, add, delete
    }
}
