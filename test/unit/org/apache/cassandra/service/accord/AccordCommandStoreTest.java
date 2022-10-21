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

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.local.Command;
import accord.local.Status;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.local.Status.Durability.Durable;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.ballot;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.processCommandResult;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

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

    @Before
    public void setUp() throws Exception
    {
        Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
    }

    @Test
    public void commandLoadSave() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        PartialTxn depTxn = createPartialTxn(0);
        Key key = (Key)depTxn.keys().get(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialDeps dependencies;
        try (PartialDeps.Builder builder = PartialDeps.builder(depTxn.covering()))
        {
            builder.add(key, txnId(1, clock.incrementAndGet(), 1));
            dependencies = builder.build();
        }

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");
        TxnId oldTxnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTxnId2 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTimestamp = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        AccordCommand command = new AccordCommand(txnId).initialize();
        command.setPartialTxn(createPartialTxn(0));
        command.homeKey(key.toUnseekable());
        command.progressKey(key.toUnseekable());
        command.setDurability(Durable);
        command.setPromised(ballot(1, clock.incrementAndGet(), 1));
        command.setAccepted(ballot(1, clock.incrementAndGet(), 1));
        command.setExecuteAt(timestamp(1, clock.incrementAndGet(), 1));
        command.setPartialDeps(dependencies);
        command.setStatus(Status.Accepted);
        command.addWaitingOnCommit(oldTxnId1);
        command.addWaitingOnApplyIfAbsent(oldTxnId2, oldTimestamp);
        command.storedListeners.clear();
        command.addListener(new AccordCommand(oldTxnId1));
        processCommandResult(commandStore, command);

        AccordKeyspace.getCommandMutation(commandStore, command, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", command);
        Command actual = AccordKeyspace.loadCommand(commandStore, txnId);
        logger.info("A: {}", actual);

        Assert.assertEquals(command, actual);
    }

    @Test
    public void commandsForKeyLoadSave()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Timestamp maxTimestamp = timestamp(1, clock.incrementAndGet(), 1);

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);
        AccordCommand command1 = new AccordCommand(txnId1).initialize();
        AccordCommand command2 = new AccordCommand(txnId2).initialize();
        command1.setPartialTxn(txn);
        command2.setPartialTxn(txn);
        command1.setExecuteAt(timestamp(1, clock.incrementAndGet(), 1));
        command2.setExecuteAt(timestamp(1, clock.incrementAndGet(), 1));

        AccordCommandsForKey cfk = new AccordCommandsForKey(commandStore, key).initialize();
        cfk.updateMax(maxTimestamp);

        Assert.assertEquals(txnId1.hlc(), cfk.timestampMicrosFor(txnId1, true));
        Assert.assertEquals(txnId2.hlc(), cfk.timestampMicrosFor(txnId2, true));
        Assert.assertEquals(txnId2, cfk.lastExecutedTimestamp.get());
        Assert.assertEquals(txnId2.hlc(), cfk.lastExecutedMicros.get());

        cfk.register(command1);
        cfk.register(command2);

        AccordKeyspace.getCommandsForKeyMutation(commandStore, cfk, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", cfk);
        AccordCommandsForKey actual = AccordKeyspace.loadCommandsForKey(commandStore, key);
        logger.info("A: {}", actual);

        Assert.assertEquals(cfk, actual);
    }

    @Test
    public void commandsForKeyBlindWitnessed()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        AccordCommandsForKey.WriteOnly writeOnlyCfk = new AccordCommandsForKey.WriteOnly(commandStore, key);
        Timestamp maxTimestamp = null;
        TreeSet<Timestamp> expected = new TreeSet<>();

        for (int i=0; i<4; i++)
        {
            maxTimestamp = timestamp(1, clock.incrementAndGet(), 1);
            expected.add(maxTimestamp);
            writeOnlyCfk.updateMax(maxTimestamp);
        }

        AccordKeyspace.getCommandsForKeyMutation(commandStore, writeOnlyCfk, commandStore.nextSystemTimestampMicros()).apply();
        AccordCommandsForKey fullCfk = AccordKeyspace.loadCommandsForKey(commandStore, key);

        Assert.assertEquals(expected, fullCfk.blindWitnessed.getView());

        fullCfk.applyBlindWitnessedTimestamps();
        Assert.assertEquals(maxTimestamp, fullCfk.max());
        Assert.assertTrue(fullCfk.blindWitnessed.getView().isEmpty());
    }

}
