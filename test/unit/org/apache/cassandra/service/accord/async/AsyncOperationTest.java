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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.CommandsForKey;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.utils.FBUtilities;

import static accord.local.TxnOperation.scopeFor;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AsyncOperationTest
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
     * Commands which were not previously on disk and were only accessed via `ifPresent`, and therefore,
     * not initialized, should not be saved at the end of the operation
     */
    @Test
    public void optionalCommandTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId = txnId(1, clock.incrementAndGet(), 0, 1);
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) Iterables.getOnlyElement(txn.keys());

        commandStore.process(scopeFor(txnId), instance -> {
            Command command = instance.ifPresent(txnId);
            Assert.assertNull(command);
        }).get();

        UntypedResultSet result = AccordKeyspace.loadCommandRow(commandStore, txnId);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void optionalCommandsForKeyTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Txn txn = createTxn(0);
        AccordKey.PartitionKey key = (AccordKey.PartitionKey) Iterables.getOnlyElement(txn.keys());

        commandStore.process(scopeFor(Collections.emptyList(), Collections.singleton(key)), instance -> {
            CommandsForKey cfk = commandStore.maybeCommandsForKey(key);
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
}
