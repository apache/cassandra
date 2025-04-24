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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.commons.collections.ListUtils.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class AccordWriteInteroperabilityTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteroperabilityTest.class);

    @Nonnull
    private final TransactionalMode mode;

    private final boolean migrated;

    public AccordWriteInteroperabilityTest(@Nonnull TransactionalMode mode, boolean migrated)
    {
        this.mode = mode;
        this.migrated = migrated;
    }

    @Parameterized.Parameters(name = "transactionalMode={0}, migrated={1}")
    public static Collection<Object[]> data() {
        List<Object[]> tests = new ArrayList<>(TransactionalMode.values().length * 2);
        for (TransactionalMode mode : TransactionalMode.values())
        {
            if (mode.accordIsEnabled)
            {
                tests.add(new Object[]{ mode, true });
                tests.add(new Object[]{ mode, false });
            }
        }
        return tests;
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder.withConfig(config -> config.set("accord.range_migration", "auto")
                                                                                  .set("paxos_variant", "v2")),
                                    3);
    }

    @After
    public void tearDown()
    {
        SHARED_CLUSTER.setMessageSink(null);
    }


    private String testTransactionInsert()
    {
        return "BEGIN TRANSACTION\n" +
               "  INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (42, 2, 3);\n" +
               "COMMIT TRANSACTION";
    }

    private String testInsert()
    {
        return "INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (42, 2, 3)";
    }

    private String testBatchInsert()
    {
        return "BEGIN BATCH\n" +
               "INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (1, 2, 3);\n" +
               "INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (42, 43, 44);\n" +
               "APPLY BATCH";
    }

    @Test
    public void testTransactionStatementApplyIsInteropApply() throws Throwable
    {
        testApplyIsInteropApply(testTransactionInsert());
    }

    @Test
    public void testNonSerialApplyIsInteropApply() throws Throwable
    {
        testApplyIsInteropApply(testInsert());
    }

    @Test
    public void testBatchInsertApplyIsInteropApply() throws Throwable
    {
        testApplyIsInteropApply(testBatchInsert());
    }

    private void testApplyIsInteropApply(String query) throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c))" + (migrated ? " WITH " + transactionalMode.asCqlParam() : ""),
             cluster -> {
                 MessageCountingSink messageCountingSink = new MessageCountingSink(SHARED_CLUSTER);
                 List<String> failures = synchronizedList(new ArrayList<>());
                 // Verify that the apply response is only sent after the row has been inserted
                 // TODO (required): Need to delay mutation stage/mutation to ensure this has time to catch it
                 SHARED_CLUSTER.setMessageSink((to, message) -> {
                     try
                     {
                         if (message.verb() == Verb.ACCORD_APPLY_RSP.id)
                         {
                             // It can be async if it's migrated
                             if (migrated)
                                 return;
                             int nodeIndex = ((InstanceClassLoader)ClassLoader.getSystemClassLoader()).getInstanceId();
                             try
                             {
                                 String keyspace = KEYSPACE;
                                 String tableName = accordTableName;
                                 SHARED_CLUSTER.get(nodeIndex).runOnInstance(() -> {
                                     ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, tableName);
                                     Memtable memtable = cfs.getCurrentMemtable();
                                     int expectedPartitions = query.startsWith("BEGIN BATCH") ? 2 : 1;
                                     assertEquals(expectedPartitions, memtable.partitionCount());
                                     UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(cfs.metadata()), DataRange.allData(cfs.getPartitioner()), SSTableReadsListener.NOOP_LISTENER);
                                     assertTrue(partitions.hasNext());
                                     for (int i = 0; i < expectedPartitions; i++)
                                     {
                                         UnfilteredRowIterator rows = partitions.next();
                                         assertTrue(rows.partitionKey().equals(dk(42)) || rows.partitionKey().equals(dk(1)));
                                         assertTrue(rows.hasNext());
                                         Row row = (Row)rows.next();
                                         assertFalse(rows.hasNext());
                                     }
                                     assertFalse(partitions.hasNext());
                                 });
                             }
                             catch (Throwable t)
                             {
                                 failures.add(getStackTraceAsString(t));
                             }
                         }
                     }
                     finally
                     {
                         messageCountingSink.accept(to, message);
                     }
                 });

                 if (!migrated)
                 {
                     cluster.coordinator(1).execute("ALTER TABLE " + qualifiedAccordTableName + " WITH " + transactionalMode.asCqlParam(), ConsistencyLevel.ALL);
                     nodetool(cluster.coordinator(1), "repair", "-skip-paxos", "-skip-accord", KEYSPACE, accordTableName);
                 }

                 String finalQuery = query;
                 org.apache.cassandra.distributed.api.ConsistencyLevel consistencyLevel = org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
                 // Need to switch to CAS for it to run through Accord at all
                 if (!transactionalMode.nonSerialWritesThroughAccord && !query.startsWith("BEGIN TRANSACTION"))
                 {
                     finalQuery = query + " IF NOT EXISTS";
                     consistencyLevel = org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
                 }
                 long startingRegularApplyCount = messageCount(Verb.ACCORD_APPLY_REQ);
                 cluster.coordinator(1).execute(finalQuery, consistencyLevel);
                 if (transactionalMode.ignoresSuppliedCommitCL() && migrated)
                 {
                     // Apply is async and there can be a lot of sources of regular APPLY
                     spinUntilTrue(() -> messageCount(Verb.ACCORD_APPLY_REQ) > startingRegularApplyCount);
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                 }
                 else
                 {
                     assertEquals(3, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                 }
                 assertTrue(failures.toString(), failures.isEmpty());
             });
    }
}
