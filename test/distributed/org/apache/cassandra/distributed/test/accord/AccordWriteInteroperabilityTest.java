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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
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
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.commons.collections.ListUtils.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Check that the query is sent to Accord and the apply is an interop apply as is required by the transactional
 * mode at each step of migration as well as that when the apply response is sent that the memtable actually contains
 * the data that the apply should have applied
 */
@RunWith(Parameterized.class)
public class AccordWriteInteroperabilityTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteroperabilityTest.class);

    enum Migration
    {
        notNeeded,
        firstPhase,
        secondPhase,
        finished;
    }

    private final Migration migration;

    public AccordWriteInteroperabilityTest(@Nonnull TransactionalMode mode, Migration migration)
    {
        super(mode);
        this.migration = migration;
    }

    @Parameterized.Parameters(name = "transactionalMode={0}, migrated={1}")
    public static Collection<Object[]> data() {
        List<Object[]> tests = new ArrayList<>(TransactionalMode.values().length * 2);
        for (TransactionalMode mode : TransactionalMode.values())
        {
            if (mode.accordIsEnabled)
            {
                for (Migration migration : Migration.values())
                    tests.add(new Object[]{ mode, migration});
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
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c))" + (migration == Migration.notNeeded ? " WITH " + transactionalMode.asCqlParam() : ""),
             cluster -> {
                 MessageCountingSink messageCountingSink = new MessageCountingSink(SHARED_CLUSTER, MessageCountingSink.EXCLUDE_SYNC_POINT_MESSAGES);
                 List<String> failures = synchronizedList(new ArrayList<>());
                 Set<List<Object>> interopApplies = Collections.newSetFromMap(new ConcurrentHashMap<>());
                 // Verify that the apply response is only sent after the row has been inserted
                 // TODO (required): Need to delay mutation stage/mutation to ensure this has time to catch it
                 SHARED_CLUSTER.setMessageSink((to, message) -> {
                     try
                     {
                         int nodeIndex = ((InstanceClassLoader)Thread.currentThread().getContextClassLoader()).getInstanceId();
                         if (message.verb() == Verb.ACCORD_INTEROP_APPLY_REQ.id)
                         {
                             int from = to.getAddress().getAddress()[3];
                             interopApplies.add(ImmutableList.of(nodeIndex, from, message.idAsLong()));
                         }
                         else if (message.verb() == Verb.ACCORD_APPLY_RSP.id)
                         {
                             int originalFromNodeIndex = to.getAddress().getAddress()[3];
                             boolean respondingToInteropApply = interopApplies.remove(ImmutableList.of(originalFromNodeIndex, nodeIndex, message.idAsLong()));
                             // It should be async/not interop because Paxos no longer reads and the mode doesn't respect commit CL
                             if (transactionalMode.ignoresSuppliedCommitCL() && (migration == Migration.notNeeded || migration == Migration.secondPhase || migration == Migration.finished))
                             {
                                 assertFalse(respondingToInteropApply);
                                 return;
                             }

                             // This isn't a response to an interop apply
                             if (!respondingToInteropApply)
                                 return;

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
                     }
                     catch (Throwable t)
                     {
                         failures.add(getStackTraceAsString(t));
                     }
                     finally
                     {
                         messageCountingSink.accept(to, message);
                     }
                 });

                 if (migration != Migration.notNeeded)
                     cluster.coordinator(1).execute("ALTER TABLE " + qualifiedAccordTableName + " WITH " + transactionalMode.asCqlParam(), ConsistencyLevel.ALL);
                 if (migration == Migration.secondPhase || migration == Migration.finished)
                     nodetool(cluster.coordinator(1), "repair", "-skip-paxos", "-skip-accord", KEYSPACE, accordTableName);
                 if (migration == Migration.finished)
                    nodetool(cluster.coordinator(1), "repair", "-skip-accord", KEYSPACE, accordTableName);

                 String finalQuery = query;
                 org.apache.cassandra.distributed.api.ConsistencyLevel consistencyLevel = org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
                 // If the non-serial request wouldn't end up running on Accord then convert it to CAS to make it run on Accord
                 // and check that it's also an interop apply
                 if (!transactionalMode.nonSerialWritesThroughAccord && !query.startsWith("BEGIN TRANSACTION") && !query.startsWith("BEGIN BATCH"))
                 {
                     finalQuery = query + " IF NOT EXISTS";
                     consistencyLevel = org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
                 }
                 boolean transactionalQuery = finalQuery.startsWith("BEGIN TRANSACTION") || finalQuery.contains("IF NOT EXISTS");
                 boolean isCAS = finalQuery.contains("IF NOT EXISTS");
                 cluster.coordinator(1).execute(finalQuery, consistencyLevel, ConsistencyLevel.ALL);
                 // If it isn't a transaction query and the mode doesn't write through Accord for non-SERIAL we don't expect to see any apply messages
                 // If it is a CAS statement, but it's the first phase of migration then we expect it to continue to run on Paxos
                 if ((!transactionalQuery && !transactionalMode.nonSerialWritesThroughAccord) ||  (isCAS && migration == migration.firstPhase))
                 {
                     assertEquals(0, messageCount(Verb.ACCORD_APPLY_REQ));
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                 }
                 // If it's a transactional mode that ignores supplied commit CL and this isn't the first phase of migration
                 // then it should be able to do regular Accord apply because nothing besides Accord should be reading the key by the time
                 // the Accord txn starts
                 else if (transactionalMode.ignoresSuppliedCommitCL() && migration != migration.firstPhase)
                 {
                     spinAssertEquals(3, () -> messageCount(Verb.ACCORD_APPLY_REQ));
                     assertEquals(0, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                 }
                 // The apply should be an interop Apply if it's not one of the above exceptions
                 // Either because the transactional mode respects commit CL or because it's a phase of migration
                 // that races with non-SERIAL reads or CAS reads and needs to provide them with the expected commit CL
                 else
                 {
                     assertEquals(3, messageCount(Verb.ACCORD_INTEROP_APPLY_REQ));
                 }
                 // Converting the stack traces to strings and passing them to fail causes test failures to be ignored
                 if (!failures.isEmpty())
                 {
                     logger.error(failures.toString());
                     fail();
                 }
             });
    }
}
