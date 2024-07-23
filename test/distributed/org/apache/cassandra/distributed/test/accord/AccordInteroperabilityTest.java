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
import java.util.List;
import java.util.function.Function;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.IAccordService;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.Util.dk;
import static org.apache.commons.collections.ListUtils.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccordInteroperabilityTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteroperabilityTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 3);
    }

    @After
    public void tearDown()
    {
        SHARED_CLUSTER.setMessageSink(null);
    }

    @Test
    public void testSerialReadDescending() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='full'",
             cluster -> {
                 ICoordinator coordinator = cluster.coordinator(1);
                 for (int i = 1; i <= 10; i++)
                     coordinator.execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 0;", org.apache.cassandra.distributed.api.ConsistencyLevel.ALL, i, i * 10);
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 1", AssertUtils.row(10, 100));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 2", AssertUtils.row(10, 100), AssertUtils.row(9, 90));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 3", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 4", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80), AssertUtils.row(7, 70));
             }
         );
    }

    @Test
    public void testApplyIsInteropApply() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='mixed_reads'",
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
                             String currentThread = Thread.currentThread().getName();
                             char nodeIndexChar = currentThread.charAt(4);
                             int nodeIndex = Integer.parseInt(String.valueOf(nodeIndexChar));
                             try
                             {
                                 String keyspace = KEYSPACE;
                                 String tableName = accordTableName;
                                 String fail = SHARED_CLUSTER.get(nodeIndex).callOnInstance(() -> {
                                     ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, tableName);
                                     Memtable memtable = cfs.getCurrentMemtable();
                                     assertEquals(1, memtable.partitionCount());
                                     UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(cfs.metadata()), DataRange.allData(cfs.getPartitioner()), SSTableReadsListener.NOOP_LISTENER);
                                     assertTrue(partitions.hasNext());
                                     UnfilteredRowIterator rows = partitions.next();
                                     assertEquals(dk(42), rows.partitionKey());
                                     assertFalse(partitions.hasNext());
                                     assertTrue(rows.hasNext());
                                     Row row = (Row)rows.next();
                                     assertFalse(rows.hasNext());
                                     return null;
                                 });
                                 if (fail != null)
                                     failures.add(fail);
                             }
                             catch (Exception e)
                             {
                                 failures.add(getStackTraceAsString(e));
                             }
                         }
                     }
                     finally
                     {
                         messageCountingSink.accept(to, message);
                     }
                 });
                cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (42, 2, 3)", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM);
                assertEquals(3, messageCounts.get(Verb.ACCORD_INTEROP_APPLY_REQ).get());
                assertTrue(failures.toString(), failures.isEmpty());
             });
    }

    @Test
    public void testReadIsAtQuorum() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='unsafe_writes'",
             cluster -> {
                 SHARED_CLUSTER.setMessageSink(new MessageCountingSink(SHARED_CLUSTER));
                 cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 0", org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL);
                 assertEquals(2, messageCounts.get(Verb.ACCORD_INTEROP_COMMIT_REQ).get());
                 assertEquals(2, messageCounts.get(Verb.ACCORD_INTEROP_READ_RSP).get());
             });
    }

    private static Object[][] assertTargetAccordRead(Function<Integer, Object[][]> query, int coordinatorIndex, int key, int expectedAccordReadCount)
    {
        int startingReadCount = getAccordReadCount(coordinatorIndex);
        Object[][] result = query.apply(key);
        assertEquals("Accord reads", expectedAccordReadCount, getAccordReadCount(coordinatorIndex) - startingReadCount);
        return result;
    }

    private static Object[][] assertTargetAccordWrite(Function<Integer, Object[][]> query, int coordinatorIndex, int key, int expectedAccordWriteCount)
    {
        int startingWriteCount = getAccordWriteCount(coordinatorIndex);
        Object[][] result = query.apply(key);
        assertEquals("Accord writes", expectedAccordWriteCount, getAccordWriteCount(coordinatorIndex) - startingWriteCount);
        return result;
    }

    @Test
    public void testNonSerialReadIsThroughAccordFull() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='full'",
             cluster -> {
                 for (ConsistencyLevel cl : ConsistencyLevel.values())
                 {
                     try
                     {
                         if (cl == ConsistencyLevel.ANY || cl == ConsistencyLevel.NODE_LOCAL)
                             continue;
                         assertTargetAccordRead(key -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = ?", org.apache.cassandra.distributed.api.ConsistencyLevel.valueOf(cl.name()), key), 1, 1, 1);
                         if (!IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cl))
                             fail("Unsupported consistency level succeeded");

                     }
                     catch (Throwable t)
                     {
                         assertEquals(InvalidRequestException.class.getName(), t.getClass().getName());
                         assertEquals(cl + " is not supported by Accord", t.getMessage());
                     }
                 }
             });
    }

    @Test
    public void testNonSerialWriteIsThroughAccordFull() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='full'",
             cluster -> {
                 for (ConsistencyLevel cl : ConsistencyLevel.values())
                 {
                     try
                     {
                         assertTargetAccordWrite(key -> cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (?, 43, 44)", org.apache.cassandra.distributed.api.ConsistencyLevel.valueOf(cl.name()), key), 1, 1, 1);
                         if (!IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(cl))
                             fail("Unsupported consistency level succeeded");
                     }
                     catch (Throwable t)
                     {
                         assertEquals(InvalidRequestException.class.getName(), t.getClass().getName());
                         if (cl == ConsistencyLevel.SERIAL || cl == ConsistencyLevel.LOCAL_SERIAL)
                             assertEquals("You must use conditional updates for serializable writes", t.getMessage());
                         else
                            assertEquals(cl + " is not supported by Accord", t.getMessage());
                     }
                 }
             });
    }
}
