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

package org.apache.cassandra.cql3;

import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

@RunWith(Parameterized.class)
public class MemtableSizeTest extends CQLTester
{
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    int partitions = 50_000;
    int rowsPerPartition = 4;

    int deletedPartitions = 10_000;
    int deletedRows = 5_000;

    @Parameterized.Parameter()
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("SkipListMemtable",
                                "TrieMemtable");
    }

    // must be within 50 bytes per partition of the actual size
    final int MAX_DIFFERENCE = (partitions + deletedPartitions + deletedRows) * 50;

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CQLTester.disablePreparedReuseForTest();
        System.err.println("setupClass done.");
    }

    @Test
    public void testTruncationReleasesLogSpace()
    {
        Util.flakyTest(this::testSize, 2, "Fails occasionally, see CASSANDRA-16684");
    }

    private void testSize()
    {
        try
        {
            keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
            table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                      " with compression = {'enabled': false}" +
                                      " and memtable = { 'class': '" + memtableClass + "'}");
            execute("use " + keyspace + ';');

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

            cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            cfs.forceBlockingFlush(UNIT_TESTS);

            long deepSizeBefore = ObjectSizes.measureDeep(cfs.getTracker().getView().getCurrentMemtable());
            System.out.printf("Memtable deep size before %s\n%n",
                              FBUtilities.prettyPrintMemory(deepSizeBefore));
            long i;
            long limit = partitions;
            System.out.println("Writing " + partitions + " partitions of " + rowsPerPartition + " rows");
            for (i = 0; i < limit; ++i)
            {
                for (long j = 0; j < rowsPerPartition; ++j)
                    execute(writeStatement, i, j, i + j);
            }

            System.out.println("Deleting " + deletedPartitions + " partitions");
            limit += deletedPartitions;
            for (; i < limit; ++i)
            {
                // no partition exists, but we will create a tombstone
                execute("DELETE FROM " + table + " WHERE userid = ?", i);
            }

            System.out.println("Deleting " + deletedRows + " rows");
            limit += deletedRows;
            for (; i < limit; ++i)
            {
                // no row exists, but we will create a tombstone (and partition)
                execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
            }


            if (!cfs.getLiveSSTables().isEmpty())
                System.out.println("Warning: " + cfs.getLiveSSTables().size() + " sstables created.");

            Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
            Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        long actualHeap = usage.ownsOnHeap;
            System.out.printf("Memtable in %s mode: %d ops, %s serialized bytes, %s %n",
                              DatabaseDescriptor.getMemtableAllocationType(),
                              memtable.getOperations(),
                              FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                              usage);

            long deepSizeAfter = ObjectSizes.measureDeep(memtable);
            System.out.printf("Memtable deep size %s\n%n",
                              FBUtilities.prettyPrintMemory(deepSizeAfter));

            long expectedHeap = deepSizeAfter - deepSizeBefore;
            String message = String.format("Expected heap usage close to %s, got %s.\n",
                                           FBUtilities.prettyPrintMemory(expectedHeap),
                                           FBUtilities.prettyPrintMemory(actualHeap));
            System.out.println(message);
            Assert.assertTrue(message, Math.abs(actualHeap - expectedHeap) <= MAX_DIFFERENCE);
        }
        catch (Throwable throwable)
        {
            Throwables.propagate(throwable);
        }
    }
}