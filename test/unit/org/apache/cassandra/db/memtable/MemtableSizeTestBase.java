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

package org.apache.cassandra.db.memtable;

import java.lang.reflect.Field;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;

// Note: This test can be run in idea with the allocation type configured in the test yaml and memtable using the
// value memtableClass is initialized with.
@RunWith(Parameterized.class)
public abstract class MemtableSizeTestBase extends CQLTester
{
    // Note: To see a printout of the usage for each object, add .printVisitedTree() here (most useful with smaller number of
    // partitions).
    static MemoryMeter meter =  MemoryMeter.builder()
                                           .withGuessing(Guess.INSTRUMENTATION_AND_SPECIFICATION,
                                                         Guess.UNSAFE)
//                                           .printVisitedTreeUpTo(1000)
                                           .build();

    static final Logger logger = LoggerFactory.getLogger(MemtableSizeTestBase.class);

    static final int partitions = 50_000;
    static final int rowsPerPartition = 4;

    static final int deletedPartitions = 10_000;
    static final int deletedRows = 5_000;

    @Parameterized.Parameter(0)
    public String memtableClass = "skiplist";

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist",
                                "skiplist_sharded",
                                "trie");
    }

    // Must be within 3% of the real usage. We are actually more precise than this, but the threshold is set higher to
    // avoid flakes. For on-heap allocators we allow for extra overheads below.
    final int MAX_DIFFERENCE_PERCENT = 3;
    // Slab overhead, added when the memtable uses heap_buffers.
    final int SLAB_OVERHEAD = 1024 * 1024;

    public static void setup(Config.MemtableAllocationType allocationType)
    {
        ServerTestUtils.daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = allocationType;
            conf.memtable_cleanup_threshold = 0.8f; // give us more space to fit test data without flushing
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }

        CQLTester.setUpClass();
        CQLTester.prepareServer();
        logger.info("setupClass done, allocation type {}", allocationType);
    }

    void checkMemtablePool()
    {
        // overridden by instances
    }

    @Test
    public void testSize() throws Throwable
    {
        // Make sure memtables use the correct allocation type, i.e. that setup has worked.
        // If this fails, make sure the test is not reusing an already-initialized JVM.
        checkMemtablePool();

        CQLTester.disablePreparedReuseForTest();
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        try
        {
            String table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                                 " with compression = {'enabled': false}" +
                                                 " and memtable = '" + memtableClass + "'");
            execute("use " + keyspace + ';');

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
            forcePreparedValues();

            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            Util.flush(cfs);

            Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
            long deepSizeBefore = meter.measureDeep(memtable);
            logger.info("Memtable deep size before {}", FBUtilities.prettyPrintMemory(deepSizeBefore));
            long i;
            long limit = partitions;
            logger.info("Writing {} partitions of {} rows", partitions, rowsPerPartition);
            for (i = 0; i < limit; ++i)
            {
                for (long j = 0; j < rowsPerPartition; ++j)
                    execute(writeStatement, i, j, i + j);
            }

            logger.info("Deleting {} partitions", deletedPartitions);
            limit += deletedPartitions;
            for (; i < limit; ++i)
            {
                // no partition exists, but we will create a tombstone
                execute("DELETE FROM " + table + " WHERE userid = ?", i);
            }

            logger.info("Deleting {} rows", deletedRows);
            limit += deletedRows;
            for (; i < limit; ++i)
            {
                // no row exists, but we will create a tombstone (and partition)
                execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
            }

            Assert.assertSame("Memtable flushed during test. Test was not carried out correctly.",
                              memtable,
                              cfs.getTracker().getView().getCurrentMemtable());

            Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
            long actualHeap = usage.ownsOnHeap;
            logger.info(String.format("Memtable in %s mode: %d ops, %s serialized bytes, %s",
                                      DatabaseDescriptor.getMemtableAllocationType(),
                                      memtable.operationCount(),
                                      FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                                      usage));

            long deepSizeAfter = meter.measureDeep(memtable);
            logger.info("Memtable deep size {}", FBUtilities.prettyPrintMemory(deepSizeAfter));

            long expectedHeap = deepSizeAfter - deepSizeBefore;
            long max_difference = MAX_DIFFERENCE_PERCENT * expectedHeap / 100;
            long trie_overhead = memtable instanceof TrieMemtable ? ((TrieMemtable) memtable).unusedReservedMemory() : 0;
            switch (DatabaseDescriptor.getMemtableAllocationType())
            {
                case heap_buffers:
                    max_difference += SLAB_OVERHEAD;
                    actualHeap += trie_overhead;    // adjust trie memory with unused buffer space if on-heap
                    break;
                case unslabbed_heap_buffers:
                    actualHeap += trie_overhead;    // adjust trie memory with unused buffer space if on-heap
                    break;
            }
            String message = String.format("Expected heap usage close to %s, got %s, %s difference.\n",
                                           FBUtilities.prettyPrintMemory(expectedHeap),
                                           FBUtilities.prettyPrintMemory(actualHeap),
                                           FBUtilities.prettyPrintMemory(expectedHeap - actualHeap));
            logger.info(message);

            Assert.assertTrue(message, Math.abs(actualHeap - expectedHeap) <= max_difference);
        }
        finally
        {
            execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspace));
        }
    }
}
