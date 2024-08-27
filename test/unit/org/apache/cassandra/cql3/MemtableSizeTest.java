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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.github.jamm.MemoryMeter;

@RunWith(Parameterized.class)
public class MemtableSizeTest extends CQLTester
{
    static final Logger logger = LoggerFactory.getLogger(MemtableSizeTest.class);

    private static final MemoryMeter meter = new MemoryMeter().omitSharedBufferOverhead()
                                                              .withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE)
                                                              .ignoreKnownSingletons();

    static final int partitions = 50_000;
    static final int rowsPerPartition = 4;

    static final int deletedPartitions = 10_000;
    static final int deletedRows = 5_000;

    @Parameterized.Parameter(0)
    public String memtableClass;

    // Must be within 3% of the real usage. We are actually more precise than this,
    // but the threshold is set higher to avoid flakes.
    // the main contributor to the actual size floating is randomness in ConcurrentSkipListMap
    final static int MAX_DIFFERENCE_PERCENT = 3;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        return ImmutableList.of(new Object[]{"skiplist"},
                                new Object[]{"skiplist_sharded"});
    }
    final long MAX_DIFFERENCE_PARTITIONS = (partitions + deletedPartitions + deletedRows);

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        logger.info("setupClass done.");
    }

    @Test
    public void testSize() throws Throwable
    {
        // a prepared statement provides the same RegularAndStaticColumns object to store under AbstractBTreePartition$Holder
        // meter.mesureDeep counts such shared object only once but the current tracking logic within memtable cannot do it
        // so to not face the discrepancy we disable prepared statements
        CQLTester.disablePreparedReuseForTest();

        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        try
        {
            String table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                                 " with compression = {'enabled': false}" +
                                                 " and memtable = '" + memtableClass + "'");
            execute("use " + keyspace + ';');

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            Util.flush(cfs);

            Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
            long deepSizeBefore = meter.measureDeep(memtable);
            logger.info("Memtable deep size before {}\n",
                        FBUtilities.prettyPrintMemory(deepSizeBefore));
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
            logger.info("Memtable in {} mode: {} ops, {} serialized bytes, {}\n",
                        DatabaseDescriptor.getMemtableAllocationType(),
                        memtable.operationCount(),
                        FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                        usage);

            long deepSizeAfter = meter.measureDeep(memtable);
            logger.info("Memtable deep size {}\n",
                        FBUtilities.prettyPrintMemory(deepSizeAfter));

            long expectedHeap = deepSizeAfter - deepSizeBefore;
            double deltaPerPartition = (actualHeap - expectedHeap) / (double) MAX_DIFFERENCE_PARTITIONS;
            long maxDifference = MAX_DIFFERENCE_PERCENT * expectedHeap / 100;
            String message = String.format("Expected heap usage close to %s, got %s. Delta per partition: %.2f bytes.\n",
                                           FBUtilities.prettyPrintMemory(expectedHeap),
                                           FBUtilities.prettyPrintMemory(actualHeap),
                                           deltaPerPartition);
            logger.info(message);
            Assert.assertTrue(message, Math.abs(actualHeap - expectedHeap) <= maxDifference);
        }
        finally
        {
            execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspace));
        }
    }
}