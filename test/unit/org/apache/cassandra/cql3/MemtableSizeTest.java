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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.github.jamm.MemoryMeter;

public class MemtableSizeTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableSizeTest.class);
    private static final MemoryMeter meter = new MemoryMeter().omitSharedBufferOverhead()
                                                              .withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE)
                                                              .ignoreKnownSingletons();
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    int partitions = 50_000;
    int rowsPerPartition = 4;

    int deletedPartitions = 10_000;
    int deletedRows = 5_000;

    final int totalPartitions = partitions + deletedPartitions + deletedRows;

    // Must be within 3% of the real usage. We are actually more precise than this,
    // but the threshold is set higher to avoid flakes.
    // the main contributor to the actual size floating is randomness in ConcurrentSkipListMap
    final static int MAX_DIFFERENCE_PERCENT = 3;

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CQLTester.disablePreparedReuseForTest();
        System.err.println("setupClass done.");
    }

    @Test
    public void testSize() throws Throwable
    {
        try
        {
            keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
            table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
            execute("use " + keyspace + ';');

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

            cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            cfs.forceBlockingFlush();

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

            long actualHeap = memtable.getAllocator().onHeap().owns();
            logger.info(String.format("Memtable in %s mode: %d ops, %s serialized bytes, %s (%.0f%%) on heap, %s (%.0f%%) off-heap",
                              DatabaseDescriptor.getMemtableAllocationType(),
                              memtable.getOperations(),
                              FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                              FBUtilities.prettyPrintMemory(actualHeap),
                              100 * memtable.getAllocator().onHeap().ownershipRatio(),
                              FBUtilities.prettyPrintMemory(memtable.getAllocator().offHeap().owns()),
                              100 * memtable.getAllocator().offHeap().ownershipRatio()));

            long deepSizeAfter = meter.measureDeep(memtable);
            logger.info("Memtable deep size {}", FBUtilities.prettyPrintMemory(deepSizeAfter));

            long expectedHeap = deepSizeAfter - deepSizeBefore;
            double deltaPerPartition = (actualHeap - expectedHeap) / (double) totalPartitions;
            long maxDifference = MAX_DIFFERENCE_PERCENT * expectedHeap / 100;
            String message = String.format("Expected heap usage close to %s, got %s. Delta per partition: %.2f bytes",
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