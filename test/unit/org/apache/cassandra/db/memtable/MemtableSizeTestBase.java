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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.github.jamm.MemoryMeter;

public abstract class MemtableSizeTestBase extends CQLTester
{
    static final Logger logger = LoggerFactory.getLogger(MemtableSizeTestBase.class);

    static final int partitions = 50_000;
    static final int rowsPerPartition = 4;

    static final int deletedPartitions = 10_000;
    static final int deletedRows = 5_000;

    static final int totalPartitions = partitions + deletedPartitions + deletedRows;

    // Must be within 3% of the real usage. We are actually more precise than this, but the threshold is set higher to
    // avoid flakes. For on-heap allocators we allow for extra overheads below.
    final int MAX_DIFFERENCE_PERCENT = 3;

    public static void setup(Config.MemtableAllocationType allocationType, IPartitioner partitioner)
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

        StorageService.instance.setPartitionerUnsafe(partitioner);
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
        // Note: To see a printout of the usage for each object, add .enableDebug() here (most useful with smaller number of
        // partitions)
        MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE)
//                                           .enableDebug(100)
                                             .ignoreKnownSingletons();
        if (DatabaseDescriptor.getMemtableAllocationType() == Config.MemtableAllocationType.heap_buffers ||
            DatabaseDescriptor.getMemtableAllocationType() == Config.MemtableAllocationType.offheap_buffers) {
            // jamm includes capacity for all ByteBuffer sub-clases (HeapByteBuffer and DirectByteBuffer) to deepMeasure result
            // we need it only when we use HeapByteBuffer because we want to measure heap usage only
            // we have to use it for DirectByteBuffer too to avoid MemoryMeter traversing through Cleaner references
            meter = meter.omitSharedBufferOverhead();
        }

        // Make sure memtables use the correct allocation type, i.e. that setup has worked.
        // If this fails, make sure the test is not reusing an already-initialized JVM.
        checkMemtablePool();

        // a prepared statement provides the same RegularAndStaticColumns object to store under AbstractBTreePartition$Holder
        // meter.mesureDeep counts such shared object only once but the current tracking logic within memtable cannot do it
        // so to not face the discrepancy we disable prepared statements
        CQLTester.disablePreparedReuseForTest();

        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        try
        {
            String table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                                 " with compression = {'enabled': false}");
            execute("use " + keyspace + ';');

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
            forcePreparedValues();

            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
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
            // jamm MemoryMeter 0.3.2 does not allow to measure heap usage for DirectHeapBuffer correctly
            //   within a bigger object graph (measureDeep).
            // If omitSharedBufferOverhead is disabled
            //    it starts to traverse and include heap usage for unpredictable global Cleaner/ReferenceQueue graphs
            // if omitSharedBufferOverhead is enabled
            //    it includes direct buffer capacity into the memory usage
            // so, we have to correct the heap usage measured in the test by subtracting the total size of data within DirectByteBuffer.
            // We assume that there is no data replacement in the test operations
            // so all the off-heap memory allocated in direct byte buffer slabs is in-use/visible by traversing Memtable object graph
            if (DatabaseDescriptor.getMemtableAllocationType() == Config.MemtableAllocationType.offheap_buffers) {
                expectedHeap -= memtable.getAllocator().offHeap().owns();
            }
            long maxDifference = MAX_DIFFERENCE_PERCENT * expectedHeap / 100;

            double deltaPerPartition = (expectedHeap - actualHeap) / (double) totalPartitions;
            String message = String.format("Expected heap usage close to %s, got %s, %s difference. " +
                                           "Delta per partition: %.2f bytes",
                                           FBUtilities.prettyPrintMemory(expectedHeap),
                                           FBUtilities.prettyPrintMemory(actualHeap),
                                           FBUtilities.prettyPrintMemory(expectedHeap - actualHeap),
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
