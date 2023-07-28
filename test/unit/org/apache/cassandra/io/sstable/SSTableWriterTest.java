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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SSTableWriterTest extends SSTableWriterTestBase
{
    @Test
    public void testAbortTxnWithOpenEarlyShouldRemoveSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        try (SSTableWriter writer = getWriter(cfs, dir, txn))
        {
            for (int i = 0; i < 10000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer.append(builder.build().unfilteredIterator());
            }

            writer.setMaxDataAge(1000);
            writer.openEarly(s -> {
                assert s != null;
                assertFileCounts(dir.tryListNames());
                for (int i = 10000; i < 20000; i++)
                {
                    UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                    for (int j = 0; j < 100; j++)
                        builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                    writer.append(builder.build().unfilteredIterator());
                }
                writer.setMaxDataAge(1000);
                writer.openEarly(s2 -> {
                    assertTrue(s.getLast().compareTo(s2.getLast()) < 0);
                    assertFileCounts(dir.tryListNames());
                    s.selfRef().release();
                    s2.selfRef().release();

                    int datafiles = assertFileCounts(dir.tryListNames());
                    assertEquals(datafiles, 1);

                    LifecycleTransaction.waitForDeletions();
                    assertFileCounts(dir.tryListNames());

                    writer.abort();
                    txn.abort();
                    LifecycleTransaction.waitForDeletions();
                    datafiles = assertFileCounts(dir.tryListNames());
                    assertEquals(datafiles, 0);
                    validateCFS(cfs);
                });
            });
        }
    }


    @Test
    public void testAbortTxnWithClosedWriterShouldRemoveSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);
        try (SSTableWriter writer = getWriter(cfs, dir, txn))
        {
            for (int i = 0; i < 10000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer.append(builder.build().unfilteredIterator());
            }

            assertFileCounts(dir.tryListNames());
            for (int i = 10000; i < 20000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer.append(builder.build().unfilteredIterator());
            }
            SSTableReader sstable = writer.finish(true);
            int datafiles = assertFileCounts(dir.tryListNames());
            assertEquals(datafiles, 1);

            sstable.selfRef().release();

            LifecycleTransaction.waitForDeletions();
            assertFileCounts(dir.tryListNames());

            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.tryListNames());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
    }

    @Test
    public void testAbortTxnWithClosedAndOpenWriterShouldRemoveAllSSTables()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        SSTableWriter writer1 = getWriter(cfs, dir, txn);
        SSTableWriter writer2 = getWriter(cfs, dir, txn);
        try
        {
            for (int i = 0; i < 10000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer1.append(builder.build().unfilteredIterator());
            }

            assertFileCounts(dir.tryListNames());
            for (int i = 10000; i < 20000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer2.append(builder.build().unfilteredIterator());
            }
            SSTableReader sstable = writer1.finish(true);
            txn.update(sstable, false);

            assertFileCounts(dir.tryListNames());

            int datafiles = assertFileCounts(dir.tryListNames());
            assertEquals(datafiles, 2);

            LifecycleTransaction.waitForDeletions();
            assertFileCounts(dir.tryListNames());

            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.tryListNames());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
        finally
        {
            writer1.close();
            writer2.close();
        }
    }

    @Test
    public void testValueTooBigCorruption() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_SMALL_MAX_VALUE);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        try (SSTableWriter writer1 = getWriter(cfs, dir, txn))
        {
            UpdateBuilder largeValue = UpdateBuilder.create(cfs.metadata(), "large_value").withTimestamp(1);
            largeValue.newRow("clustering").add("val", ByteBuffer.allocate(2 * 1024 * 1024));
            writer1.append(largeValue.build().unfilteredIterator());

            SSTableReader sstable = writer1.finish(true);

            txn.update(sstable, false);

            try
            {
                DecoratedKey dk = Util.dk("large_value");
                UnfilteredRowIterator rowIter = sstable.rowIterator(dk,
                                                                    Slices.ALL,
                                                                    ColumnFilter.all(cfs.metadata()),
                                                                    false,
                                                                    SSTableReadsListener.NOOP_LISTENER);
                while (rowIter.hasNext())
                {
                    rowIter.next();
                    // no-op read, as values may not appear expected
                }
                fail("Expected a CorruptSSTableException to be thrown");
            }
            catch (CorruptSSTableException e)
            {
            }

            txn.abort();
            LifecycleTransaction.waitForDeletions();
        }
    }

    private static void assertValidRepairMetadata(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_SMALL_MAX_VALUE);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        try (SSTableWriter writer = getWriter(cfs, dir, txn, repairedAt, pendingRepair, isTransient))
        {
            // expected
        }
        catch (IllegalArgumentException e)
        {
            throw new AssertionError("Unexpected IllegalArgumentException", e);
        }

        txn.abort();
        LifecycleTransaction.waitForDeletions();
    }

    private static void assertInvalidRepairMetadata(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_SMALL_MAX_VALUE);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        try (SSTableWriter writer = getWriter(cfs, dir, txn, repairedAt, pendingRepair, isTransient))
        {
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        txn.abort();
        LifecycleTransaction.waitForDeletions();
    }

    /**
     * It should only be possible to create sstables marked transient that also have a pending repair
     */
    @Test
    public void testRepairMetadataValidation()
    {
        assertValidRepairMetadata(UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false);
        assertValidRepairMetadata(1, NO_PENDING_REPAIR, false);
        assertValidRepairMetadata(UNREPAIRED_SSTABLE, nextTimeUUID(), false);
        assertValidRepairMetadata(UNREPAIRED_SSTABLE, nextTimeUUID(), true);

        assertInvalidRepairMetadata(UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, true);
        assertInvalidRepairMetadata(1, nextTimeUUID(), false);
        assertInvalidRepairMetadata(1, NO_PENDING_REPAIR, true);

    }
}