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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.fail;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableWriterTest extends SSTableWriterTestBase
{
    @Test
    public void testAbortTxnWithOpenEarlyShouldRemoveSSTable() throws InterruptedException
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

            writer.setMaxDataAge(1000).openEarly(s -> {
                assert s != null;
                assertFileCounts(dir.list());
                for (int i = 10000; i < 20000; i++)
                {
                    UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                    for (int j = 0; j < 100; j++)
                        builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                    writer.append(builder.build().unfilteredIterator());
                }
                writer.setMaxDataAge(1000).openEarly(s2 -> {
                    assertTrue(s.last.compareTo(s2.last) < 0);
                    assertFileCounts(dir.list());
                    s2.selfRef().release();
                    s.selfRef().release();
                });
            });

            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 1);

            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }
            writer.abort();
            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
    }


    @Test
    public void testAbortTxnWithClosedWriterShouldRemoveSSTable() throws InterruptedException
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

            assertFileCounts(dir.list());
            for (int i = 10000; i < 20000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer.append(builder.build().unfilteredIterator());
            }
            SSTableReader sstable = writer.finish(true);
            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 1);

            sstable.selfRef().release();
            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }

            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
    }

    @Test
    public void testAbortTxnWithClosedAndOpenWriterShouldRemoveAllSSTables() throws InterruptedException
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

            assertFileCounts(dir.list());
            for (int i = 10000; i < 20000; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), random(i, 10)).withTimestamp(1);
                for (int j = 0; j < 100; j++)
                    builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
                writer2.append(builder.build().unfilteredIterator());
            }
            SSTableReader sstable = writer1.finish(true);
            txn.update(sstable, false);

            assertFileCounts(dir.list());

            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 2);

            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }
            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
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
                UnfilteredRowIterator rowIter = sstable.iterator(dk,
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

    private static void assertValidRepairMetadata(long repairedAt, UUID pendingRepair, boolean isTransient)
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

    private static void assertInvalidRepairMetadata(long repairedAt, UUID pendingRepair, boolean isTransient)
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
        assertValidRepairMetadata(UNREPAIRED_SSTABLE, UUID.randomUUID(), false);
        assertValidRepairMetadata(UNREPAIRED_SSTABLE, UUID.randomUUID(), true);

        assertInvalidRepairMetadata(UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, true);
        assertInvalidRepairMetadata(1, UUID.randomUUID(), false);
        assertInvalidRepairMetadata(1, NO_PENDING_REPAIR, true);

    }
}
