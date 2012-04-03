/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.MappedFileDataInput;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.Assert.assertEquals;


public class LazilyCompactedRowTest extends SchemaLoader
{
    private static void assertBytes(ColumnFamilyStore cfs, int gcBefore) throws IOException
    {
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
        Collection<SSTableReader> sstables = cfs.getSSTables();

        // compare eager and lazy compactions
        AbstractCompactionIterable eager = new CompactionIterable(OperationType.UNKNOWN,
                                                                  strategy.getScanners(sstables),
                                                                  new PreCompactingController(cfs, sstables, gcBefore, false));
        AbstractCompactionIterable lazy = new CompactionIterable(OperationType.UNKNOWN,
                                                                 strategy.getScanners(sstables),
                                                                 new LazilyCompactingController(cfs, sstables, gcBefore, false));
        assertBytes(cfs, sstables, eager, lazy);

        // compare eager and parallel-lazy compactions
        eager = new CompactionIterable(OperationType.UNKNOWN,
                                       strategy.getScanners(sstables),
                                       new PreCompactingController(cfs, sstables, gcBefore, false));
        AbstractCompactionIterable parallel = new ParallelCompactionIterable(OperationType.UNKNOWN,
                                                                             strategy.getScanners(sstables),
                                                                             new CompactionController(cfs, sstables, gcBefore, false),
                                                                             0);
        assertBytes(cfs, sstables, eager, parallel);
    }

    private static void assertBytes(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, AbstractCompactionIterable ci1, AbstractCompactionIterable ci2) throws IOException
    {
        CloseableIterator<AbstractCompactedRow> iter1 = ci1.iterator();
        CloseableIterator<AbstractCompactedRow> iter2 = ci2.iterator();

        while (true)
        {
            if (!iter1.hasNext())
            {
                assert !iter2.hasNext();
                break;
            }

            AbstractCompactedRow row1 = iter1.next();
            AbstractCompactedRow row2 = iter2.next();
            DataOutputBuffer out1 = new DataOutputBuffer();
            DataOutputBuffer out2 = new DataOutputBuffer();
            row1.write(out1);
            row2.write(out2);

            File tmpFile1 = File.createTempFile("lcrt1", null);
            File tmpFile2 = File.createTempFile("lcrt2", null);

            tmpFile1.deleteOnExit();
            tmpFile2.deleteOnExit();

            new FileOutputStream(tmpFile1).write(out1.getData()); // writing data from row1
            new FileOutputStream(tmpFile2).write(out2.getData()); // writing data from row2

            MappedFileDataInput in1 = new MappedFileDataInput(new FileInputStream(tmpFile1), tmpFile1.getAbsolutePath(), 0, 0);
            MappedFileDataInput in2 = new MappedFileDataInput(new FileInputStream(tmpFile2), tmpFile2.getAbsolutePath(), 0, 0);

            // key isn't part of what CompactedRow writes, that's done by SSTW.append

            // row size can differ b/c of bloom filter counts being different
            long rowSize1 = SSTableReader.readRowSize(in1, sstables.iterator().next().descriptor);
            long rowSize2 = SSTableReader.readRowSize(in2, sstables.iterator().next().descriptor);
            assertEquals(rowSize1 + 8, out1.getLength());
            assertEquals(rowSize2 + 8, out2.getLength());

            // cf metadata
            ColumnFamily cf1 = ColumnFamily.create(cfs.metadata);
            ColumnFamily cf2 = ColumnFamily.create(cfs.metadata);
            cf1.delete(DeletionInfo.serializer().deserializeFromSSTable(in1, Descriptor.Version.CURRENT));
            cf2.delete(DeletionInfo.serializer().deserializeFromSSTable(in2, Descriptor.Version.CURRENT));
            assert cf1.deletionInfo().equals(cf2.deletionInfo());
            // columns
            int columns = in1.readInt();
            assert columns == in2.readInt();
            for (int i = 0; i < columns; i++)
            {
                IColumn c1 = (IColumn)cf1.getOnDiskSerializer().deserializeFromSSTable(in1, Descriptor.Version.CURRENT);
                IColumn c2 = (IColumn)cf2.getOnDiskSerializer().deserializeFromSSTable(in2, Descriptor.Version.CURRENT);
                assert c1.equals(c2) : c1.getString(cfs.metadata.comparator) + " != " + c2.getString(cfs.metadata.comparator);
            }
            // that should be everything
            assert in1.available() == 0;
            assert in2.available() == 0;
        }
    }

    private void assertDigest(ColumnFamilyStore cfs, int gcBefore) throws IOException, NoSuchAlgorithmException
    {
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
        Collection<SSTableReader> sstables = cfs.getSSTables();
        AbstractCompactionIterable ci1 = new CompactionIterable(OperationType.UNKNOWN, strategy.getScanners(sstables), new PreCompactingController(cfs, sstables, gcBefore, false));
        AbstractCompactionIterable ci2 = new CompactionIterable(OperationType.UNKNOWN, strategy.getScanners(sstables), new LazilyCompactingController(cfs, sstables, gcBefore, false));
        CloseableIterator<AbstractCompactedRow> iter1 = ci1.iterator();
        CloseableIterator<AbstractCompactedRow> iter2 = ci2.iterator();

        while (true)
        {
            if (!iter1.hasNext())
            {
                assert !iter2.hasNext();
                break;
            }

            AbstractCompactedRow row1 = iter1.next();
            AbstractCompactedRow row2 = iter2.next();
            MessageDigest digest1 = MessageDigest.getInstance("MD5");
            MessageDigest digest2 = MessageDigest.getInstance("MD5");

            row1.update(digest1);
            row2.update(digest2);

            assert MessageDigest.isEqual(digest1.digest(), digest2.digest());
        }
    }

    @Test
    public void testOneRow() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testOneRowTwoColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("d")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testOneRowManyColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBuffer.wrap("k".getBytes());
        RowMutation rm = new RowMutation("Keyspace1", key);
        for (int i = 0; i < 1000; i++)
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        DataOutputBuffer out = new DataOutputBuffer();
        RowMutation.serializer.serialize(rm, out, MessagingService.current_version);
        assert out.getLength() > DatabaseDescriptor.getColumnIndexSize();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testTwoRows() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testTwoRowsTwoColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("d")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testManyRows() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        for (int j = 0; j < (DatabaseDescriptor.getIndexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(i % 2));
                RowMutation rm = new RowMutation("Keyspace1", key);
                rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(String.valueOf(i / 2))), ByteBufferUtil.EMPTY_BYTE_BUFFER, j * ROWS_PER_SSTABLE + i);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        assertBytes(cfs, Integer.MAX_VALUE);
        assertDigest(cfs, Integer.MAX_VALUE);
    }

    @Test
    public void testTwoRowSuperColumn() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace4");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Super5");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace4", key);
        ByteBuffer scKey = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress())));
        rm.add(new QueryPath("Super5", scKey , ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE);
    }


    private static class LazilyCompactingController extends CompactionController
    {
        public LazilyCompactingController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, boolean forceDeserialize)
        {
            super(cfs, sstables, gcBefore, forceDeserialize);
        }

        @Override
        public AbstractCompactedRow getCompactedRow(List<SSTableIdentityIterator> rows)
        {
            return new LazilyCompactedRow(this, rows);
        }
    }

    private static class PreCompactingController extends CompactionController
    {
        public PreCompactingController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, boolean forceDeserialize)
        {
            super(cfs, sstables, gcBefore, forceDeserialize);
        }

        @Override
        public AbstractCompactedRow getCompactedRow(List<SSTableIdentityIterator> rows)
        {
            return new PrecompactedRow(this, rows);
        }
    }
}
