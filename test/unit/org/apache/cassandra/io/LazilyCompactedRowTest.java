package org.apache.cassandra.io;
/*
 * 
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
 * 
 */


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.CollatingOrderPreservingPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;


public class LazilyCompactedRowTest extends CleanupHelper
{
    private void assertBytes(ColumnFamilyStore cfs, int gcBefore, boolean major) throws IOException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionIterator ci1 = new CompactionIterator(cfs, sstables, gcBefore, major);
        LazyCompactionIterator ci2 = new LazyCompactionIterator(cfs, sstables, gcBefore, major);

        while (true)
        {
            if (!ci1.hasNext())
            {
                assert !ci2.hasNext();
                break;
            }

            AbstractCompactedRow row1 = ci1.next();
            AbstractCompactedRow row2 = ci2.next();
            DataOutputBuffer out1 = new DataOutputBuffer();
            DataOutputBuffer out2 = new DataOutputBuffer();
            row1.write(out1);
            row2.write(out2);
            DataInputStream in1 = new DataInputStream(new ByteArrayInputStream(out1.getData(), 0, out1.getLength()));
            DataInputStream in2 = new DataInputStream(new ByteArrayInputStream(out2.getData(), 0, out2.getLength()));

            // key isn't part of what CompactedRow writes, that's done by SSTW.append

            // row size can differ b/c of bloom filter counts being different
            long rowSize1 = SSTableReader.readRowSize(in1, sstables.iterator().next().getDescriptor());
            long rowSize2 = SSTableReader.readRowSize(in2, sstables.iterator().next().getDescriptor());
            assertEquals(out1.getLength(), rowSize1 + 8);
            assertEquals(out2.getLength(), rowSize2 + 8);
            // bloom filter
            IndexHelper.defreezeBloomFilter(in1);
            IndexHelper.defreezeBloomFilter(in2);
            // index
            int indexSize1 = in1.readInt();
            int indexSize2 = in2.readInt();
            assertEquals(indexSize1, indexSize2);
            byte[] bytes1 = new byte[indexSize1];
            byte[] bytes2 = new byte[indexSize2];
            in1.readFully(bytes1);
            in2.readFully(bytes2);
            assert Arrays.equals(bytes1, bytes2);
            // cf metadata
            ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
            ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf1, in1);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf2, in2);
            assert cf1.getLocalDeletionTime() == cf2.getLocalDeletionTime();
            assert cf1.getMarkedForDeleteAt().equals(cf2.getMarkedForDeleteAt());   
            // columns
            int columns = in1.readInt();
            assert columns == in2.readInt();
            for (int i = 0; i < columns; i++)
            {
                IColumn c1 = cf1.getColumnSerializer().deserialize(in1);
                IColumn c2 = cf2.getColumnSerializer().deserialize(in2);
                assert c1.equals(c2);
            }
            // that should be everything
            assert in1.available() == 0;
            assert in2.available() == 0;
        }
    }

    @Test
    public void testOneRow() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        byte[] key = "k".getBytes();
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, "c".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testOneRowTwoColumns() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        byte[] key = "k".getBytes();
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, "c".getBytes()), new byte[0], new TimestampClock(0));
        rm.add(new QueryPath("Standard1", null, "d".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testTwoRows() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        byte[] key = "k".getBytes();
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, "c".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testTwoRowsTwoColumns() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        byte[] key = "k".getBytes();
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, "c".getBytes()), new byte[0], new TimestampClock(0));
        rm.add(new QueryPath("Standard1", null, "d".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testManyRows() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        for (int j = 0; j < (SSTableReader.indexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                byte[] key = String.valueOf(i % 2).getBytes();
                RowMutation rm = new RowMutation("Keyspace1", key);
                rm.add(new QueryPath("Standard1", null, String.valueOf(i / 2).getBytes()), new byte[0], new TimestampClock(j * ROWS_PER_SSTABLE + i));
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    private static class LazyCompactionIterator extends CompactionIterator
    {
        private final ColumnFamilyStore cfStore;

        public LazyCompactionIterator(ColumnFamilyStore cfStore, Iterable<SSTableReader> sstables, int gcBefore, boolean major) throws IOException
        {
            super(cfStore, sstables, gcBefore, major);
            this.cfStore = cfStore;
        }

        @Override
        protected AbstractCompactedRow getCompactedRow()
        {
            return new LazilyCompactedRow(cfStore, rows, true, Integer.MAX_VALUE);
        }
    }
}
