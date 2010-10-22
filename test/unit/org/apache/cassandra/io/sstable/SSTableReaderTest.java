package org.apache.cassandra.io.sstable;
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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class SSTableReaderTest extends CleanupHelper
{
    static Token t(int i)
    {
        return StorageService.getPartitioner().getToken(ByteBuffer.wrap(String.valueOf(i).getBytes()));
    }

    @Test
    public void testGetPositionsForRanges() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            ByteBuffer key = ByteBuffer.wrap(String.valueOf(j).getBytes());
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard2", null, ByteBuffer.wrap("0".getBytes())), FBUtilities.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMajor(store);

        List<Range> ranges = new ArrayList<Range>();
        // 1 key
        ranges.add(new Range(t(0), t(1)));
        // 2 keys
        ranges.add(new Range(t(2), t(4)));
        // wrapping range from key to end
        ranges.add(new Range(t(6), StorageService.getPartitioner().getMinimumToken()));
        // empty range (should be ignored)
        ranges.add(new Range(t(9), t(91)));

        // confirm that positions increase continuously
        SSTableReader sstable = store.getSSTables().iterator().next();
        long previous = -1;
        for (Pair<Long,Long> section : sstable.getPositionsForRanges(ranges))
        {
            assert previous <= section.left : previous + " ! < " + section.left;
            assert section.left < section.right : section.left + " ! < " + section.right;
            previous = section.right;
        }
    }

    @Test
    public void testSpannedIndexPositions() throws IOException, ExecutionException, InterruptedException
    {
        MmappedSegmentedFile.MAX_SEGMENT_SIZE = 40; // each index entry is ~11 bytes, so this will generate lots of segments

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 100; j += 2)
        {
            ByteBuffer key = ByteBuffer.wrap(String.valueOf(j).getBytes());
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("0".getBytes())), FBUtilities.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMajor(store);

        // check that all our keys are found correctly
        SSTableReader sstable = store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                                                             sstable.descriptor,
                                                             FBUtilities.readShortByteArray(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            assert sstable.getPosition(dk, SSTableReader.Operator.EQ) == -1;
        }
    }

    @Test
    public void testPersistentStatistics() throws IOException, ExecutionException, InterruptedException
    {

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        for (int j = 0; j < 100; j += 2)
        {
            ByteBuffer key = ByteBuffer.wrap(String.valueOf(j).getBytes());
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("0".getBytes())), FBUtilities.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        assert store.getMaxRowSize() != 0;
    }
}
