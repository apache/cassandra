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


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Pair;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class SSTableReaderTest extends CleanupHelper
{
    static Token t(int i)
    {
        return StorageService.getPartitioner().getToken(ByteBufferUtil.bytes(String.valueOf(i)));
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
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes("0")), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMaximal(store);

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
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("0")), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMaximal(store);

        // check that all our keys are found correctly
        SSTableReader sstable = store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                                                             sstable.descriptor,
                                                             ByteBufferUtil.readWithShortLength(file));
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
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("0")), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();

        store.clearUnsafe();
        store.loadNewSSTables();
        assert store.getMaxRowSize() != 0;
    }

    @Test
    public void testGetPositionsForRangesWithKeyCache() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        store.getKeyCache().setCapacity(100);

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes("0")), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMaximal(store);

        SSTableReader sstable = store.getSSTables().iterator().next();
        long p2 = sstable.getPosition(k(2), SSTableReader.Operator.EQ);
        long p3 = sstable.getPosition(k(3), SSTableReader.Operator.EQ);
        long p6 = sstable.getPosition(k(6), SSTableReader.Operator.EQ);
        long p7 = sstable.getPosition(k(7), SSTableReader.Operator.EQ);

        Pair<Long, Long> p = sstable.getPositionsForRanges(makeRanges(t(2), t(6))).iterator().next();

        // range are start exclusive so we should start at 3
        assert p.left == p3;

        // to capture 6 we have to stop at the start of 7
        assert p.right == p7;
    }

    @Test
    public void testPersistentStatisticsWithSecondaryIndex() throws IOException, ExecutionException, InterruptedException
    {
        // Create secondary index and flush to disk
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Indexed1");
        ByteBuffer key = ByteBufferUtil.bytes(String.valueOf("k1"));
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), System.currentTimeMillis());
        rm.apply();
        store.forceBlockingFlush();

        // check if opening and querying works
        assertIndexQueryWorks(store);
    }

    @Test
    public void testPersistentStatisticsFromOlderIndexedSSTable() throws IOException, ExecutionException, InterruptedException
    {
        // copy legacy indexed sstables
        String root = System.getProperty("legacy-sstable-root");
        assert root != null;
        File rootDir = new File(root + File.separator + "hb" + File.separator + "Keyspace1");
        assert rootDir.isDirectory();

        String[] destDirs = DatabaseDescriptor.getAllDataFileLocationsForTable("Keyspace1");
        assert destDirs != null;
        assert destDirs.length > 0;

        FileUtils.createDirectory(destDirs[0]);
        for (File srcFile : rootDir.listFiles())
        {
            if (!srcFile.getName().startsWith("Indexed1"))
                continue;
            File destFile = new File(destDirs[0] + File.separator + srcFile.getName());
            CLibrary.createHardLinkWithExec(srcFile, destFile);

            destFile = new File(destDirs[0] + File.separator + srcFile.getName());

            assert destFile.exists() : destFile.getAbsoluteFile();
        }
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Indexed1");

        // check if opening and querying works
        assertIndexQueryWorks(store);
    }

    private void assertIndexQueryWorks(ColumnFamilyStore indexedCFS)
    {
        assert "Indexed1".equals(indexedCFS.getColumnFamilyName());

        // make sure all sstables including 2ary indexes load from disk
        indexedCFS.clearUnsafe();
        for (ColumnFamilyStore indexCfs : indexedCFS.indexManager.getIndexesBackedByCfs())
        {
            indexCfs.clearUnsafe();
            indexCfs.loadNewSSTables(); // v1.0.4 would fail here (see CASSANDRA-3540)
        }
        indexedCFS.loadNewSSTables();

        // query using index to see if sstable for secondary index opens
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = indexedCFS.search(clause, range, new IdentityQueryFilter());
        assert rows.size() == 1;
    }

    private List<Range> makeRanges(Token left, Token right)
    {
        return Arrays.asList(new Range[]{ new Range(left, right) });
    }

    private DecoratedKey k(int i)
    {
        return new DecoratedKey(t(i), ByteBufferUtil.bytes(String.valueOf(i)));
    }
}
