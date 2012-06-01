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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Pair;

public class SSTableReaderTest extends SchemaLoader
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

        List<Range<Token>> ranges = new ArrayList<Range<Token>>();
        // 1 key
        ranges.add(new Range<Token>(t(0), t(1)));
        // 2 keys
        ranges.add(new Range<Token>(t(2), t(4)));
        // wrapping range from key to end
        ranges.add(new Range<Token>(t(6), StorageService.getPartitioner().getMinimumToken()));
        // empty range (should be ignored)
        ranges.add(new Range<Token>(t(9), t(91)));

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
            FileDataInput file = sstable.getFileDataInput(sstable.getPosition(dk, SSTableReader.Operator.EQ).position);
            DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                                                             sstable.descriptor,
                                                             ByteBufferUtil.readWithShortLength(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            assert sstable.getPosition(dk, SSTableReader.Operator.EQ) == null;
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

        clearAndLoad(store);
        assert store.getMaxRowSize() != 0;
    }

    private void clearAndLoad(ColumnFamilyStore cfs) throws IOException
    {
        cfs.clearUnsafe();
        cfs.loadNewSSTables();
    }

    @Test
    public void testGetPositionsForRangesWithKeyCache() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        CacheService.instance.keyCache.setCapacity(100);

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
        long p2 = sstable.getPosition(k(2), SSTableReader.Operator.EQ).position;
        long p3 = sstable.getPosition(k(3), SSTableReader.Operator.EQ).position;
        long p6 = sstable.getPosition(k(6), SSTableReader.Operator.EQ).position;
        long p7 = sstable.getPosition(k(7), SSTableReader.Operator.EQ).position;

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

        File destDir = Directories.create("Keyspace1", "Indexed1").getDirectoryForNewSSTables(0);
        assert destDir != null;

        FileUtils.createDirectory(destDir);
        for (File srcFile : rootDir.listFiles())
        {
            if (!srcFile.getName().startsWith("Indexed1"))
                continue;
            File destFile = new File(destDir, srcFile.getName());
            CLibrary.createHardLink(srcFile, destFile);

            assert destFile.exists() : destFile.getAbsoluteFile();
        }
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Indexed1");

        // check if opening and querying works
        assertIndexQueryWorks(store);
    }

    @Test
    public void testOpeningSSTable() throws Exception
    {
        String ks = "Keyspace1";
        String cf = "Standard1";

        // clear and create just one sstable for this test
        Table table = Table.open(ks);
        ColumnFamilyStore store = table.getColumnFamilyStore(cf);
        store.clearUnsafe();
        store.disableAutoCompaction();

        DecoratedKey firstKey = null, lastKey = null;
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < DatabaseDescriptor.getIndexInterval(); i++) {
            DecoratedKey key = Util.dk(String.valueOf(i));
            if (firstKey == null)
                firstKey = key;
            if (lastKey == null)
                lastKey = key;
            if (store.metadata.getKeyValidator().compare(lastKey.key, key.key) < 0)
                lastKey = key;
            RowMutation rm = new RowMutation(ks, key.key);
            rm.add(new QueryPath(cf, null, ByteBufferUtil.bytes("col")),
                          ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp);
            rm.apply();
        }
        store.forceBlockingFlush();

        SSTableReader sstable = store.getSSTables().iterator().next();
        Descriptor desc = sstable.descriptor;

        // test to see if sstable can be opened as expected
        SSTableReader target = SSTableReader.open(desc);
        Collection<DecoratedKey> keySamples = target.getKeySamples();
        assert keySamples.size() == 1 && keySamples.iterator().next().equals(firstKey);
        assert target.first.equals(firstKey);
        assert target.last.equals(lastKey);
    }

    @Test
    public void testLoadingSummaryUsesCorrectPartitioner() throws Exception
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Indexed1");
        ByteBuffer key = ByteBufferUtil.bytes(String.valueOf("k1"));
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), System.currentTimeMillis());
        rm.apply();
        store.forceBlockingFlush();

        ColumnFamilyStore indexCfs = store.indexManager.getIndexForColumn(ByteBufferUtil.bytes("birthdate")).getIndexCfs();
        assert indexCfs.partitioner instanceof LocalPartitioner;
        SSTableReader sstable = indexCfs.getSSTables().iterator().next();
        assert sstable.first.token instanceof LocalToken;

        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = sstable.compression
                                          ? SegmentedFile.getCompressedBuilder()
                                          : SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        SSTableReader.saveSummary(sstable, ibuilder, dbuilder);

        SSTableReader reopened = SSTableReader.open(sstable.descriptor);
        assert reopened.first.token instanceof LocalToken;
    }

    private void assertIndexQueryWorks(ColumnFamilyStore indexedCFS) throws IOException
    {
        assert "Indexed1".equals(indexedCFS.getColumnFamilyName());

        // make sure all sstables including 2ary indexes load from disk
        for (ColumnFamilyStore cfs : indexedCFS.concatWithIndexes())
            clearAndLoad(cfs);

        // query using index to see if sstable for secondary index opens
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = indexedCFS.search(clause, range, 100, new IdentityQueryFilter());
        assert rows.size() == 1;
    }

    private List<Range<Token>> makeRanges(Token left, Token right)
    {
        return Arrays.<Range<Token>>asList(new Range[]{ new Range<Token>(left, right) });
    }

    private DecoratedKey k(int i)
    {
        return new DecoratedKey(t(i), ByteBufferUtil.bytes(String.valueOf(i)));
    }
}
