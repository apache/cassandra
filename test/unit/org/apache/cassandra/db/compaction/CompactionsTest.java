/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CompactionsTest extends SchemaLoader
{
    private static final String STANDARD1 = "Standard1";
    public static final String KEYSPACE1 = "Keyspace1";

    public ColumnFamilyStore testSingleSSTableCompaction(String strategyClassName) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(STANDARD1);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.setCompactionStrategyClass(strategyClassName);

        // disable compaction while flushing
        store.disableAutoCompaction();

        long timestamp = populate(KEYSPACE1, STANDARD1, 0, 9, 3); //ttl=3s

        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        long originalSize = store.getSSTables().iterator().next().uncompressedLength();

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // enable compaction, submit background and wait for it to complete
        store.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        // and sstable with ttl should be compacted
        assertEquals(1, store.getSSTables().size());
        long size = store.getSSTables().iterator().next().uncompressedLength();
        assertTrue("should be less than " + originalSize + ", but was " + size, size < originalSize);

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        assertMaxTimestamp(store, timestamp);

        return store;
    }

    private long populate(String ks, String cf, int startRowKey, int endRowKey, int ttl) {
        long timestamp = System.currentTimeMillis();
        for (int i = startRowKey; i <= endRowKey; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(ks, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add(cf,  Util.cellname(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       j > 0 ? ttl : 0); // let first column never expire, since deleting all columns does not produce sstable
            rm.apply();
        }
        return timestamp;
    }

    /**
     * Test to see if sstable has enough expired columns, it is compacted itself.
     */
    @Test
    public void testSingleSSTableCompactionWithSizeTieredCompaction() throws Exception
    {
        testSingleSSTableCompaction(SizeTieredCompactionStrategy.class.getCanonicalName());
    }

    @Test
    public void testSingleSSTableCompactionWithLeveledCompaction() throws Exception
    {
        ColumnFamilyStore store = testSingleSSTableCompaction(LeveledCompactionStrategy.class.getCanonicalName());
        WrappingCompactionStrategy strategy = (WrappingCompactionStrategy) store.getCompactionStrategy();
        // tombstone removal compaction should not promote level
        assert strategy.getSSTableCountPerLevel()[0] == 1;
    }

    @Test
    public void testSuperColumnTombstones() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Super1");
        cfs.disableAutoCompaction();

        DecoratedKey key = Util.dk("tskey");
        ByteBuffer scName = ByteBufferUtil.bytes("TestSuperColumn");

        // a subcolumn
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        rm.add("Super1", Util.cellname(scName, ByteBufferUtil.bytes(0)),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               FBUtilities.timestampMicros());
        rm.apply();
        cfs.forceBlockingFlush();

        // shadow the subcolumn with a supercolumn tombstone
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.deleteRange("Super1", SuperColumns.startOf(scName), SuperColumns.endOf(scName), FBUtilities.timestampMicros());
        rm.apply();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performMaximal(cfs);
        assertEquals(1, cfs.getSSTables().size());

        // check that the shadowed column is gone
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(key, sstable.partitioner.getMinimumToken().maxKeyBound());
        ISSTableScanner scanner = sstable.getScanner(new DataRange(bounds, new IdentityQueryFilter()));
        OnDiskAtomIterator iter = scanner.next();
        assertEquals(key, iter.getKey());
        assert iter.next() instanceof RangeTombstone;
        assert !iter.hasNext();
    }

    @Test
    public void testUncheckedTombstoneSizeTieredCompaction() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(STANDARD1);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.metadata.compactionStrategyOptions.put("tombstone_compaction_interval", "1");
        store.metadata.compactionStrategyOptions.put("unchecked_tombstone_compaction", "false");
        store.reload();
        store.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getName());

        // disable compaction while flushing
        store.disableAutoCompaction();

        //Populate sstable1 with with keys [0..9]
        populate(KEYSPACE1, STANDARD1, 0, 9, 3); //ttl=3s
        store.forceBlockingFlush();

        //Populate sstable2 with with keys [10..19] (keys do not overlap with SSTable1)
        long timestamp2 = populate(KEYSPACE1, STANDARD1, 10, 19, 3); //ttl=3s
        store.forceBlockingFlush();

        assertEquals(2, store.getSSTables().size());

        Iterator<SSTableReader> it = store.getSSTables().iterator();
        long originalSize1 = it.next().uncompressedLength();
        long originalSize2 = it.next().uncompressedLength();

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // enable compaction, submit background and wait for it to complete
        store.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        // even though both sstables were candidate for tombstone compaction
        // it was not executed because they have an overlapping token range
        assertEquals(2, store.getSSTables().size());
        it = store.getSSTables().iterator();
        long newSize1 = it.next().uncompressedLength();
        long newSize2 = it.next().uncompressedLength();
        assertEquals("candidate sstable should not be tombstone-compacted because its key range overlap with other sstable",
                      originalSize1, newSize1);
        assertEquals("candidate sstable should not be tombstone-compacted because its key range overlap with other sstable",
                      originalSize2, newSize2);

        // now let's enable the magic property
        store.metadata.compactionStrategyOptions.put("unchecked_tombstone_compaction", "true");
        store.reload();

        //submit background task again and wait for it to complete
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        //we still have 2 sstables, since they were not compacted against each other
        assertEquals(2, store.getSSTables().size());
        it = store.getSSTables().iterator();
        newSize1 = it.next().uncompressedLength();
        newSize2 = it.next().uncompressedLength();
        assertTrue("should be less than " + originalSize1 + ", but was " + newSize1, newSize1 < originalSize1);
        assertTrue("should be less than " + originalSize2 + ", but was " + newSize2, newSize2 < originalSize2);

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        assertMaxTimestamp(store, timestamp2);
    }

    public static void assertMaxTimestamp(ColumnFamilyStore cfs, long maxTimestampExpected)
    {
        long maxTimestampObserved = Long.MIN_VALUE;
        for (SSTableReader sstable : cfs.getSSTables())
            maxTimestampObserved = Math.max(sstable.getMaxTimestamp(), maxTimestampObserved);
        assertEquals(maxTimestampExpected, maxTimestampObserved);
    }

    @Test
    public void testEchoedRow()
    {
        // This test check that EchoedRow doesn't skipp rows: see CASSANDRA-2653

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        // Insert 4 keys in two sstables. We need the sstables to have 2 rows
        // at least to trigger what was causing CASSANDRA-2653
        for (int i=1; i < 5; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add("Standard2", Util.cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
            rm.apply();

            if (i % 2 == 0)
                cfs.forceBlockingFlush();
        }
        Collection<SSTableReader> toCompact = cfs.getSSTables();
        assert toCompact.size() == 2;

        // Reinserting the same keys. We will compact only the previous sstable, but we need those new ones
        // to make sure we use EchoedRow, otherwise it won't be used because purge can be done.
        for (int i=1; i < 5; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add("Standard2", Util.cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        SSTableReader tmpSSTable = null;
        for (SSTableReader sstable : cfs.getSSTables())
            if (!toCompact.contains(sstable))
                tmpSSTable = sstable;
        assert tmpSSTable != null;

        // Force compaction on first sstables. Since each row is in only one sstable, we will be using EchoedRow.
        Util.compact(cfs, toCompact);
        assertEquals(2, cfs.getSSTables().size());

        // Now, we remove the sstable that was just created to force the use of EchoedRow (so that it doesn't hide the problem)
        cfs.markObsolete(Collections.singleton(tmpSSTable), OperationType.UNKNOWN);
        assertEquals(1, cfs.getSSTables().size());

        // Now assert we do have the 4 keys
        assertEquals(4, Util.getRangeSlice(cfs).size());
    }

    @Test
    public void testDontPurgeAccidentaly() throws InterruptedException
    {
        testDontPurgeAccidentaly("test1", "Super5");

        // Use CF with gc_grace=0, see last bug of CASSANDRA-2786
        testDontPurgeAccidentaly("test1", "SuperDirectGC");
    }

    @Test
    public void testUserDefinedCompaction() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final String cfname = "Standard3"; // use clean(no sstable) CF
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        final int ROWS_PER_SSTABLE = 10;
        for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
            DecoratedKey key = Util.dk(String.valueOf(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(cfname, Util.cellname("col"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   System.currentTimeMillis());
            rm.apply();
        }
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstables = cfs.getSSTables();

        assert sstables.size() == 1;
        SSTableReader sstable = sstables.iterator().next();

        int prevGeneration = sstable.descriptor.generation;
        String file = new File(sstable.descriptor.filenameFor(Component.DATA)).getName();
        // submit user defined compaction on flushed sstable
        CompactionManager.instance.forceUserDefinedCompaction(file);
        // wait until user defined compaction finishes
        do
        {
            Thread.sleep(100);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);
        // CF should have only one sstable with generation number advanced
        sstables = cfs.getSSTables();
        assert sstables.size() == 1;
        assert sstables.iterator().next().descriptor.generation == prevGeneration + 1;
    }

    @Test
    public void testRangeTombstones() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");
        cfs.clearUnsafe();

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        final CFMetaData cfmeta = cfs.metadata;
        Directories dir = cfs.directories;

        ArrayList<DecoratedKey> keys = new ArrayList<DecoratedKey>();

        for (int i=0; i < 4; i++)
        {
            keys.add(Util.dk(""+i));
        }

        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfmeta);
        cf.addColumn(Util.column("01", "a", 1)); // this must not resurrect
        cf.addColumn(Util.column("a", "a", 3));
        cf.deletionInfo().add(new RangeTombstone(Util.cellname("0"), Util.cellname("b"), 2, (int) (System.currentTimeMillis()/1000)),cfmeta.comparator);

        SSTableWriter writer = new SSTableWriter(cfs.getTempSSTablePath(dir.getDirectoryForNewSSTables()),
                                                 0,
                                                 0,
                                                 cfs.metadata,
                                                 StorageService.getPartitioner(),
                                                 new MetadataCollector(cfs.metadata.comparator));


        writer.append(Util.dk("0"), cf);
        writer.append(Util.dk("1"), cf);
        writer.append(Util.dk("3"), cf);

        cfs.addSSTable(writer.closeAndOpenReader());
        writer = new SSTableWriter(cfs.getTempSSTablePath(dir.getDirectoryForNewSSTables()),
                                   0,
                                   0,
                                   cfs.metadata,
                                   StorageService.getPartitioner(),
                                   new MetadataCollector(cfs.metadata.comparator));

        writer.append(Util.dk("0"), cf);
        writer.append(Util.dk("1"), cf);
        writer.append(Util.dk("2"), cf);
        writer.append(Util.dk("3"), cf);
        cfs.addSSTable(writer.closeAndOpenReader());

        Collection<SSTableReader> toCompact = cfs.getSSTables();
        assert toCompact.size() == 2;

        // Force compaction on first sstables. Since each row is in only one sstable, we will be using EchoedRow.
        Util.compact(cfs, toCompact);
        assertEquals(1, cfs.getSSTables().size());

        // Now assert we do have the 4 keys
        assertEquals(4, Util.getRangeSlice(cfs).size());

        ArrayList<DecoratedKey> k = new ArrayList<DecoratedKey>();
        for (Row r : Util.getRangeSlice(cfs))
        {
            k.add(r.key);
            assertEquals(ByteBufferUtil.bytes("a"),r.cf.getColumn(Util.cellname("a")).value());
            assertNull(r.cf.getColumn(Util.cellname("01")));
            assertEquals(3,r.cf.getColumn(Util.cellname("a")).timestamp());
        }

        for (SSTableReader sstable : cfs.getSSTables())
        {
            StatsMetadata stats = sstable.getSSTableMetadata();
            assertEquals(ByteBufferUtil.bytes("0"), stats.minColumnNames.get(0));
            assertEquals(ByteBufferUtil.bytes("b"), stats.maxColumnNames.get(0));
        }

        assertEquals(keys, k);
    }

    @Test
    public void testCompactionLog() throws Exception
    {
        SystemKeyspace.discardCompactionsInProgress();

        String cf = "Standard4";
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(cf);
        insertData(KEYSPACE1, cf, 0, 1);
        cfs.forceBlockingFlush();

        Collection<SSTableReader> sstables = cfs.getSSTables();
        assert !sstables.isEmpty();
        Set<Integer> generations = Sets.newHashSet(Iterables.transform(sstables, new Function<SSTableReader, Integer>()
        {
            public Integer apply(SSTableReader sstable)
            {
                return sstable.descriptor.generation;
            }
        }));
        UUID taskId = SystemKeyspace.startCompaction(cfs, sstables);
        Map<Pair<String, String>, Map<Integer, UUID>> compactionLogs = SystemKeyspace.getUnfinishedCompactions();
        Set<Integer> unfinishedCompactions = compactionLogs.get(Pair.create(KEYSPACE1, cf)).keySet();
        assert unfinishedCompactions.containsAll(generations);

        SystemKeyspace.finishCompaction(taskId);
        compactionLogs = SystemKeyspace.getUnfinishedCompactions();
        assert !compactionLogs.containsKey(Pair.create(KEYSPACE1, cf));
    }

    private void testDontPurgeAccidentaly(String k, String cfname) throws InterruptedException
    {
        // This test catches the regression of CASSANDRA-2786
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        // disable compaction while flushing
        cfs.clearUnsafe();
        cfs.disableAutoCompaction();

        // Add test row
        DecoratedKey key = Util.dk(k);
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        rm.add(cfname, Util.cellname(ByteBufferUtil.bytes("sc"), ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();

        cfs.forceBlockingFlush();

        Collection<SSTableReader> sstablesBefore = cfs.getSSTables();

        QueryFilter filter = QueryFilter.getIdentityFilter(key, cfname, System.currentTimeMillis());
        assertTrue(cfs.getColumnFamily(filter).hasColumns());

        // Remove key
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.delete(cfname, 2);
        rm.apply();

        ColumnFamily cf = cfs.getColumnFamily(filter);
        assertTrue("should be empty: " + cf, cf == null || !cf.hasColumns());

        // Sleep one second so that the removal is indeed purgeable even with gcgrace == 0
        Thread.sleep(1000);

        cfs.forceBlockingFlush();

        Collection<SSTableReader> sstablesAfter = cfs.getSSTables();
        Collection<SSTableReader> toCompact = new ArrayList<SSTableReader>();
        for (SSTableReader sstable : sstablesAfter)
            if (!sstablesBefore.contains(sstable))
                toCompact.add(sstable);

        Util.compact(cfs, toCompact);

        cf = cfs.getColumnFamily(filter);
        assertTrue("should be empty: " + cf, cf == null || !cf.hasColumns());
    }

    private static Range<Token> rangeFor(int start, int end)
    {
        return new Range<Token>(new BytesToken(String.format("%03d", start).getBytes()),
                                new BytesToken(String.format("%03d", end).getBytes()));
    }

    private static Collection<Range<Token>> makeRanges(int ... keys)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(keys.length / 2);
        for (int i = 0; i < keys.length; i += 2)
            ranges.add(rangeFor(keys[i], keys[i + 1]));
        return ranges;
    }

    private static void insertRowWithKey(int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey decoratedKey = Util.dk(String.format("%03d", key));
        Mutation rm = new Mutation(KEYSPACE1, decoratedKey.getKey());
        rm.add("Standard1", Util.cellname("col"), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, 1000);
        rm.apply();
    }

    @Test
    public void testNeedsCleanup()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        // write three groups of 9 keys: 001, 002, ... 008, 009
        //                               101, 102, ... 108, 109
        //                               201, 202, ... 208, 209
        for (int i = 1; i < 10; i++)
        {
            insertRowWithKey(i);
            insertRowWithKey(i + 100);
            insertRowWithKey(i + 200);
        }
        store.forceBlockingFlush();

        assertEquals(1, store.getSSTables().size());
        SSTableReader sstable = store.getSSTables().iterator().next();


        // contiguous range spans all data
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 209)));
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 210)));

        // separate ranges span all data
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                       100, 109,
                                                                       200, 209)));
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 109,
                                                                       200, 210)));
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                       100, 210)));

        // one range is missing completely
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(100, 109,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      100, 109)));


        // the beginning of one range is missing
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(1, 9,
                                                                      100, 109,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      101, 109,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      100, 109,
                                                                      201, 209)));

        // the end of one range is missing
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 8,
                                                                      100, 109,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      100, 108,
                                                                      200, 209)));
        assertTrue(CompactionManager.needsCleanup(sstable, makeRanges(0, 9,
                                                                      100, 109,
                                                                      200, 208)));

        // some ranges don't contain any data
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 0,
                                                                       0, 9,
                                                                       50, 51,
                                                                       100, 109,
                                                                       150, 199,
                                                                       200, 209,
                                                                       300, 301)));
        // same case, but with a middle range not covering some of the existing data
        assertFalse(CompactionManager.needsCleanup(sstable, makeRanges(0, 0,
                                                                       0, 9,
                                                                       50, 51,
                                                                       100, 103,
                                                                       150, 199,
                                                                       200, 209,
                                                                       300, 301)));
    }
}
