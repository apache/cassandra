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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CompactionsTest extends SchemaLoader
{
    private static final String STANDARD1 = "Standard1";
    public static final String TABLE1 = "Keyspace1";

    @Test
    public void testBlacklistingWithSizeTieredCompactionStrategy() throws Exception
    {
        testBlacklisting(SizeTieredCompactionStrategy.class.getCanonicalName());
    }

    @Test
    public void testBlacklistingWithLeveledCompactionStrategy() throws Exception
    {
        testBlacklisting(LeveledCompactionStrategy.class.getCanonicalName());
    }

    public ColumnFamilyStore testSingleSSTableCompaction(String strategyClassName) throws Exception
    {
        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore(STANDARD1);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.setCompactionStrategyClass(strategyClassName);

        // disable compaction while flushing
        store.disableAutoCompaction();

        long timestamp = populate(STANDARD1, TABLE1, 0, 9, 3); //ttl=3s
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
            RowMutation rm = new RowMutation(cf, key.key);
            for (int j = 0; j < 10; j++)
                rm.add(new QueryPath(ks, null, ByteBufferUtil.bytes(Integer.toString(j))),
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
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) store.getCompactionStrategy();
        // tombstone removal compaction should not promote level
        assert strategy.getLevelSize(0) == 1;
    }

    @Test
    public void testSuperColumnTombstones() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Super1");
        cfs.disableAutoCompaction();

        DecoratedKey key = Util.dk("tskey");
        ByteBuffer scName = ByteBufferUtil.bytes("TestSuperColumn");

        // a subcolumn
        RowMutation rm = new RowMutation(TABLE1, key.key);
        rm.add(new QueryPath("Super1", scName, ByteBufferUtil.bytes(0)),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               FBUtilities.timestampMicros());
        rm.apply();
        cfs.forceBlockingFlush();

        // shadow the subcolumn with a supercolumn tombstone
        rm = new RowMutation(TABLE1, key.key);
        rm.delete(new QueryPath("Super1", scName), FBUtilities.timestampMicros());
        rm.apply();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performMaximal(cfs);
        assertEquals(1, cfs.getSSTables().size());

        // check that the shadowed column is gone
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        SSTableScanner scanner = sstable.getScanner(new QueryFilter(null, new QueryPath("Super1", scName), new IdentityQueryFilter()));
        scanner.seekTo(key);
        OnDiskAtomIterator iter = scanner.next();
        assertEquals(key, iter.getKey());
        SuperColumn sc = (SuperColumn) iter.next();
        assert sc.getSubColumns().isEmpty();
    }

    @Test
    public void testUncheckedTombstoneSizeTieredCompaction() throws Exception
    {
        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore(STANDARD1);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.metadata.compactionStrategyOptions.put("tombstone_compaction_interval", "1");
        store.metadata.compactionStrategyOptions.put("unchecked_tombstone_compaction", "false");
        store.reload();
        store.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getName());

        // disable compaction while flushing
        store.disableAutoCompaction();

        //Populate sstable1 with with keys [0..9]
        populate(STANDARD1, TABLE1, 0, 9, 3); //ttl=3s
        store.forceBlockingFlush();

        //Populate sstable2 with with keys [10..19] (keys do not overlap with SSTable1)
        long timestamp2 = populate(STANDARD1, TABLE1, 10, 19, 3); //ttl=3s
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

    /*
     * Tombstone compaction only makes sense for L>0 sstables,
     * because if there are L0 sstables that can be compacted,
     * they will be preferred instead of tombstone compaction
     */
    @Test
    public void testUncheckedTombstoneCompactionLeveledCompaction() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Table table = Table.open(ksname);
        ColumnFamilyStore store = table.getColumnFamilyStore(cfname);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.metadata.compactionStrategyOptions.put("tombstone_compaction_interval", "1");
        store.metadata.compactionStrategyOptions.put("unchecked_tombstone_compaction", "false");
        store.reload();

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;
        long timestamp = System.currentTimeMillis();
        int TTL = 30; //seconds
        // Adds enough data to trigger multiple sstable per level (with ttl=30)
        // We also interleave columns from different rows to make sstables
        // key ranges from different levels overlap.
        for (int c = 0; c < columns; c++)
        {
            for (int r = 0; r < rows; r++)
            {
                DecoratedKey key = Util.dk(String.valueOf(r));
                RowMutation rm = new RowMutation(ksname, key.key);
                rm.add(new QueryPath(cfname, null, ByteBufferUtil.bytes("column" + c)), value,
                                        timestamp, c > 0 ? TTL : 0); // let first column never expire,
                                        // since deleting all columns does not produce sstable
                rm.apply();
            }
            store.forceBlockingFlush();
        }

        //make sure all L0 sstables are gone.
        LeveledCompactionStrategy strat = (LeveledCompactionStrategy)store.getCompactionStrategy();
        while (strat.getLevelSize(0) > 1)
        {
            store.forceMajorCompaction();
            Thread.sleep(200);
        }
        // Checking we're not completely bad at math
        int level1Size = strat.getLevelSize(1);
        assertTrue(level1Size > 0);
        int level2Size = strat.getLevelSize(2);
        assertTrue(level2Size > 0);


        //let's save the original number of sstables and original size to use later
        int numSStables = store.getSSTables().size();
        long originalSize = 0;
        for (SSTableReader sstable : store.getSSTables()) {
            originalSize += sstable.uncompressedLength();
        }

        // wait enough time for sstables to expire and force compaction to be executed
        long elapsedSeconds = (System.currentTimeMillis() - timestamp)/1000;
        long sleepTime = Math.max(TTL - elapsedSeconds, 0) + 5; //ttl + gc grace seconds + 5 should be enough
        TimeUnit.SECONDS.sleep(sleepTime);
        store.forceMajorCompaction();
        Thread.sleep(200);

        // even though L1 AND L2 sstables were candidate for
        //tombstone compaction it was not executed because their range overlap
        assertEquals(numSStables, store.getSSTables().size());
        long newSize = 0;
        for (SSTableReader sstable : store.getSSTables()) {
            newSize += sstable.uncompressedLength();
        }
        assertEquals("candidate sstables should not be tombstone-compacted because their key" +
                      " range overlap with other sstables", originalSize, newSize);

        // now let's enable the magic property
        store.metadata.compactionStrategyOptions.put("unchecked_tombstone_compaction", "true");
        store.reload();

        //let's force compaction again...
        store.forceMajorCompaction();
        Thread.sleep(200);

        //we still have the same number of sstables, since they were not compacted against each other
        assertEquals(numSStables, store.getSSTables().size());
        newSize = 0;
        for (SSTableReader sstable : store.getSSTables()) {
            newSize += sstable.uncompressedLength();
        }

        //but now we store less data, since single sstable tombstone compaction was finally executed! :-)
        assertTrue("should be less than " + originalSize + ", but was " + newSize, newSize < originalSize);

        //let's make sure tombstone compaction did not screw levels
        assertEquals("tombstone removal compaction should not promote level", level1Size, strat.getLevelSize(1));
        assertEquals("tombstone removal compaction should not promote level", level2Size, strat.getLevelSize(2));
    }

    public static void assertMaxTimestamp(ColumnFamilyStore cfs, long maxTimestampExpected)
    {
        long maxTimestampObserved = Long.MIN_VALUE;
        for (SSTableReader sstable : cfs.getSSTables())
            maxTimestampObserved = Math.max(sstable.getMaxTimestamp(), maxTimestampObserved);
        assertEquals(maxTimestampExpected, maxTimestampObserved);
    }

    @Test
    public void testEchoedRow() throws IOException, ExecutionException, InterruptedException
    {
        // This test check that EchoedRow doesn't skipp rows: see CASSANDRA-2653

        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard2");

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        // Insert 4 keys in two sstables. We need the sstables to have 2 rows
        // at least to trigger what was causing CASSANDRA-2653
        for (int i=1; i < 5; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes(String.valueOf(i))), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
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
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes(String.valueOf(i))), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
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
        cfs.markCompacted(Collections.singleton(tmpSSTable), OperationType.UNKNOWN);
        assertEquals(1, cfs.getSSTables().size());

        // Now assert we do have the 4 keys
        assertEquals(4, Util.getRangeSlice(cfs).size());
    }

    @Test
    public void testDontPurgeAccidentaly() throws IOException, ExecutionException, InterruptedException
    {
        // Testing with and without forcing deserialization. Without deserialization, EchoedRow will be used.
        testDontPurgeAccidentaly("test1", "Super5", false);
        testDontPurgeAccidentaly("test2", "Super5", true);

        // Use CF with gc_grace=0, see last bug of CASSANDRA-2786
        testDontPurgeAccidentaly("test1", "SuperDirectGC", false);
        testDontPurgeAccidentaly("test2", "SuperDirectGC", true);
    }

    @Test
    public void testUserDefinedCompaction() throws Exception
    {
        Table table = Table.open(TABLE1);
        final String cfname = "Standard3"; // use clean(no sstable) CF
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfname);

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        final int ROWS_PER_SSTABLE = 10;
        for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
            DecoratedKey key = Util.dk(String.valueOf(i));
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(new QueryPath(cfname, null, ByteBufferUtil.bytes("col")),
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
        CompactionManager.instance.forceUserDefinedCompaction(TABLE1, file);
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

    private void testDontPurgeAccidentaly(String k, String cfname, boolean forceDeserialize) throws IOException, ExecutionException, InterruptedException
    {
        // This test catches the regression of CASSANDRA-2786
        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfname);

        // disable compaction while flushing
        cfs.clearUnsafe();
        cfs.disableAutoCompaction();

        // Add test row
        DecoratedKey key = Util.dk(k);
        RowMutation rm = new RowMutation(TABLE1, key.key);
        rm.add(new QueryPath(cfname, ByteBufferUtil.bytes("sc"), ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();

        cfs.forceBlockingFlush();

        Collection<SSTableReader> sstablesBefore = cfs.getSSTables();

        QueryFilter filter = QueryFilter.getIdentityFilter(key, new QueryPath(cfname, null, null));
        assert !cfs.getColumnFamily(filter).isEmpty();

        // Remove key
        rm = new RowMutation(TABLE1, key.key);
        rm.delete(new QueryPath(cfname, null, null), 2);
        rm.apply();

        ColumnFamily cf = cfs.getColumnFamily(filter);
        assert cf == null || cf.isEmpty() : "should be empty: " + cf;

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
        assert cf == null || cf.isEmpty() : "should be empty: " + cf;
    }

    public void testBlacklisting(String compactionStrategy) throws Exception
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open(TABLE1);
        final ColumnFamilyStore cfs = table.getColumnFamilyStore(STANDARD1);

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = DatabaseDescriptor.getIndexInterval() * 2 / ROWS_PER_SSTABLE;

        cfs.setCompactionStrategyClass(compactionStrategy);

        // disable compaction while flushing
        cfs.disableAutoCompaction();
        //test index corruption
        //now create a few new SSTables
        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                RowMutation rm = new RowMutation(TABLE1, key.key);
                long timestamp = j * ROWS_PER_SSTABLE + i;
                rm.add(new QueryPath(STANDARD1, null, ByteBufferUtil.bytes(String.valueOf(i / 2))),
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        timestamp);
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                rm.apply();
                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(cfs).size());
        }

        Collection<SSTableReader> sstables = cfs.getSSTables();
        int currentSSTable = 0;
        int sstablesToCorrupt = 8;

        // corrupt first 'sstablesToCorrupt' SSTables
        for (SSTableReader sstable : sstables)
        {
            if(currentSSTable + 1 > sstablesToCorrupt)
                break;

            RandomAccessFile raf = null;

            try
            {
                raf = new RandomAccessFile(sstable.getFilename(), "rw");
                assertNotNull(raf);
                raf.write(0xFFFFFF);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }

            currentSSTable++;
        }

        int failures = 0;

        // close error output steam to avoid printing ton of useless RuntimeException
        System.err.close();

        try
        {
            // in case something will go wrong we don't want to loop forever using for (;;)
            for (int i = 0; i < sstables.size(); i++)
            {
                try
                {
                    cfs.forceMajorCompaction();
                }
                catch (Exception e)
                {
                    failures++;
                    continue;
                }

                assertEquals(sstablesToCorrupt + 1, cfs.getSSTables().size());
                break;
            }
        }
        finally
        {
            System.setErr(new PrintStream(new ByteArrayOutputStream()));
        }


        cfs.truncate();
        assertEquals(failures, sstablesToCorrupt);
    }
}
