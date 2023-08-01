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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ValueAccessors;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactionsTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_DENSE1 = "CF_DENSE1";
    private static final String CF_STANDARD1 = "CF_STANDARD1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_STANDARD3 = "Standard3";
    private static final String CF_STANDARD4 = "Standard4";
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> compactionOptions = new HashMap<>();
        compactionOptions.put("tombstone_compaction_interval", "1");

        // Disable tombstone histogram rounding for tests
        CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.setInt(1);

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1)
                                                .compaction(CompactionParams.stcs(compactionOptions)),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD4));
    }

    public static long populate(String ks, String cf, int startRowKey, int endRowKey, int ttl)
    {
        return populate(ks, cf, startRowKey, endRowKey, "", ttl);
    }

    public static long populate(String ks, String cf, int startRowKey, int endRowKey, String suffix, int ttl)
    {
        long timestamp = System.currentTimeMillis();
        TableMetadata cfm = Keyspace.open(ks).getColumnFamilyStore(cf).metadata();
        for (int i = startRowKey; i <= endRowKey; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i) + suffix);
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(cfm, timestamp, j > 0 ? ttl : 0, key.getKey())
                    .clustering(Integer.toString(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
            }
        }
        return timestamp;
    }

    // Test to see if sstable has enough expired columns, it is compacted itself.
    @Test
    public void testSingleSSTableCompaction() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.clearUnsafe();
        SchemaTestUtil.announceTableUpdate(store.metadata().unbuild().gcGraceSeconds(1).build());

        // disable compaction while flushing
        store.disableAutoCompaction();

        long timestamp = populate(KEYSPACE1, CF_STANDARD1, 0, 9, 3); //ttl=3s

        Util.flush(store);
        assertEquals(1, store.getLiveSSTables().size());
        long originalSize = store.getLiveSSTables().iterator().next().uncompressedLength();

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // enable compaction, submit background and wait for it to complete
        store.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        // and sstable with ttl should be compacted
        assertEquals(1, store.getLiveSSTables().size());
        long size = store.getLiveSSTables().iterator().next().uncompressedLength();
        assertTrue("should be less than " + originalSize + ", but was " + size, size < originalSize);

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        assertMaxTimestamp(store, timestamp);
    }

    @Test
    public void testUncheckedTombstoneSizeTieredCompaction() throws Exception
    {
        Map<String, String> compactionOptions = new HashMap<>();
        compactionOptions.put("tombstone_compaction_interval", "1");
        compactionOptions.put("unchecked_tombstone_compaction", "false");

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.clearUnsafe();

        SchemaTestUtil.announceTableUpdate(store.metadata().unbuild().gcGraceSeconds(1).compaction(CompactionParams.stcs(compactionOptions)).build());

        // disable compaction while flushing
        store.disableAutoCompaction();

        //Populate sstable1 with with keys [0a..29aaaaaaaaaaa] Partitions have to be big enough 
        //that prevent the size dependent AbstractCompactionStrategy.worthDroppingTombstones to trigger 
        //a compaction.
        populate(KEYSPACE1, CF_STANDARD1, 0, 29, "aaaaaaaaaaa",3); //ttl=3s
        Util.flush(store);

        //Populate sstable2 with with keys [0b..29b] (keys do not overlap with SSTable1, but the range is almost fully covered)
        long timestamp2 = populate(KEYSPACE1, CF_STANDARD1, 0, 29, "bbbbbbbbbbb", 3); //ttl=3s
        Util.flush(store);

        assertEquals(2, store.getLiveSSTables().size());

        Iterator<SSTableReader> it = store.getLiveSSTables().iterator();
        long originalSize1 = it.next().uncompressedLength();
        long originalSize2 = it.next().uncompressedLength();

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // enable compaction, submit background and wait for it to complete
        store.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        // even though both sstables were candidate for tombstone compaction
        // it was not executed because they have an overlapping token range
        assertEquals(2, store.getLiveSSTables().size());
        it = store.getLiveSSTables().iterator();
        long newSize1 = it.next().uncompressedLength();
        long newSize2 = it.next().uncompressedLength();
        assertEquals("candidate sstable should not be tombstone-compacted because its key range overlap with other sstable",
                      originalSize1, newSize1);
        assertEquals("candidate sstable should not be tombstone-compacted because its key range overlap with other sstable",
                      originalSize2, newSize2);

        // now let's enable the magic property
        compactionOptions.put("unchecked_tombstone_compaction", "true");
        SchemaTestUtil.announceTableUpdate(store.metadata().unbuild().gcGraceSeconds(1).compaction(CompactionParams.stcs(compactionOptions)).build());

        //submit background task again and wait for it to complete
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        //we still have 2 sstables, since they were not compacted against each other
        assertEquals(2, store.getLiveSSTables().size());
        it = store.getLiveSSTables().iterator();
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
        for (SSTableReader sstable : cfs.getLiveSSTables())
            maxTimestampObserved = Math.max(sstable.getMaxTimestamp(), maxTimestampObserved);
        assertEquals(maxTimestampExpected, maxTimestampObserved);
    }

    @Test
    public void testUserDefinedCompaction() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final String cfname = "Standard3"; // use clean(no sstable) CF
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        TableMetadata table = cfs.metadata();

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        final int ROWS_PER_SSTABLE = 10;
        for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
            DecoratedKey key = Util.dk(String.valueOf(i));
            new RowUpdateBuilder(table, FBUtilities.timestampMicros(), key.getKey())
            .clustering(ByteBufferUtil.bytes("cols"))
            .add("val", "val1")
            .build().applyUnsafe();
        }
        Util.flush(cfs);
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());
        SSTableReader sstable = sstables.iterator().next();

        SSTableId prevGeneration = sstable.descriptor.id;
        String file = sstable.descriptor.fileFor(Components.DATA).absolutePath();
        // submit user defined compaction on flushed sstable
        CompactionManager.instance.forceUserDefinedCompaction(file);
        // wait until user defined compaction finishes
        do
        {
            Thread.sleep(100);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);
        // CF should have only one sstable with generation number advanced
        sstables = cfs.getLiveSSTables();
        assertEquals(1, sstables.size());
        assertThat(SSTableIdFactory.COMPARATOR.compare(prevGeneration, sstables.iterator().next().descriptor.id)).isLessThan(0);
    }

    public static void writeSSTableWithRangeTombstoneMaskingOneColumn(ColumnFamilyStore cfs, TableMetadata table, int[] dks) {
        for (int dk : dks)
        {
            RowUpdateBuilder deletedRowUpdateBuilder = new RowUpdateBuilder(table, 1, Util.dk(Integer.toString(dk)));
            deletedRowUpdateBuilder.clustering("01").add("val", "a"); //Range tombstone covers this (timestamp 2 > 1)
            Clustering<?> startClustering = Clustering.make(ByteBufferUtil.bytes("0"));
            Clustering<?> endClustering = Clustering.make(ByteBufferUtil.bytes("b"));
            deletedRowUpdateBuilder.addRangeTombstone(new RangeTombstone(Slice.make(startClustering, endClustering), DeletionTime.build(2, (int) (System.currentTimeMillis() / 1000))));
            deletedRowUpdateBuilder.build().applyUnsafe();

            RowUpdateBuilder notYetDeletedRowUpdateBuilder = new RowUpdateBuilder(table, 3, Util.dk(Integer.toString(dk)));
            notYetDeletedRowUpdateBuilder.clustering("02").add("val", "a"); //Range tombstone doesn't cover this (timestamp 3 > 2)
            notYetDeletedRowUpdateBuilder.build().applyUnsafe();
        }
        Util.flush(cfs);
    }

    @Test
    public void testRangeTombstones()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");
        cfs.clearUnsafe();

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        final TableMetadata table = cfs.metadata();
        Directories dir = cfs.getDirectories();

        ArrayList<DecoratedKey> keys = new ArrayList<DecoratedKey>();

        for (int i=0; i < 4; i++)
        {
            keys.add(Util.dk(Integer.toString(i)));
        }

        int[] dks = {0, 1, 3};
        writeSSTableWithRangeTombstoneMaskingOneColumn(cfs, table, dks);

        int[] dkays = {0, 1, 2, 3};
        writeSSTableWithRangeTombstoneMaskingOneColumn(cfs, table, dkays);

        Collection<SSTableReader> toCompact = cfs.getLiveSSTables();
        assert toCompact.size() == 2;

        Util.compact(cfs, toCompact);
        assertEquals(1, cfs.getLiveSSTables().size());

        // Now assert we do have the 4 keys
        assertEquals(4, Util.getAll(Util.cmd(cfs).build()).size());

        ArrayList<DecoratedKey> k = new ArrayList<>();

        for (FilteredPartition p : Util.getAll(Util.cmd(cfs).build()))
        {
            k.add(p.partitionKey());
            final SinglePartitionReadCommand command = SinglePartitionReadCommand.create(cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.none(), DataLimits.NONE, p.partitionKey(), new ClusteringIndexSliceFilter(Slices.ALL, false));
            try (ReadExecutionController executionController = command.executionController();
                 PartitionIterator iterator = command.executeInternal(executionController))
            {
                try (RowIterator rowIterator = iterator.next())
                {
                    Row row = rowIterator.next();
                    Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("val", false)));
                    assertEquals(ByteBufferUtil.bytes("a"), cell.buffer());
                    assertEquals(3, cell.timestamp());
                    ValueAccessors.assertDataNotEquals(ByteBufferUtil.bytes("01"), row.clustering().getRawValues()[0]);
                    ValueAccessors.assertDataEquals(ByteBufferUtil.bytes("02"), row.clustering().getRawValues()[0]);
                }
            }
        }
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            StatsMetadata stats = sstable.getSSTableMetadata();
            assertEquals(ByteBufferUtil.bytes("0"), stats.coveredClustering.start().bufferAt(0));
            assertEquals(ByteBufferUtil.bytes("b"), stats.coveredClustering.end().bufferAt(0));
        }

        assertEquals(keys, k);
    }

    private void testDontPurgeAccidentally(String k, String cfname) throws InterruptedException
    {
        // This test catches the regression of CASSANDRA-2786
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        TableMetadata table = cfs.metadata();

        // disable compaction while flushing
        cfs.clearUnsafe();
        cfs.disableAutoCompaction();

        // Add test row
        DecoratedKey key = Util.dk(k);
        RowUpdateBuilder rowUpdateBuilder = new RowUpdateBuilder(table, 0, key);
        rowUpdateBuilder.clustering("c").add("val", "a");
        rowUpdateBuilder.build().applyUnsafe();

        Util.flush(cfs);

        Collection<SSTableReader> sstablesBefore = cfs.getLiveSSTables();

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertTrue(!partition.isEmpty());

        RowUpdateBuilder deleteRowBuilder = new RowUpdateBuilder(table, 2, key);
        deleteRowBuilder.clustering("c").delete("val");
        deleteRowBuilder.build().applyUnsafe();
        // Remove key

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertTrue(partition.iterator().next().cells().iterator().next().isTombstone());

        // Sleep one second so that the removal is indeed purgeable even with gcgrace == 0
        Thread.sleep(1000);

        Util.flush(cfs);

        Collection<SSTableReader> sstablesAfter = cfs.getLiveSSTables();
        Collection<SSTableReader> toCompact = new ArrayList<SSTableReader>();
        for (SSTableReader sstable : sstablesAfter)
            if (!sstablesBefore.contains(sstable))
                toCompact.add(sstable);

        Util.compact(cfs, toCompact);

        SSTableReader newSSTable = null;
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            assert !toCompact.contains(reader);
            if (!sstablesBefore.contains(reader))
                newSSTable = reader;
        }

        // We cannot read the data, since {@link ReadCommand#withoutPurgeableTombstones} will purge droppable tombstones
        // but we just want to check here that compaction did *NOT* drop the tombstone, so we read from the SSTable directly
        // instead
        ISSTableScanner scanner = newSSTable.getScanner();
        assertTrue(scanner.hasNext());
        UnfilteredRowIterator rowIt = scanner.next();
        assertTrue(rowIt.hasNext());
        Unfiltered unfiltered = rowIt.next();
        assertTrue(unfiltered.isRow());
        Row row = (Row)unfiltered;
        assertTrue(row.cells().iterator().next().isTombstone());
        assertFalse(rowIt.hasNext());
        assertFalse(scanner.hasNext());
    }

    private static Range<Token> rangeFor(int start, int end)
    {
        return new Range<Token>(new ByteOrderedPartitioner.BytesToken(String.format("%03d", start).getBytes()),
                                new ByteOrderedPartitioner.BytesToken(String.format("%03d", end).getBytes()));
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
        DecoratedKey dk = Util.dk(String.format("%03d", key));
        new RowUpdateBuilder(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata(), timestamp, dk.getKey())
                .add("val", "val")
                .build()
                .applyUnsafe();
        /*
        Mutation rm = new Mutation(KEYSPACE1, decoratedKey.getKey());
        rm.add("CF_STANDARD1", Util.cellname("col"), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, 1000);
        rm.applyUnsafe();
        */
    }

    @Test
    @Ignore("making ranges based on the keys, not on the tokens")
    public void testNeedsCleanup()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("CF_STANDARD1");
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
        Util.flush(store);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();


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

    @Test
    public void testConcurrencySettings()
    {
        CompactionManager.instance.setConcurrentCompactors(2);
        assertEquals(2, CompactionManager.instance.getCoreCompactorThreads());
        CompactionManager.instance.setConcurrentCompactors(3);
        assertEquals(3, CompactionManager.instance.getCoreCompactorThreads());
        CompactionManager.instance.setConcurrentCompactors(1);
        assertEquals(1, CompactionManager.instance.getCoreCompactorThreads());
    }
}