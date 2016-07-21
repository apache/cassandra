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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.apache.cassandra.Util.dk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeTombstoneTest extends SchemaLoader
{
    private static final String KSNAME = "Keyspace1";
    private static final String CFNAME = "StandardInteger1";

    @Test
    public void simpleQueryWithRangeTombstoneTest() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k1";
        Mutation rm;
        ColumnFamily cf;

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 40; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 10, 22, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 1; i < 40; i += 2)
            add(rm, i, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 19, 27, 3);
        rm.apply();
        // We don't flush to test with both a range tomsbtone in memtable and in sstable

        // Queries by name
        int[] live = new int[]{ 4, 9, 11, 17, 28 };
        int[] dead = new int[]{ 12, 19, 21, 24, 27 };
        SortedSet<CellName> columns = new TreeSet<CellName>(cfs.getComparator());
        for (int i : live)
            columns.add(b(i));
        for (int i : dead)
            columns.add(b(i));
        cf = cfs.getColumnFamily(QueryFilter.getNamesFilter(dk(key), CFNAME, columns, System.currentTimeMillis()));

        for (int i : live)
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i : dead)
            assert !isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " shouldn't be live";

        // Queries by slices
        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(7), b(30), false, Integer.MAX_VALUE, System.currentTimeMillis()));

        for (int i : new int[]{ 7, 8, 9, 11, 13, 15, 17, 28, 29, 30 })
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i : new int[]{ 10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 })
            assert !isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " shouldn't be live";
    }

    @Test
    public void rangeTombstoneFilteringTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k111";
        Mutation rm;
        ColumnFamily cf;

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 40; i += 2)
            add(rm, i, 0);
        rm.apply();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 10, 1);
        rm.apply();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 15, 20, 2);
        rm.apply();

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(11), b(14), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        Collection<RangeTombstone> rt = rangeTombstones(cf);
        assertEquals(0, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(11), b(15), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(20), b(25), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(12), b(25), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(25), b(35), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(0, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(1), b(40), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(2, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(7), b(17), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(2, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(5), b(20), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(2, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(5), b(15), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(2, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(1), b(2), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(0, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(1), b(5), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(1), b(10), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(5), b(6), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(17), b(20), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(17), b(18), false, Integer.MAX_VALUE, System.currentTimeMillis()));
        rt = rangeTombstones(cf);
        assertEquals(1, rt.size());

        ColumnSlice[] slices = new ColumnSlice[]{new ColumnSlice( b(1), b(10)), new ColumnSlice( b(16), b(20))};
        IDiskAtomFilter sqf = new SliceQueryFilter(slices, false, Integer.MAX_VALUE);
        cf = cfs.getColumnFamily( new QueryFilter(dk(key), CFNAME, sqf, System.currentTimeMillis()) );
        rt = rangeTombstones(cf);
        assertEquals(2, rt.size());
    }

    private Collection<RangeTombstone> rangeTombstones(ColumnFamily cf)
    {
        List<RangeTombstone> tombstones = new ArrayList<RangeTombstone>();
        Iterators.addAll(tombstones, cf.deletionInfo().rangeIterator());
        return tombstones;
    }

    @Test
    public void testTrackTimesRowTombstone() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";
        Mutation rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        long timestamp = System.currentTimeMillis();
        cf.delete(new DeletionInfo(1000, (int)(timestamp/1000)));
        rm.apply();
        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, (int)(timestamp/1000));
        cfs.forceMajorCompaction();
        sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, (int)(timestamp/1000));
    }

    @Test
    public void testTrackTimesRowTombstoneWithData() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";
        Mutation rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        add(rm, 5, 999);
        rm.apply();
        key = "rt_times2";
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        int timestamp = (int)(System.currentTimeMillis()/1000);
        cf.delete(new DeletionInfo(1000, timestamp));
        rm.apply();
        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Integer.MAX_VALUE);
        cfs.forceMajorCompaction();
        sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Integer.MAX_VALUE);
    }
    @Test
    public void testTrackTimesRangeTombstone() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";
        Mutation rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        long timestamp = System.currentTimeMillis();
        cf.delete(new DeletionInfo(b(1), b(2), cfs.getComparator(), 1000, (int)(timestamp/1000)));
        rm.apply();
        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, (int)(timestamp/1000));
        cfs.forceMajorCompaction();
        sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, (int)(timestamp/1000));
    }

    @Test
    public void testTrackTimesRangeTombstoneWithData() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";
        Mutation rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        add(rm, 5, 999);
        rm.apply();
        key = "rt_times2";
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        int timestamp = (int)(System.currentTimeMillis()/1000);
        cf.delete(new DeletionInfo(b(1), b(2), cfs.getComparator(), 1000, timestamp));
        rm.apply();
        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Integer.MAX_VALUE);
        cfs.forceMajorCompaction();
        sstable = cfs.getSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Integer.MAX_VALUE);
    }

    private void assertTimes(StatsMetadata metadata, long min, long max, int localDeletionTime)
    {
        assertEquals(min, metadata.minTimestamp);
        assertEquals(max, metadata.maxTimestamp);
        assertEquals(localDeletionTime, metadata.maxLocalDeletionTime);
    }

    @Test
    public void test7810() throws ExecutionException, InterruptedException, IOException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.metadata.gcGraceSeconds(2);

        String key = "7810";
        Mutation rm;
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 10; i < 20; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        cf.delete(new DeletionInfo(b(10),b(11), cfs.getComparator(), 1, 1));
        rm.apply();
        cfs.forceBlockingFlush();
        Thread.sleep(5);
        cfs.forceMajorCompaction();
        assertEquals(8, Util.getColumnFamily(ks, Util.dk(key), CFNAME).getColumnCount());
    }

    @Test
    public void test7808_1() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.metadata.gcGraceSeconds(2);

        String key = "7808_1";
        Mutation rm;
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 40; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        cf.delete(new DeletionInfo(1, 1));
        rm.apply();
        cfs.forceBlockingFlush();
        Thread.sleep(5);
        cfs.forceMajorCompaction();
    }

    @Test
    public void test7808_2() throws ExecutionException, InterruptedException, IOException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.metadata.gcGraceSeconds(2);

        String key = "7808_2";
        Mutation rm;
        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 10; i < 20; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        ColumnFamily cf = rm.addOrGet(CFNAME);
        cf.delete(new DeletionInfo(0,0));
        rm.apply();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        add(rm, 5, 1);
        rm.apply();

        cfs.forceBlockingFlush();
        Thread.sleep(5);
        cfs.forceMajorCompaction();
        assertEquals(1, Util.getColumnFamily(ks, Util.dk(key), CFNAME).getColumnCount());
    }

    @Test
    public void overlappingRangeTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k2";
        Mutation rm;
        ColumnFamily cf;

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 20; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 15, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 10, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 8, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), CFNAME, System.currentTimeMillis()));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " shouldn't be live";

        // Compact everything and re-test
        CompactionManager.instance.performMaximal(cfs);
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), CFNAME, System.currentTimeMillis()));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Cell " + i + " shouldn't be live";
    }

    @Test
    public void reverseQueryTest() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k3";
        Mutation rm;
        ColumnFamily cf;

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        add(rm, 2, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, ByteBufferUtil.bytes(key));
        // Deletes everything but without being a row tombstone
        delete(rm.addOrGet(CFNAME), 0, 10, 1);
        add(rm, 1, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        // Get the last value of the row
        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, Composites.EMPTY, Composites.EMPTY, true, 1, System.currentTimeMillis()));

        assert !cf.isEmpty();
        int last = i(cf.getSortedColumns().iterator().next().name());
        assert last == 1 : "Last column should be column 1 since column 2 has been deleted";
    }

    @Test
    public void testRowWithRangeTombstonesUpdatesSecondaryIndex() throws Exception
    {
        runCompactionWithRangeTombstoneAndCheckSecondaryIndex();
    }

    @Test
    public void testRangeTombstoneCompaction() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);
        ByteBuffer key = ByteBufferUtil.bytes("k4");

        // remove any existing sstables before starting
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());

        Mutation rm = new Mutation(KSNAME, key);
        for (int i = 0; i < 10; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, key);
        ColumnFamily cf = rm.addOrGet(CFNAME);
        for (int i = 0; i < 10; i += 2)
            delete(cf, 0, 7, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        // there should be 2 sstables
        assertEquals(2, cfs.getSSTables().size());

        // compact down to single sstable
        CompactionManager.instance.performMaximal(cfs);
        assertEquals(1, cfs.getSSTables().size());

        // test the physical structure of the sstable i.e. rt & columns on disk
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        OnDiskAtomIterator iter = sstable.getScanner().next();
        int cnt = 0;
        // after compaction, the first element should be an RT followed by the remaining non-deleted columns
        while(iter.hasNext())
        {
            OnDiskAtom atom = iter.next();
            if (cnt == 0)
                assertTrue(atom instanceof RangeTombstone);
            if (cnt > 0)
                assertTrue(atom instanceof Cell);
            cnt++;
        }
        assertEquals(2, cnt);
    }

    @Test
    public void testOverwritesToDeletedColumns() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);
        ByteBuffer key = ByteBufferUtil.bytes("k6");
        ByteBuffer indexedColumnName = ByteBufferUtil.bytes(1);

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());
        if (cfs.indexManager.getIndexForColumn(indexedColumnName) == null)
        {
            ColumnDefinition cd = new ColumnDefinition(cfs.metadata, indexedColumnName, Int32Type.instance, null, ColumnDefinition.Kind.REGULAR);
            cd.setIndex("test_index", IndexType.CUSTOM, ImmutableMap.of(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, TestIndex.class.getName()));
            cfs.indexManager.addIndexedColumn(cd);
        }

        TestIndex index = ((TestIndex)cfs.indexManager.getIndexForColumn(indexedColumnName));
        index.resetCounts();

        Mutation rm = new Mutation(KSNAME, key);
        add(rm, 1, 0);
        rm.apply();

        // add a RT which hides the column we just inserted
        rm = new Mutation(KSNAME, key);
        ColumnFamily cf = rm.addOrGet(CFNAME);
        delete(cf, 0, 1, 1);
        rm.apply();

        // now re-insert that column
        rm = new Mutation(KSNAME, key);
        add(rm, 1, 2);
        rm.apply();

        cfs.forceBlockingFlush();

        // We should have 1 insert and 1 update to the indexed "1" column
        // CASSANDRA-6640 changed index update to just update, not insert then delete
        assertEquals(1, index.inserts.size());
        assertEquals(1, index.updates.size());
    }

    private void runCompactionWithRangeTombstoneAndCheckSecondaryIndex() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);
        ByteBuffer key = ByteBufferUtil.bytes("k5");
        ByteBuffer indexedColumnName = ByteBufferUtil.bytes(1);

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());
        if (cfs.indexManager.getIndexForColumn(indexedColumnName) == null)
        {
            ColumnDefinition cd = ColumnDefinition.regularDef(cfs.metadata, indexedColumnName, cfs.getComparator().asAbstractType(), 0)
                                                  .setIndex("test_index", IndexType.CUSTOM, ImmutableMap.of(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, TestIndex.class.getName()));
            cfs.indexManager.addIndexedColumn(cd);
        }

        TestIndex index = ((TestIndex)cfs.indexManager.getIndexForColumn(indexedColumnName));
        index.resetCounts();

        Mutation rm = new Mutation(KSNAME, key);
        for (int i = 0; i < 10; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KSNAME, key);
        ColumnFamily cf = rm.addOrGet(CFNAME);
        for (int i = 0; i < 10; i += 2)
            delete(cf, 0, 7, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        // We should have indexed 1 column
        assertEquals(1, index.inserts.size());

        CompactionManager.instance.performMaximal(cfs);

        // compacted down to single sstable
        assertEquals(1, cfs.getSSTables().size());

        // verify that the 1 indexed column was removed from the index
        assertEquals(1, index.deletes.size());
        assertEquals(index.deletes.get(0), index.inserts.get(0));
    }

    private static boolean isLive(ColumnFamily cf, Cell c)
    {
        return c != null && c.isLive() && !cf.deletionInfo().isDeleted(c);
    }

    private static CellName b(int i)
    {
        return CellNames.simpleDense(ByteBufferUtil.bytes(i));
    }

    private static int i(CellName i)
    {
        return ByteBufferUtil.toInt(i.toByteBuffer());
    }

    private static void add(Mutation rm, int value, long timestamp)
    {
        rm.add(CFNAME, b(value), ByteBufferUtil.bytes(value), timestamp);
    }

    private static void delete(ColumnFamily cf, int from, int to, long timestamp)
    {
        cf.delete(new DeletionInfo(b(from),
                                   b(to),
                                   cf.getComparator(),
                                   timestamp,
                                   (int)(System.currentTimeMillis() / 1000)));
    }

    public static class TestIndex extends PerColumnSecondaryIndex
    {
        public List<Cell> inserts = new ArrayList<>();
        public List<Cell> deletes = new ArrayList<>();
        public List<Cell> updates = new ArrayList<>();

        public void resetCounts()
        {
            inserts.clear();
            deletes.clear();
            updates.clear();
        }

        public void delete(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup)
        {
            deletes.add(col);
        }

        @Override
        public void deleteForCleanup(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup) {}

        public void insert(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup)
        {
            inserts.add(col);
        }

        public void update(ByteBuffer rowKey, Cell oldCol, Cell col, OpOrder.Group opGroup)
        {
            updates.add(col);
        }

        public void init(){}

        public void reload(){}

        public void validateOptions() throws ConfigurationException{}

        public String getIndexName(){ return "TestIndex";}

        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns){ return null; }

        public void forceBlockingFlush(){}

        public ColumnFamilyStore getIndexCfs(){ return null; }

        public void removeIndex(ByteBuffer columnName){}

        public void invalidate(){}

        public void truncateBlocking(long truncatedAt) { }

        public boolean indexes(CellName name) { return name.toByteBuffer().equals(ByteBufferUtil.bytes(1)); }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }
}
