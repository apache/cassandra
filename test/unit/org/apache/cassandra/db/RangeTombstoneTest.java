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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.IndexType;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.utils.ByteBufferUtil;

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
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 40; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 10, 22, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 1; i < 40; i += 2)
            add(rm, i, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 19, 27, 3);
        rm.apply();
        // We don't flush to test with both a range tomsbtone in memtable and in sstable

        // Queries by name
        int[] live = new int[]{ 4, 9, 11, 17, 28 };
        int[] dead = new int[]{ 12, 19, 21, 24, 27 };
        SortedSet<ByteBuffer> columns = new TreeSet<>(cfs.getComparator());
        for (int i : live)
            columns.add(b(i));
        for (int i : dead)
            columns.add(b(i));
        cf = cfs.getColumnFamily(QueryFilter.getNamesFilter(dk(key), CFNAME, columns, System.currentTimeMillis()));

        for (int i : live)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i : dead)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";

        // Queries by slices
        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, b(7), b(30), false, Integer.MAX_VALUE, System.currentTimeMillis()));

        for (int i : new int[]{ 7, 8, 9, 11, 13, 15, 17, 28, 29, 30 })
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i : new int[]{ 10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 })
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";
    }

    @Test
    public void overlappingRangeTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k2";
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 20; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 15, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 10, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 8, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), CFNAME, System.currentTimeMillis()));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";

        // Compact everything and re-test
        CompactionManager.instance.performMaximal(cfs);
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), CFNAME, System.currentTimeMillis()));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";
    }

    @Test
    public void reverseQueryTest() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k3";
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        add(rm, 2, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        // Deletes everything but without being a row tombstone
        delete(rm.addOrGet(CFNAME), 0, 10, 1);
        add(rm, 1, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        // Get the last value of the row
        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), CFNAME, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 1, System.currentTimeMillis()));

        assert !cf.isEmpty();
        int last = i(cf.getSortedColumns().iterator().next().name());
        assert last == 1 : "Last column should be column 1 since column 2 has been deleted";
    }

    @Test
    public void testPreCompactedRowWithRangeTombstonesUpdatesSecondaryIndex() throws Exception
    {
        // nothing special to do here, just run the test
        runCompactionWithRangeTombstoneAndCheckSecondaryIndex();
    }

    @Test
    public void testLazilyCompactedRowWithRangeTombstonesUpdatesSecondaryIndex() throws Exception
    {
        // make sure we use LazilyCompactedRow by exceeding in_memory_compaction_limit
        DatabaseDescriptor.setInMemoryCompactionLimit(0);
        runCompactionWithRangeTombstoneAndCheckSecondaryIndex();
    }

    @Test
    public void testLazilyCompactedRowGeneratesSameSSTablesAsPreCompactedRow() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);
        ByteBuffer key = ByteBufferUtil.bytes("k4");

        // remove any existing sstables before starting
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());

        RowMutation rm = new RowMutation(KSNAME, key);
        for (int i = 0; i < 10; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, key);
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
                assertTrue(atom instanceof Column);
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
            ColumnDefinition cd = new ColumnDefinition(indexedColumnName,
                    cfs.getComparator(),
                    IndexType.CUSTOM,
                    ImmutableMap.of(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, TestIndex.class.getName()),
                    "test_index",
                    0,
                    null);
            cfs.indexManager.addIndexedColumn(cd);
        }

        TestIndex index = ((TestIndex)cfs.indexManager.getIndexForColumn(indexedColumnName));
        index.resetCounts();

        RowMutation rm = new RowMutation(KSNAME, key);
        add(rm, 1, 0);
        rm.apply();

        // add a RT which hides the column we just inserted
        rm = new RowMutation(KSNAME, key);
        ColumnFamily cf = rm.addOrGet(CFNAME);
        delete(cf, 0, 1, 1);
        rm.apply();

        // now re-insert that column
        rm = new RowMutation(KSNAME, key);
        add(rm, 1, 2);
        rm.apply();

        cfs.forceBlockingFlush();

        // We should have 2 updates to the indexed "1" column
        assertEquals(2, index.inserts.size());

        CompactionManager.instance.performMaximal(cfs);

        // verify that the "1" indexed column removed from the index
        // only once, by the re-indexing caused by the second insertion.
        // This second write deletes from the 2i because the original column
        // was still in the main cf's memtable (shadowed by the RT). One
        // thing we're checking for here is that there wasn't an additional,
        // bogus delete issued to the 2i (CASSANDRA-6517)
        assertEquals(1, index.deletes.size());
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
            ColumnDefinition cd = new ColumnDefinition(indexedColumnName,
                                                       cfs.getComparator(),
                                                       IndexType.CUSTOM,
                                                       ImmutableMap.of(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, TestIndex.class.getName()),
                                                       "test_index",
                                                       0,
                                                       null);
            cfs.indexManager.addIndexedColumn(cd);
        }

        TestIndex index = ((TestIndex)cfs.indexManager.getIndexForColumn(indexedColumnName));
        index.resetCounts();

        RowMutation rm = new RowMutation(KSNAME, key);
        for (int i = 0; i < 10; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, key);
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

    private static boolean isLive(ColumnFamily cf, Column c)
    {
        return c != null && !c.isMarkedForDelete(System.currentTimeMillis()) && !cf.deletionInfo().isDeleted(c);
    }

    private static ByteBuffer b(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static int i(ByteBuffer i)
    {
        return ByteBufferUtil.toInt(i);
    }

    private static void add(RowMutation rm, int value, long timestamp)
    {
        rm.add(CFNAME, b(value), b(value), timestamp);
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
        public List<Column> inserts = new ArrayList<>();
        public List<Column> deletes = new ArrayList<>();

        public void resetCounts()
        {
            inserts.clear();
            deletes.clear();
        }

        public void delete(ByteBuffer rowKey, Column col)
        {
            deletes.add(col);
        }

        public void insert(ByteBuffer rowKey, Column col)
        {
            inserts.add(col);
        }

        public void update(ByteBuffer rowKey, Column col){}

        public void init(){}

        public void reload(){}

        public void validateOptions() throws ConfigurationException{}

        public String getIndexName(){ return "TestIndex";}

        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns){ return null; }

        public void forceBlockingFlush(){}

        public long getLiveSize(){ return 0; }

        public ColumnFamilyStore getIndexCfs(){ return null; }

        public void removeIndex(ByteBuffer columnName){}

        public void invalidate(){}

        public void truncateBlocking(long truncatedAt) { }
    }
}
