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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.PerRowSecondaryIndexTest;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.Util.rp;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ColumnFamilyStoreTest extends SchemaLoader
{
    static byte[] bytes1, bytes2;

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Test
    // create two sstables, and verify that we only deserialize data from the most recent one
    public void testTimeSortedQuery()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();

        Mutation rm;
        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add("Standard1", cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add("Standard1", cellname("Column1"), ByteBufferUtil.bytes("asdf"), 1);
        rm.apply();
        cfs.forceBlockingFlush();

        cfs.getRecentSSTablesPerReadHistogram(); // resets counts
        cfs.getColumnFamily(Util.namesQueryFilter(cfs, Util.dk("key1"), "Column1"));
        assertEquals(1, cfs.getRecentSSTablesPerReadHistogram()[0]);
    }

    @Test
    public void testGetColumnWithWrongBF()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();

        List<Mutation> rms = new LinkedList<>();
        Mutation rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add("Standard1", cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.add("Standard1", cellname("Column2"), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        List<SSTableReader> ssTables = keyspace.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), "Standard1", System.currentTimeMillis()));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        Mutation rm;

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.delete("Standard2", System.currentTimeMillis());
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                QueryFilter sliceFilter = QueryFilter.getSliceFilter(Util.dk("key1"), "Standard2", Composites.EMPTY, Composites.EMPTY, false, 1, System.currentTimeMillis());
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assertTrue(cf.isMarkedForDelete());
                assertFalse(cf.hasColumns());

                QueryFilter namesFilter = Util.namesQueryFilter(store, Util.dk("key1"), "a");
                cf = store.getColumnFamily(namesFilter);
                assertTrue(cf.isMarkedForDelete());
                assertFalse(cf.hasColumns());
            }
        };

        KeyspaceTest.reTest(store, r);
    }

    @Test
    public void testSkipStartKey()
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(Util.range(p, "key1", "key2"),
                                             null,
                                             Util.namesFilter(cfs, "asdf"),
                                             10);
        assertEquals(1, result.size());
        assert result.get(0).key.getKey().equals(ByteBufferUtil.bytes("key2"));
    }

    @Test
    public void testIndexScan()
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Indexed1");
        Mutation rm;
        CellName nobirthdate = cellname("notbirthdate");
        CellName birthdate = cellname("birthdate");

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(1L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k2"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(2L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k3"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k4aaaa"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(3L), 0);
        rm.apply();

        // basic single-expression query
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(range, clause, filter, 100);

        assert rows != null;
        assert rows.size() == 2 : StringUtils.join(rows, ",");

        String key = new String(rows.get(0).key.getKey().array(), rows.get(0).key.getKey().position(), rows.get(0).key.getKey().remaining());
        assert "k1".equals( key ) : key;

        key = new String(rows.get(1).key.getKey().array(), rows.get(1).key.getKey().position(), rows.get(1).key.getKey().remaining());
        assert "k3".equals(key) : key;

        assert ByteBufferUtil.bytes(1L).equals( rows.get(0).cf.getColumn(birthdate).value());
        assert ByteBufferUtil.bytes(1L).equals( rows.get(1).cf.getColumn(birthdate).value());

        // add a second expression
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), Operator.GTE, ByteBufferUtil.bytes(2L));
        clause = Arrays.asList(expr, expr2);
        rows = cfs.search(range, clause, filter, 100);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.getKey().array(), rows.get(0).key.getKey().position(), rows.get(0).key.getKey().remaining());
        assert "k3".equals( key );

        // same query again, but with resultset not including the subordinate expression
        rows = cfs.search(range, clause, Util.namesFilter(cfs, "birthdate"), 100);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.getKey().array(), rows.get(0).key.getKey().position(), rows.get(0).key.getKey().remaining());
        assert "k3".equals( key );

        assert rows.get(0).cf.getColumnCount() == 1 : rows.get(0).cf;

        // once more, this time with a slice rowset that needs to be expanded
        SliceQueryFilter emptyFilter = new SliceQueryFilter(Composites.EMPTY, Composites.EMPTY, false, 0);
        rows = cfs.search(range, clause, emptyFilter, 100);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.getKey().array(), rows.get(0).key.getKey().position(), rows.get(0).key.getKey().remaining());
        assert "k3".equals( key );

        assertFalse(rows.get(0).cf.hasColumns());

        // query with index hit but rejected by secondary clause, with a small enough count that just checking count
        // doesn't tell the scan loop that it's done
        IndexExpression expr3 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), Operator.EQ, ByteBufferUtil.bytes(-1L));
        clause = Arrays.asList(expr, expr3);
        rows = cfs.search(range, clause, filter, 100);

        assert rows.isEmpty();
    }

    @Test
    public void testLargeScan()
    {
        Mutation rm;
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Indexed1");
        for (int i = 0; i < 100; i++)
        {
            rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key" + i));
            rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(34L), 0);
            rm.add("Indexed1", cellname("notbirthdate"), ByteBufferUtil.bytes((long) (i % 2)), 0);
            rm.applyUnsafe();
        }

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(34L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr, expr2);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(range, clause, filter, 100);

        assert rows != null;
        assert rows.size() == 50 : rows.size();
        Set<DecoratedKey> keys = new HashSet<DecoratedKey>();
        // extra check that there are no duplicate results -- see https://issues.apache.org/jira/browse/CASSANDRA-2406
        for (Row row : rows)
            keys.add(row.key);
        assert rows.size() == keys.size();
    }

    @Test
    public void testIndexDeletions() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace3").getColumnFamilyStore("Indexed1");
        Mutation rm;

        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        String key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

        // delete the column directly
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete("Indexed1", cellname("birthdate"), 1);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.isEmpty();

        // verify that it's not being indexed under the deletion column value either
        Cell deletion = rm.getColumnFamilies().iterator().next().iterator().next();
        ByteBuffer deletionLong = ByteBufferUtil.bytes((long) ByteBufferUtil.toInt(deletion.value()));
        IndexExpression expr0 = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, deletionLong);
        List<IndexExpression> clause0 = Arrays.asList(expr0);
        rows = cfs.search(range, clause0, filter, 100);
        assert rows.isEmpty();

        // resurrect w/ a newer timestamp
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 2);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

        // verify that row and delete w/ older timestamp does nothing
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete("Indexed1", 1);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

        // similarly, column delete w/ older timestamp should do nothing
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete("Indexed1", cellname("birthdate"), 1);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

        // delete the entire row (w/ newer timestamp this time)
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete("Indexed1", 3);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // make sure obsolete mutations don't generate an index entry
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 3);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try insert followed by row delete in the same mutation
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 1);
        rm.delete("Indexed1", 2);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try row delete followed by insert in the same mutation
        rm = new Mutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete("Indexed1", 3);
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(1L), 4);
        rm.apply();
        rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );
    }

    @Test
    public void testIndexUpdate() throws IOException
    {
        Keyspace keyspace = Keyspace.open("Keyspace2");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Indexed1");
        CellName birthdate = cellname("birthdate");

        // create a row and update the birthdate value, test that the index query fetches the new version
        Mutation rm;
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 1);
        rm.apply();
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(2L), 2);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(range, clause, filter, 100);
        assert rows.size() == 0;

        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(2L));
        clause = Arrays.asList(expr);
        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        String key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(3L), 0);
        rm.apply();

        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        key = ByteBufferUtil.string(rows.get(0).key.getKey());
        assert "k1".equals( key );

    }

    @Test
    public void testIndexUpdateOverwritingExpiringColumns() throws Exception
    {
        // see CASSANDRA-7268
        Keyspace keyspace = Keyspace.open("Keyspace2");

        // create a row and update the birthdate value with an expiring column
        Mutation rm;
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k100"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(100L), 1, 1000);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(100L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        // requires a 1s sleep because we calculate local expiry time as (now() / 1000) + ttl
        TimeUnit.SECONDS.sleep(1);

        // now overwrite with the same name/value/ttl, but the local expiry time will be different
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k100"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(100L), 1, 1000);
        rm.apply();

        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        // check that modifying the indexed value using the same timestamp behaves as expected
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k101"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(101L), 1, 1000);
        rm.apply();

        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(101L));
        clause = Arrays.asList(expr);
        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        TimeUnit.SECONDS.sleep(1);
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("k101"));
        rm.add("Indexed1", cellname("birthdate"), ByteBufferUtil.bytes(102L), 1, 1000);
        rm.apply();
        // search for the old value
        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        assertEquals(0, rows.size());
        // and for the new
        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(102L));
        clause = Arrays.asList(expr);
        rows = keyspace.getColumnFamilyStore("Indexed1").search(range, clause, filter, 100);
        assertEquals(1, rows.size());
    }

    @Test
    public void testDeleteOfInconsistentValuesInKeysIndex() throws Exception
    {
        String keySpace = "Keyspace2";
        String cfName = "Indexed1";

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.truncateBlocking();

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        CellName colName = cellname("birthdate"); 
        ByteBuffer val1 = ByteBufferUtil.bytes(1L);
        ByteBuffer val2 = ByteBufferUtil.bytes(2L);

        // create a row and update the "birthdate" value, test that the index query fetches this version
        Mutation rm;
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, colName, val1, 0);
        rm.apply();
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, val1);
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        // force a flush, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(cfName).forceBlockingFlush();

        // now apply another update, but force the index update to be skipped
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, colName, val2, 1);
        keyspace.apply(rm, true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());
        // now check for the updated value
        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, val2);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, colName, ByteBufferUtil.bytes(1L), 3);
        keyspace.apply(rm, true, false);

        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndex() throws Exception
    {
        String keySpace = "Keyspace2";
        String cfName = "Indexed2";

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.truncateBlocking();

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer clusterKey = ByteBufferUtil.bytes("ck1");
        ByteBuffer colName = ByteBufferUtil.bytes("col1"); 

        CellNameType baseComparator = cfs.getComparator();
        CellName compositeName = baseComparator.makeCellName(clusterKey, colName);

        ByteBuffer val1 = ByteBufferUtil.bytes("v1");
        ByteBuffer val2 = ByteBufferUtil.bytes("v2");

        // create a row and update the author value
        Mutation rm;
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, compositeName, val1, 0);
        rm.apply();

        // test that the index query fetches this version
        IndexExpression expr = new IndexExpression(colName, Operator.EQ, val1);
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        // force a flush and retry the query, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(cfName).forceBlockingFlush();
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(1, rows.size());

        // now apply another update, but force the index update to be skipped
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, compositeName, val2, 1);
        keyspace.apply(rm, true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());
        // now check for the updated value
        expr = new IndexExpression(colName, Operator.EQ, val2);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, compositeName, val1, 2);
        keyspace.apply(rm, true, false);

        expr = new IndexExpression(colName, Operator.EQ, val1);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());
    }

    // See CASSANDRA-6098
    @Test
    public void testDeleteCompositeIndex() throws Exception
    {
        String keySpace = "Keyspace2";
        String cfName = "Indexed3"; // has gcGrace 0

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.truncateBlocking();

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer clusterKey = ByteBufferUtil.bytes("ck1");
        ByteBuffer colName = ByteBufferUtil.bytes("col1");

        CellNameType baseComparator = cfs.getComparator();
        CellName compositeName = baseComparator.makeCellName(clusterKey, colName);

        ByteBuffer val1 = ByteBufferUtil.bytes("v2");

        // Insert indexed value.
        Mutation rm;
        rm = new Mutation(keySpace, rowKey);
        rm.add(cfName, compositeName, val1, 0);
        rm.apply();

        // Now delete the value and flush too.
        rm = new Mutation(keySpace, rowKey);
        rm.delete(cfName, 1);
        rm.apply();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        IndexExpression expr = new IndexExpression(colName, Operator.EQ, val1);
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        assertEquals(0, rows.size());
    }

    // See CASSANDRA-2628
    @Test
    public void testIndexScanWithLimitOne()
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Indexed1");
        Mutation rm;

        CellName nobirthdate = cellname("notbirthdate");
        CellName birthdate = cellname("birthdate");

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("kk1"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(1L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("kk2"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("kk3"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("kk4"));
        rm.add("Indexed1", nobirthdate, ByteBufferUtil.bytes(2L), 0);
        rm.add("Indexed1", birthdate, ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        // basic single-expression query
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), Operator.GT, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(range, clause, filter, 1);

        assert rows != null;
        assert rows.size() == 1 : StringUtils.join(rows, ",");
    }

    @Test
    public void testIndexCreate() throws IOException, InterruptedException, ExecutionException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Indexed2");

        // create a row and update the birthdate value, test that the index query fetches the new version
        Mutation rm;
        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed2", cellname("birthdate"), ByteBufferUtil.bytes(1L), 1);
        rm.apply();

        ColumnDefinition old = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate"));
        ColumnDefinition cd = ColumnDefinition.regularDef(cfs.metadata, old.name.bytes, old.type, null).setIndex("birthdate_index", IndexType.KEYS, null);
        Future<?> future = cfs.indexManager.addIndexedColumn(cd);
        future.get();
        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        assert cfs.indexManager.getIndexForColumn(cd.name.bytes).getIndexCfs().getSSTables().size() > 0;

        queryBirthdate(keyspace);

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        SecondaryIndex indexedCfs = cfs.indexManager.getIndexForColumn(ByteBufferUtil.bytes("birthdate"));
        cfs.indexManager.removeIndexedColumn(ByteBufferUtil.bytes("birthdate"));
        assert !indexedCfs.isIndexBuilt(ByteBufferUtil.bytes("birthdate"));

        // rebuild & re-query
        future = cfs.indexManager.addIndexedColumn(cd);
        future.get();
        queryBirthdate(keyspace);
    }

    private void queryBirthdate(Keyspace keyspace) throws CharacterCodingException
    {
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        List<Row> rows = keyspace.getColumnFamilyStore("Indexed2").search(Util.range("", ""), clause, filter, 100);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assertEquals("k1", ByteBufferUtil.string(rows.get(0).key.getKey()));
    }

    @Test
    public void testCassandra6778() throws CharacterCodingException
    {
        String cfname = "StandardInteger1";
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        // insert two columns that represent the same integer but have different binary forms (the
        // second one is padded with extra zeros)
        Mutation rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        CellName column1 = cellname(ByteBuffer.wrap(new byte[]{1}));
        rm.add(cfname, column1, ByteBufferUtil.bytes("data1"), 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        CellName column2 = cellname(ByteBuffer.wrap(new byte[]{0, 0, 1}));
        rm.add(cfname, column2, ByteBufferUtil.bytes("data2"), 2);
        rm.apply();
        cfs.forceBlockingFlush();

        // fetch by the first column name; we should get the second version of the column value
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(
            "Keyspace1", ByteBufferUtil.bytes("k1"), cfname, System.currentTimeMillis(),
            new NamesQueryFilter(FBUtilities.singleton(column1, cfs.getComparator())));

        ColumnFamily cf = cmd.getRow(keyspace).cf;
        assertEquals(1, cf.getColumnCount());
        Cell cell = cf.getColumn(column1);
        assertEquals("data2", ByteBufferUtil.string(cell.value()));
        assertEquals(column2, cell.name());

        // fetch by the second column name; we should get the second version of the column value
        cmd = new SliceByNamesReadCommand(
            "Keyspace1", ByteBufferUtil.bytes("k1"), cfname, System.currentTimeMillis(),
            new NamesQueryFilter(FBUtilities.singleton(column2, cfs.getComparator())));

        cf = cmd.getRow(keyspace).cf;
        assertEquals(1, cf.getColumnCount());
        cell = cf.getColumn(column2);
        assertEquals("data2", ByteBufferUtil.string(cell.value()));
        assertEquals(column2, cell.name());
    }

    @Test
    public void testInclusiveBounds()
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        List<Row> result = cfs.getRangeSlice(Util.bounds("key1", "key2"),
                                             null,
                                             Util.namesFilter(cfs, "asdf"),
                                             10);
        assertEquals(2, result.size());
        assert result.get(0).key.getKey().equals(ByteBufferUtil.bytes("key1"));
    }

    @Test
    public void testDeleteSuperRowSticksAfterFlush() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName= "Super1";
        ByteBuffer scfName = ByteBufferUtil.bytes("SuperDuper");
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("flush-resurrection");

        // create an isolated sstable.
        putColsSuper(cfs, key, scfName,
                new BufferCell(cellname(1L), ByteBufferUtil.bytes("val1"), 1),
                new BufferCell(cellname(2L), ByteBufferUtil.bytes("val2"), 1),
                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 1));
        cfs.forceBlockingFlush();

        // insert, don't flush.
        putColsSuper(cfs, key, scfName,
                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1),
                new BufferCell(cellname(5L), ByteBufferUtil.bytes("val5"), 1),
                new BufferCell(cellname(6L), ByteBufferUtil.bytes("val6"), 1));

        // verify insert.
        final SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(100);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        assertRowAndColCount(1, 6, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));

        // delete
        Mutation rm = new Mutation(keyspace.getName(), key.getKey());
        rm.deleteRange(cfName, SuperColumns.startOf(scfName), SuperColumns.endOf(scfName), 2);
        rm.apply();

        // verify delete.
        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete.
        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));

        // late insert.
        putColsSuper(cfs, key, scfName,
                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1L),
                new BufferCell(cellname(7L), ByteBufferUtil.bytes("val7"), 1L));

        // re-verify delete.
        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));

        // make sure new writes are recognized.
        putColsSuper(cfs, key, scfName,
                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 3),
                new BufferCell(cellname(8L), ByteBufferUtil.bytes("val8"), 3),
                new BufferCell(cellname(9L), ByteBufferUtil.bytes("val9"), 3));
        assertRowAndColCount(1, 3, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
    }

    private static void assertRowAndColCount(int rowCount, int colCount, boolean isDeleted, Collection<Row> rows) throws CharacterCodingException
    {
        assert rows.size() == rowCount : "rowcount " + rows.size();
        for (Row row : rows)
        {
            assert row.cf != null : "cf was null";
            assert row.cf.getColumnCount() == colCount : "colcount " + row.cf.getColumnCount() + "|" + str(row.cf);
            if (isDeleted)
                assert row.cf.isMarkedForDelete() : "cf not marked for delete";
        }
    }

    private static String str(ColumnFamily cf) throws CharacterCodingException
    {
        StringBuilder sb = new StringBuilder();
        for (Cell col : cf.getSortedColumns())
            sb.append(String.format("(%s,%s,%d),", ByteBufferUtil.string(col.name().toByteBuffer()), ByteBufferUtil.string(col.value()), col.timestamp()));
        return sb.toString();
    }

    private static void putColsSuper(ColumnFamilyStore cfs, DecoratedKey key, ByteBuffer scfName, Cell... cols) throws Throwable
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        for (Cell col : cols)
            cf.addColumn(col.withUpdatedName(CellNames.compositeDense(scfName, col.name().toByteBuffer())));
        Mutation rm = new Mutation(cfs.keyspace.getName(), key.getKey(), cf);
        rm.apply();
    }

    private static void putColsStandard(ColumnFamilyStore cfs, DecoratedKey key, Cell... cols) throws Throwable
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.name);
        for (Cell col : cols)
            cf.addColumn(col);
        Mutation rm = new Mutation(cfs.keyspace.getName(), key.getKey(), cf);
        rm.apply();
    }

    @Test
    public void testDeleteStandardRowSticksAfterFlush() throws Throwable
    {
        // test to make sure flushing after a delete doesn't resurrect delted cols.
        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("f-flush-resurrection");

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(100);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        // insert
        putColsStandard(cfs, key, column("col1", "val1", 1), column("col2", "val2", 1));
        assertRowAndColCount(1, 2, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // flush.
        cfs.forceBlockingFlush();

        // insert, don't flush
        putColsStandard(cfs, key, column("col3", "val3", 1), column("col4", "val4", 1));
        assertRowAndColCount(1, 4, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // delete (from sstable and memtable)
        Mutation rm = new Mutation(keyspace.getName(), key.getKey());
        rm.delete(cfs.name, 2);
        rm.apply();

        // verify delete
        assertRowAndColCount(1, 0, true, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRowAndColCount(1, 0, true, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        putColsStandard(cfs, key, column("col5", "val5", 1), column("col2", "val2", 1));

        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRowAndColCount(1, 0, true, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // make sure that new writes are recognized.
        putColsStandard(cfs, key, column("col6", "val6", 3), column("col7", "val7", 3));
        assertRowAndColCount(1, 2, true, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));

        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush();
        assertRowAndColCount(1, 2, true, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 100));
    }


    private ColumnFamilyStore insertKey1Key2()
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace2").getColumnFamilyStore("Standard1");
        List<Mutation> rms = new LinkedList<>();
        Mutation rm;
        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("key1"));
        rm.add("Standard1", cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        rm = new Mutation("Keyspace2", ByteBufferUtil.bytes("key2"));
        rm.add("Standard1", cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        return Util.writeColumnFamily(rms);
    }

    @Test
    public void testBackupAfterFlush() throws Throwable
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        for (int version = 1; version <= 2; ++version)
        {
            Descriptor existing = new Descriptor(cfs.directories.getDirectoryForNewSSTables(), "Keyspace2", "Standard1", version, Descriptor.Type.FINAL);
            Descriptor desc = new Descriptor(Directories.getBackupsDirectory(existing), "Keyspace2", "Standard1", version, Descriptor.Type.FINAL);
            for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.STATS })
                assertTrue("can not find backedup file:" + desc.filenameFor(c), new File(desc.filenameFor(c)).exists());
        }
    }

    // CASSANDRA-3467.  the key here is that supercolumn and subcolumn comparators are different
    @Test
    public void testSliceByNamesCommandOnUUIDTypeSCF() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName = "Super6";
        ByteBuffer superColName = LexicalUUIDType.instance.fromString("a4ed3562-0e8e-4b41-bdfd-c45a2774682d");
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("slice-get-uuid-type");

        // Insert a row with one supercolumn and multiple subcolumns
        putColsSuper(cfs, key, superColName, new BufferCell(cellname("a"), ByteBufferUtil.bytes("A"), 1),
                                             new BufferCell(cellname("b"), ByteBufferUtil.bytes("B"), 1));

        // Get the entire supercolumn like normal
        ColumnFamily cfGet = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
        assertEquals(ByteBufferUtil.bytes("A"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
        assertEquals(ByteBufferUtil.bytes("B"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());

        // Now do the SliceByNamesCommand on the supercolumn, passing both subcolumns in as columns to get
        SortedSet<CellName> sliceColNames = new TreeSet<CellName>(cfs.metadata.comparator);
        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a")));
        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b")));
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(keyspaceName, key.getKey(), cfName, System.currentTimeMillis(), new NamesQueryFilter(sliceColNames));
        ColumnFamily cfSliced = cmd.getRow(keyspace).cf;

        // Make sure the slice returns the same as the straight get
        assertEquals(ByteBufferUtil.bytes("A"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
        assertEquals(ByteBufferUtil.bytes("B"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
    }

    @Test
    public void testSliceByNamesCommandOldMetadata() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName= "Standard1";
        DecoratedKey key = Util.dk("slice-name-old-metadata");
        CellName cname = cellname("c1");
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        // Create a cell a 'high timestamp'
        putColsStandard(cfs, key, new BufferCell(cname, ByteBufferUtil.bytes("a"), 2));
        cfs.forceBlockingFlush();

        // Nuke the metadata and reload that sstable
        Collection<SSTableReader> ssTables = cfs.getSSTables();
        assertEquals(1, ssTables.size());
        cfs.clearUnsafe();
        assertEquals(0, cfs.getSSTables().size());

        new File(ssTables.iterator().next().descriptor.filenameFor(Component.STATS)).delete();
        cfs.loadNewSSTables();

        // Add another cell with a lower timestamp
        putColsStandard(cfs, key, new BufferCell(cname, ByteBufferUtil.bytes("b"), 1));

        // Test fetching the cell by name returns the first cell
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(keyspaceName, key.getKey(), cfName, System.currentTimeMillis(), new NamesQueryFilter(FBUtilities.singleton(cname, cfs.getComparator())));
        ColumnFamily cf = cmd.getRow(keyspace).cf;
        Cell cell = cf.getColumn(cname);
        assert cell.value().equals(ByteBufferUtil.bytes("a")) : "expecting a, got " + ByteBufferUtil.string(cell.value());

        Keyspace.clear("Keyspace1"); // CASSANDRA-7195
    }

    private static void assertTotalColCount(Collection<Row> rows, int expectedCount)
    {
        int columns = 0;
        for (Row row : rows)
        {
            columns += row.getLiveCount(new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, expectedCount), System.currentTimeMillis());
        }
        assert columns == expectedCount : "Expected " + expectedCount + " live columns but got " + columns + ": " + rows;
    }


    @Test
    public void testRangeSliceColumnsLimit() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[5];
        for (int i = 0; i < 5; i++)
            cols[i] = column("c" + i, "value", 1);

        putColsStandard(cfs, Util.dk("a"), cols[0], cols[1], cols[2], cols[3], cols[4]);
        putColsStandard(cfs, Util.dk("b"), cols[0], cols[1]);
        putColsStandard(cfs, Util.dk("c"), cols[0], cols[1], cols[2], cols[3]);
        cfs.forceBlockingFlush();

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(1);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              3,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            3);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              5,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            5);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              8,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            8);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              10,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            10);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              100,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            11);

        // Check that when querying by name, we always include all names for a
        // gien row even if it means returning more columns than requested (this is necesseray for CQL)
        sp = new SlicePredicate();
        sp.setColumn_names(Arrays.asList(
            ByteBufferUtil.bytes("c0"),
            ByteBufferUtil.bytes("c1"),
            ByteBufferUtil.bytes("c2")
        ));

        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              1,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            3);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              4,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            5);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              5,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            5);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              6,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            8);
        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
                                              null,
                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
                                              100,
                                              System.currentTimeMillis(),
                                              true,
                                              false),
                            8);
    }

    @Test
    public void testRangeSlicePaging() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[4];
        for (int i = 0; i < 4; i++)
            cols[i] = column("c" + i, "value", 1);

        DecoratedKey ka = Util.dk("a");
        DecoratedKey kb = Util.dk("b");
        DecoratedKey kc = Util.dk("c");

        RowPosition min = Util.rp("");

        putColsStandard(cfs, ka, cols[0], cols[1], cols[2], cols[3]);
        putColsStandard(cfs, kb, cols[0], cols[1], cols[2]);
        putColsStandard(cfs, kc, cols[0], cols[1], cols[2], cols[3]);
        cfs.forceBlockingFlush();

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(1);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        Collection<Row> rows;
        Row row, row1, row2;
        IDiskAtomFilter filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);

        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(Util.range("", ""), filter, null, 3, true, true, System.currentTimeMillis()));
        assert rows.size() == 1 : "Expected 1 row, got " + toString(rows);
        row = rows.iterator().next();
        assertColumnNames(row, "c0", "c1", "c2");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(ka, min), filter, null, 3, true, true, System.currentTimeMillis()));
        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
        Iterator<Row> iter = rows.iterator();
        row1 = iter.next();
        row2 = iter.next();
        assertColumnNames(row1, "c2", "c3");
        assertColumnNames(row2, "c0");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c0")));
        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(row2.key, min), filter, null, 3, true, true, System.currentTimeMillis()));
        assert rows.size() == 1 : "Expected 1 row, got " + toString(rows);
        row = rows.iterator().next();
        assertColumnNames(row, "c0", "c1", "c2");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(row.key, min), filter, null, 3, true, true, System.currentTimeMillis()));
        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
        iter = rows.iterator();
        row1 = iter.next();
        row2 = iter.next();
        assertColumnNames(row1, "c2");
        assertColumnNames(row2, "c0", "c1");

        // Paging within bounds
        SliceQueryFilter sf = new SliceQueryFilter(cellname("c1"),
                                                   cellname("c2"),
                                                   false,
                                                   0);
        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(ka, kc), sf, cellname("c2"), cellname("c1"), null, 2, true, System.currentTimeMillis()));
        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
        iter = rows.iterator();
        row1 = iter.next();
        row2 = iter.next();
        assertColumnNames(row1, "c2");
        assertColumnNames(row2, "c1");

        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(kb, kc), sf, cellname("c1"), cellname("c1"), null, 10, true, System.currentTimeMillis()));
        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
        iter = rows.iterator();
        row1 = iter.next();
        row2 = iter.next();
        assertColumnNames(row1, "c1", "c2");
        assertColumnNames(row2, "c1");
    }

    private static String toString(Collection<Row> rows)
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            for (Row row : rows)
            {
                sb.append("{");
                sb.append(ByteBufferUtil.string(row.key.getKey()));
                sb.append(":");
                if (row.cf != null && !row.cf.isEmpty())
                {
                    for (Cell c : row.cf)
                        sb.append(" ").append(row.cf.getComparator().getString(c.name()));
                }
                sb.append("} ");
            }
            return sb.toString();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void assertColumnNames(Row row, String ... columnNames) throws Exception
    {
        if (row == null || row.cf == null)
            throw new AssertionError("The row should not be empty");

        Iterator<Cell> columns = row.cf.getSortedColumns().iterator();
        Iterator<String> names = Arrays.asList(columnNames).iterator();

        while (columns.hasNext())
        {
            Cell c = columns.next();
            assert names.hasNext() : "Got more columns that expected (first unexpected column: " + ByteBufferUtil.string(c.name().toByteBuffer()) + ")";
            String n = names.next();
            assert c.name().toByteBuffer().equals(ByteBufferUtil.bytes(n)) : "Expected " + n + ", got " + ByteBufferUtil.string(c.name().toByteBuffer());
        }
        assert !names.hasNext() : "Missing expected column " + names.next();
    }

    private static DecoratedKey idk(int i)
    {
        return Util.dk(String.valueOf(i));
    }

    @Test
    public void testRangeSliceInclusionExclusion() throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Cell[] cols = new Cell[5];
        for (int i = 0; i < 5; i++)
            cols[i] = column("c" + i, "value", 1);

        for (int i = 0; i <= 9; i++)
        {
            putColsStandard(cfs, idk(i), column("name", "value", 1));
        }
        cfs.forceBlockingFlush();

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(1);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
        IDiskAtomFilter qf = ThriftValidation.asIFilter(sp, cfs.metadata, null);

        List<Row> rows;

        // Start and end inclusive
        rows = cfs.getRangeSlice(new Bounds<RowPosition>(rp("2"), rp("7")), null, qf, 100);
        assert rows.size() == 6;
        assert rows.get(0).key.equals(idk(2));
        assert rows.get(rows.size() - 1).key.equals(idk(7));

        // Start and end excluded
        rows = cfs.getRangeSlice(new ExcludingBounds<RowPosition>(rp("2"), rp("7")), null, qf, 100);
        assert rows.size() == 4;
        assert rows.get(0).key.equals(idk(3));
        assert rows.get(rows.size() - 1).key.equals(idk(6));

        // Start excluded, end included
        rows = cfs.getRangeSlice(new Range<RowPosition>(rp("2"), rp("7")), null, qf, 100);
        assert rows.size() == 5;
        assert rows.get(0).key.equals(idk(3));
        assert rows.get(rows.size() - 1).key.equals(idk(7));

        // Start included, end excluded
        rows = cfs.getRangeSlice(new IncludingExcludingBounds<RowPosition>(rp("2"), rp("7")), null, qf, 100);
        assert rows.size() == 5;
        assert rows.get(0).key.equals(idk(2));
        assert rows.get(rows.size() - 1).key.equals(idk(6));
    }

    @Test
    public void testKeysSearcher() throws Exception
    {
        // Create secondary index and flush to disk
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Indexed1");

        store.truncateBlocking();

        for (int i = 0; i < 10; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf("k" + i));
            Mutation rm = new Mutation("Keyspace1", key);
            rm.add("Indexed1", cellname("birthdate"), LongType.instance.decompose(1L), System.currentTimeMillis());
            rm.apply();
        }

        store.forceBlockingFlush();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), Operator.EQ, LongType.instance.decompose(1L));
        // explicitly tell to the KeysSearcher to use column limiting for rowsPerQuery to trigger bogus columnsRead--; (CASSANDRA-3996)
        List<Row> rows = store.search(store.makeExtendedFilter(Util.range("", ""), new IdentityQueryFilter(), Arrays.asList(expr), 10, true, false, System.currentTimeMillis()));

        assert rows.size() == 10;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeSomeEmptyNoIndex() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colA")),
                new ColumnSlice(cellname("colC"), cellname("colE")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colI"), Composites.EMPTY) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colI")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colE"), cellname("colC")),
                new ColumnSlice(cellname("colA"), Composites.EMPTY) };

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace table = Keyspace.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "i" };
        Cell[] cols = new Cell[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
                    ByteBuffer.wrap(new byte[1]), 1);
        }

        putColsStandard(cfs, dk("a"), cols);

        cfs.forceBlockingFlush();

        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);

        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colI");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colD", "colC", "colA");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colD", "colC");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeSomeEmptyIndexed() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colA")),
                new ColumnSlice(cellname("colC"), cellname("colE")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colI"), Composites.EMPTY) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY,  cellname("colI")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colE"), cellname("colC")),
                new ColumnSlice(cellname("colA"), Composites.EMPTY) };

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace table = Keyspace.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "i" };
        Cell[] cols = new Cell[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
                    ByteBuffer.wrap(new byte[1366]), 1);
        }

        putColsStandard(cfs, dk("a"), cols);

        cfs.forceBlockingFlush();

        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);

        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colI");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colD", "colC", "colA");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colD", "colC");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeContiguousNoIndex() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colA")),
                new ColumnSlice(cellname("colC"), cellname("colE")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colI"), Composites.EMPTY) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colI")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colE"), cellname("colC")),
                new ColumnSlice(cellname("colA"), Composites.EMPTY) };

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace table = Keyspace.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        Cell[] cols = new Cell[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
                    ByteBuffer.wrap(new byte[1]), 1);
        }

        putColsStandard(cfs, dk("a"), cols);

        cfs.forceBlockingFlush();

        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);

        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colF", "colG", "colI");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colF", "colE", "colD", "colC", "colA");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colF");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeContiguousIndexed() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colA")),
                new ColumnSlice(cellname("colC"), cellname("colE")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colI"), Composites.EMPTY) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colI")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colF"), cellname("colF")),
                new ColumnSlice(cellname("colE"), cellname("colC")),
                new ColumnSlice(cellname("colA"), Composites.EMPTY) };

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace table = Keyspace.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        Cell[] cols = new Cell[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
                    ByteBuffer.wrap(new byte[1366]), 1);
        }

        putColsStandard(cfs, dk("a"), cols);

        cfs.forceBlockingFlush();

        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);

        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colF", "colG", "colI");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colF", "colE", "colD", "colC", "colA");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colF");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeIndexed() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colA")),
                new ColumnSlice(cellname("colC"), cellname("colE")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colI"), Composites.EMPTY) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colI")),
                new ColumnSlice(cellname("colG"), cellname("colG")),
                new ColumnSlice(cellname("colE"), cellname("colC")),
                new ColumnSlice(cellname("colA"), Composites.EMPTY) };

        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        Cell[] cols = new Cell[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
                    // use 1366 so that three cols make an index segment
                    ByteBuffer.wrap(new byte[1366]), 1);
        }

        putColsStandard(cfs, dk("a"), cols);

        cfs.forceBlockingFlush();

        // this setup should generate the following row (assuming indexes are of 4Kb each):
        // [colA, colB, colC, colD, colE, colF, colG, colH, colI]
        // indexed as:
        // index0 [colA, colC]
        // index1 [colD, colF]
        // index2 [colG, colI]
        // and we're looking for the ranges:
        // range0 [____, colA]
        // range1 [colC, colE]
        // range2 [colG, ColG]
        // range3 [colI, ____]

        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);

        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colG", "colI");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colE", "colD", "colC", "colA");
        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colE");

    }

    @Test
    public void testMultipleRangesSlicesNoIndexedColumns() throws Throwable
    {
        // small values so that cols won't be indexed
        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(10, true));
    }

    @Test
    public void testMultipleRangesSlicesWithIndexedColumns() throws Throwable
    {
        // min val size before cols are indexed is 4kb while testing so lets make sure cols are indexed
        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(1024, true));
    }

    @Test
    public void testMultipleRangesSlicesInMemory() throws Throwable
    {
        // small values so that cols won't be indexed
        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(10, false));
    }

    @Test
    public void testRemoveUnfinishedCompactionLeftovers() throws Throwable
    {
        String ks = "Keyspace1";
        String cf = "Standard3"; // should be empty

        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
        Directories dir = new Directories(cfmeta);
        ByteBuffer key = bytes("key");

        // 1st sstable
        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(), cfmeta, StorageService.getPartitioner());
        writer.newRow(key);
        writer.addColumn(bytes("col"), bytes("val"), 1);
        writer.close();

        Map<Descriptor, Set<Component>> sstables = dir.sstableLister().list();
        assertEquals(1, sstables.size());

        Map.Entry<Descriptor, Set<Component>> sstableToOpen = sstables.entrySet().iterator().next();
        final SSTableReader sstable1 = SSTableReader.open(sstableToOpen.getKey());

        // simulate incomplete compaction
        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
                                         cfmeta, StorageService.getPartitioner())
        {
            protected SSTableWriter getWriter()
            {
                MetadataCollector collector = new MetadataCollector(cfmeta.comparator);
                collector.addAncestor(sstable1.descriptor.generation); // add ancestor from previously written sstable
                return new SSTableWriter(makeFilename(directory, metadata.ksName, metadata.cfName),
                                         0,
                                         ActiveRepairService.UNREPAIRED_SSTABLE,
                                         metadata,
                                         StorageService.getPartitioner(),
                                         collector);
            }
        };
        writer.newRow(key);
        writer.addColumn(bytes("col"), bytes("val"), 1);
        writer.close();

        // should have 2 sstables now
        sstables = dir.sstableLister().list();
        assertEquals(2, sstables.size());

        SSTableReader sstable2 = SSTableReader.open(sstable1.descriptor);
        UUID compactionTaskID = SystemKeyspace.startCompaction(
                Keyspace.open(ks).getColumnFamilyStore(cf),
                Collections.singleton(sstable2));

        Map<Integer, UUID> unfinishedCompaction = new HashMap<>();
        unfinishedCompaction.put(sstable1.descriptor.generation, compactionTaskID);
        ColumnFamilyStore.removeUnfinishedCompactionLeftovers(cfmeta, unfinishedCompaction);

        // 2nd sstable should be removed (only 1st sstable exists in set of size 1)
        sstables = dir.sstableLister().list();
        assertEquals(1, sstables.size());
        assertTrue(sstables.containsKey(sstable1.descriptor));

        Map<Pair<String, String>, Map<Integer, UUID>> unfinished = SystemKeyspace.getUnfinishedCompactions();
        assertTrue(unfinished.isEmpty());
        sstable1.selfRef().release();
        sstable2.selfRef().release();
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-6086">CASSANDRA-6086</a>
     */
    @Test
    public void testFailedToRemoveUnfinishedCompactionLeftovers() throws Throwable
    {
        final String ks = "Keyspace1";
        final String cf = "Standard4"; // should be empty

        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
        Directories dir = new Directories(cfmeta);
        ByteBuffer key = bytes("key");

        // Write SSTable generation 3 that has ancestors 1 and 2
        final Set<Integer> ancestors = Sets.newHashSet(1, 2);
        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
                                                cfmeta, StorageService.getPartitioner())
        {
            protected SSTableWriter getWriter()
            {
                MetadataCollector collector = new MetadataCollector(cfmeta.comparator);
                for (int ancestor : ancestors)
                    collector.addAncestor(ancestor);
                String file = new Descriptor(directory, ks, cf, 3, Descriptor.Type.TEMP).filenameFor(Component.DATA);
                return new SSTableWriter(file,
                                         0,
                                         ActiveRepairService.UNREPAIRED_SSTABLE,
                                         metadata,
                                         StorageService.getPartitioner(),
                                         collector);
            }
        };
        writer.newRow(key);
        writer.addColumn(bytes("col"), bytes("val"), 1);
        writer.close();

        Map<Descriptor, Set<Component>> sstables = dir.sstableLister().list();
        assert sstables.size() == 1;

        Map.Entry<Descriptor, Set<Component>> sstableToOpen = sstables.entrySet().iterator().next();
        final SSTableReader sstable1 = SSTableReader.open(sstableToOpen.getKey());

        // simulate we don't have generation in compaction_history
        Map<Integer, UUID> unfinishedCompactions = new HashMap<>();
        UUID compactionTaskID = UUID.randomUUID();
        for (Integer ancestor : ancestors)
            unfinishedCompactions.put(ancestor, compactionTaskID);
        ColumnFamilyStore.removeUnfinishedCompactionLeftovers(cfmeta, unfinishedCompactions);

        // SSTable should not be deleted
        sstables = dir.sstableLister().list();
        assert sstables.size() == 1;
        assert sstables.containsKey(sstable1.descriptor);
    }

    @Test
    public void testLoadNewSSTablesAvoidsOverwrites() throws Throwable
    {
        String ks = "Keyspace1";
        String cf = "Standard1";
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);
        cfs.truncateBlocking();
        SSTableDeletingTask.waitForDeletions();

        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
        Directories dir = new Directories(cfs.metadata);

        // clear old SSTables (probably left by CFS.clearUnsafe() calls in other tests)
        for (Map.Entry<Descriptor, Set<Component>> entry : dir.sstableLister().list().entrySet())
        {
            for (Component component : entry.getValue())
            {
                FileUtils.delete(entry.getKey().filenameFor(component));
            }
        }

        // sanity check
        int existingSSTables = dir.sstableLister().list().keySet().size();
        assert existingSSTables == 0 : String.format("%d SSTables unexpectedly exist", existingSSTables);

        ByteBuffer key = bytes("key");

        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
                                                             cfmeta, StorageService.getPartitioner())
        {
            @Override
            protected SSTableWriter getWriter()
            {
                // hack for reset generation
                generation.set(0);
                return super.getWriter();
            }
        };
        writer.newRow(key);
        writer.addColumn(bytes("col"), bytes("val"), 1);
        writer.close();

        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
                                         cfmeta, StorageService.getPartitioner());
        writer.newRow(key);
        writer.addColumn(bytes("col"), bytes("val"), 1);
        writer.close();

        Set<Integer> generations = new HashSet<>();
        for (Descriptor descriptor : dir.sstableLister().list().keySet())
            generations.add(descriptor.generation);

        // we should have two generations: [1, 2]
        assertEquals(2, generations.size());
        assertTrue(generations.contains(1));
        assertTrue(generations.contains(2));

        assertEquals(0, cfs.getSSTables().size());

        // start the generation counter at 1 again (other tests have incremented it already)
        cfs.resetFileIndexGenerator();

        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
        try
        {
            // avoid duplicate hardlinks to incremental backups
            DatabaseDescriptor.setIncrementalBackupsEnabled(false);
            cfs.loadNewSSTables();
        }
        finally
        {
            DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
        }

        assertEquals(2, cfs.getSSTables().size());
        generations = new HashSet<>();
        for (Descriptor descriptor : dir.sstableLister().list().keySet())
            generations.add(descriptor.generation);

        // normally they would get renamed to generations 1 and 2, but since those filenames already exist,
        // they get skipped and we end up with generations 3 and 4
        assertEquals(2, generations.size());
        assertTrue(generations.contains(3));
        assertTrue(generations.contains(4));
    }

    private ColumnFamilyStore prepareMultiRangeSlicesTest(int valueSize, boolean flush) throws Throwable
    {
        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" };
        Cell[] cols = new Cell[12];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new BufferCell(cellname("col" + letters[i]), ByteBuffer.wrap(new byte[valueSize]), 1);
        }

        for (int i = 0; i < 12; i++)
        {
            putColsStandard(cfs, dk(letters[i]), Arrays.copyOfRange(cols, 0, i + 1));
        }

        if (flush)
        {
            cfs.forceBlockingFlush();
        }
        else
        {
            // The intent is to validate memtable code, so check we really didn't flush
            assert cfs.getSSTables().isEmpty();
        }

        return cfs;
    }

    private void testMultiRangeSlicesBehavior(ColumnFamilyStore cfs)
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] startMiddleAndEndRanges = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colc")),
                new ColumnSlice(cellname("colf"), cellname("colg")),
                new ColumnSlice(cellname("colj"), Composites.EMPTY) };

        ColumnSlice[] startMiddleAndEndRangesReversed = new ColumnSlice[] {
                new ColumnSlice(Composites.EMPTY, cellname("colj")),
                new ColumnSlice(cellname("colg"), cellname("colf")),
                new ColumnSlice(cellname("colc"), Composites.EMPTY) };

        ColumnSlice[] startOnlyRange =
                new ColumnSlice[] { new ColumnSlice(Composites.EMPTY, cellname("colc")) };

        ColumnSlice[] startOnlyRangeReversed =
                new ColumnSlice[] { new ColumnSlice(cellname("colc"), Composites.EMPTY) };

        ColumnSlice[] middleOnlyRanges =
                new ColumnSlice[] { new ColumnSlice(cellname("colf"), cellname("colg")) };

        ColumnSlice[] middleOnlyRangesReversed =
                new ColumnSlice[] { new ColumnSlice(cellname("colg"), cellname("colf")) };

        ColumnSlice[] endOnlyRanges =
                new ColumnSlice[] { new ColumnSlice(cellname("colj"), Composites.EMPTY) };

        ColumnSlice[] endOnlyRangesReversed =
                new ColumnSlice[] { new ColumnSlice(Composites.EMPTY, cellname("colj")) };

        SliceQueryFilter startOnlyFilter = new SliceQueryFilter(startOnlyRange, false,
                Integer.MAX_VALUE);
        SliceQueryFilter startOnlyFilterReversed = new SliceQueryFilter(startOnlyRangeReversed, true,
                Integer.MAX_VALUE);
        SliceQueryFilter startOnlyFilterWithCounting = new SliceQueryFilter(startOnlyRange, false, 1);
        SliceQueryFilter startOnlyFilterReversedWithCounting = new SliceQueryFilter(startOnlyRangeReversed,
                true, 1);

        SliceQueryFilter middleOnlyFilter = new SliceQueryFilter(middleOnlyRanges,
                false,
                Integer.MAX_VALUE);
        SliceQueryFilter middleOnlyFilterReversed = new SliceQueryFilter(middleOnlyRangesReversed, true,
                Integer.MAX_VALUE);
        SliceQueryFilter middleOnlyFilterWithCounting = new SliceQueryFilter(middleOnlyRanges, false, 1);
        SliceQueryFilter middleOnlyFilterReversedWithCounting = new SliceQueryFilter(middleOnlyRangesReversed,
                true, 1);

        SliceQueryFilter endOnlyFilter = new SliceQueryFilter(endOnlyRanges, false,
                Integer.MAX_VALUE);
        SliceQueryFilter endOnlyReversed = new SliceQueryFilter(endOnlyRangesReversed, true,
                Integer.MAX_VALUE);
        SliceQueryFilter endOnlyWithCounting = new SliceQueryFilter(endOnlyRanges, false, 1);
        SliceQueryFilter endOnlyWithReversedCounting = new SliceQueryFilter(endOnlyRangesReversed,
                true, 1);

        SliceQueryFilter startMiddleAndEndFilter = new SliceQueryFilter(startMiddleAndEndRanges, false,
                Integer.MAX_VALUE);
        SliceQueryFilter startMiddleAndEndFilterReversed = new SliceQueryFilter(startMiddleAndEndRangesReversed, true,
                Integer.MAX_VALUE);
        SliceQueryFilter startMiddleAndEndFilterWithCounting = new SliceQueryFilter(startMiddleAndEndRanges, false,
                1);
        SliceQueryFilter startMiddleAndEndFilterReversedWithCounting = new SliceQueryFilter(
                startMiddleAndEndRangesReversed, true,
                1);

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "a", "cola");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "a", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "a", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "a", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "a", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "a", "cola");

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "c", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "c", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "c", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "c", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "c", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "c", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "c", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "c", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "c", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "c", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "c", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "f", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "f", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "f", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "f", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "f", "colf");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "f", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "f", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "f", "colf");

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "f", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "f", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "f", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "f", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "f", "cola", "colb", "colc", "colf");

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "f", "colf", "colc", "colb",
                "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "f", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "f", "colf");

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "h", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "h", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "h", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "h", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "h", "colf", "colg");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "h", "colg", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "h", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "h", "colg");

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "h", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "h", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "h", new String[] {});
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "h", new String[] {});

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "h", "cola", "colb", "colc", "colf",
                "colg");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "h", "colg", "colf", "colc", "colb",
                "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "h", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "h", "colg");

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "j", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "j", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "j", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "j", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "j", "colf", "colg");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "j", "colg", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "j", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "j", "colg");

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "j", "colj");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "j", "colj");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "j", "colj");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "j", "colj");

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "j", "cola", "colb", "colc", "colf", "colg",
                "colj");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "j", "colj", "colg", "colf", "colc",
                "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "j", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "j", "colj");

        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "l", "cola", "colb", "colc");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "l", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "l", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "l", "colc");

        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "l", "colf", "colg");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "l", "colg", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "l", "colf");
        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "l", "colg");

        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "l", "colj", "colk", "coll");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "l", "coll", "colk", "colj");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "l", "colj");
        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "l", "coll");

        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "l", "cola", "colb", "colc", "colf", "colg",
                "colj", "colk", "coll");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "l", "coll", "colk", "colj", "colg",
                "colf", "colc", "colb", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "l", "cola");
        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "l", "coll");
    }

    private void findRowGetSlicesAndAssertColsFound(ColumnFamilyStore cfs, SliceQueryFilter filter, String rowKey,
            String... colNames)
    {
        List<Row> rows = cfs.getRangeSlice(new Bounds<RowPosition>(rp(rowKey), rp(rowKey)),
                                           null,
                                           filter,
                                           Integer.MAX_VALUE,
                                           System.currentTimeMillis(),
                                           false,
                                           false);
        assertSame("unexpected number of rows ", 1, rows.size());
        Row row = rows.get(0);
        Collection<Cell> cols = !filter.isReversed() ? row.cf.getSortedColumns() : row.cf.getReverseSortedColumns();
        // printRow(cfs, new String(row.key.key.array()), cols);
        String[] returnedColsNames = Iterables.toArray(Iterables.transform(cols, new Function<Cell, String>()
        {
            public String apply(Cell arg0)
            {
                return Util.string(arg0.name().toByteBuffer());
            }
        }), String.class);

        assertTrue(
                "Columns did not match. Expected: " + Arrays.toString(colNames) + " but got:"
                        + Arrays.toString(returnedColsNames), Arrays.equals(colNames, returnedColsNames));
        int i = 0;
        for (Cell col : cols)
        {
            assertEquals(colNames[i++], Util.string(col.name().toByteBuffer()));
        }
    }

    private void printRow(ColumnFamilyStore cfs, String rowKey, Collection<Cell> cols)
    {
        DecoratedKey ROW = Util.dk(rowKey);
        System.err.println("Original:");
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(ROW, "Standard1", System.currentTimeMillis()));
        System.err.println("Row key: " + rowKey + " Cols: "
                + Iterables.transform(cf.getSortedColumns(), new Function<Cell, String>()
                {
                    public String apply(Cell arg0)
                    {
                        return Util.string(arg0.name().toByteBuffer());
                    }
                }));
        System.err.println("Filtered:");
        Iterable<String> transformed = Iterables.transform(cols, new Function<Cell, String>()
        {
            public String apply(Cell arg0)
            {
                return Util.string(arg0.name().toByteBuffer());
            }
        });
        System.err.println("Row key: " + rowKey + " Cols: " + transformed);
    }
    
    @Test
    public void testRebuildSecondaryIndex() throws IOException
    {
        CellName indexedCellName = cellname("indexed");
        Mutation rm;

        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", indexedCellName, ByteBufferUtil.bytes("foo"), 1);

        rm.apply();
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
        
        Keyspace.open("PerRowSecondaryIndex").getColumnFamilyStore("Indexed1").forceBlockingFlush();
        
        PerRowSecondaryIndexTest.TestIndex.reset();
        
        ColumnFamilyStore.rebuildSecondaryIndex("PerRowSecondaryIndex", "Indexed1", PerRowSecondaryIndexTest.TestIndex.class.getSimpleName());
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
        
        PerRowSecondaryIndexTest.TestIndex.reset();
        PerRowSecondaryIndexTest.TestIndex.ACTIVE = false;
        ColumnFamilyStore.rebuildSecondaryIndex("PerRowSecondaryIndex", "Indexed1", PerRowSecondaryIndexTest.TestIndex.class.getSimpleName());
        assertNull(PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY);
        
        PerRowSecondaryIndexTest.TestIndex.reset();
    }
}
