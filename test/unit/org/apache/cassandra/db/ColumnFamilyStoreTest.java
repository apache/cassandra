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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.Util.getBytes;
import static org.apache.cassandra.Util.rp;
import static org.apache.cassandra.db.TableTest.assertColumns;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.commons.lang.ArrayUtils.EMPTY_BYTE_ARRAY;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;

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
    public void testTimeSortedQuery() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");
        cfs.truncate().get();

        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 1);
        rm.apply();
        cfs.forceBlockingFlush();

        cfs.getRecentSSTablesPerReadHistogram(); // resets counts
        cfs.getColumnFamily(QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard1", null), ByteBufferUtil.bytes("Column1")));
        assertEquals(1, cfs.getRecentSSTablesPerReadHistogram()[0]);
    }

    @Test
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");
        cfs.truncate().get();

        List<IMutation> rms = new LinkedList<IMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column2")), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        List<SSTableReader> ssTables = table.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1"))));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.delete(new QueryPath("Standard2", null, null), System.currentTimeMillis());
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                QueryFilter sliceFilter = QueryFilter.getSliceFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assert cf.isMarkedForDelete();
                assert cf.isEmpty();

                QueryFilter namesFilter = QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ByteBufferUtil.bytes("a"));
                cf = store.getColumnFamily(namesFilter);
                assert cf.isMarkedForDelete();
                assert cf.isEmpty();
            }
        };

        TableTest.reTest(store, r);
    }

    @Test
    public void testSkipStartKey() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                             Util.range(p, "key1", "key2"),
                                             10,
                                             new NamesQueryFilter(ByteBufferUtil.bytes("asdf")),
                                             null);
        assertEquals(1, result.size());
        assert result.get(0).key.key.equals(ByteBufferUtil.bytes("key2"));
    }

    @Test
    public void testIndexScan() throws IOException
    {
        RowMutation rm;

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("k2"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("k3"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("k4aaaa"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(3L), 0);
        rm.apply();

        // basic single-expression query
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);

        assert rows != null;
        assert rows.size() == 2 : StringUtils.join(rows, ",");

        String key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k1".equals( key ) : key;

        key = new String(rows.get(1).key.key.array(),rows.get(1).key.key.position(),rows.get(1).key.key.remaining());
        assert "k3".equals(key) : key;

        assert ByteBufferUtil.bytes(1L).equals( rows.get(0).cf.getColumn(ByteBufferUtil.bytes("birthdate")).value());
        assert ByteBufferUtil.bytes(1L).equals( rows.get(1).cf.getColumn(ByteBufferUtil.bytes("birthdate")).value());

        // add a second expression
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.GTE, ByteBufferUtil.bytes(2L));
        clause = Arrays.asList(expr, expr2);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k3".equals( key );

        // same query again, but with resultset not including the subordinate expression
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, new NamesQueryFilter(ByteBufferUtil.bytes("birthdate")));

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k3".equals( key );

        assert rows.get(0).cf.getColumnCount() == 1 : rows.get(0).cf;

        // once more, this time with a slice rowset that needs to be expanded
        SliceQueryFilter emptyFilter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 0);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, emptyFilter);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k3".equals( key );

        assert rows.get(0).cf.getColumnCount() == 0;

        // query with index hit but rejected by secondary clause, with a small enough count that just checking count
        // doesn't tell the scan loop that it's done
        IndexExpression expr3 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(-1L));
        clause = Arrays.asList(expr, expr3);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);

        assert rows.isEmpty();
    }

    @Test
    public void testLargeScan() throws IOException
    {
        RowMutation rm;
        for (int i = 0; i < 100; i++)
        {
            rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key" + i));
            rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(34L), 0);
            rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes((long) (i % 2)), 0);
            rm.applyUnsafe();
        }

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(34L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr, expr2);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);

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
        ColumnFamilyStore cfs = Table.open("Keyspace3").getColumnFamilyStore("Indexed1");
        RowMutation rm;

        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = cfs.search(clause, range, 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        String key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

        // delete the column directly
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), 1);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.isEmpty();

        // verify that it's not being indexed under the deletion column value either
        IColumn deletion = rm.getColumnFamilies().iterator().next().iterator().next();
        ByteBuffer deletionLong = ByteBufferUtil.bytes((long) ByteBufferUtil.toInt(deletion.value()));
        IndexExpression expr0 = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, deletionLong);
        List<IndexExpression> clause0 = Arrays.asList(expr0);
        rows = cfs.search(clause0, range, 100, filter);
        assert rows.isEmpty();

        // resurrect w/ a newer timestamp
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 2);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

        // verify that row and delete w/ older timestamp does nothing
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 1);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

        // similarly, column delete w/ older timestamp should do nothing
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), 1);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

        // delete the entire row (w/ newer timestamp this time)
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 3);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // make sure obsolete mutations don't generate an index entry
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 3);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try insert followed by row delete in the same mutation
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 1);
        rm.delete(new QueryPath("Indexed1"), 2);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try row delete followed by insert in the same mutation
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 3);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 4);
        rm.apply();
        rows = cfs.search(clause, range, 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );
    }

    @Test
    public void testIndexUpdate() throws IOException
    {
        Table table = Table.open("Keyspace2");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 1);
        rm.apply();
        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(2L), 2);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = table.getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);
        assert rows.size() == 0;

        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(2L));
        clause = Arrays.asList(expr);
        rows = table.getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);
        String key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(3L), 0);
        rm.apply();

        rows = table.getColumnFamilyStore("Indexed1").search(clause, range, 100, filter);
        key = ByteBufferUtil.string(rows.get(0).key.key);
        assert "k1".equals( key );

    }
    
    @Test
    public void testDeleteOfInconsistentValuesInKeysIndex() throws Exception
    {
        String keySpace = "Keyspace2";
        String cfName = "Indexed1";

        Table table = Table.open(keySpace);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.truncate().get();

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer colName = ByteBufferUtil.bytes("birthdate"); 
        ByteBuffer val1 = ByteBufferUtil.bytes(1L);
        ByteBuffer val2 = ByteBufferUtil.bytes(2L);

        // create a row and update the "birthdate" value, test that the index query fetches this version
        RowMutation rm;
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null, colName), val1, 0);
        rm.apply();
        IndexExpression expr = new IndexExpression(colName, IndexOperator.EQ, val1);
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(1, rows.size());

        // force a flush, so our index isn't being read from a memtable
        table.getColumnFamilyStore(cfName).forceBlockingFlush();

        // now apply another update, but force the index update to be skipped
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null, colName), val2, 1);
        table.apply(rm, true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored 
        // (and in fact purged from the index cf).
        // first check for the old value
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());
        // now check for the updated value
        expr = new IndexExpression(colName, IndexOperator.EQ, val2);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null, colName), ByteBufferUtil.bytes(1L), 3);
        table.apply(rm, true, false);

        expr = new IndexExpression(colName, IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndex() throws Exception
    {
        String keySpace = "Keyspace2";
        String cfName = "Indexed2";

        Table table = Table.open(keySpace);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.truncate().get();

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer clusterKey = ByteBufferUtil.bytes("ck1");
        ByteBuffer colName = ByteBufferUtil.bytes("col1"); 
        CompositeType baseComparator = (CompositeType)cfs.getComparator();
        CompositeType.Builder builder = baseComparator.builder();
        builder.add(clusterKey);
        builder.add(colName);
        ByteBuffer compositeName = builder.build();

        ByteBuffer val1 = ByteBufferUtil.bytes("v1");
        ByteBuffer val2 = ByteBufferUtil.bytes("v2");

        // create a row and update the author value
        RowMutation rm;
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null , compositeName), val1, 0);
        rm.apply();

        // test that the index query fetches this version
        IndexExpression expr = new IndexExpression(colName, IndexOperator.EQ, val1);
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(1, rows.size());

        // force a flush and retry the query, so our index isn't being read from a memtable
        table.getColumnFamilyStore(cfName).forceBlockingFlush();
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(1, rows.size());

        // now apply another update, but force the index update to be skipped
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null, compositeName), val2, 1);
        table.apply(rm, true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored 
        // (and in fact purged from the index cf).
        // first check for the old value
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());
        // now check for the updated value
        expr = new IndexExpression(colName, IndexOperator.EQ, val2);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        rm = new RowMutation(keySpace, rowKey);
        rm.add(new QueryPath(cfName, null , compositeName), val1, 2);
        table.apply(rm, true, false);

        expr = new IndexExpression(colName, IndexOperator.EQ, val1);
        clause = Arrays.asList(expr);
        filter = new IdentityQueryFilter();
        range = Util.range("", "");
        rows = table.getColumnFamilyStore(cfName).search(clause, range, 100, filter);
        assertEquals(0, rows.size());
    }

    // See CASSANDRA-2628
    @Test
    public void testIndexScanWithLimitOne() throws IOException
    {
        RowMutation rm;

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("kk1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("kk2"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("kk3"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("kk4"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("notbirthdate")), ByteBufferUtil.bytes(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 0);
        rm.apply();

        // basic single-expression query
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.GT, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(new IndexExpression[]{ expr1, expr2 });
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range<RowPosition> range = Util.range("", "");
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").search(clause, range, 1, filter);

        assert rows != null;
        assert rows.size() == 1 : StringUtils.join(rows, ",");
    }

    @Test
    public void testIndexCreate() throws IOException, ConfigurationException, InterruptedException, ExecutionException
    {
        Table table = Table.open("Keyspace1");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed2", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 1);
        rm.apply();

        ColumnFamilyStore cfs = table.getColumnFamilyStore("Indexed2");
        ColumnDefinition old = cfs.metadata.getColumn_metadata().get(ByteBufferUtil.bytes("birthdate"));
        ColumnDefinition cd = new ColumnDefinition(old.name, old.getValidator(), IndexType.KEYS, null, "birthdate_index", null);
        Future<?> future = cfs.indexManager.addIndexedColumn(cd);
        future.get();
        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        assert cfs.indexManager.getIndexForColumn(cd.name).getIndexCfs().getSSTables().size() > 0;

        queryBirthdate(table);

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        SecondaryIndex indexedCfs = cfs.indexManager.getIndexForColumn(ByteBufferUtil.bytes("birthdate"));
        cfs.indexManager.removeIndexedColumn(ByteBufferUtil.bytes("birthdate"));
        assert !indexedCfs.isIndexBuilt(ByteBufferUtil.bytes("birthdate"));

        // rebuild & re-query
        future = cfs.indexManager.addIndexedColumn(cd);
        future.get();
        queryBirthdate(table);
    }

    private void queryBirthdate(Table table) throws CharacterCodingException
    {
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        List<IndexExpression> clause = Arrays.asList(expr);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        List<Row> rows = table.getColumnFamilyStore("Indexed2").search(clause, Util.range("", ""), 100, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assertEquals("k1", ByteBufferUtil.string(rows.get(0).key.key));
    }

    @Test
    public void testInclusiveBounds() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                             Util.bounds("key1", "key2"),
                                             10,
                                             new NamesQueryFilter(ByteBufferUtil.bytes("asdf")),
                                             null);
        assertEquals(2, result.size());
        assert result.get(0).key.key.equals(ByteBufferUtil.bytes("key1"));
    }

    @Test
    public void testDeleteSuperRowSticksAfterFlush() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName= "Super1";
        ByteBuffer scfName = ByteBufferUtil.bytes("SuperDuper");
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("flush-resurrection");

        // create an isolated sstable.
        putColsSuper(cfs, key, scfName,
                new Column(getBytes(1L), ByteBufferUtil.bytes("val1"), 1),
                new Column(getBytes(2L), ByteBufferUtil.bytes("val2"), 1),
                new Column(getBytes(3L), ByteBufferUtil.bytes("val3"), 1));
        cfs.forceBlockingFlush();

        // insert, don't flush.
        putColsSuper(cfs, key, scfName,
                new Column(getBytes(4L), ByteBufferUtil.bytes("val4"), 1),
                new Column(getBytes(5L), ByteBufferUtil.bytes("val5"), 1),
                new Column(getBytes(6L), ByteBufferUtil.bytes("val6"), 1));

        // verify insert.
        final SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(100);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        assertRowAndColCount(1, 6, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // deeleet.
        RowMutation rm = new RowMutation(table.name, key.key);
        rm.delete(new QueryPath(cfName, scfName), 2);
        rm.apply();

        // verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // late insert.
        putColsSuper(cfs, key, scfName,
                new Column(getBytes(4L), ByteBufferUtil.bytes("val4"), 1L),
                new Column(getBytes(7L), ByteBufferUtil.bytes("val7"), 1L));

        // re-verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // make sure new writes are recognized.
        putColsSuper(cfs, key, scfName,
                new Column(getBytes(3L), ByteBufferUtil.bytes("val3"), 3),
                new Column(getBytes(8L), ByteBufferUtil.bytes("val8"), 3),
                new Column(getBytes(9L), ByteBufferUtil.bytes("val9"), 3));
        assertRowAndColCount(1, 3, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));
    }

    private static void assertRowAndColCount(int rowCount, int colCount, ByteBuffer sc, boolean isDeleted, Collection<Row> rows) throws CharacterCodingException
    {
        assert rows.size() == rowCount : "rowcount " + rows.size();
        for (Row row : rows)
        {
            assert row.cf != null : "cf was null";
            if (sc != null)
                assert row.cf.getColumn(sc).getSubColumns().size() == colCount : row.cf.getColumn(sc).getSubColumns().size();
            else
                assert row.cf.getColumnCount() == colCount : "colcount " + row.cf.getColumnCount() + "|" + str(row.cf);
            if (isDeleted)
                assert row.cf.isMarkedForDelete() : "cf not marked for delete";
        }
    }

    private static String str(ColumnFamily cf) throws CharacterCodingException
    {
        StringBuilder sb = new StringBuilder();
        for (IColumn col : cf.getSortedColumns())
            sb.append(String.format("(%s,%s,%d),", ByteBufferUtil.string(col.name()), ByteBufferUtil.string(col.value()), col.timestamp()));
        return sb.toString();
    }

    private static void putColsSuper(ColumnFamilyStore cfs, DecoratedKey key, ByteBuffer scfName, Column... cols) throws Throwable
    {
        RowMutation rm = new RowMutation(cfs.table.name, key.key);
        ColumnFamily cf = ColumnFamily.create(cfs.table.name, cfs.getColumnFamilyName());
        SuperColumn sc = new SuperColumn(scfName, cfs.metadata.subcolumnComparator);
        for (Column col : cols)
            sc.addColumn(col);
        cf.addColumn(sc);
        rm.add(cf);
        rm.apply();
    }

    private static void putColsStandard(ColumnFamilyStore cfs, DecoratedKey key, Column... cols) throws Throwable
    {
        RowMutation rm = new RowMutation(cfs.table.name, key.key);
        ColumnFamily cf = ColumnFamily.create(cfs.table.name, cfs.getColumnFamilyName());
        for (Column col : cols)
            cf.addColumn(col);
        rm.add(cf);
        rm.apply();
    }

    @Test
    public void testDeleteStandardRowSticksAfterFlush() throws Throwable
    {
        // test to make sure flushing after a delete doesn't resurrect delted cols.
        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("f-flush-resurrection");

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(100);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        // insert
        putColsStandard(cfs, key, column("col1", "val1", 1), column("col2", "val2", 1));
        assertRowAndColCount(1, 2, null, false, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // flush.
        cfs.forceBlockingFlush();

        // insert, don't flush
        putColsStandard(cfs, key, column("col3", "val3", 1), column("col4", "val4", 1));
        assertRowAndColCount(1, 4, null, false, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // delete (from sstable and memtable)
        RowMutation rm = new RowMutation(table.name, key.key);
        rm.delete(new QueryPath(cfs.columnFamily, null, null), 2);
        rm.apply();

        // verify delete
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        putColsStandard(cfs, key, column("col5", "val5", 1), column("col2", "val2", 1));

        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // make sure that new writes are recognized.
        putColsStandard(cfs, key, column("col6", "val6", 3), column("col7", "val7", 3));
        assertRowAndColCount(1, 2, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));

        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush();
        assertRowAndColCount(1, 2, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null));
    }


    private ColumnFamilyStore insertKey1Key2() throws IOException, ExecutionException, InterruptedException
    {
        List<IMutation> rms = new LinkedList<IMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("key2"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        return Util.writeColumnFamily(rms);
    }

    @Test
    public void testBackupAfterFlush() throws Throwable
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        for (int version = 1; version <= 2; ++version)
        {
            Descriptor existing = new Descriptor(cfs.directories.getDirectoryForNewSSTables(1), "Keyspace2", "Standard1", version, false);
            Descriptor desc = new Descriptor(Directories.getBackupsDirectory(existing), "Keyspace2", "Standard1", version, false);
            for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.STATS })
                assertTrue("can not find backedup file:" + desc.filenameFor(c), new File(desc.filenameFor(c)).exists());
        }
    }

    @Test
    public void testSuperSliceByNamesCommand() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName= "Super4";
        ByteBuffer superColName = ByteBufferUtil.bytes("HerpDerp");
        DecoratedKey key = Util.dk("multiget-slice-resurrection");
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        // Initially create a SC with 1 subcolumn
        putColsSuper(cfs, key, superColName, new Column(ByteBufferUtil.bytes("c1"), ByteBufferUtil.bytes("a"), 1));
        cfs.forceBlockingFlush();

        // Add another column
        putColsSuper(cfs, key, superColName, new Column(ByteBufferUtil.bytes("c2"), ByteBufferUtil.bytes("b"), 2));

        // Test fetching the supercolumn by name
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(tableName, key.key, new QueryPath(cfName), Collections.singletonList(superColName));
        ColumnFamily cf = cmd.getRow(table).cf;
        SuperColumn superColumn = (SuperColumn) cf.getColumn(superColName);
        assertColumns(superColumn, "c1", "c2");
    }

    // CASSANDRA-3467.  the key here is that supercolumn and subcolumn comparators are different
    @Test
    public void testSliceByNamesCommandOnUUIDTypeSCF() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName = "Super6";
        ByteBuffer superColName = LexicalUUIDType.instance.fromString("a4ed3562-0e8e-4b41-bdfd-c45a2774682d");
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("slice-get-uuid-type");

        // Insert a row with one supercolumn and multiple subcolumns
        putColsSuper(cfs, key, superColName, new Column(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("A"), 1),
                                             new Column(ByteBufferUtil.bytes("b"), ByteBufferUtil.bytes("B"), 1));

        // Get the entire supercolumn like normal
        IColumn columnGet = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(cfName, superColName))).getColumn(superColName);
        assertEquals(ByteBufferUtil.bytes("A"), columnGet.getSubColumn(ByteBufferUtil.bytes("a")).value());
        assertEquals(ByteBufferUtil.bytes("B"), columnGet.getSubColumn(ByteBufferUtil.bytes("b")).value());

        // Now do the SliceByNamesCommand on the supercolumn, passing both subcolumns in as columns to get
        ArrayList<ByteBuffer> sliceColNames = new ArrayList<ByteBuffer>();
        sliceColNames.add(ByteBufferUtil.bytes("a"));
        sliceColNames.add(ByteBufferUtil.bytes("b"));
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(tableName, key.key, new QueryPath(cfName, superColName), sliceColNames);
        IColumn columnSliced = cmd.getRow(table).cf.getColumn(superColName);

        // Make sure the slice returns the same as the straight get
        assertEquals(ByteBufferUtil.bytes("A"), columnSliced.getSubColumn(ByteBufferUtil.bytes("a")).value());
        assertEquals(ByteBufferUtil.bytes("B"), columnSliced.getSubColumn(ByteBufferUtil.bytes("b")).value());
    }

    @Test
    public void testSliceByNamesCommandOldMetatada() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName= "Standard1";
        DecoratedKey key = Util.dk("slice-name-old-metadata");
        ByteBuffer cname = ByteBufferUtil.bytes("c1");
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        // Create a column a 'high timestamp'
        putColsStandard(cfs, key, new Column(cname, ByteBufferUtil.bytes("a"), 2));
        cfs.forceBlockingFlush();

        // Nuke the metadata and reload that sstable
        Collection<SSTableReader> ssTables = cfs.getSSTables();
        assertEquals(1, ssTables.size());
        cfs.clearUnsafe();
        assertEquals(0, cfs.getSSTables().size());

        new File(ssTables.iterator().next().descriptor.filenameFor(SSTable.COMPONENT_STATS)).delete();
        cfs.loadNewSSTables();

        // Add another column with a lower timestamp
        putColsStandard(cfs, key, new Column(cname, ByteBufferUtil.bytes("b"), 1));

        // Test fetching the column by name returns the first column
        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(tableName, key.key, new QueryPath(cfName), Collections.singletonList(cname));
        ColumnFamily cf = cmd.getRow(table).cf;
        Column column = (Column) cf.getColumn(cname);
        assert column.value().equals(ByteBufferUtil.bytes("a")) : "expecting a, got " + ByteBufferUtil.string(column.value());
    }

    private static void assertTotalColCount(Collection<Row> rows, int expectedCount) throws CharacterCodingException
    {
        int columns = 0;
        for (Row row : rows)
        {
            columns += row.getLiveColumnCount();
        }
        assert columns == expectedCount : "Expected " + expectedCount + " live columns but got " + columns + ": " + rows;
    }

    @Test
    public void testRangeSliceColumnsLimit() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Column[] cols = new Column[5];
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

        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 3, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 3);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 5, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 5);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 8, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 8);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 10, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 10);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 11);

        // Check that when querying by name, we always include all names for a
        // gien row even if it means returning more columns than requested (this is necesseray for CQL)
        sp = new SlicePredicate();
        sp.setColumn_names(Arrays.asList(
            ByteBufferUtil.bytes("c0"),
            ByteBufferUtil.bytes("c1"),
            ByteBufferUtil.bytes("c2")
        ));

        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 1, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 3);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 4, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 5);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 5, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 5);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 6, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 8);
        assertTotalColCount(cfs.getRangeSlice(null, Util.range("", ""), 100, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, false), 8);
    }

    @Test
    public void testRangeSlicePaging() throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Column[] cols = new Column[4];
        for (int i = 0; i < 4; i++)
            cols[i] = column("c" + i, "value", 1);

        putColsStandard(cfs, Util.dk("a"), cols[0], cols[1], cols[2], cols[3]);
        putColsStandard(cfs, Util.dk("b"), cols[0], cols[1], cols[2]);
        putColsStandard(cfs, Util.dk("c"), cols[0], cols[1], cols[2], cols[3]);
        cfs.forceBlockingFlush();

        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(1);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

        Collection<Row> rows = cfs.getRangeSlice(null, Util.range("", ""), 3, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, true);
        assert rows.size() == 1 : "Expected 1 row, got " + rows;
        Row row = rows.iterator().next();
        assertColumnNames(row, "c0", "c1", "c2");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
        rows = cfs.getRangeSlice(null, Util.range("", ""), 3, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, true);
        assert rows.size() == 2 : "Expected 2 rows, got " + rows;
        Iterator<Row> iter = rows.iterator();
        Row row1 = iter.next();
        Row row2 = iter.next();
        assertColumnNames(row1, "c2", "c3");
        assertColumnNames(row2, "c0");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c0")));
        rows = cfs.getRangeSlice(null, new Bounds<RowPosition>(row2.key, Util.rp("")), 3, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, true);
        assert rows.size() == 1 : "Expected 1 row, got " + rows;
        row = rows.iterator().next();
        assertColumnNames(row, "c0", "c1", "c2");

        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
        rows = cfs.getRangeSlice(null, new Bounds<RowPosition>(row.key, Util.rp("")), 3, ThriftValidation.asIFilter(sp, cfs.getComparator()), null, true, true);
        assert rows.size() == 2 : "Expected 2 rows, got " + rows;
        iter = rows.iterator();
        row1 = iter.next();
        row2 = iter.next();
        assertColumnNames(row1, "c2");
        assertColumnNames(row2, "c0", "c1");
    }

    private static void assertColumnNames(Row row, String ... columnNames) throws Exception
    {
        if (row == null || row.cf == null)
            throw new AssertionError("The row should not be empty");

        Iterator<IColumn> columns = row.cf.getSortedColumns().iterator();
        Iterator<String> names = Arrays.asList(columnNames).iterator();

        while (columns.hasNext())
        {
            IColumn c = columns.next();
            assert names.hasNext() : "Got more columns that expected (first unexpected column: " + ByteBufferUtil.string(c.name()) + ")";
            String n = names.next();
            assert c.name().equals(ByteBufferUtil.bytes(n)) : "Expected " + n + ", got " + ByteBufferUtil.string(c.name());
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
        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        Column[] cols = new Column[5];
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
        IFilter qf = ThriftValidation.asIFilter(sp, cfs.getComparator());

        List<Row> rows;

        // Start and end inclusive
        rows = cfs.getRangeSlice(null, new Bounds<RowPosition>(rp("2"), rp("7")), 100, qf, null);
        assert rows.size() == 6;
        assert rows.get(0).key.equals(idk(2));
        assert rows.get(rows.size() - 1).key.equals(idk(7));

        // Start and end excluded
        rows = cfs.getRangeSlice(null, new ExcludingBounds<RowPosition>(rp("2"), rp("7")), 100, qf, null);
        assert rows.size() == 4;
        assert rows.get(0).key.equals(idk(3));
        assert rows.get(rows.size() - 1).key.equals(idk(6));

        // Start excluded, end included
        rows = cfs.getRangeSlice(null, new Range<RowPosition>(rp("2"), rp("7")), 100, qf, null);
        assert rows.size() == 5;
        assert rows.get(0).key.equals(idk(3));
        assert rows.get(rows.size() - 1).key.equals(idk(7));

        // Start included, end excluded
        rows = cfs.getRangeSlice(null, new IncludingExcludingBounds<RowPosition>(rp("2"), rp("7")), 100, qf, null);
        assert rows.size() == 5;
        assert rows.get(0).key.equals(idk(2));
        assert rows.get(rows.size() - 1).key.equals(idk(6));
    }

    @Test
    public void testKeysSearcher() throws Exception
    {
        // Create secondary index and flush to disk
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Indexed1");

        store.truncate();

        for (int i = 0; i < 10; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf("k" + i));
            RowMutation rm = new RowMutation("Keyspace1", key);

            rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")),
                   LongType.instance.decompose(1L),
                   System.currentTimeMillis());

            rm.apply();
        }

        store.forceBlockingFlush();

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, LongType.instance.decompose(1L));
        // explicitly tell to the KeysSearcher to use column limiting for rowsPerQuery to trigger bogus columnsRead--; (CASSANDRA-3996)
        List<Row> rows = store.search(Arrays.asList(expr), Util.range("", ""), 10, new IdentityQueryFilter(), true);

        assert rows.size() == 10;
    }

    private static String keys(List<Row> rows) throws Throwable
    {
        String k = "";
        for (Row r : rows)
            k += " " + ByteBufferUtil.string(r.key.key);
        return k;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiRangeIndexed() throws Throwable
    {
        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
        // directly instead of using QueryFilter to build it for us
        ColumnSlice[] ranges = new ColumnSlice[] {
                new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colA")),
                new ColumnSlice(bytes("colC"), bytes("colE")),
                new ColumnSlice(bytes("colG"), bytes("colG")),
                new ColumnSlice(bytes("colI"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        ColumnSlice[] rangesReversed = new ColumnSlice[] {
                new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colI")),
                new ColumnSlice(bytes("colG"), bytes("colG")),
                new ColumnSlice(bytes("colE"), bytes("colC")),
                new ColumnSlice(bytes("colA"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        Column[] cols = new Column[letters.length];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new Column(ByteBufferUtil.bytes("col" + letters[i].toUpperCase()),
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

    private ColumnFamilyStore prepareMultiRangeSlicesTest(int valueSize, boolean flush) throws Throwable
    {
        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" };
        Column[] cols = new Column[12];
        for (int i = 0; i < cols.length; i++)
        {
            cols[i] = new Column(ByteBufferUtil.bytes("col" + letters[i]), ByteBuffer.wrap(new byte[valueSize]), 1);
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
                new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colc")),
                new ColumnSlice(bytes("colf"), bytes("colg")),
                new ColumnSlice(bytes("colj"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        ColumnSlice[] startMiddleAndEndRangesReversed = new ColumnSlice[] {
                new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colj")),
                new ColumnSlice(bytes("colg"), bytes("colf")),
                new ColumnSlice(bytes("colc"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        ColumnSlice[] startOnlyRange =
                new ColumnSlice[] { new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colc")) };

        ColumnSlice[] startOnlyRangeReversed =
                new ColumnSlice[] { new ColumnSlice(bytes("colc"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        ColumnSlice[] middleOnlyRanges =
                new ColumnSlice[] { new ColumnSlice(bytes("colf"), bytes("colg")) };

        ColumnSlice[] middleOnlyRangesReversed =
                new ColumnSlice[] { new ColumnSlice(bytes("colg"), bytes("colf")) };

        ColumnSlice[] endOnlyRanges =
                new ColumnSlice[] { new ColumnSlice(bytes("colj"), ByteBuffer.wrap(EMPTY_BYTE_ARRAY)) };

        ColumnSlice[] endOnlyRangesReversed =
                new ColumnSlice[] { new ColumnSlice(ByteBuffer.wrap(EMPTY_BYTE_ARRAY), bytes("colj")) };

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
        List<Row> rows = cfs.getRangeSlice(null, new Bounds<RowPosition>(rp(rowKey), rp(rowKey)),
                Integer.MAX_VALUE,
                filter, null, false, false);
        assertSame("unexpected number of rows ", 1, rows.size());
        Row row = rows.get(0);
        Collection<IColumn> cols = !filter.isReversed() ? row.cf.getSortedColumns() : row.cf.getReverseSortedColumns();
        // printRow(cfs, new String(row.key.key.array()), cols);
        String[] returnedColsNames = Iterables.toArray(Iterables.transform(cols, new Function<IColumn, String>()
        {
            public String apply(IColumn arg0)
            {
                return new String(arg0.name().array());
            }
        }), String.class);

        assertTrue(
                "Columns did not match. Expected: " + Arrays.toString(colNames) + " but got:"
                        + Arrays.toString(returnedColsNames), Arrays.equals(colNames, returnedColsNames));
        int i = 0;
        for (IColumn col : cols)
        {
            assertEquals(colNames[i++], new String(col.name().array()));
        }
    }

    private void printRow(ColumnFamilyStore cfs, String rowKey, Collection<IColumn> cols)
    {
        DecoratedKey ROW = Util.dk(rowKey);
        System.err.println("Original:");
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(ROW, new QueryPath("Standard1")));
        System.err.println("Row key: " + rowKey + " Cols: "
                + Iterables.transform(cf.getSortedColumns(), new Function<IColumn, String>()
                {
                    public String apply(IColumn arg0)
                    {
                        return new String(arg0.name().array());
                    }
                }));
        System.err.println("Filtered:");
        Iterable<String> transformed = Iterables.transform(cols, new Function<IColumn, String>()
        {
            public String apply(IColumn arg0)
            {
                return new String(arg0.name().array());
            }
        });
        System.err.println("Row key: " + rowKey + " Cols: " + transformed);
    }
}
