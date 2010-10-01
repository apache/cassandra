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
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.CollatingOrderPreservingPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;

public class ColumnFamilyStoreTest extends CleanupHelper
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
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace1", "key1".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rm.add(new QueryPath("Standard1", null, "Column2".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = store.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), new QueryPath("Standard1", null, "Column1".getBytes())));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "key1".getBytes());
        rm.delete(new QueryPath("Standard2", null, null), new TimestampClock(System.currentTimeMillis()));
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                QueryFilter sliceFilter = QueryFilter.getSliceFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();

                QueryFilter namesFilter = QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), "a".getBytes());
                cf = store.getColumnFamily(namesFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();
            }
        };

        TableTest.reTest(store, r);
    }

    /**
     * Writes out a bunch of keys into an SSTable, then runs anticompaction on a range.
     * Checks to see if anticompaction returns true.
     */
    private void testAntiCompaction(String columnFamilyName, int insertsPerTable) throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new ArrayList<RowMutation>();
        for (int j = 0; j < insertsPerTable; j++)
        {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key.getBytes());
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], new TimestampClock(j));
            rms.add(rm);
        }
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        List<Range> ranges  = new ArrayList<Range>();
        IPartitioner partitioner = new CollatingOrderPreservingPartitioner();
        Range r = Util.range(partitioner, "0", "zzzzzzz");
        ranges.add(r);

        List<SSTableReader> fileList = CompactionManager.instance.submitAnticompaction(store, ranges, InetAddress.getByName("127.0.0.1")).get();
        assert fileList.size() >= 1;
    }

    @Test
    public void testAntiCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testAntiCompaction("Standard1", 100);
    }

    @Test
    public void testSkipStartKey() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(ArrayUtils.EMPTY_BYTE_ARRAY,
                                             Util.range(p, "key1", "key2"),
                                             10,
                                             new NamesQueryFilter("asdf".getBytes()));
        assertEquals(1, result.size());
        assert Arrays.equals(result.get(0).key.key, "key2".getBytes());
    }

    @Test
    public void testIndexScan() throws IOException
    {
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "k1".getBytes());
        rm.add(new QueryPath("Indexed1", null, "notbirthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(0));
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(0));
        rm.apply();

        rm = new RowMutation("Keyspace1", "k2".getBytes());
        rm.add(new QueryPath("Indexed1", null, "notbirthdate".getBytes("UTF8")), FBUtilities.toByteArray(2L), new TimestampClock(0));
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(2L), new TimestampClock(0));
        rm.apply();

        rm = new RowMutation("Keyspace1", "k3".getBytes());
        rm.add(new QueryPath("Indexed1", null, "notbirthdate".getBytes("UTF8")), FBUtilities.toByteArray(2L), new TimestampClock(0));
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(0));
        rm.apply();
        
        rm = new RowMutation("Keyspace1", "k4aaaa".getBytes());
        rm.add(new QueryPath("Indexed1", null, "notbirthdate".getBytes("UTF8")), FBUtilities.toByteArray(2L), new TimestampClock(0));
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(3L), new TimestampClock(0));
        rm.apply();

        // basic single-expression query
        IndexExpression expr = new IndexExpression("birthdate".getBytes("UTF8"), IndexOperator.EQ, FBUtilities.toByteArray(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ArrayUtils.EMPTY_BYTE_ARRAY, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows != null;
        assert rows.size() == 2 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key);
        assert Arrays.equals("k3".getBytes(), rows.get(1).key.key);
        assert Arrays.equals(FBUtilities.toByteArray(1L), rows.get(0).cf.getColumn("birthdate".getBytes("UTF8")).value());
        assert Arrays.equals(FBUtilities.toByteArray(1L), rows.get(1).cf.getColumn("birthdate".getBytes("UTF8")).value());

        // add a second expression
        IndexExpression expr2 = new IndexExpression("notbirthdate".getBytes("UTF8"), IndexOperator.GTE, FBUtilities.toByteArray(2L));
        clause = new IndexClause(Arrays.asList(expr, expr2), ArrayUtils.EMPTY_BYTE_ARRAY, 100);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key);

        // same query again, but with resultset not including the subordinate expression
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, new NamesQueryFilter("birthdate".getBytes("UTF8")));

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key);
        assert rows.get(0).cf.getColumnCount() == 1 : rows.get(0).cf;

        // once more, this time with a slice rowset that needs to be expanded
        SliceQueryFilter sqf = new SliceQueryFilter(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 0);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, sqf);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key);
        assert rows.get(0).cf.getColumnCount() == 0;
    }

    @Test
    public void testIndexUpdate() throws IOException
    {
        Table table = Table.open("Keyspace2");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace2", "k1".getBytes());
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(1));
        rm.apply();
        rm = new RowMutation("Keyspace2", "k1".getBytes());
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(2L), new TimestampClock(2));
        rm.apply();

        IndexExpression expr = new IndexExpression("birthdate".getBytes("UTF8"), IndexOperator.EQ, FBUtilities.toByteArray(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ArrayUtils.EMPTY_BYTE_ARRAY, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert rows.size() == 0;

        expr = new IndexExpression("birthdate".getBytes("UTF8"), IndexOperator.EQ, FBUtilities.toByteArray(2L));
        clause = new IndexClause(Arrays.asList(expr), ArrayUtils.EMPTY_BYTE_ARRAY, 100);
        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key);

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        rm = new RowMutation("Keyspace2", "k1".getBytes());
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(3L), new TimestampClock(0));
        rm.apply();

        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key);
    }

    @Test
    public void testIndexCreate() throws IOException, ConfigurationException, InterruptedException
    {
        Table table = Table.open("Keyspace1");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace1", "k1".getBytes());
        rm.add(new QueryPath("Indexed2", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(1));
        rm.apply();

        ColumnFamilyStore cfs = table.getColumnFamilyStore("Indexed2");
        ColumnDefinition old = cfs.metadata.column_metadata.get("birthdate".getBytes("UTF8"));
        ColumnDefinition cd = new ColumnDefinition(old.name, old.validator.getClass().getName(), IndexType.KEYS, "birthdate_index");
        cfs.addIndex(cd);
        while (!SystemTable.isIndexBuilt("Keyspace1", cfs.getIndexedColumnFamilyStore("birthdate".getBytes("UTF8")).columnFamily))
            TimeUnit.MILLISECONDS.sleep(100);

        IndexExpression expr = new IndexExpression("birthdate".getBytes("UTF8"), IndexOperator.EQ, FBUtilities.toByteArray(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ArrayUtils.EMPTY_BYTE_ARRAY, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed2").scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key);
    }

    private ColumnFamilyStore insertKey1Key2() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace2", "key1".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        Util.writeColumnFamily(rms);

        rm = new RowMutation("Keyspace2", "key2".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        return Util.writeColumnFamily(rms);
    }
}
