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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("key1".getBytes()));
        rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("Column1".getBytes())), ByteBuffer.wrap("asdf".getBytes()), 0);
        rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("Column2".getBytes())), ByteBuffer.wrap("asdf".getBytes()), 0);
        rms.add(rm);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = store.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), new QueryPath("Standard1", null, ByteBuffer.wrap("Column1".getBytes()))));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("key1".getBytes()));
        rm.delete(new QueryPath("Standard2", null, null), System.currentTimeMillis());
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                QueryFilter sliceFilter = QueryFilter.getSliceFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, 1);
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();

                QueryFilter namesFilter = QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ByteBuffer.wrap("a".getBytes()));
                cf = store.getColumnFamily(namesFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();
            }
        };

        TableTest.reTest(store, r);
    }

    @Test
    public void testSkipStartKey() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(FBUtilities.EMPTY_BYTE_BUFFER,
                                             Util.range(p, "key1", "key2"),
                                             10,
                                             new NamesQueryFilter(ByteBuffer.wrap("asdf".getBytes())));
        assertEquals(1, result.size());
        assert result.get(0).key.key.equals(ByteBuffer.wrap("key2".getBytes()));
    }

    @Test
    public void testIndexScan() throws IOException
    {
        RowMutation rm;

        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("notbirthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("k2".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("notbirthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(2L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("k3".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("notbirthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 0);
        rm.apply();

        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("k4aaaa".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("notbirthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(2L), 0);
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(3L), 0);
        rm.apply();

        // basic single-expression query
        IndexExpression expr = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr),FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows != null;
        assert rows.size() == 2 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());
        assert Arrays.equals("k3".getBytes(), rows.get(1).key.key.array());
        assert FBUtilities.toByteBuffer(1L).equals( rows.get(0).cf.getColumn(ByteBuffer.wrap("birthdate".getBytes("UTF8"))).value());
        assert FBUtilities.toByteBuffer(1L).equals( rows.get(1).cf.getColumn(ByteBuffer.wrap("birthdate".getBytes("UTF8"))).value());

        // add a second expression
        IndexExpression expr2 = new IndexExpression(ByteBuffer.wrap("notbirthdate".getBytes("UTF8")), IndexOperator.GTE, FBUtilities.toByteBuffer(2L));
        clause = new IndexClause(Arrays.asList(expr, expr2), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key.array());

        // same query again, but with resultset not including the subordinate expression
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, new NamesQueryFilter(ByteBuffer.wrap("birthdate".getBytes("UTF8"))));

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key.array());
        assert rows.get(0).cf.getColumnCount() == 1 : rows.get(0).cf;

        // once more, this time with a slice rowset that needs to be expanded
        SliceQueryFilter emptyFilter = new SliceQueryFilter(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, 0);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, emptyFilter);
      
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k3".getBytes(), rows.get(0).key.key.array());
        assert rows.get(0).cf.getColumnCount() == 0;

        // query with index hit but rejected by secondary clause, with a small enough count that just checking count
        // doesn't tell the scan loop that it's done
        IndexExpression expr3 = new IndexExpression(ByteBuffer.wrap("notbirthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(-1L));
        clause = new IndexClause(Arrays.asList(expr, expr3), FBUtilities.EMPTY_BYTE_BUFFER, 1);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows.isEmpty();
    }

    @Test
    public void testIndexDeletions() throws IOException
    {
        ColumnFamilyStore cfs = Table.open("Keyspace3").getColumnFamilyStore("Indexed1");
        RowMutation rm;

        rm = new RowMutation("Keyspace3", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 0);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());

        // delete the column directly
        rm = new RowMutation("Keyspace3", ByteBuffer.wrap("k1".getBytes()));
        rm.delete(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), 1);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty();

        // verify that it's not being indexed under the deletion column value either
        IColumn deletion = rm.getColumnFamilies().iterator().next().iterator().next();
        IndexExpression expr0 = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, deletion.value());
        IndexClause clause0 = new IndexClause(Arrays.asList(expr0), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        rows = cfs.scan(clause0, range, filter);
        assert rows.isEmpty();

        // resurrect w/ a newer timestamp
        rm = new RowMutation("Keyspace3", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 2);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());

        // delete the entire row
        rm = new RowMutation("Keyspace3", ByteBuffer.wrap("k1".getBytes()));
        rm.delete(new QueryPath("Indexed1"), 3);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");
    }

    @Test
    public void testIndexUpdate() throws IOException
    {
        Table table = Table.open("Keyspace2");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace2", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 1);
        rm.apply();
        rm = new RowMutation("Keyspace2", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(2L), 2);
        rm.apply();

        IndexExpression expr = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert rows.size() == 0;

        expr = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(2L));
        clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        rm = new RowMutation("Keyspace2", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed1", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(3L), 0);
        rm.apply();

        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());
    }

    @Test
    public void testIndexCreate() throws IOException, ConfigurationException, InterruptedException
    {
        Table table = Table.open("Keyspace1");

        // create a row and update the birthdate value, test that the index query fetches the new version
        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBuffer.wrap("k1".getBytes()));
        rm.add(new QueryPath("Indexed2", null, ByteBuffer.wrap("birthdate".getBytes("UTF8"))), FBUtilities.toByteBuffer(1L), 1);
        rm.apply();

        ColumnFamilyStore cfs = table.getColumnFamilyStore("Indexed2");
        ColumnDefinition old = cfs.metadata.column_metadata.get(ByteBuffer.wrap("birthdate".getBytes("UTF8")));
        ColumnDefinition cd = new ColumnDefinition(old.name, old.validator.getClass().getName(), IndexType.KEYS, "birthdate_index");
        cfs.addIndex(cd);
        while (!SystemTable.isIndexBuilt("Keyspace1", cfs.getIndexedColumnFamilyStore(ByteBuffer.wrap("birthdate".getBytes("UTF8"))).columnFamily))
            TimeUnit.MILLISECONDS.sleep(100);

        IndexExpression expr = new IndexExpression(ByteBuffer.wrap("birthdate".getBytes("UTF8")), IndexOperator.EQ, FBUtilities.toByteBuffer(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed2").scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assert Arrays.equals("k1".getBytes(), rows.get(0).key.key.array());
    }

    private ColumnFamilyStore insertKey1Key2() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace2", ByteBuffer.wrap("key1".getBytes()));
        rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("Column1".getBytes())), ByteBuffer.wrap("asdf".getBytes()), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        rm = new RowMutation("Keyspace2", ByteBuffer.wrap("key2".getBytes()));
        rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("Column1".getBytes())), ByteBuffer.wrap("asdf".getBytes()), 0);
        rms.add(rm);
        return Util.writeColumnFamily(rms);
    }
}
