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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.getBytes;
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
        List<IMutation> rms = new LinkedList<IMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column2")), ByteBufferUtil.bytes("asdf"), 0);
        rms.add(rm);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = store.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1"))));
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
                assert cf.getColumnsMap().isEmpty();

                QueryFilter namesFilter = QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ByteBufferUtil.bytes("a"));
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
        List<Row> result = cfs.getRangeSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                             Util.range(p, "key1", "key2"),
                                             10,
                                             new NamesQueryFilter(ByteBufferUtil.bytes("asdf")));
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
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows != null;
        assert rows.size() == 2 : StringUtils.join(rows, ",");
        
        String key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k1".equals( key );
    
        key = new String(rows.get(1).key.key.array(),rows.get(1).key.key.position(),rows.get(1).key.key.remaining()); 
        assert "k3".equals(key);
        
        assert ByteBufferUtil.bytes(1L).equals( rows.get(0).cf.getColumn(ByteBufferUtil.bytes("birthdate")).value());
        assert ByteBufferUtil.bytes(1L).equals( rows.get(1).cf.getColumn(ByteBufferUtil.bytes("birthdate")).value());

        // add a second expression
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.GTE, ByteBufferUtil.bytes(2L));
        clause = new IndexClause(Arrays.asList(expr, expr2), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k3".equals( key );
    
        // same query again, but with resultset not including the subordinate expression
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, new NamesQueryFilter(ByteBufferUtil.bytes("birthdate")));

        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k3".equals( key );
    
        assert rows.get(0).cf.getColumnCount() == 1 : rows.get(0).cf;

        // once more, this time with a slice rowset that needs to be expanded
        SliceQueryFilter emptyFilter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 0);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, emptyFilter);
      
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k3".equals( key );
    
        assert rows.get(0).cf.getColumnCount() == 0;

        // query with index hit but rejected by secondary clause, with a small enough count that just checking count
        // doesn't tell the scan loop that it's done
        IndexExpression expr3 = new IndexExpression(ByteBufferUtil.bytes("notbirthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(-1L));
        clause = new IndexClause(Arrays.asList(expr, expr3), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1);
        rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

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
        IndexClause clause = new IndexClause(Arrays.asList(expr, expr2), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

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
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        String key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k1".equals( key );

        // delete the column directly
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), 1);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty();

        // verify that it's not being indexed under the deletion column value either
        IColumn deletion = rm.getColumnFamilies().iterator().next().iterator().next();
        ByteBuffer deletionLong = ByteBufferUtil.bytes((long) ByteBufferUtil.toInt(deletion.value()));
        IndexExpression expr0 = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, deletionLong);
        IndexClause clause0 = new IndexClause(Arrays.asList(expr0), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        rows = cfs.scan(clause0, range, filter);
        assert rows.isEmpty();

        // resurrect w/ a newer timestamp
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 2);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k1".equals( key );

        // verify that row and delete w/ older timestamp does nothing
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 1);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k1".equals( key );

        // similarly, column delete w/ older timestamp should do nothing
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), 1);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
        assert "k1".equals( key );

        // delete the entire row (w/ newer timestamp this time)
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 3);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // make sure obsolete mutations don't generate an index entry
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 3);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try insert followed by row delete in the same mutation
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 1);
        rm.delete(new QueryPath("Indexed1"), 2);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.isEmpty() : StringUtils.join(rows, ",");

        // try row delete followed by insert in the same mutation
        rm = new RowMutation("Keyspace3", ByteBufferUtil.bytes("k1"));
        rm.delete(new QueryPath("Indexed1"), 3);
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(1L), 4);
        rm.apply();
        rows = cfs.scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining());
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
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        assert rows.size() == 0;

        expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(2L));
        clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        String key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k1".equals( key );
        
        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        rm = new RowMutation("Keyspace2", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("birthdate")), ByteBufferUtil.bytes(3L), 0);
        rm.apply();

        rows = table.getColumnFamilyStore("Indexed1").scan(clause, range, filter);
        key = new String(rows.get(0).key.key.array(),rows.get(0).key.key.position(),rows.get(0).key.key.remaining()); 
        assert "k1".equals( key );
    
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
        IndexClause clause = new IndexClause(Arrays.asList(new IndexExpression[]{ expr1, expr2 }), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = Table.open("Keyspace1").getColumnFamilyStore("Indexed1").scan(clause, range, filter);

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
        ColumnDefinition cd = new ColumnDefinition(old.name, old.getValidator(), IndexType.KEYS, "birthdate_index");
        Future<?> future = cfs.addIndex(cd);
        future.get();
        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        assert cfs.getIndexedColumnFamilyStore(cd.name).getSSTables().size() > 0;

        queryBirthdate(table);

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        ColumnFamilyStore indexedCfs = cfs.getIndexedColumnFamilyStore(ByteBufferUtil.bytes("birthdate"));
        cfs.removeIndex(ByteBufferUtil.bytes("birthdate"));
        assert !indexedCfs.isIndexBuilt();

        // rebuild & re-query
        future = cfs.addIndex(cd);
        future.get();
        queryBirthdate(table);
    }

    private void queryBirthdate(Table table) throws CharacterCodingException
    {
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore("Indexed2").scan(clause, range, filter);
        assert rows.size() == 1 : StringUtils.join(rows, ",");
        assertEquals("k1", ByteBufferUtil.string(rows.get(0).key.key));
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
                new Column(getBytes(1), ByteBufferUtil.bytes("val1"), 1),
                new Column(getBytes(2), ByteBufferUtil.bytes("val2"), 1),
                new Column(getBytes(3), ByteBufferUtil.bytes("val3"), 1));
        cfs.forceBlockingFlush();
        
        // insert, don't flush.
        putColsSuper(cfs, key, scfName, 
                new Column(getBytes(4), ByteBufferUtil.bytes("val4"), 1),
                new Column(getBytes(5), ByteBufferUtil.bytes("val5"), 1),
                new Column(getBytes(6), ByteBufferUtil.bytes("val6"), 1));
        
        // verify insert.
        final SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(new SliceRange());
        sp.getSlice_range().setCount(100);
        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
        
        assertRowAndColCount(1, 6, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // deeleet.
        RowMutation rm = new RowMutation(table.name, key.key);
        rm.delete(new QueryPath(cfName, scfName), 2);
        rm.apply();
        
        // verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // flush
        cfs.forceBlockingFlush();
        
        // re-verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // late insert.
        putColsSuper(cfs, key, scfName, 
                new Column(getBytes(4), ByteBufferUtil.bytes("val4"), 1L),
                new Column(getBytes(7), ByteBufferUtil.bytes("val7"), 1L));
        
        // re-verify delete.
        assertRowAndColCount(1, 0, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // make sure new writes are recognized.
        putColsSuper(cfs, key, scfName, 
                new Column(getBytes(3), ByteBufferUtil.bytes("val3"), 3),
                new Column(getBytes(8), ByteBufferUtil.bytes("val8"), 3),
                new Column(getBytes(9), ByteBufferUtil.bytes("val9"), 3));
        assertRowAndColCount(1, 3, scfName, false, cfs.getRangeSlice(scfName, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
    }
    
    private static void assertRowAndColCount(int rowCount, int colCount, ByteBuffer sc, boolean isDeleted, Collection<Row> rows)
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
    
    private static String str(ColumnFamily cf)
    {
        StringBuilder sb = new StringBuilder();
        for (IColumn col : cf.getSortedColumns())
            sb.append(String.format("(%s,%s,%d),", new String(col.name().array()), new String(col.value().array()), col.timestamp()));
        return sb.toString();
    }
    
    private static void putColsSuper(ColumnFamilyStore cfs, DecoratedKey key, ByteBuffer scfName, Column... cols) throws Throwable
    {
        RowMutation rm = new RowMutation(cfs.table.name, key.key);
        ColumnFamily cf = ColumnFamily.create(cfs.table.name, cfs.getColumnFamilyName());
        SuperColumn sc = new SuperColumn(scfName, LongType.instance);
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
        assertRowAndColCount(1, 2, null, false, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // flush.
        cfs.forceBlockingFlush();
        
        // insert, don't flush
        putColsStandard(cfs, key, column("col3", "val3", 1), column("col4", "val4", 1));
        assertRowAndColCount(1, 4, null, false, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // delete (from sstable and memtable)
        RowMutation rm = new RowMutation(table.name, key.key);
        rm.delete(new QueryPath(cfs.columnFamily, null, null), 2);
        rm.apply();
        
        // verify delete
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // flush
        cfs.forceBlockingFlush();
        
        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        putColsStandard(cfs, key, column("col5", "val5", 1), column("col2", "val2", 1));
        
        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRowAndColCount(1, 0, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // make sure that new writes are recognized.
        putColsStandard(cfs, key, column("col6", "val6", 3), column("col7", "val7", 3));
        assertRowAndColCount(1, 2, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
        
        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush();
        assertRowAndColCount(1, 2, null, true, cfs.getRangeSlice(null, Util.range("f", "g"), 100, QueryFilter.getFilter(sp, cfs.getComparator())));
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
        insertKey1Key2();

        File backupDir = new File(DatabaseDescriptor.getDataFileLocationForTable("Keyspace2", 0), "backups");

        for (int version = 1; version <= 2; ++version)
        {
            Descriptor desc = new Descriptor(backupDir, "Keyspace2", "Standard1", version, false);
            for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.STATS })
                assertTrue("can not find backedup file:" + desc.filenameFor(c), new File(desc.filenameFor(c)).exists());
        }
    }
}
