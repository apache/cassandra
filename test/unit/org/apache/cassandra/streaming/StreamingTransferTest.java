package org.apache.cassandra.streaming;

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

import static junit.framework.Assert.assertEquals;
import static org.apache.cassandra.Util.column;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;

public class StreamingTransferTest extends CleanupHelper
{
    public static final InetAddress LOCAL = FBUtilities.getLocalAddress();

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
    }

    @Test
    public void testTransferTable() throws Exception
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Indexed1");
        
        // write a temporary SSTable, and unregister it
        for (int i = 1; i <= 3; i++)
        {
            String key = "key" + i;
            RowMutation rm = new RowMutation("Keyspace1", ByteBuffer.wrap(key.getBytes()));
            ColumnFamily cf = ColumnFamily.create(table.name, cfs.columnFamily);
            cf.addColumn(column(key, "v", 0));
            cf.addColumn(new Column(ByteBufferUtil.bytes("birthdate"), FBUtilities.toByteBuffer((long) i), 0));
            rm.add(cf);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        assert cfs.getSSTables().size() == 1;
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        cfs.removeAllSSTables();

        // transfer the first and last key
        IPartitioner p = StorageService.getPartitioner();
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));
        StreamOutSession session = StreamOutSession.create(table.name, LOCAL, null);
        StreamOut.transferSSTables(session, Arrays.asList(sstable), ranges, OperationType.BOOTSTRAP);
        session.await();

        // confirm that the SSTable was transferred and registered
        List<Row> rows = Util.getRangeSlice(cfs);
        assertEquals(2, rows.size());
        assert rows.get(0).key.key.equals( ByteBufferUtil.bytes("key1"));
        assert rows.get(1).key.key.equals( ByteBufferUtil.bytes("key3"));
        assertEquals(2, rows.get(0).cf.getColumnsMap().size());
        assertEquals(2, rows.get(1).cf.getColumnsMap().size());
        assert rows.get(1).cf.getColumn(ByteBufferUtil.bytes("key3")) != null;

        // and that the index and filter were properly recovered
        assert null != cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key1"), new QueryPath(cfs.columnFamily)));
        assert null != cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key3"), new QueryPath(cfs.columnFamily)));

        // and that the secondary index works
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, FBUtilities.toByteBuffer(3L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        rows = cfs.scan(clause, range, filter);
        assertEquals(1, rows.size());
        assert rows.get(0).key.key.equals( ByteBufferUtil.bytes("key3")) ;
    }

    @Test
    public void testTransferTableMultiple() throws Exception
    {
        // write a temporary SSTable, but don't register it
        Set<String> content = new HashSet<String>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableReader sstable = SSTableUtils.writeSSTable(content);
        String tablename = sstable.getTableName();
        String cfname = sstable.getColumnFamilyName();

        Set<String> content2 = new HashSet<String>();
        content2.add("test");
        content2.add("test2");
        content2.add("test3");
        SSTableReader sstable2 = SSTableUtils.writeSSTable(content2);

        // transfer the first and last key
        IPartitioner p = StorageService.getPartitioner();
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("transfer1"))));
        ranges.add(new Range(p.getToken(ByteBufferUtil.bytes("test2")), p.getMinimumToken()));
        StreamOutSession session = StreamOutSession.create(tablename, LOCAL, null);
        StreamOut.transferSSTables(session, Arrays.asList(sstable, sstable2), ranges, OperationType.BOOTSTRAP);
        session.await();

        // confirm that the SSTable was transferred and registered
        ColumnFamilyStore cfstore = Table.open(tablename).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(6, rows.size());
        assert rows.get(0).key.key.equals( ByteBufferUtil.bytes("test"));
        assert rows.get(3).key.key.equals(ByteBuffer.wrap( "transfer1".getBytes() ));
        assert rows.get(0).cf.getColumnsMap().size() == 1;
        assert rows.get(3).cf.getColumnsMap().size() == 1;

        // these keys fall outside of the ranges and should not be transferred.
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer2"), new QueryPath("Standard1")));
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer3"), new QueryPath("Standard1")));
        
        // and that the index and filter were properly recovered
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test"), new QueryPath("Standard1")));
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer1"), new QueryPath("Standard1")));
    }
}
