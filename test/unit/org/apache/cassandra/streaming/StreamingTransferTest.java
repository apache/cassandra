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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
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
            RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes(key));
            ColumnFamily cf = ColumnFamily.create(table.name, cfs.columnFamily);
            cf.addColumn(column(key, "v", 0));
            cf.addColumn(new Column(ByteBufferUtil.bytes("birthdate"), ByteBufferUtil.bytes((long) i), 0));
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
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, ByteBufferUtil.bytes(3L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        rows = cfs.scan(clause, range, filter);
        assertEquals(1, rows.size());
        assert rows.get(0).key.key.equals( ByteBufferUtil.bytes("key3")) ;
    }

    @Test
    public void testTransferTableMultiple() throws Exception
    {
        // write temporary SSTables, but don't register them
        Set<String> content = new HashSet<String>();
        content.add("test");
        content.add("test2");
        content.add("test3");
        SSTableReader sstable = SSTableUtils.prepare().write(content);
        String tablename = sstable.getTableName();
        String cfname = sstable.getColumnFamilyName();

        content = new HashSet<String>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableReader sstable2 = SSTableUtils.prepare().write(content);

        // transfer the first and last key
        IPartitioner p = StorageService.getPartitioner();
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("test"))));
        ranges.add(new Range(p.getToken(ByteBufferUtil.bytes("transfer2")), p.getMinimumToken()));
        StreamOutSession session = StreamOutSession.create(tablename, LOCAL, null);
        StreamOut.transferSSTables(session, Arrays.asList(sstable, sstable2), ranges, OperationType.BOOTSTRAP);
        session.await();

        // confirm that the sstables were transferred and registered and that 2 keys arrived
        ColumnFamilyStore cfstore = Table.open(tablename).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(2, rows.size());
        assert rows.get(0).key.key.equals(ByteBufferUtil.bytes("test"));
        assert rows.get(1).key.key.equals(ByteBufferUtil.bytes("transfer3"));
        assert rows.get(0).cf.getColumnsMap().size() == 1;
        assert rows.get(1).cf.getColumnsMap().size() == 1;

        // these keys fall outside of the ranges and should not be transferred
        assert null == cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer1"), new QueryPath("Standard1")));
        assert null == cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer2"), new QueryPath("Standard1")));
        assert null == cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test2"), new QueryPath("Standard1")));
        assert null == cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test3"), new QueryPath("Standard1")));
    }

    @Test
    public void testTransferOfMultipleColumnFamilies() throws Exception
    {
        String keyspace = "KeyCacheSpace";
        IPartitioner p = StorageService.getPartitioner();
        String[] columnFamilies = new String[] { "Standard1", "Standard2", "Standard3" };
        List<SSTableReader> ssTableReaders = new ArrayList<SSTableReader>();

        NavigableMap<DecoratedKey,String> keys = new TreeMap<DecoratedKey,String>();
        for (String cf : columnFamilies)
        {
            Set<String> content = new HashSet<String>();
            content.add("data-" + cf + "-1");
            content.add("data-" + cf + "-2");
            content.add("data-" + cf + "-3");
            SSTableUtils.Context context = SSTableUtils.prepare().ks(keyspace).cf(cf);
            ssTableReaders.add(context.write(content));

            // collect dks for each string key
            for (String str : content)
                keys.put(Util.dk(str), cf);
        }

        // transfer the first and last keys
        Map.Entry<DecoratedKey,String> first = keys.firstEntry();
        Map.Entry<DecoratedKey,String> last = keys.lastEntry();
        Map.Entry<DecoratedKey,String> secondtolast = keys.lowerEntry(last.getKey());
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), first.getKey().token));
        // the left hand side of the range is exclusive, so we transfer from the second-to-last token
        ranges.add(new Range(secondtolast.getKey().token, p.getMinimumToken()));

        StreamOutSession session = StreamOutSession.create(keyspace, LOCAL, null);
        StreamOut.transferSSTables(session, ssTableReaders, ranges, OperationType.BOOTSTRAP);

        session.await();

        // check that only two keys were transferred
        for (Map.Entry<DecoratedKey,String> entry : Arrays.asList(first, last))
        {
            ColumnFamilyStore store = Table.open(keyspace).getColumnFamilyStore(entry.getValue());
            List<Row> rows = Util.getRangeSlice(store);
            assertEquals(rows.toString(), 1, rows.size());
            assertEquals(entry.getKey(), rows.get(0).key);
        }
    }
}
