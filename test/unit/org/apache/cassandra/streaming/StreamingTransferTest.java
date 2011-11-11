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
import org.apache.cassandra.Util;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.addMutation;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.context.CounterContext;
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
import org.apache.cassandra.utils.NodeId;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;

public class StreamingTransferTest extends CleanupHelper
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingTransferTest.class);

    public static final InetAddress LOCAL = FBUtilities.getBroadcastAddress();

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
    }

    /**
     * Create and transfer a single sstable, and return the keys that should have been transferred.
     * The Mutator must create the given column, but it may also create any other columns it pleases.
     */
    private List<String> createAndTransfer(Table table, ColumnFamilyStore cfs, Mutator mutator) throws Exception
    {
        // write a temporary SSTable, and unregister it
        logger.debug("Mutating " + cfs.columnFamily);
        long timestamp = 1234;
        for (int i = 1; i <= 3; i++)
            mutator.mutate("key" + i, "col" + i, timestamp);
        cfs.forceBlockingFlush();
        Util.compactAll(cfs).get();
        assertEquals(1, cfs.getSSTables().size());
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        cfs.clearUnsafe();

        // transfer the first and last key
        logger.debug("Transferring " + cfs.columnFamily);
        transfer(table, sstable);

        // confirm that a single SSTable was transferred and registered
        assertEquals(1, cfs.getSSTables().size());

        // and that the index and filter were properly recovered
        int[] offs = new int[]{1, 3};
        List<Row> rows = Util.getRangeSlice(cfs);
        assertEquals(offs.length, rows.size());
        for (int i = 0; i < offs.length; i++)
        {
            String key = "key" + offs[i];
            String col = "col" + offs[i];
            assert null != cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk(key),
                                               new QueryPath(cfs.columnFamily)));
            assert rows.get(i).key.key.equals(ByteBufferUtil.bytes(key));
            assert rows.get(i).cf.getColumn(ByteBufferUtil.bytes(col)) != null;
        }

        // and that the max timestamp for the file was rediscovered
        assertEquals(timestamp, cfs.getSSTables().iterator().next().getMaxTimestamp());
        
        List<String> keys = new ArrayList<String>();
        for (int off : offs)
            keys.add("key" + off);

        logger.debug("... everything looks good for " + cfs.columnFamily);
        return keys;
    }

    private void transfer(Table table, SSTableReader sstable) throws Exception
    {
        IPartitioner p = StorageService.getPartitioner();
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));
        StreamOutSession session = StreamOutSession.create(table.name, LOCAL, null);
        StreamOut.transferSSTables(session, Arrays.asList(sstable), ranges, OperationType.BOOTSTRAP);
        session.await();
    }

    @Test
    public void testTransferTable() throws Exception
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfs = table.getColumnFamilyStore("Indexed1");
        
        List<String> keys = createAndTransfer(table, cfs, new Mutator()
        {
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                long val = key.hashCode();
                RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes(key));
                ColumnFamily cf = ColumnFamily.create(table.name, cfs.columnFamily);
                cf.addColumn(column(col, "v", timestamp));
                cf.addColumn(new Column(ByteBufferUtil.bytes("birthdate"), ByteBufferUtil.bytes(val), timestamp));
                rm.add(cf);
                logger.debug("Applying row to transfer " + rm);
                rm.apply();
            }
        });

        // confirm that the secondary index was recovered
        for (String key : keys)
        {
            long val = key.hashCode();
            IPartitioner p = StorageService.getPartitioner();
            IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"),
                                                       IndexOperator.EQ,
                                                       ByteBufferUtil.bytes(val));
            IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
            IFilter filter = new IdentityQueryFilter();
            Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
            List<Row> rows = cfs.search(clause, range, filter);
            assertEquals(1, rows.size());
            assert rows.get(0).key.key.equals(ByteBufferUtil.bytes(key));
        }
    }

    @Test
    public void testTransferTableSuper() throws Exception
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfs = table.getColumnFamilyStore("Super1");
        
        createAndTransfer(table, cfs, new Mutator()
        {
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                RowMutation rm = new RowMutation(table.name, ByteBufferUtil.bytes(key));
                addMutation(rm, cfs.columnFamily, col, 1, "val1", timestamp);
                rm.apply();
            }
        });
    }

    @Test
    public void testTransferTableCounter() throws Exception
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfs = table.getColumnFamilyStore("Counter1");
        final CounterContext cc = new CounterContext();
        
        final Map<String, ColumnFamily> cleanedEntries = new HashMap<String, ColumnFamily>();

        List<String> keys = createAndTransfer(table, cfs, new Mutator()
        {
            /** Creates a new SSTable per key: all will be merged before streaming. */
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                Map<String, ColumnFamily> entries = new HashMap<String, ColumnFamily>();
                ColumnFamily cf = ColumnFamily.create(cfs.metadata);
                ColumnFamily cfCleaned = ColumnFamily.create(cfs.metadata);
                CounterContext.ContextState state = CounterContext.ContextState.allocate(4, 1);
                state.writeElement(NodeId.fromInt(2), 9L, 3L, true);
                state.writeElement(NodeId.fromInt(4), 4L, 2L);
                state.writeElement(NodeId.fromInt(6), 3L, 3L);
                state.writeElement(NodeId.fromInt(8), 2L, 4L);
                cf.addColumn(new CounterColumn(ByteBufferUtil.bytes(col),
                                               state.context,
                                               timestamp));
                cfCleaned.addColumn(new CounterColumn(ByteBufferUtil.bytes(col),
                                                      cc.clearAllDelta(state.context),
                                                      timestamp));

                entries.put(key, cf);
                cleanedEntries.put(key, cfCleaned);
                cfs.addSSTable(SSTableUtils.prepare()
                    .ks(table.name)
                    .cf(cfs.columnFamily)
                    .generation(0)
                    .write(entries));
            }
        });

        // filter pre-cleaned entries locally, and ensure that the end result is equal
        cleanedEntries.keySet().retainAll(keys);
        SSTableReader cleaned = SSTableUtils.prepare()
            .ks(table.name)
            .cf(cfs.columnFamily)
            .generation(0)
            .write(cleanedEntries);
        SSTableReader streamed = cfs.getSSTables().iterator().next();
        SSTableUtils.assertContentEquals(cleaned, streamed);

        // Retransfer the file, making sure it is now idempotent (see CASSANDRA-3481)
        cfs.clearUnsafe();
        transfer(table, streamed);
        SSTableReader restreamed = cfs.getSSTables().iterator().next();
        SSTableUtils.assertContentEquals(streamed, restreamed);
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
        // Acquiring references, transferSSTables needs it
        sstable.acquireReference();
        sstable2.acquireReference();
        StreamOutSession session = StreamOutSession.create(tablename, LOCAL, null);
        StreamOut.transferSSTables(session, Arrays.asList(sstable, sstable2), ranges, OperationType.BOOTSTRAP);
        session.await();

        // confirm that the sstables were transferred and registered and that 2 keys arrived
        ColumnFamilyStore cfstore = Table.open(tablename).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(2, rows.size());
        assert rows.get(0).key.key.equals(ByteBufferUtil.bytes("test"));
        assert rows.get(1).key.key.equals(ByteBufferUtil.bytes("transfer3"));
        assert rows.get(0).cf.getColumnCount() == 1;
        assert rows.get(1).cf.getColumnCount() == 1;

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

        // Acquiring references, transferSSTables needs it
        if (!SSTableReader.acquireReferences(ssTableReaders))
            throw new AssertionError();

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

    public interface Mutator
    {
        public void mutate(String key, String col, long timestamp) throws Exception;
    }
}
