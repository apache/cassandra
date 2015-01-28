/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.utils.concurrent.Refs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.column;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StreamingTransferTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingTransferTest.class);

    public static final InetAddress LOCAL = FBUtilities.getBroadcastAddress();
    public static final String KEYSPACE1 = "StreamingTransferTest1";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COUNTER = "Counter1";
    public static final String CF_STANDARDINT = "StandardInteger1";
    public static final String CF_INDEX = "Indexed1";
    public static final String KEYSPACE_CACHEKEY = "KeyStreamingTransferTestSpace";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD3 = "Standard3";
    public static final String KEYSPACE2 = "StreamingTransferTest2";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_STANDARDINT, IntegerType.instance),
                                    SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX, true));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1));
        SchemaLoader.createKeyspace(KEYSPACE_CACHEKEY,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD2),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD3));
    }

    /**
     * Test if empty {@link StreamPlan} returns success with empty result.
     */
    @Test
    public void testEmptyStreamPlan() throws Exception
    {
        StreamResultFuture futureResult = new StreamPlan("StreamingTransferTest").execute();
        final UUID planId = futureResult.planId;
        Futures.addCallback(futureResult, new FutureCallback<StreamState>()
        {
            public void onSuccess(StreamState result)
            {
                assert planId.equals(result.planId);
                assert result.description.equals("StreamingTransferTest");
                assert result.sessions.isEmpty();
            }

            public void onFailure(Throwable t)
            {
                fail();
            }
        });
        // should be complete immediately
        futureResult.get(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRequestEmpty() throws Exception
    {
        // requesting empty data should succeed
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));

        StreamResultFuture futureResult = new StreamPlan("StreamingTransferTest")
                                                  .requestRanges(LOCAL, LOCAL, KEYSPACE2, ranges)
                                                  .execute();

        UUID planId = futureResult.planId;
        StreamState result = futureResult.get();
        assert planId.equals(result.planId);
        assert result.description.equals("StreamingTransferTest");

        // we should have completed session with empty transfer
        assert result.sessions.size() == 1;
        SessionInfo session = Iterables.get(result.sessions, 0);
        assert session.peer.equals(LOCAL);
        assert session.getTotalFilesReceived() == 0;
        assert session.getTotalFilesSent() == 0;
        assert session.getTotalSizeReceived() == 0;
        assert session.getTotalSizeSent() == 0;
    }

    /**
     * Create and transfer a single sstable, and return the keys that should have been transferred.
     * The Mutator must create the given column, but it may also create any other columns it pleases.
     */
    private List<String> createAndTransfer(ColumnFamilyStore cfs, Mutator mutator, boolean transferSSTables) throws Exception
    {
        // write a temporary SSTable, and unregister it
        logger.debug("Mutating {}", cfs.name);
        long timestamp = 1234;
        for (int i = 1; i <= 3; i++)
            mutator.mutate("key" + i, "col" + i, timestamp);
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertEquals(1, cfs.getSSTables().size());

        // transfer the first and last key
        logger.debug("Transferring {}", cfs.name);
        int[] offs;
        if (transferSSTables)
        {
            SSTableReader sstable = cfs.getSSTables().iterator().next();
            cfs.clearUnsafe();
            transferSSTables(sstable);
            offs = new int[]{1, 3};
        }
        else
        {
            long beforeStreaming = System.currentTimeMillis();
            transferRanges(cfs);
            cfs.discardSSTables(beforeStreaming);
            offs = new int[]{2, 3};
        }

        // confirm that a single SSTable was transferred and registered
        assertEquals(1, cfs.getSSTables().size());

        // and that the index and filter were properly recovered
        List<Row> rows = Util.getRangeSlice(cfs);
        assertEquals(offs.length, rows.size());
        for (int i = 0; i < offs.length; i++)
        {
            String key = "key" + offs[i];
            String col = "col" + offs[i];
            assert cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk(key), cfs.name, System.currentTimeMillis())) != null;
            assert rows.get(i).key.getKey().equals(ByteBufferUtil.bytes(key));
            assert rows.get(i).cf.getColumn(cellname(col)) != null;
        }

        // and that the max timestamp for the file was rediscovered
        assertEquals(timestamp, cfs.getSSTables().iterator().next().getMaxTimestamp());

        List<String> keys = new ArrayList<>();
        for (int off : offs)
            keys.add("key" + off);

        logger.debug("... everything looks good for {}", cfs.name);
        return keys;
    }

    private void transferSSTables(SSTableReader sstable) throws Exception
    {
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));
        transfer(sstable, ranges);
    }

    private void transferRanges(ColumnFamilyStore cfs) throws Exception
    {
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        // wrapped range
        ranges.add(new Range<Token>(p.getToken(ByteBufferUtil.bytes("key1")), p.getToken(ByteBufferUtil.bytes("key0"))));
        new StreamPlan("StreamingTransferTest").transferRanges(LOCAL, cfs.keyspace.getName(), ranges, cfs.getColumnFamilyName()).execute().get();
    }

    private void transfer(SSTableReader sstable, List<Range<Token>> ranges) throws Exception
    {
        new StreamPlan("StreamingTransferTest").transferFiles(LOCAL, makeStreamingDetails(ranges, Refs.tryRef(Arrays.asList(sstable)))).execute().get();
    }

    private Collection<StreamSession.SSTableStreamingSections> makeStreamingDetails(List<Range<Token>> ranges, Refs<SSTableReader> sstables)
    {
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            details.add(new StreamSession.SSTableStreamingSections(sstable, sstables.get(sstable),
                                                                   sstable.getPositionsForRanges(ranges),
                                                                   sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        }
        return details;
    }

    private void doTransferTable(boolean transferSSTables) throws Exception
    {
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Indexed1");

        List<String> keys = createAndTransfer(cfs, new Mutator()
        {
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                long val = key.hashCode();
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(keyspace.getName(), cfs.name);
                cf.addColumn(column(col, "v", timestamp));
                cf.addColumn(new BufferCell(cellname("birthdate"), ByteBufferUtil.bytes(val), timestamp));
                Mutation rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key), cf);
                logger.debug("Applying row to transfer {}", rm);
                rm.applyUnsafe();
            }
        }, transferSSTables);

        // confirm that the secondary index was recovered
        for (String key : keys)
        {
            long val = key.hashCode();
            IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"),
                                                       Operator.EQ,
                                                       ByteBufferUtil.bytes(val));
            List<IndexExpression> clause = Arrays.asList(expr);
            IDiskAtomFilter filter = new IdentityQueryFilter();
            Range<RowPosition> range = Util.range("", "");
            List<Row> rows = cfs.search(range, clause, filter, 100);
            assertEquals(1, rows.size());
            assert rows.get(0).key.getKey().equals(ByteBufferUtil.bytes(key));
        }
    }

    /**
     * Test to make sure RangeTombstones at column index boundary transferred correctly.
     */
    @Test
    public void testTransferRangeTombstones() throws Exception
    {
        String ks = KEYSPACE1;
        String cfname = "StandardInteger1";
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        String key = "key1";
        Mutation rm = new Mutation(ks, ByteBufferUtil.bytes(key));
        // add columns of size slightly less than column_index_size to force insert column index
        rm.add(cfname, cellname(1), ByteBuffer.wrap(new byte[DatabaseDescriptor.getColumnIndexSize() - 64]), 2);
        rm.add(cfname, cellname(6), ByteBuffer.wrap(new byte[DatabaseDescriptor.getColumnIndexSize()]), 2);
        ColumnFamily cf = rm.addOrGet(cfname);
        // add RangeTombstones
        cf.delete(new DeletionInfo(cellname(2), cellname(3), cf.getComparator(), 1, (int) (System.currentTimeMillis() / 1000)));
        cf.delete(new DeletionInfo(cellname(5), cellname(7), cf.getComparator(), 1, (int) (System.currentTimeMillis() / 1000)));
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        SSTableReader sstable = cfs.getSSTables().iterator().next();
        cfs.clearUnsafe();
        transferSSTables(sstable);

        // confirm that a single SSTable was transferred and registered
        assertEquals(1, cfs.getSSTables().size());

        List<Row> rows = Util.getRangeSlice(cfs);
        assertEquals(1, rows.size());
    }

    @Test
    public void testTransferTableViaRanges() throws Exception
    {
        doTransferTable(false);
    }

    @Test
    public void testTransferTableViaSSTables() throws Exception
    {
        doTransferTable(true);
    }

    @Test
    public void testTransferTableCounter() throws Exception
    {
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Counter1");
        final CounterContext cc = new CounterContext();

        final Map<String, ColumnFamily> cleanedEntries = new HashMap<>();

        List<String> keys = createAndTransfer(cfs, new Mutator()
        {
            /** Creates a new SSTable per key: all will be merged before streaming. */
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                Map<String, ColumnFamily> entries = new HashMap<>();
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
                ColumnFamily cfCleaned = ArrayBackedSortedColumns.factory.create(cfs.metadata);
                CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 1, 3);
                state.writeLocal(CounterId.fromInt(2), 9L, 3L);
                state.writeRemote(CounterId.fromInt(4), 4L, 2L);
                state.writeRemote(CounterId.fromInt(6), 3L, 3L);
                state.writeRemote(CounterId.fromInt(8), 2L, 4L);
                cf.addColumn(new BufferCounterCell(cellname(col), state.context, timestamp));
                cfCleaned.addColumn(new BufferCounterCell(cellname(col), cc.clearAllLocal(state.context), timestamp));

                entries.put(key, cf);
                cleanedEntries.put(key, cfCleaned);
                cfs.addSSTable(SSTableUtils.prepare()
                    .ks(keyspace.getName())
                    .cf(cfs.name)
                    .generation(0)
                    .write(entries));
            }
        }, true);

        // filter pre-cleaned entries locally, and ensure that the end result is equal
        cleanedEntries.keySet().retainAll(keys);
        SSTableReader cleaned = SSTableUtils.prepare()
            .ks(keyspace.getName())
            .cf(cfs.name)
            .generation(0)
            .write(cleanedEntries);
        SSTableReader streamed = cfs.getSSTables().iterator().next();
        SSTableUtils.assertContentEquals(cleaned, streamed);

        // Retransfer the file, making sure it is now idempotent (see CASSANDRA-3481)
        cfs.clearUnsafe();
        transferSSTables(streamed);
        SSTableReader restreamed = cfs.getSSTables().iterator().next();
        SSTableUtils.assertContentEquals(streamed, restreamed);
    }

    @Test
    public void testTransferTableMultiple() throws Exception
    {
        // write temporary SSTables, but don't register them
        Set<String> content = new HashSet<>();
        content.add("test");
        content.add("test2");
        content.add("test3");
        SSTableReader sstable = new SSTableUtils(KEYSPACE1, CF_STANDARD).prepare().write(content);
        String keyspaceName = sstable.getKeyspaceName();
        String cfname = sstable.getColumnFamilyName();

        content = new HashSet<>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableReader sstable2 = SSTableUtils.prepare().write(content);

        // transfer the first and last key
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("test"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("transfer2")), p.getMinimumToken()));
        // Acquiring references, transferSSTables needs it
        Refs<SSTableReader> refs = Refs.tryRef(Arrays.asList(sstable, sstable2));
        assert refs != null;
        new StreamPlan("StreamingTransferTest").transferFiles(LOCAL, makeStreamingDetails(ranges, refs)).execute().get();

        // confirm that the sstables were transferred and registered and that 2 keys arrived
        ColumnFamilyStore cfstore = Keyspace.open(keyspaceName).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(2, rows.size());
        assert rows.get(0).key.getKey().equals(ByteBufferUtil.bytes("test"));
        assert rows.get(1).key.getKey().equals(ByteBufferUtil.bytes("transfer3"));
        assert rows.get(0).cf.getColumnCount() == 1;
        assert rows.get(1).cf.getColumnCount() == 1;

        // these keys fall outside of the ranges and should not be transferred
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer1"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer2"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test2"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test3"), "Standard1", System.currentTimeMillis())) == null;
    }

    @Test
    public void testTransferOfMultipleColumnFamilies() throws Exception
    {
        String keyspace = KEYSPACE_CACHEKEY;
        IPartitioner p = StorageService.getPartitioner();
        String[] columnFamilies = new String[] { "Standard1", "Standard2", "Standard3" };
        List<SSTableReader> ssTableReaders = new ArrayList<>();

        NavigableMap<DecoratedKey,String> keys = new TreeMap<>();
        for (String cf : columnFamilies)
        {
            Set<String> content = new HashSet<>();
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
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), first.getKey().getToken()));
        // the left hand side of the range is exclusive, so we transfer from the second-to-last token
        ranges.add(new Range<>(secondtolast.getKey().getToken(), p.getMinimumToken()));

        // Acquiring references, transferSSTables needs it
        Refs<SSTableReader> refs = Refs.tryRef(ssTableReaders);
        if (refs == null)
            throw new AssertionError();

        new StreamPlan("StreamingTransferTest").transferFiles(LOCAL, makeStreamingDetails(ranges, refs)).execute().get();

        // check that only two keys were transferred
        for (Map.Entry<DecoratedKey,String> entry : Arrays.asList(first, last))
        {
            ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(entry.getValue());
            List<Row> rows = Util.getRangeSlice(store);
            assertEquals(rows.toString(), 1, rows.size());
            assertEquals(entry.getKey(), rows.get(0).key);
        }
    }

    @Test
    public void testRandomSSTableTransfer() throws Exception
    {
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        Mutator mutator = new Mutator()
        {
            public void mutate(String key, String colName, long timestamp) throws Exception
            {
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(keyspace.getName(), cfs.name);
                cf.addColumn(column(colName, "value", timestamp));
                cf.addColumn(new BufferCell(cellname("birthdate"), ByteBufferUtil.bytes(new Date(timestamp).toString()), timestamp));
                Mutation rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key), cf);
                logger.debug("Applying row to transfer {}", rm);
                rm.applyUnsafe();
            }
        };
        // write a lot more data so the data is spread in more than 1 chunk.
        for (int i = 1; i <= 6000; i++)
            mutator.mutate("key" + i, "col" + i, System.currentTimeMillis());
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        cfs.clearUnsafe();

        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key1")), p.getToken(ByteBufferUtil.bytes("key1000"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key5")), p.getToken(ByteBufferUtil.bytes("key500"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key9")), p.getToken(ByteBufferUtil.bytes("key900"))));
        transfer(sstable, ranges);
        assertEquals(1, cfs.getSSTables().size());
        assertEquals(7, Util.getRangeSlice(cfs).size());
    }

    public interface Mutator
    {
        public void mutate(String key, String col, long timestamp) throws Exception;
    }
}
