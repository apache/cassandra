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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.locator.RangesAtEndpoint;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.SchemaLoader.compositeIndexCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StreamingTransferTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingTransferTest.class);

    static
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9054
        DatabaseDescriptor.daemonInitialization();
    }

    public static final InetAddressAndPort LOCAL = FBUtilities.getBroadcastAddressAndPort();
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

        createKeyspace(KEYSPACE1,
                       KeyspaceParams.simple(1),
                       standardCFMD(KEYSPACE1, CF_STANDARD),
                       TableMetadata.builder(KEYSPACE1, CF_COUNTER)
                                    .isCounter(true)
                                    .addPartitionKeyColumn("key", BytesType.instance),
                       TableMetadata.builder(KEYSPACE1, CF_STANDARDINT)
                                    .addPartitionKeyColumn("key", AsciiType.instance)
                                    .addClusteringColumn("cols", Int32Type.instance)
                                    .addRegularColumn("val", BytesType.instance),
                       compositeIndexCFMD(KEYSPACE1, CF_INDEX, true));

        createKeyspace(KEYSPACE2, KeyspaceParams.simple(1));

        createKeyspace(KEYSPACE_CACHEKEY,
                       KeyspaceParams.simple(1),
                       standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD),
                       standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD2),
                       standardCFMD(KEYSPACE_CACHEKEY, CF_STANDARD3));
    }

    /**
     * Test if empty {@link StreamPlan} returns success with empty result.
     */
    @Test
    public void testEmptyStreamPlan() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13065
        StreamResultFuture futureResult = new StreamPlan(StreamOperation.OTHER).execute();
        final UUID planId = futureResult.planId;
        Futures.addCallback(futureResult, new FutureCallback<StreamState>()
        {
            public void onSuccess(StreamState result)
            {
                assert planId.equals(result.planId);
                assert result.streamOperation == StreamOperation.OTHER;
                assert result.sessions.isEmpty();
            }

            public void onFailure(Throwable t)
            {
                fail();
            }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14655
        }, MoreExecutors.directExecutor());
        // should be complete immediately
        futureResult.get(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRequestEmpty() throws Exception
    {
        // requesting empty data should succeed
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = Util.testPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13065
        StreamResultFuture futureResult = new StreamPlan(StreamOperation.OTHER)
                                                  .requestRanges(LOCAL, KEYSPACE2, RangesAtEndpoint.toDummyList(ranges), RangesAtEndpoint.toDummyList(Collections.emptyList()))
                                                  .execute();

        UUID planId = futureResult.planId;
        StreamState result = futureResult.get();
        assert planId.equals(result.planId);
        assert result.streamOperation == StreamOperation.OTHER;

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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6008
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertEquals(1, cfs.getLiveSSTables().size());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699

        // transfer the first and last key
        logger.debug("Transferring {}", cfs.name);
        int[] offs;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5948
        if (transferSSTables)
        {
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
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
        assertEquals(1, cfs.getLiveSSTables().size());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699

        // and that the index and filter were properly recovered
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9932
        List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(Util.cmd(cfs).build());
        assertEquals(offs.length, partitions.size());
        for (int i = 0; i < offs.length; i++)
        {
            String key = "key" + offs[i];
            String col = "col" + offs[i];

            assert !Util.getAll(Util.cmd(cfs, key).build()).isEmpty();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9932
            ImmutableBTreePartition partition = partitions.get(i);
            assert ByteBufferUtil.compareUnsigned(partition.partitionKey().getKey(), ByteBufferUtil.bytes(key)) == 0;
            assert ByteBufferUtil.compareUnsigned(partition.iterator().next().clustering().get(0), ByteBufferUtil.bytes(col)) == 0;
        }

        // and that the max timestamp for the file was rediscovered
        assertEquals(timestamp, cfs.getLiveSSTables().iterator().next().getMaxTimestamp());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699

        List<String> keys = new ArrayList<>();
        for (int off : offs)
            keys.add("key" + off);

        logger.debug("... everything looks good for {}", cfs.name);
        return keys;
    }

    private void transferSSTables(SSTableReader sstable) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));
        transfer(sstable, ranges);
    }

    private void transferRanges(ColumnFamilyStore cfs) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = cfs.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        // wrapped range
        ranges.add(new Range<Token>(p.getToken(ByteBufferUtil.bytes("key1")), p.getToken(ByteBufferUtil.bytes("key0"))));
        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferRanges(LOCAL, cfs.keyspace.getName(), RangesAtEndpoint.toDummyList(ranges), cfs.getTableName());
        streamPlan.execute().get();

        //cannot add ranges after stream session is finished
        try
        {
            streamPlan.transferRanges(LOCAL, cfs.keyspace.getName(), RangesAtEndpoint.toDummyList(ranges), cfs.getTableName());
            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            //do nothing
        }
    }

    private void transfer(SSTableReader sstable, List<Range<Token>> ranges) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14115
        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferStreams(LOCAL, makeOutgoingStreams(ranges, Refs.tryRef(Arrays.asList(sstable))));
        streamPlan.execute().get();

        //cannot add files after stream session is finished
        try
        {
            streamPlan.transferStreams(LOCAL, makeOutgoingStreams(ranges, Refs.tryRef(Arrays.asList(sstable))));
            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            //do nothing
        }
    }

    private Collection<OutgoingStream> makeOutgoingStreams(StreamOperation operation, List<Range<Token>> ranges, Refs<SSTableReader> sstables)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14115
        ArrayList<OutgoingStream> streams = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            streams.add(new CassandraOutgoingFile(operation,
                                                  sstables.get(sstable),
                                                  sstable.getPositionsForRanges(ranges),
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14566
                                                  ranges,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13430
                                                  sstable.estimatedKeysForRanges(ranges)));
        }
        return streams;
    }

    private Collection<OutgoingStream> makeOutgoingStreams(List<Range<Token>> ranges, Refs<SSTableReader> sstables)
    {
        return makeOutgoingStreams(StreamOperation.OTHER, ranges, sstables);
    }

    private void doTransferTable(boolean transferSSTables) throws Exception
    {
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099

        List<String> keys = createAndTransfer(cfs, new Mutator()
        {
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                long val = key.hashCode();

                RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), timestamp, key);
                builder.clustering(col).add("birthdate", ByteBufferUtil.bytes(val));
                builder.build().applyUnsafe();
            }
        }, transferSSTables);

        // confirm that the secondary index was recovered
        for (String key : keys)
        {
            long val = key.hashCode();

            // test we can search:
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
            UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE birthdate = %d",
                                                                                   cfs.metadata.keyspace, cfs.metadata.name, val));
            assertEquals(1, result.size());

            assert result.iterator().next().getBytes("key").equals(ByteBufferUtil.bytes(key));
        }
    }

    /**
     * Test to make sure RangeTombstones at column index boundary transferred correctly.
     */
    @Test
    public void testTransferRangeTombstones() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
        String ks = KEYSPACE1;
        String cfname = "StandardInteger1";
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        ClusteringComparator comparator = cfs.getComparator();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099

        String key = "key1";


        RowUpdateBuilder updates = new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), key);

        // add columns of size slightly less than column_index_size to force insert column index
        updates.clustering(1)
                .add("val", ByteBuffer.wrap(new byte[DatabaseDescriptor.getColumnIndexSize() - 64]))
                .build()
                .apply();

        updates = new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), key);
        updates.clustering(6)
                .add("val", ByteBuffer.wrap(new byte[DatabaseDescriptor.getColumnIndexSize()]))
                .build()
                .apply();

        // add RangeTombstones
        //updates = new RowUpdateBuilder(cfs.metadata, FBUtilities.timestampMicros() + 1 , key);
        //updates.addRangeTombstone(Slice.make(comparator, comparator.make(2), comparator.make(4)))
        //        .build()
        //        .apply();


        updates = new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros() + 1, key);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12236
        updates.addRangeTombstone(5, 7)
                .build()
                .apply();

        cfs.forceBlockingFlush();

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        cfs.clearUnsafe();
        transferSSTables(sstable);

        // confirm that a single SSTable was transferred and registered
        assertEquals(1, cfs.getLiveSSTables().size());

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        Row r = Util.getOnlyRow(Util.cmd(cfs).build());
        Assert.assertFalse(r.isEmpty());
        Assert.assertTrue(1 == Int32Type.instance.compose(r.clustering().get(0)));
    }

    @Test
    public void testTransferTableViaRanges() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5948
        doTransferTable(false);
    }

    @Test
    public void testTransferTableViaSSTables() throws Exception
    {
        doTransferTable(true);
    }

    /*
    @Test
    public void testTransferTableCounter() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Counter1");
        final CounterContext cc = new CounterContext();

        final Map<String, ColumnFamily> cleanedEntries = new HashMap<>();

        List<String> keys = createAndTransfer(cfs, new Mutator()
        {
            // Creates a new SSTable per key: all will be merged before streaming.
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                Map<String, ColumnFamily> entries = new HashMap<>();
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
                ColumnFamily cfCleaned = ArrayBackedSortedColumns.factory.create(cfs.metadata);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6506
                CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 1, 3);
                state.writeLocal(CounterId.fromInt(2), 9L, 3L);
                state.writeRemote(CounterId.fromInt(4), 4L, 2L);
                state.writeRemote(CounterId.fromInt(6), 3L, 3L);
                state.writeRemote(CounterId.fromInt(8), 2L, 4L);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6694
                cf.addColumn(new BufferCounterCell(cellname(col), state.context, timestamp));
                cfCleaned.addColumn(new BufferCounterCell(cellname(col), cc.clearAllLocal(state.context), timestamp));

                entries.put(key, cf);
                cleanedEntries.put(key, cfCleaned);
                cfs.addSSTable(SSTableUtils.prepare()
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
                    .ks(keyspace.getName())
                    .cf(cfs.name)
                    .generation(0)
                    .write(entries));
            }
        }, true);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5948

        // filter pre-cleaned entries locally, and ensure that the end result is equal
        cleanedEntries.keySet().retainAll(keys);
        SSTableReader cleaned = SSTableUtils.prepare()
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
            .ks(keyspace.getName())
            .cf(cfs.name)
            .generation(0)
            .write(cleanedEntries);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
        SSTableReader streamed = cfs.getLiveSSTables().iterator().next();
        SSTableUtils.assertContentEquals(cleaned, streamed);

        // Retransfer the file, making sure it is now idempotent (see CASSANDRA-3481)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3437
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-1
        cfs.clearUnsafe();
        transferSSTables(streamed);
        SSTableReader restreamed = cfs.getLiveSSTables().iterator().next();
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
        SSTableReader sstable = new SSTableUtils(KEYSPACE1, CF_STANDARD).prepare().write(content);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
        String keyspaceName = sstable.getKeyspaceName();
        String cfname = sstable.getColumnFamilyName();

        content = new HashSet<>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableReader sstable2 = SSTableUtils.prepare().write(content);

        // transfer the first and last key
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = Util.testPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("test"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("transfer2")), p.getMinimumToken()));
        // Acquiring references, transferSSTables needs it
        Refs<SSTableReader> refs = Refs.tryRef(Arrays.asList(sstable, sstable2));
        assert refs != null;
        new StreamPlan("StreamingTransferTest").transferStreams(LOCAL, makeOutgoingStreams(ranges, refs)).execute().get();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14115

        // confirm that the sstables were transferred and registered and that 2 keys arrived
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
        ColumnFamilyStore cfstore = Keyspace.open(keyspaceName).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(2, rows.size());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6694
        assert rows.get(0).key.getKey().equals(ByteBufferUtil.bytes("test"));
        assert rows.get(1).key.getKey().equals(ByteBufferUtil.bytes("transfer3"));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2843
        assert rows.get(0).cf.getColumnCount() == 1;
        assert rows.get(1).cf.getColumnCount() == 1;

        // these keys fall outside of the ranges and should not be transferred
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5149
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer1"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("transfer2"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test2"), "Standard1", System.currentTimeMillis())) == null;
        assert cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("test3"), "Standard1", System.currentTimeMillis())) == null;
    }

    @Test
    public void testTransferOfMultipleColumnFamilies() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
        String keyspace = KEYSPACE_CACHEKEY;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = Util.testPartitioner();
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6694
        ranges.add(new Range<>(p.getMinimumToken(), first.getKey().getToken()));
        // the left hand side of the range is exclusive, so we transfer from the second-to-last token
        ranges.add(new Range<>(secondtolast.getKey().getToken(), p.getMinimumToken()));

        // Acquiring references, transferSSTables needs it
        Refs<SSTableReader> refs = Refs.tryRef(ssTableReaders);
        if (refs == null)
            throw new AssertionError();

        new StreamPlan("StreamingTransferTest").transferStreams(LOCAL, makeOutgoingStreams(ranges, refs)).execute().get();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14115

        // check that only two keys were transferred
        for (Map.Entry<DecoratedKey,String> entry : Arrays.asList(first, last))
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
            ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(entry.getValue());
            List<Row> rows = Util.getRangeSlice(store);
            assertEquals(rows.toString(), 1, rows.size());
            assertEquals(entry.getKey(), rows.get(0).key);
        }
    }

    @Test
    public void testRandomSSTableTransfer() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        Mutator mutator = new Mutator()
        {
            public void mutate(String key, String colName, long timestamp) throws Exception
            {
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(keyspace.getName(), cfs.name);
                cf.addColumn(column(colName, "value", timestamp));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6694
                cf.addColumn(new BufferCell(cellname("birthdate"), ByteBufferUtil.bytes(new Date(timestamp).toString()), timestamp));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6968
                Mutation rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key), cf);
                logger.debug("Applying row to transfer {}", rm);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6969
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6969
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6969
                rm.applyUnsafe();
            }
        };
        // write a lot more data so the data is spread in more than 1 chunk.
        for (int i = 1; i <= 6000; i++)
            mutator.mutate("key" + i, "col" + i, System.currentTimeMillis());
        cfs.forceBlockingFlush();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6008
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        cfs.clearUnsafe();

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        IPartitioner p = Util.testPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key1")), p.getToken(ByteBufferUtil.bytes("key1000"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key5")), p.getToken(ByteBufferUtil.bytes("key500"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key9")), p.getToken(ByteBufferUtil.bytes("key900"))));
        transfer(sstable, ranges);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(7, Util.getRangeSlice(cfs).size());
    }
    */
    public interface Mutator
    {
        public void mutate(String key, String col, long timestamp) throws Exception;
    }
}
