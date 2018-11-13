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

package org.apache.cassandra.db;

import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadCommandTest
{
    private static final String KEYSPACE = "ReadCommandTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";
    private static final String CF3 = "Standard3";
    private static final String CF4 = "Standard4";
    private static final String CF5 = "Standard5";
    private static final String CF6 = "Standard6";

    private static final InetAddressAndPort REPAIR_COORDINATOR;
    static {
        try
        {
            REPAIR_COORDINATOR = InetAddressAndPort.getByName("10.0.0.1");
        }
        catch (UnknownHostException e)
        {

            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder metadata1 = SchemaLoader.standardCFMD(KEYSPACE, CF1);

        TableMetadata.Builder metadata2 =
            TableMetadata.builder(KEYSPACE, CF2)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("col", AsciiType.instance)
                         .addRegularColumn("a", AsciiType.instance)
                         .addRegularColumn("b", AsciiType.instance);

        TableMetadata.Builder metadata3 =
            TableMetadata.builder(KEYSPACE, CF3)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("col", AsciiType.instance)
                         .addRegularColumn("a", AsciiType.instance)
                         .addRegularColumn("b", AsciiType.instance)
                         .addRegularColumn("c", AsciiType.instance)
                         .addRegularColumn("d", AsciiType.instance)
                         .addRegularColumn("e", AsciiType.instance)
                         .addRegularColumn("f", AsciiType.instance);

        TableMetadata.Builder metadata4 =
        TableMetadata.builder(KEYSPACE, CF4)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .addRegularColumn("c", AsciiType.instance)
                     .addRegularColumn("d", AsciiType.instance)
                     .addRegularColumn("e", AsciiType.instance)
                     .addRegularColumn("f", AsciiType.instance);

        TableMetadata.Builder metadata5 =
        TableMetadata.builder(KEYSPACE, CF5)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .addRegularColumn("c", AsciiType.instance)
                     .addRegularColumn("d", AsciiType.instance)
                     .addRegularColumn("e", AsciiType.instance)
                     .addRegularColumn("f", AsciiType.instance);

        TableMetadata.Builder metadata6 =
        TableMetadata.builder(KEYSPACE, CF6)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .caching(CachingParams.CACHE_EVERYTHING);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata1,
                                    metadata2,
                                    metadata3,
                                    metadata4,
                                    metadata5,
                                    metadata6);

        LocalSessionAccessor.startup();
    }

    @Test
    public void testPartitionRangeAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key1"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key2"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs).build();
        assertEquals(2, Util.getAll(readCommand).size());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }

    @Test
    public void testSinglePartitionSliceAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();

        List<FilteredPartition> partitions = Util.getAll(readCommand);
        assertEquals(1, partitions.size());
        assertEquals(2, partitions.get(0).rowCount());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }

    @Test
    public void testSinglePartitionNamesAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("cc").includeRow("dd").build();

        List<FilteredPartition> partitions = Util.getAll(readCommand);
        assertEquals(1, partitions.size());
        assertEquals(2, partitions.get(0).rowCount());

        readCommand.abort();
        assertEquals(0, Util.getAll(readCommand).size());
    }

    @Test
    public void testSinglePartitionGroupMerge() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF3);

        String[][][] groups = new String[][][] {
            new String[][] {
                new String[] { "1", "key1", "aa", "a" }, // "1" indicates to create the data, "-1" to delete the row
                new String[] { "1", "key2", "bb", "b" },
                new String[] { "1", "key3", "cc", "c" }
            },
            new String[][] {
                new String[] { "1", "key3", "dd", "d" },
                new String[] { "1", "key2", "ee", "e" },
                new String[] { "1", "key1", "ff", "f" }
            },
            new String[][] {
                new String[] { "1", "key6", "aa", "a" },
                new String[] { "1", "key5", "bb", "b" },
                new String[] { "1", "key4", "cc", "c" }
            },
            new String[][] {
                new String[] { "-1", "key6", "aa", "a" },
                new String[] { "-1", "key2", "bb", "b" }
            }
        };

        // Given the data above, when the keys are sorted and the deletions removed, we should
        // get these clustering rows in this order
        String[] expectedRows = new String[] { "aa", "ff", "ee", "cc", "dd", "cc", "bb"};

        List<ByteBuffer> buffers = new ArrayList<>(groups.length);
        int nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata()).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.TOP);
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.with(cfs.metadata().comparator, slice), false);

        for (String[][] group : groups)
        {
            cfs.truncateBlocking();

            List<SinglePartitionReadCommand> commands = new ArrayList<>(group.length);

            for (String[] data : group)
            {
                if (data[0].equals("1"))
                {
                    new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes(data[1]))
                    .clustering(data[2])
                    .add(data[3], ByteBufferUtil.bytes("blah"))
                    .build()
                    .apply();
                }
                else
                {
                    RowUpdateBuilder.deleteRow(cfs.metadata(), FBUtilities.timestampMicros(), ByteBufferUtil.bytes(data[1]), data[2]).apply();
                }
                commands.add(SinglePartitionReadCommand.create(cfs.metadata(), nowInSeconds, columnFilter, rowFilter, DataLimits.NONE, Util.dk(data[1]), sliceFilter));
            }

            cfs.forceBlockingFlush();

            ReadQuery query = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

            try (ReadExecutionController executionController = query.executionController();
                 UnfilteredPartitionIterator iter = query.executeLocally(executionController);
                 DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter,
                                                                                columnFilter,
                                                                                buffer,
                                                                                MessagingService.current_version);
                buffers.add(buffer.buffer());
            }
        }

        // deserialize, merge and check the results are all there
        List<UnfilteredPartitionIterator> iterators = new ArrayList<>();

        for (ByteBuffer buffer : buffers)
        {
            try (DataInputBuffer in = new DataInputBuffer(buffer, true))
            {
                iterators.add(UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                                                                                                MessagingService.current_version,
                                                                                                cfs.metadata(),
                                                                                                columnFilter,
                                                                                                SerializationHelper.Flag.LOCAL));
            }
        }

        UnfilteredPartitionIterators.MergeListener listener =
            new UnfilteredPartitionIterators.MergeListener()
            {
                public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
                {
                    return null;
                }

                public void close()
                {

                }
            };

        try (PartitionIterator partitionIterator = UnfilteredPartitionIterators.filter(UnfilteredPartitionIterators.merge(iterators, listener), nowInSeconds))
        {

            int i = 0;
            int numPartitions = 0;
            while (partitionIterator.hasNext())
            {
                numPartitions++;
                try(RowIterator rowIterator = partitionIterator.next())
                {
                    while (rowIterator.hasNext())
                    {
                        Row row = rowIterator.next();
                        assertEquals("col=" + expectedRows[i++], row.clustering().toString(cfs.metadata()));
                        //System.out.print(row.toString(cfs.metadata, true));
                    }
                }
            }

            assertEquals(5, numPartitions);
            assertEquals(expectedRows.length, i);
        }
    }

    @Test
    public void testSerializer() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        new RowUpdateBuilder(cfs.metadata.get(), 0, ByteBufferUtil.bytes("key"))
        .clustering("dd")
        .add("a", ByteBufferUtil.bytes("abcd"))
        .build()
        .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("dd").build();
        int messagingVersion = MessagingService.current_version;
        FakeOutputStream out = new FakeOutputStream();
        Tracing.instance.newSession(Tracing.TraceType.QUERY);
        MessageOut<ReadCommand> messageOut = new MessageOut(MessagingService.Verb.READ, readCommand, ReadCommand.serializer);
        long size = messageOut.serializedSize(messagingVersion);
        messageOut.serialize(new WrappedDataOutputStreamPlus(out), messagingVersion);
        Assert.assertEquals(size, out.count);
    }

    static class FakeOutputStream extends OutputStream
    {
        long count;

        public void write(int b) throws IOException
        {
            count++;
        }
    }

    @Test
    public void testCountDeletedRows() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF4);

        String[][][] groups = new String[][][] {
                new String[][] {
                        new String[] { "1", "key1", "aa", "a" }, // "1" indicates to create the data, "-1" to delete the
                                                                 // row
                        new String[] { "1", "key2", "bb", "b" },
                        new String[] { "1", "key3", "cc", "c" }
                },
                new String[][] {
                        new String[] { "1", "key3", "dd", "d" },
                        new String[] { "1", "key2", "ee", "e" },
                        new String[] { "1", "key1", "ff", "f" }
                },
                new String[][] {
                        new String[] { "1", "key6", "aa", "a" },
                        new String[] { "1", "key5", "bb", "b" },
                        new String[] { "1", "key4", "cc", "c" }
                },
                new String[][] {
                        new String[] { "1", "key2", "aa", "a" },
                        new String[] { "1", "key2", "cc", "c" },
                        new String[] { "1", "key2", "dd", "d" }
                },
                new String[][] {
                        new String[] { "-1", "key6", "aa", "a" },
                        new String[] { "-1", "key2", "bb", "b" },
                        new String[] { "-1", "key2", "ee", "e" },
                        new String[] { "-1", "key2", "aa", "a" },
                        new String[] { "-1", "key2", "cc", "c" },
                        new String[] { "-1", "key2", "dd", "d" }
                }
        };

        List<ByteBuffer> buffers = new ArrayList<>(groups.length);
        int nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata()).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.TOP);
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(
                Slices.with(cfs.metadata().comparator, slice), false);

        for (String[][] group : groups)
        {
            cfs.truncateBlocking();

            List<SinglePartitionReadCommand> commands = new ArrayList<>(group.length);

            for (String[] data : group)
            {
                if (data[0].equals("1"))
                {
                    new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes(data[1]))
                            .clustering(data[2])
                            .add(data[3], ByteBufferUtil.bytes("blah"))
                            .build()
                            .apply();
                }
                else
                {
                    RowUpdateBuilder.deleteRow(cfs.metadata(), FBUtilities.timestampMicros(),
                            ByteBufferUtil.bytes(data[1]), data[2]).apply();
                }
                commands.add(SinglePartitionReadCommand.create(cfs.metadata(), nowInSeconds, columnFilter, rowFilter,
                        DataLimits.NONE, Util.dk(data[1]), sliceFilter));
            }

            cfs.forceBlockingFlush();

            ReadQuery query = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

            try (ReadExecutionController executionController = query.executionController();
                    UnfilteredPartitionIterator iter = query.executeLocally(executionController);
                    DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter,
                        columnFilter,
                        buffer,
                        MessagingService.current_version);
                buffers.add(buffer.buffer());
            }
        }

        assertEquals(5, cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void testCountWithNoDeletedRow() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF5);

        String[][][] groups = new String[][][] {
                new String[][] {
                        new String[] { "1", "key1", "aa", "a" }, // "1" indicates to create the data, "-1" to delete the
                                                                 // row
                        new String[] { "1", "key2", "bb", "b" },
                        new String[] { "1", "key3", "cc", "c" }
                },
                new String[][] {
                        new String[] { "1", "key3", "dd", "d" },
                        new String[] { "1", "key2", "ee", "e" },
                        new String[] { "1", "key1", "ff", "f" }
                },
                new String[][] {
                        new String[] { "1", "key6", "aa", "a" },
                        new String[] { "1", "key5", "bb", "b" },
                        new String[] { "1", "key4", "cc", "c" }
                }
        };

        List<ByteBuffer> buffers = new ArrayList<>(groups.length);
        int nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata()).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.TOP);
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(
                Slices.with(cfs.metadata().comparator, slice), false);

        for (String[][] group : groups)
        {
            cfs.truncateBlocking();

            List<SinglePartitionReadCommand> commands = new ArrayList<>(group.length);

            for (String[] data : group)
            {
                if (data[0].equals("1"))
                {
                    new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes(data[1]))
                            .clustering(data[2])
                            .add(data[3], ByteBufferUtil.bytes("blah"))
                            .build()
                            .apply();
                }
                else
                {
                    RowUpdateBuilder.deleteRow(cfs.metadata(), FBUtilities.timestampMicros(),
                            ByteBufferUtil.bytes(data[1]), data[2]).apply();
                }
                commands.add(SinglePartitionReadCommand.create(cfs.metadata(), nowInSeconds, columnFilter, rowFilter,
                        DataLimits.NONE, Util.dk(data[1]), sliceFilter));
            }

            cfs.forceBlockingFlush();

            ReadQuery query = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

            try (ReadExecutionController executionController = query.executionController();
                    UnfilteredPartitionIterator iter = query.executeLocally(executionController);
                    DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter,
                        columnFilter,
                        buffer,
                        MessagingService.current_version);
                buffers.add(buffer.buffer());
            }
        }

        assertEquals(1, cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void testSinglePartitionSliceRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testPartitionRangeRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs).build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testSinglePartitionNamesRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("cc").includeRow("dd").build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testSinglePartitionNamesSkipsOptimisationsIfTrackingRepairedData()
    {
        // when tracking, the optimizations of querying sstables in timestamp order and
        // returning once all requested columns are not available so just assert that
        // all sstables are read when performing such queries
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
            .clustering("dd")
            .add("a", ByteBufferUtil.bytes("abcd"))
            .build()
            .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 1, ByteBufferUtil.bytes("key"))
            .clustering("dd")
            .add("a", ByteBufferUtil.bytes("wxyz"))
            .build()
            .apply();

        cfs.forceBlockingFlush();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        Collections.sort(sstables, SSTableReader.maxTimestampDescending);

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("dd").columns("a").build();

        assertEquals(0, readCount(sstables.get(0)));
        assertEquals(0, readCount(sstables.get(1)));
        ReadCommand withTracking = readCommand.copy();
        withTracking.trackRepairedStatus();
        Util.getAll(withTracking);
        assertEquals(1, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));

        // same command without tracking touches only the table with the higher timestamp
        Util.getAll(readCommand.copy());
        assertEquals(2, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));
    }

    private long readCount(SSTableReader sstable)
    {
        return sstable.getReadMeter().count();
    }

    @Test
    public void skipRowCacheIfTrackingRepairedData()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        assertTrue(cfs.isRowCacheEnabled());
        // warm the cache
        assertFalse(Util.getAll(readCommand).isEmpty());
        long cacheHits = cfs.metric.rowCacheHit.getCount();

        Util.getAll(readCommand);
        assertTrue(cfs.metric.rowCacheHit.getCount() > cacheHits);
        cacheHits = cfs.metric.rowCacheHit.getCount();

        ReadCommand withRepairedInfo = readCommand.copy();
        withRepairedInfo.trackRepairedStatus();
        Util.getAll(withRepairedInfo);
        assertEquals(cacheHits, cfs.metric.rowCacheHit.getCount());
    }

    @Test (expected = IllegalArgumentException.class)
    public void copyFullAsTransientTest()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        readCommand.copyAsTransientQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void copyTransientAsDigestQuery()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        readCommand.copyAsDigestQuery(ReplicaUtils.trans(FBUtilities.getBroadcastAddressAndPort()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void copyMultipleFullAsTransientTest()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        DecoratedKey key = Util.dk("key");
        Token token = key.getToken();
        // Address is unimportant for this test
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        ReadCommand readCommand = Util.cmd(cfs, key).build();
        readCommand.copyAsTransientQuery(EndpointsForToken.of(token,
                                                              ReplicaUtils.trans(addr, token),
                                                              ReplicaUtils.full(addr, token)));
    }

    @Test (expected = IllegalArgumentException.class)
    public void copyMultipleTransientAsDigestQuery()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        DecoratedKey key = Util.dk("key");
        Token token = key.getToken();
        // Address is unimportant for this test
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        ReadCommand readCommand = Util.cmd(cfs, key).build();
        readCommand.copyAsDigestQuery(EndpointsForToken.of(token,
                                                           ReplicaUtils.trans(addr, token),
                                                           ReplicaUtils.full(addr, token)));
    }

    private void testRepairedDataTracking(ColumnFamilyStore cfs, ReadCommand readCommand) throws IOException
    {
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 1, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        sstables.forEach(sstable -> assertFalse(sstable.isRepaired() || sstable.isPendingRepair()));
        SSTableReader sstable1 = sstables.get(0);
        SSTableReader sstable2 = sstables.get(1);

        int numPartitions = 1;
        int rowsPerPartition = 2;

        // Capture all the digest versions as we mutate the table's repaired status. Each time
        // we make a change, we expect a different digest.
        Set<ByteBuffer> digests = new HashSet<>();
        // first time round, nothing has been marked repaired so we expect digest to be an empty buffer and to be marked conclusive
        ByteBuffer digest = performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, true);
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);
        digests.add(digest);

        // add a pending repair session to table1, digest should remain the same but now we expect it to be marked inconclusive
        UUID session1 = UUIDGen.getTimeUUID();
        mutateRepaired(cfs, sstable1, ActiveRepairService.UNREPAIRED_SSTABLE, session1);
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(1, digests.size());

        // add a different pending session to table2, digest should remain the same and still consider it inconclusive
        UUID session2 = UUIDGen.getTimeUUID();
        mutateRepaired(cfs, sstable2, ActiveRepairService.UNREPAIRED_SSTABLE, session2);
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(1, digests.size());

        // mark one table repaired
        mutateRepaired(cfs, sstable1, 111, null);
        // this time, digest should not be empty, session2 still means that the result is inconclusive
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(2, digests.size());

        // mark the second table repaired
        mutateRepaired(cfs, sstable2, 222, null);
        // digest should be updated again and as there are no longer any pending sessions, it should be considered conclusive
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, true));
        assertEquals(3, digests.size());

        // insert a partition tombstone into the memtable, then re-check the repaired info.
        // This is to ensure that when the optimisations which skip reading from sstables
        // when a newer partition tombstone has already been cause the digest to be marked
        // as inconclusive.
        // the exception to this case is for partition range reads, where we always read
        // and generate digests for all sstables, so we only test this path for single partition reads
        if (readCommand.isLimitedToOnePartition())
        {
            new Mutation(PartitionUpdate.simpleBuilder(cfs.metadata(), ByteBufferUtil.bytes("key"))
                                        .delete()
                                        .build()).apply();
            digest = performReadAndVerifyRepairedInfo(readCommand, 0, rowsPerPartition, false);
            assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);

            // now flush so we have an unrepaired table with the deletion and repeat the check
            cfs.forceBlockingFlush();
            digest = performReadAndVerifyRepairedInfo(readCommand, 0, rowsPerPartition, false);
            assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);
        }
    }

    private void mutateRepaired(ColumnFamilyStore cfs, SSTableReader sstable, long repairedAt, UUID pendingSession) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, repairedAt, pendingSession, false);
        sstable.reloadSSTableMetadata();
        if (pendingSession != null)
        {
            // setup a minimal repair session. This is necessary because we
            // check for sessions which have exceeded timeout and been purged
            Range<Token> range = new Range<>(cfs.metadata().partitioner.getMinimumToken(),
                                             cfs.metadata().partitioner.getRandomToken());
            ActiveRepairService.instance.registerParentRepairSession(pendingSession,
                                                                     REPAIR_COORDINATOR,
                                                                     Lists.newArrayList(cfs),
                                                                     Sets.newHashSet(range),
                                                                     true,
                                                                     repairedAt,
                                                                     true,
                                                                     PreviewKind.NONE);

            LocalSessionAccessor.prepareUnsafe(pendingSession, null, Sets.newHashSet(REPAIR_COORDINATOR));
        }
    }

    private ByteBuffer performReadAndVerifyRepairedInfo(ReadCommand command,
                                                        int expectedPartitions,
                                                        int expectedRowsPerPartition,
                                                        boolean expectConclusive)
    {
        // perform equivalent read command multiple times and assert that
        // the repaired data info is always consistent. Return the digest
        // so we can verify that it changes when the repaired status of
        // the queried tables does.
        Set<ByteBuffer> digests = new HashSet<>();
        for (int i = 0; i < 10; i++)
        {
            ReadCommand withRepairedInfo = command.copy();
            withRepairedInfo.trackRepairedStatus();

            List<FilteredPartition> partitions = Util.getAll(withRepairedInfo);
            assertEquals(expectedPartitions, partitions.size());
            partitions.forEach(p -> assertEquals(expectedRowsPerPartition, p.rowCount()));

            ByteBuffer digest = withRepairedInfo.getRepairedDataDigest();
            digests.add(digest);
            assertEquals(1, digests.size());
            assertEquals(expectConclusive, withRepairedInfo.isRepairedDataDigestConclusive());
        }
        return digests.iterator().next();
    }

}
