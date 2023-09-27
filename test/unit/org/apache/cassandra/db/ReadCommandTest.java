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
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableSet;
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
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadCommandTest
{
    private static final String KEYSPACE = "ReadCommandTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";
    private static final String CF3 = "Standard3";
    private static final String CF4 = "Standard4";
    private static final String CF5 = "Standard5";
    private static final String CF6 = "Standard6";
    private static final String CF7 = "Counter7";
    private static final String CF8 = "Standard8";
    private static final String CF9 = "Standard9";

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
                     .addStaticColumn("s", AsciiType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .caching(CachingParams.CACHE_EVERYTHING);

        TableMetadata.Builder metadata7 =
        TableMetadata.builder(KEYSPACE, CF7)
                     .flags(EnumSet.of(TableMetadata.Flag.COUNTER, TableMetadata.Flag.COMPOUND))
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("c", CounterColumnType.instance);

        TableMetadata.Builder metadata8 =
        TableMetadata.builder(KEYSPACE, CF8)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .addRegularColumn("c", SetType.getInstance(AsciiType.instance, true));

        TableMetadata.Builder metadata9 =
        TableMetadata.builder(KEYSPACE, CF9)
                     .addPartitionKeyColumn("key", Int32Type.instance)
                     .addClusteringColumn("col", ReversedType.getInstance(Int32Type.instance))
                     .addRegularColumn("a", AsciiType.instance);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata1,
                                    metadata2,
                                    metadata3,
                                    metadata4,
                                    metadata5,
                                    metadata6,
                                    metadata7,
                                    metadata8,
                                    metadata9);

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

        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key2"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ReadCommand readCommand = Util.cmd(cfs).build();
        assertEquals(2, Util.getAll(readCommand).size());

        readCommand.abort();
        boolean cancelled = false;
        try
        {
            Util.getAll(readCommand);
        }
        catch (QueryCancelledException e)
        {
            cancelled = true;
        }
        assertTrue(cancelled);
    }

    @Test
    public void testSinglePartitionSliceAbort()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Util.flush(cfs);

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
        boolean cancelled = false;
        try
        {
            Util.getAll(readCommand);
        }
        catch (QueryCancelledException e)
        {
            cancelled = true;
        }
        assertTrue(cancelled);
    }

    @Test
    public void testSinglePartitionNamesAbort()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Util.flush(cfs);

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
        boolean cancelled = false;
        try
        {
            Util.getAll(readCommand);
        }
        catch (QueryCancelledException e)
        {
            cancelled = true;
        }
        assertTrue(cancelled);
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
        long nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata(), false).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.TOP);
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

            Util.flush(cfs);

            ReadQuery query = SinglePartitionReadCommand.Group.create(commands, DataLimits.NONE);

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
                                                                                                DeserializationHelper.Flag.LOCAL));
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
        Message<ReadCommand> messageOut = Message.out(Verb.READ_REQ, readCommand);
        long size = messageOut.serializedSize(messagingVersion);
        Message.serializer.serialize(messageOut, new WrappedDataOutputStreamPlus(out), messagingVersion);
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
        long nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata(), false).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.TOP);
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

            Util.flush(cfs);

            ReadQuery query = SinglePartitionReadCommand.Group.create(commands, DataLimits.NONE);

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
        long nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(cfs.metadata(), false).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.TOP);
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

            Util.flush(cfs);

            ReadQuery query = SinglePartitionReadCommand.Group.create(commands, DataLimits.NONE);

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

        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, ByteBufferUtil.bytes("key"))
            .clustering("dd")
            .add("a", ByteBufferUtil.bytes("wxyz"))
            .build()
            .apply();

        Util.flush(cfs);
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        sstables.sort(SSTableReader.maxTimestampDescending);

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("dd").columns("a").build();

        assertEquals(0, readCount(sstables.get(0)));
        assertEquals(0, readCount(sstables.get(1)));
        ReadCommand withTracking = readCommand.copy();
        Util.getAll(withTracking, withTracking.executionController(true));
        assertEquals(1, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));

        // same command without tracking touches only the table with the higher timestamp
        Util.getAll(readCommand.copy());
        assertEquals(2, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));
    }

    @Test
    public void dontIncludeLegacyCounterContextInDigest()
    {
        // Serializations of a CounterContext containing legacy (pre-2.1) shards
        // can legitimately differ across replicas. For this reason, the context
        // bytes are omitted from the repaired digest if they contain legacy shards.
        // This clearly has a tradeoff with the efficacy of the digest, without doing
        // so false positive digest mismatches will be reported for scenarios where
        // there is nothing that can be done to "fix" the replicas
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF7);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        // insert a row with the counter column having value 0, in a legacy shard.
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .addLegacyCounterCell("c", 0L)
                .build()
                .apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        // execute a read and capture the digest
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        ByteBuffer digestWithLegacyCounter0 = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertNotEquals(EMPTY_BYTE_BUFFER, digestWithLegacyCounter0);

        // truncate, then re-insert the same partition, but this time with a legacy
        // shard having the value 1. The repaired digest should match the previous, as
        // the values (context) are not included, only the cell metadata (ttl, timestamp, etc)
        cfs.truncateBlocking();
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .addLegacyCounterCell("c", 1L)
                .build()
                .apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        ByteBuffer digestWithLegacyCounter1 = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertEquals(digestWithLegacyCounter0, digestWithLegacyCounter1);

        // truncate, then re-insert the same partition, but this time with a non-legacy
        // counter cell present. The repaired digest should not match the previous ones
        // as this time the value (context) is included.
        cfs.truncateBlocking();
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .add("c", 1L)
                .build()
                .apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        ByteBuffer digestWithCounterCell = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertNotEquals(EMPTY_BYTE_BUFFER, digestWithCounterCell);
        assertNotEquals(digestWithLegacyCounter0, digestWithCounterCell);
        assertNotEquals(digestWithLegacyCounter1, digestWithCounterCell);
    }

    /**
     * Writes a single partition containing a single row and reads using a partition read. The single
     * row includes 1 live simple column, 1 simple tombstone and 1 complex column with a complex
     * deletion and a live cell. The repaired data digests generated by executing the same query
     * before and after the tombstones become eligible for purging should not match each other.
     * Also, neither digest should be empty as the partition is not made empty by the purging.
     */
    @Test
    public void purgeGCableTombstonesBeforeCalculatingDigest()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF8);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        setGCGrace(cfs, 600);

        DecoratedKey[] keys = new DecoratedKey[] { Util.dk("key0"), Util.dk("key1"), Util.dk("key2"), Util.dk("key3") };
        long nowInSec = FBUtilities.nowInSeconds();

        // A simple tombstone
        new RowUpdateBuilder(cfs.metadata(), 0, keys[0]).clustering("cc").delete("a").build().apply();

        // Collection with an associated complex deletion
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfs.metadata(), keys[1]).timestamp(0);
        builder.row("cc").add("c", ImmutableSet.of("element1", "element2"));
        builder.buildAsMutation().apply();

        // RangeTombstone and a row (not covered by the RT). The row contains a regular tombstone which will not be
        // purged. This is to prevent the partition from being fully purged and removed from the final results
        new RowUpdateBuilder(cfs.metadata(), nowInSec, 0L, keys[2]).addRangeTombstone("aa", "bb").build().apply();
        new RowUpdateBuilder(cfs.metadata(), nowInSec+ 1000, 1000L, keys[2]).clustering("cc").delete("a").build().apply();

        // Partition with 2 rows, one fully deleted
        new RowUpdateBuilder(cfs.metadata.get(), 0, keys[3]).clustering("bb").add("a", ByteBufferUtil.bytes("a")).delete("b").build().apply();
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, keys[3], "cc").apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        Map<DecoratedKey, ByteBuffer> digestsWithTombstones = new HashMap<>();
        //Tombstones are not yet purgable
        for (DecoratedKey key : keys)
        {
            ReadCommand cmd = Util.cmd(cfs, key).withNowInSeconds(nowInSec).build();

            try (ReadExecutionController controller = cmd.executionController(true))
            {
                Partition partition = Util.getOnlyPartitionUnfiltered(cmd, controller);
                assertFalse(partition.isEmpty());
                partition.unfilteredIterator().forEachRemaining(u -> {
                    // must be either a RT, or a row containing some kind of deletion
                    assertTrue(u.isRangeTombstoneMarker() || ((Row) u).hasDeletion(cmd.nowInSec()));
                });
                ByteBuffer digestWithTombstones = controller.getRepairedDataDigest();
                // None should generate an empty digest
                assertDigestsDiffer(EMPTY_BYTE_BUFFER, digestWithTombstones);
                digestsWithTombstones.put(key, digestWithTombstones);
            }
        }

        // Make tombstones eligible for purging and re-run cmd with an incremented nowInSec
        setGCGrace(cfs, 0);

        //Tombstones are now purgable, so won't be in the read results and produce different digests
        for (DecoratedKey key : keys)
        {
            ReadCommand cmd = Util.cmd(cfs, key).withNowInSeconds(nowInSec + 60).build();
            
            try (ReadExecutionController controller = cmd.executionController(true))
            {
                Partition partition = Util.getOnlyPartitionUnfiltered(cmd, controller);
                assertFalse(partition.isEmpty());
                partition.unfilteredIterator().forEachRemaining(u -> {
                    // After purging, only rows without any deletions should remain.
                    // The one exception is "key2:cc" which has a regular column tombstone which is not
                    // eligible for purging. This is to prevent the partition from being fully purged
                    // when its RT is removed.
                    assertTrue(u.isRow());
                    Row r = (Row) u;
                    assertTrue(!r.hasDeletion(cmd.nowInSec())
                               || (key.equals(keys[2]) && r.clustering()
                                                           .bufferAt(0)
                                                           .equals(AsciiType.instance.fromString("cc"))));
                });
                ByteBuffer digestWithoutTombstones = controller.getRepairedDataDigest();
                // not an empty digest
                assertDigestsDiffer(EMPTY_BYTE_BUFFER, digestWithoutTombstones);
                // should not match the pre-purge digest
                assertDigestsDiffer(digestsWithTombstones.get(key), digestWithoutTombstones);
            }
        }
    }

    @Test
    public void testRepairedDataOverreadMetrics()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF9);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata().withSwapped(cfs.metadata().params.unbuild()
                                                        .caching(CachingParams.CACHE_NOTHING)
                                                        .build());
        // Insert and repair
        insert(cfs, IntStream.range(0, 10), () -> IntStream.range(0, 10));
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));
        // Insert and leave unrepaired
        insert(cfs, IntStream.range(0, 10), () -> IntStream.range(10, 20));

        // Single partition reads
        int limit = 5;
        ReadCommand cmd = Util.cmd(cfs, ByteBufferUtil.bytes(0)).withLimit(limit).build();
        assertEquals(0, getAndResetOverreadCount(cfs));

        // No overreads if not tracking
        readAndCheckRowCount(Collections.singletonList(Util.getOnlyPartition(cmd)), limit);
        assertEquals(0, getAndResetOverreadCount(cfs));

        // Overread up to (limit - 1) if tracking is enabled
        cmd = cmd.copy();
        readAndCheckRowCount(Collections.singletonList(Util.getOnlyPartition(cmd, true)), limit);
        // overread count is always < limit as the first read is counted during merging (and so is expected)
        assertEquals(limit - 1, getAndResetOverreadCount(cfs));

        // if limit already requires reading all repaired data, no overreads should be recorded
        limit = 20;
        cmd = Util.cmd(cfs, ByteBufferUtil.bytes(0)).withLimit(limit).build();
        readAndCheckRowCount(Collections.singletonList(Util.getOnlyPartition(cmd)), limit);
        assertEquals(0, getAndResetOverreadCount(cfs));

        // Range reads
        limit = 5;
        cmd = Util.cmd(cfs).withLimit(limit).build();
        assertEquals(0, getAndResetOverreadCount(cfs));
        // No overreads if not tracking
        readAndCheckRowCount(Util.getAll(cmd), limit);
        assertEquals(0, getAndResetOverreadCount(cfs));

        // Overread up to (limit - 1) if tracking is enabled
        cmd = cmd.copy();
        readAndCheckRowCount(Util.getAll(cmd, cmd.executionController(true)), limit);
        assertEquals(limit - 1, getAndResetOverreadCount(cfs));

        // if limit already requires reading all repaired data, no overreads should be recorded
        limit = 100;
        cmd = Util.cmd(cfs).withLimit(limit).build();
        readAndCheckRowCount(Util.getAll(cmd), limit);
        assertEquals(0, getAndResetOverreadCount(cfs));
    }

    private void setGCGrace(ColumnFamilyStore cfs, int gcGrace)
    {
        TableParams newParams = cfs.metadata().params.unbuild().gcGraceSeconds(gcGrace).build();
        KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(cfs.metadata().keyspace);
        SchemaTestUtil.addOrUpdateKeyspace(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(cfs.metadata().withSwapped(newParams))), true);
    }

    private long getAndResetOverreadCount(ColumnFamilyStore cfs)
    {
        // always clear the histogram after reading to make comparisons & asserts easier
        long rows = cfs.metric.repairedDataTrackingOverreadRows.cf.getSnapshot().getMax();
        ((ClearableHistogram)cfs.metric.repairedDataTrackingOverreadRows.cf).clear();
        return rows;
    }

    private void readAndCheckRowCount(Iterable<FilteredPartition> partitions, int expected)
    {
        int count = 0;
        for (Partition partition : partitions)
        {
            assertFalse(partition.isEmpty());
            try (UnfilteredRowIterator iter = partition.unfilteredIterator())
            {
                while (iter.hasNext())
                {
                    iter.next();
                    count++;
                }
            }
        }
        assertEquals(expected, count);
    }

    private void insert(ColumnFamilyStore cfs, IntStream partitionIds, Supplier<IntStream> rowIds)
    {
        partitionIds.mapToObj(ByteBufferUtil::bytes)
                    .forEach( pk ->
                        rowIds.get().forEach( c ->
                            new RowUpdateBuilder(cfs.metadata(), 0, pk)
                                .clustering(c)
                                .add("a", ByteBufferUtil.bytes("abcd"))
                                .build()
                                .apply()

                    ));
    }

    private void assertDigestsDiffer(ByteBuffer b0, ByteBuffer b1)
    {
        assertTrue(ByteBufferUtil.compareUnsigned(b0, b1) != 0);
    }

    @Test
    public void partitionReadFullyPurged() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        ReadCommand partitionRead = Util.cmd(cfs, Util.dk("key")).build();
        fullyPurgedPartitionCreatesEmptyDigest(cfs, partitionRead);
    }

    @Test
    public void rangeReadFullyPurged() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        ReadCommand rangeRead = Util.cmd(cfs).build();
        fullyPurgedPartitionCreatesEmptyDigest(cfs, rangeRead);
    }

    /**
     * Writes a single partition containing only a single row deletion and reads with either a range or
     * partition query. Before the row deletion is eligible for purging, it should appear in the query
     * results and cause a non-empty repaired data digest to be generated. Repeating the query after
     * the row deletion is eligible for purging, both the result set and the repaired data digest should
     * be empty.
     */
    private void fullyPurgedPartitionCreatesEmptyDigest(ColumnFamilyStore cfs, ReadCommand command)
    {
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        setGCGrace(cfs, 600);

        // Partition with a fully deleted static row and a single, fully deleted regular row
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, ByteBufferUtil.bytes("key")).apply();
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, ByteBufferUtil.bytes("key"), "cc").apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        try (ReadExecutionController controller = command.executionController(true))
        {
            List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(command, controller);
            assertEquals(1, partitions.size());
            ByteBuffer digestWithTombstones = controller.getRepairedDataDigest();
            assertTrue(ByteBufferUtil.compareUnsigned(EMPTY_BYTE_BUFFER, digestWithTombstones) != 0);

            // Make tombstones eligible for purging and re-run cmd with an incremented nowInSec
            setGCGrace(cfs, 0);
        }

        AbstractReadCommandBuilder builder = command instanceof PartitionRangeReadCommand
                                             ? Util.cmd(cfs)
                                             : Util.cmd(cfs, Util.dk("key"));
        builder.withNowInSeconds(command.nowInSec() + 60);
        command = builder.build();

        try (ReadExecutionController controller = command.executionController(true))
        {
            List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(command, controller);
            assertTrue(partitions.isEmpty());
            ByteBuffer digestWithoutTombstones = controller.getRepairedDataDigest();
            assertEquals(0, ByteBufferUtil.compareUnsigned(EMPTY_BYTE_BUFFER, digestWithoutTombstones));
        }
    }

    /**
     * Verifies that during range reads which include multiple partitions, fully purged partitions
     * have no material effect on the calculated digest. This test writes two sstables, each containing
     * a single partition; the first is live and the second fully deleted and eligible for purging.
     * Initially, only the sstable containing the live partition is marked repaired, while a range read
     * which covers both partitions is performed to generate a digest. Then the sstable containing the
     * purged partition is also marked repaired and the query reexecuted. The digests produced by both
     * queries should match as the digest calculation should exclude the fully purged partition.
     */
    @Test
    public void mixedPurgedAndNonPurgedPartitions()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        setGCGrace(cfs, 0);

        ReadCommand command = Util.cmd(cfs).withNowInSeconds(FBUtilities.nowInSeconds() + 60).build();

        // Live partition in a repaired sstable, so included in the digest calculation
        new RowUpdateBuilder(cfs.metadata.get(), 0, ByteBufferUtil.bytes("key-0")).clustering("cc").add("a", ByteBufferUtil.bytes("a")).build().apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));
        // Fully deleted partition (static and regular rows) in an unrepaired sstable, so not included in the intial digest
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, ByteBufferUtil.bytes("key-1")).apply();
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, ByteBufferUtil.bytes("key-1"), "cc").apply();
        Util.flush(cfs);

        ByteBuffer digestWithoutPurgedPartition = null;
        
        try (ReadExecutionController controller = command.executionController(true))
        {
            List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(command, controller);
            assertEquals(1, partitions.size());
            digestWithoutPurgedPartition = controller.getRepairedDataDigest();
            assertTrue(ByteBufferUtil.compareUnsigned(EMPTY_BYTE_BUFFER, digestWithoutPurgedPartition) != 0);
        }
        
        // mark the sstable containing the purged partition as repaired, so both partitions are now
        // read during in the digest calculation. Because the purged partition is entirely
        // discarded, the resultant digest should match the earlier one.
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));
        command = Util.cmd(cfs).withNowInSeconds(command.nowInSec()).build();

        try (ReadExecutionController controller = command.executionController(true))
        {
            List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(command, controller);
            assertEquals(1, partitions.size());
            ByteBuffer digestWithPurgedPartition = controller.getRepairedDataDigest();
            assertEquals(0, ByteBufferUtil.compareUnsigned(digestWithPurgedPartition, digestWithoutPurgedPartition));
        }
    }

    @Test
    public void purgingConsidersRepairedDataOnly()
    {
        // 2 sstables, first is repaired and contains data that is all purgeable
        // the second is unrepaired and contains non-purgable data. Even though
        // the partition itself is not fully purged, the repaired data digest
        // should be empty as there was no non-purgeable, repaired data read.
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF6);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        setGCGrace(cfs, 0);

        // Partition with a fully deleted static row and a single, fully deleted row which will be fully purged
        DecoratedKey key = Util.dk("key");
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, key).apply();
        RowUpdateBuilder.deleteRow(cfs.metadata(), 0, key, "cc").apply();
        Util.flush(cfs);
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        new RowUpdateBuilder(cfs.metadata(), 1, key).clustering("cc").add("a", ByteBufferUtil.bytes("a")).build().apply();
        Util.flush(cfs);

        long nowInSec = FBUtilities.nowInSeconds() + 10;
        ReadCommand cmd = Util.cmd(cfs, key).withNowInSeconds(nowInSec).build();

        try (ReadExecutionController controller = cmd.executionController(true))
        {
            Partition partition = Util.getOnlyPartitionUnfiltered(cmd, controller);
            assertFalse(partition.isEmpty());
            // check that
            try (UnfilteredRowIterator rows = partition.unfilteredIterator())
            {
                assertFalse(rows.isEmpty());
                Unfiltered unfiltered = rows.next();
                assertFalse(rows.hasNext());
                assertTrue(unfiltered.isRow());
                assertFalse(((Row) unfiltered).hasDeletion(nowInSec));
            }
            assertEquals(EMPTY_BYTE_BUFFER, controller.getRepairedDataDigest());
        }
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

        Util.flush(cfs);

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        assertTrue(cfs.isRowCacheEnabled());
        // warm the cache
        assertFalse(Util.getAll(readCommand).isEmpty());
        long cacheHits = cfs.metric.rowCacheHit.getCount();

        Util.getAll(readCommand);
        assertTrue(cfs.metric.rowCacheHit.getCount() > cacheHits);
        cacheHits = cfs.metric.rowCacheHit.getCount();

        ReadCommand withRepairedInfo = readCommand.copy();
        try (ReadExecutionController controller = withRepairedInfo.executionController(true))
        {
            Util.getAll(withRepairedInfo, controller);
            assertEquals(cacheHits, cfs.metric.rowCacheHit.getCount());
        }
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

    @Test
    public void testToCQLString()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        DecoratedKey key = Util.dk("key");

        ReadCommand readCommand = Util.cmd(cfs, key).build();

        String result = readCommand.toCQLString();

        assertEquals(result, String.format("SELECT * FROM \"ReadCommandTest\".\"Standard2\" WHERE key = 0x%s ALLOW FILTERING", ByteBufferUtil.bytesToHex(key.getKey())));
    }

    private void testRepairedDataTracking(ColumnFamilyStore cfs, ReadCommand readCommand)
    {
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Util.flush(cfs);
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
        assertEquals(EMPTY_BYTE_BUFFER, digest);
        digests.add(digest);

        // add a pending repair session to table1, digest should remain the same but now we expect it to be marked inconclusive
        TimeUUID session1 = nextTimeUUID();
        mutateRepaired(cfs, sstable1, ActiveRepairService.UNREPAIRED_SSTABLE, session1);
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(1, digests.size());

        // add a different pending session to table2, digest should remain the same and still consider it inconclusive
        TimeUUID session2 = nextTimeUUID();
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
            assertEquals(EMPTY_BYTE_BUFFER, digest);

            // now flush so we have an unrepaired table with the deletion and repeat the check
            Util.flush(cfs);
            digest = performReadAndVerifyRepairedInfo(readCommand, 0, rowsPerPartition, false);
            assertEquals(EMPTY_BYTE_BUFFER, digest);
        }
    }

    private void mutateRepaired(ColumnFamilyStore cfs, SSTableReader sstable, long repairedAt, TimeUUID pendingSession)
    {
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, repairedAt, pendingSession, false);
            sstable.reloadSSTableMetadata();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail("Caught IOException when mutating sstable metadata");
        }

        if (pendingSession != null)
        {
            // setup a minimal repair session. This is necessary because we
            // check for sessions which have exceeded timeout and been purged
            Range<Token> range = new Range<>(cfs.metadata().partitioner.getMinimumToken(),
                                             cfs.metadata().partitioner.getRandomToken());
            ActiveRepairService.instance().registerParentRepairSession(pendingSession,
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

            try (ReadExecutionController controller = withRepairedInfo.executionController(true))
            {
                List<FilteredPartition> partitions = Util.getAll(withRepairedInfo, controller);
                assertEquals(expectedPartitions, partitions.size());
                partitions.forEach(p -> assertEquals(expectedRowsPerPartition, p.rowCount()));

                ByteBuffer digest = controller.getRepairedDataDigest();
                digests.add(digest);
                assertEquals(1, digests.size());
                assertEquals(expectConclusive, controller.isRepairedDataDigestConclusive());
            }
        }
        return digests.iterator().next();
    }

}
