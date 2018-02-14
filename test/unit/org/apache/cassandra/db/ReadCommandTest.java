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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class ReadCommandTest
{
    private static final String KEYSPACE = "ReadCommandTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";
    private static final String CF3 = "Standard3";
    private static final String CF4 = "Standard4";
    private static final String CF5 = "Standard5";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        CFMetaData metadata1 = SchemaLoader.standardCFMD(KEYSPACE, CF1);

        CFMetaData metadata2 = CFMetaData.Builder.create(KEYSPACE, CF2)
                                                         .addPartitionKey("key", BytesType.instance)
                                                         .addClusteringColumn("col", AsciiType.instance)
                                                         .addRegularColumn("a", AsciiType.instance)
                                                         .addRegularColumn("b", AsciiType.instance).build();

        CFMetaData metadata3 = CFMetaData.Builder.create(KEYSPACE, CF3)
                                                 .addPartitionKey("key", BytesType.instance)
                                                 .addClusteringColumn("col", AsciiType.instance)
                                                 .addRegularColumn("a", AsciiType.instance)
                                                 .addRegularColumn("b", AsciiType.instance)
                                                 .addRegularColumn("c", AsciiType.instance)
                                                 .addRegularColumn("d", AsciiType.instance)
                                                 .addRegularColumn("e", AsciiType.instance)
                                                 .addRegularColumn("f", AsciiType.instance).build();

        CFMetaData metadata4 = CFMetaData.Builder.create(KEYSPACE, CF4)
                                                 .addPartitionKey("key", BytesType.instance)
                                                 .addClusteringColumn("col", AsciiType.instance)
                                                 .addRegularColumn("a", AsciiType.instance)
                                                 .addRegularColumn("b", AsciiType.instance)
                                                 .addRegularColumn("c", AsciiType.instance)
                                                 .addRegularColumn("d", AsciiType.instance)
                                                 .addRegularColumn("e", AsciiType.instance)
                                                 .addRegularColumn("f", AsciiType.instance).build();

        CFMetaData metadata5 = CFMetaData.Builder.create(KEYSPACE, CF5)
                                                 .addPartitionKey("key", BytesType.instance)
                                                 .addClusteringColumn("col", AsciiType.instance)
                                                 .addRegularColumn("a", AsciiType.instance)
                                                 .addRegularColumn("b", AsciiType.instance)
                                                 .addRegularColumn("c", AsciiType.instance)
                                                 .addRegularColumn("d", AsciiType.instance)
                                                 .addRegularColumn("e", AsciiType.instance)
                                                 .addRegularColumn("f", AsciiType.instance).build();


        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata1,
                                    metadata2,
                                    metadata3,
                                    metadata4,
                                    metadata5);
    }

    @Test
    public void testPartitionRangeAbort() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key2"))
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

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
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

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
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
        int nowInSeconds = FBUtilities.nowInSeconds();

        List<UnfilteredPartitionIterator> iterators = writeAndThenReadPartitions(cfs, groups, nowInSeconds);
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

        try (PartitionIterator partitionIterator = UnfilteredPartitionIterators.filter(UnfilteredPartitionIterators.merge(iterators, nowInSeconds, listener), nowInSeconds))
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
                        assertEquals("col=" + expectedRows[i++], row.clustering().toString(cfs.metadata));
                        //System.out.print(row.toString(cfs.metadata, true));
                    }
                }
            }

            assertEquals(5, numPartitions);
            assertEquals(expectedRows.length, i);
        }
    }

    /**
     * This test will create several partitions with several rows each. Then, it will perform up to 5 row deletions on
     * some partitions. We check that when reading the partitions, the maximum number of tombstones reported in the
     * metrics is indeed equal to 5.
     */
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
        int nowInSeconds = FBUtilities.nowInSeconds();

        writeAndThenReadPartitions(cfs, groups, nowInSeconds);

        assertEquals(5, cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMax());
    }

    /**
     * This test will create several partitions with several rows each and no deletions. We check that when reading the
     * partitions, the maximum number of tombstones reported in the metrics is equal to 1, which is apparently the
     * default max value for histograms in the metrics lib (equivalent to having no element reported).
     */
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

        int nowInSeconds = FBUtilities.nowInSeconds();

        writeAndThenReadPartitions(cfs, groups, nowInSeconds);

        assertEquals(1, cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMax());
    }

    /**
     * Writes rows to the column family store using the groups as input and then reads them. Returns the iterators from
     * the read.
     */
    private List<UnfilteredPartitionIterator> writeAndThenReadPartitions(ColumnFamilyStore cfs, String[][][] groups,
            int nowInSeconds) throws IOException
    {
        List<ByteBuffer> buffers = new ArrayList<>(groups.length);
        ColumnFilter columnFilter = ColumnFilter.allColumnsBuilder(cfs.metadata).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.TOP);
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(
                Slices.with(cfs.metadata.comparator, slice), false);

        for (String[][] group : groups)
        {
            cfs.truncateBlocking();

            List<SinglePartitionReadCommand> commands = new ArrayList<>(group.length);

            for (String[] data : group)
            {
                if (data[0].equals("1"))
                {
                    new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes(data[1]))
                            .clustering(data[2])
                            .add(data[3], ByteBufferUtil.bytes("blah"))
                            .build()
                            .apply();
                }
                else
                {
                    RowUpdateBuilder.deleteRow(cfs.metadata, FBUtilities.timestampMicros(),
                            ByteBufferUtil.bytes(data[1]), data[2]).apply();
                }
                commands.add(SinglePartitionReadCommand.create(cfs.metadata, nowInSeconds, columnFilter, rowFilter,
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

        // deserialize, merge and check the results are all there
        List<UnfilteredPartitionIterator> iterators = new ArrayList<>();

        for (ByteBuffer buffer : buffers)
        {
            try (DataInputBuffer in = new DataInputBuffer(buffer, true))
            {
                iterators.add(UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                        MessagingService.current_version,
                        cfs.metadata,
                        columnFilter,
                        SerializationHelper.Flag.LOCAL));
            }
        }

        return iterators;
    }

}
