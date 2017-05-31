/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SinglePartitionSliceCommandTest
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionSliceCommandTest.class);

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static TableMetadata metadata;
    private static ColumnMetadata v;
    private static ColumnMetadata s;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        metadata =
            TableMetadata.builder(KEYSPACE, TABLE)
                         .addPartitionKeyColumn("k", UTF8Type.instance)
                         .addStaticColumn("s", UTF8Type.instance)
                         .addClusteringColumn("i", IntegerType.instance)
                         .addRegularColumn("v", UTF8Type.instance)
                         .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
        v = metadata.getColumn(new ColumnIdentifier("v", true));
        s = metadata.getColumn(new ColumnIdentifier("s", true));
    }

    @Before
    public void truncate()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
    }

    private void checkForS(UnfilteredPartitionIterator pi)
    {
        Assert.assertTrue(pi.toString(), pi.hasNext());
        UnfilteredRowIterator ri = pi.next();
        Assert.assertTrue(ri.columns().contains(s));
        Row staticRow = ri.staticRow();
        Iterator<Cell> cellIterator = staticRow.cells().iterator();
        Assert.assertTrue(staticRow.toString(metadata, true), cellIterator.hasNext());
        Cell cell = cellIterator.next();
        Assert.assertEquals(s, cell.column());
        Assert.assertEquals(ByteBufferUtil.bytesToHex(cell.value()), ByteBufferUtil.bytes("s"), cell.value());
        Assert.assertFalse(cellIterator.hasNext());
    }

    @Test
    public void staticColumnsAreReturned() throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k1', 's')");
        Assert.assertFalse(QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k1'").isEmpty());

        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(s));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.NONE, false);
        ReadCommand cmd = new SinglePartitionReadCommand(false, MessagingService.VERSION_30, metadata,
                                                         FBUtilities.nowInSeconds(),
                                                         columnFilter,
                                                         RowFilter.NONE,
                                                         DataLimits.NONE,
                                                         key,
                                                         sliceFilter);

        // check raw iterator for static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            checkForS(pi);
        }

        ReadResponse response;
        DataOutputBuffer out;
        DataInputPlus in;
        ReadResponse dst;

        // check (de)serialized iterator for memtable static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd);
        }

        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }

        // check (de)serialized iterator for sstable static cell
        Schema.instance.getColumnFamilyStoreInstance(metadata.id).forceBlockingFlush();
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd);
        }
        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }
    }

    @Test
    public void toCQLStringIsSafeToCall() throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(s));
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes("i1")));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, slice), false);
        ReadCommand cmd = new SinglePartitionReadCommand(false, MessagingService.VERSION_30, metadata,
                                                         FBUtilities.nowInSeconds(),
                                                         columnFilter,
                                                         RowFilter.NONE,
                                                         DataLimits.NONE,
                                                         key,
                                                         sliceFilter);

        String ret = cmd.toCQLString();
        Assert.assertNotNull(ret);
        Assert.assertFalse(ret.isEmpty());
    }
}
