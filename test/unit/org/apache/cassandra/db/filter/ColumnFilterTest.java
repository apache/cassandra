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

package org.apache.cassandra.db.filter;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ColumnFilterTest
{
    private static final ColumnFilter.Serializer serializer = new ColumnFilter.Serializer();

    @Test
    public void columnFilterSerialisationRoundTrip() throws Exception
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .addPartitionKey("pk", Int32Type.instance)
                                                .addClusteringColumn("ck", Int32Type.instance)
                                                .addRegularColumn("v1", Int32Type.instance)
                                                .addRegularColumn("v2", Int32Type.instance)
                                                .addRegularColumn("v3", Int32Type.instance)
                                                .build();

        ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));

        testRoundTrip(ColumnFilter.all(metadata), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.all(metadata), metadata, MessagingService.VERSION_3014);

        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_3014);
    }

    private static void testRoundTrip(ColumnFilter columnFilter, CFMetaData metadata, int version) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        serializer.serialize(columnFilter, output, version);
        Assert.assertEquals(serializer.serializedSize(columnFilter, version), output.position());
        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        Assert.assertEquals(serializer.deserialize(input, version, metadata), columnFilter);
    }

    /**
     * Tests whether a filter fetches and/or queries columns and cells.
     */
    @Test
    public void testFetchedQueried()
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                              .withPartitioner(Murmur3Partitioner.instance)
                                              .addPartitionKey("k", Int32Type.instance)
                                              .addRegularColumn("simple", Int32Type.instance)
                                              .addRegularColumn("complex", SetType.getInstance(Int32Type.instance, true))
                                              .build();

        ColumnDefinition simple = metadata.getColumnDefinition(ByteBufferUtil.bytes("simple"));
        ColumnDefinition complex = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex"));
        CellPath path1 = CellPath.create(ByteBufferUtil.bytes(1));
        CellPath path2 = CellPath.create(ByteBufferUtil.bytes(2));
        ColumnFilter filter;

        // select only the simple column, without table metadata
        filter = ColumnFilter.selection(PartitionColumns.builder().add(simple).build());
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(false, false, filter, complex);
        assertFetchedQueried(false, false, filter, complex, path1);
        assertFetchedQueried(false, false, filter, complex, path2);

        // select only the complex column, without table metadata
        filter = ColumnFilter.selection(PartitionColumns.builder().add(complex).build());
        assertFetchedQueried(false, false, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, true, filter, complex, path2);

        // select both the simple and complex columns, without table metadata
        filter = ColumnFilter.selection(PartitionColumns.builder().add(simple).add(complex).build());
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, true, filter, complex, path2);

        // select only the simple column, with table metadata
        filter = ColumnFilter.selection(metadata, PartitionColumns.builder().add(simple).build());
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(true, false, filter, complex);
        assertFetchedQueried(true, false, filter, complex, path1);
        assertFetchedQueried(true, false, filter, complex, path2);

        // select only the complex column, with table metadata
        filter = ColumnFilter.selection(metadata, PartitionColumns.builder().add(complex).build());
        assertFetchedQueried(true, false, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, true, filter, complex, path2);

        // select both the simple and complex columns, with table metadata
        filter = ColumnFilter.selection(metadata, PartitionColumns.builder().add(simple).add(complex).build());
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, true, filter, complex, path2);

        // select only the simple column, with selection builder
        filter = ColumnFilter.selectionBuilder().add(simple).build();
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(false, false, filter, complex);
        assertFetchedQueried(false, false, filter, complex, path1);
        assertFetchedQueried(false, false, filter, complex, path2);

        // select only a cell of the complex column, with selection builder
        filter = ColumnFilter.selectionBuilder().select(complex, path1).build();
        assertFetchedQueried(false, false, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, false, filter, complex, path2);

        // select both the simple column and a cell of the complex column, with selection builder
        filter = ColumnFilter.selectionBuilder().add(simple).select(complex, path1).build();
        assertFetchedQueried(true, true, filter, simple);
        assertFetchedQueried(true, true, filter, complex);
        assertFetchedQueried(true, true, filter, complex, path1);
        assertFetchedQueried(true, false, filter, complex, path2);
    }

    private static void assertFetchedQueried(boolean expectedFetched,
                                             boolean expectedQueried,
                                             ColumnFilter filter,
                                             ColumnDefinition column)
    {
        assert !expectedQueried || expectedFetched;
        boolean actualFetched = filter.fetches(column);
        assertEquals(expectedFetched, actualFetched);
        assertEquals(expectedQueried, actualFetched && filter.fetchedColumnIsQueried(column));
    }

    private static void assertFetchedQueried(boolean expectedFetched,
                                             boolean expectedQueried,
                                             ColumnFilter filter,
                                             ColumnDefinition column,
                                             CellPath path)
    {
        assert !expectedQueried || expectedFetched;
        boolean actualFetched = filter.fetches(column);
        assertEquals(expectedFetched, actualFetched);
        assertEquals(expectedQueried, actualFetched && filter.fetchedCellIsQueried(column, path));
    }
}