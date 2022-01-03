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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ColumnFilterTest
{
    private static final ColumnFilter.Serializer serializer = new ColumnFilter.Serializer();

    private final CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                          .withPartitioner(Murmur3Partitioner.instance)
                                                          .addPartitionKey("pk", Int32Type.instance)
                                                          .addClusteringColumn("ck", Int32Type.instance)
                                                          .addStaticColumn("s1", Int32Type.instance)
                                                          .addStaticColumn("s2", SetType.getInstance(Int32Type.instance, true))
                                                          .addRegularColumn("v1", Int32Type.instance)
                                                          .addRegularColumn("v2", SetType.getInstance(Int32Type.instance, true))
                                                          .addRegularColumn(ColumnIdentifier.getInterned("Escaped Name", true), Int32Type.instance)
                                                          .build();

    private final ColumnDefinition s1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s1"));
    private final ColumnDefinition s2 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s2"));
    private final ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));
    private final ColumnDefinition v2 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v2"));
    private final ColumnDefinition escaped = metadata.getColumnDefinition(ByteBufferUtil.bytes("Escaped Name"));
    private final CellPath path0 = CellPath.create(ByteBufferUtil.bytes(0));
    private final CellPath path1 = CellPath.create(ByteBufferUtil.bytes(1));
    private final CellPath path2 = CellPath.create(ByteBufferUtil.bytes(2));
    private final CellPath path3 = CellPath.create(ByteBufferUtil.bytes(3));
    private final CellPath path4 = CellPath.create(ByteBufferUtil.bytes(4));

    @Parameterized.Parameter
    public boolean anyNodeOn30;

    @Parameterized.Parameters(name = "{index}: anyNodeOn30={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ true }, new Object[]{ false });
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void before()
    {
        Gossiper.instance.setAnyNodeOn30(anyNodeOn30);
    }

    // Select all

    @Test
    public void testSelectAll()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("*/*", filter.toString());
            assertEquals("*", filter.toCQLString());
            assertFetchedQueried(true, true, filter, v1, v2, s1, s2);
            assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.all(metadata));
        check.accept(ColumnFilter.allColumnsBuilder(metadata).build());
    }

    // Selections

    @Test
    public void testSelectNothing()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[]", filter.toString());
            assertEquals("*", filter.toCQLString());
            assertFetchedQueried(false, false, filter, v1, v2, s1, s2);
            assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.NONE));
        check.accept(ColumnFilter.selectionBuilder().build());
    }

    @Test
    public void testSelectSimpleColumn()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[v1]", filter.toString());
            assertEquals("v1", filter.toCQLString());
            assertFetchedQueried(true, true, filter, v1);
            assertFetchedQueried(false, false, filter, v2, s1, s2);
            assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(v1).build()));
        check.accept(ColumnFilter.selectionBuilder().add(v1).build());
    }

    @Test
    public void testSelectEscapedColumn()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[Escaped Name]", filter.toString());
            assertEquals("\"Escaped Name\"", filter.toCQLString());
            assertFetchedQueried(true, true, filter, escaped);
            assertFetchedQueried(false, false, filter, v2, s1, s2);
            assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(escaped).build()));
        check.accept(ColumnFilter.selectionBuilder().add(escaped).build());
    }

    @Test
    public void testSelectComplexColumn()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[v2]", filter.toString());
            assertEquals("v2", filter.toCQLString());
            assertFetchedQueried(true, true, filter, v2);
            assertFetchedQueried(false, false, filter, v1, s1, s2);
            assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(v2).build()));
        check.accept(ColumnFilter.selectionBuilder().add(v2).build());
    }

    @Test
    public void testSelectStaticColumn()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[s1]", filter.toString());
            assertEquals("s1", filter.toCQLString());
            assertFetchedQueried(true, true, filter, s1);
            assertFetchedQueried(false, false, filter, v1, v2, s2);
            assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(s1).build()));
        check.accept(ColumnFilter.selectionBuilder().add(s1).build());
    }

    @Test
    public void testSelectStaticComplexColumn()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[s2]", filter.toString());
            assertEquals("s2", filter.toCQLString());
            assertFetchedQueried(true, true, filter, s2);
            assertFetchedQueried(false, false, filter, v1, v2, s1);
            assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(s2).build()));
        check.accept(ColumnFilter.selectionBuilder().add(s2).build());
    }

    @Test
    public void testSelectColumns()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertEquals("[s1, s2, v1, v2]", filter.toString());
            assertEquals("s1, s2, v1, v2", filter.toCQLString());
            assertFetchedQueried(true, true, filter, v1, v2, s1, s2);
            assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
        };

        check.accept(ColumnFilter.selection(PartitionColumns.builder().add(v1).add(v2).add(s1).add(s2).build()));
        check.accept(ColumnFilter.selectionBuilder().add(v1).add(v2).add(s1).add(s2).build());
    }

    @Test
    public void testSelectIndividualCells()
    {
        ColumnFilter filter = ColumnFilter.selectionBuilder().select(v2, path1).select(v2, path3).build();
        testRoundTrips(filter);
        assertEquals("[v2[1], v2[3]]", filter.toString());
        assertEquals("v2[1], v2[3]", filter.toCQLString());
        assertFetchedQueried(true, true, filter, v2);
        assertFetchedQueried(false, false, filter, v1, s1, s2);
        assertCellFetchedQueried(true, true, filter, v2, path1, path3);
        assertCellFetchedQueried(false, false, filter, v2, path0, path2, path4);
        assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
    }

    @Test
    public void testSelectIndividualCellsFromStatic()
    {
        ColumnFilter filter = ColumnFilter.selectionBuilder().select(s2, path1).select(s2, path3).build();
        testRoundTrips(filter);
        assertEquals("[s2[1], s2[3]]", filter.toString());
        assertEquals("s2[1], s2[3]", filter.toCQLString());
        assertFetchedQueried(true, true, filter, s2);
        assertFetchedQueried(false, false, filter, v1, v2, s1);
        assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
        assertCellFetchedQueried(true, true, filter, s2, path1, path3);
        assertCellFetchedQueried(false, false, filter, s2, path0, path2, path4);
    }

    @Test
    public void testSelectCellSlice()
    {
        ColumnFilter filter = ColumnFilter.selectionBuilder().slice(v2, path1, path3).build();
        testRoundTrips(filter);
        assertEquals("[v2[1:3]]", filter.toString());
        assertEquals("v2[1:3]", filter.toCQLString());
        assertFetchedQueried(true, true, filter, v2);
        assertFetchedQueried(false, false, filter, v1, s1, s2);
        assertCellFetchedQueried(true, true, filter, v2, path1, path2, path3);
        assertCellFetchedQueried(false, false, filter, v2, path0, path4);
        assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
    }

    @Test
    public void testSelectCellSliceFromStatic()
    {
        ColumnFilter filter = ColumnFilter.selectionBuilder().slice(s2, path1, path3).build();
        testRoundTrips(filter);
        assertEquals("[s2[1:3]]", filter.toString());
        assertEquals("s2[1:3]", filter.toCQLString());
        assertFetchedQueried(true, true, filter, s2);
        assertFetchedQueried(false, false, filter, v1, v2, s1);
        assertCellFetchedQueried(false, false, filter, v2, path0, path1, path2, path3, path4);
        assertCellFetchedQueried(true, true, filter, s2, path1, path2, path3);
        assertCellFetchedQueried(false, false, filter, s2, path0, path4);
    }

    @Test
    public void testSelectColumnsWithCellsAndSlices()
    {
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .add(v1)
                                          .add(s1)
                                          .slice(v2, path0, path2)
                                          .select(v2, path4)
                                          .select(s2, path0)
                                          .slice(s2, path2, path4)
                                          .build();
        testRoundTrips(filter);
        assertEquals("[s1, s2[0], s2[2:4], v1, v2[0:2], v2[4]]", filter.toString());
        assertEquals("s1, s2[0], s2[2:4], v1, v2[0:2], v2[4]", filter.toCQLString());
        assertFetchedQueried(true, true, filter, v1, v2, s1, s2);
        assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path4);
        assertCellFetchedQueried(false, false, filter, v2, path3);
        assertCellFetchedQueried(true, true, filter, s2, path0, path2, path3, path4);
        assertCellFetchedQueried(false, false, filter, s2, path1);
    }

    // select with metadata

    @Test
    public void testSelectSimpleColumnWithMetadata()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertFetchedQueried(true, true, filter, v1);
            if (anyNodeOn30)
            {
                assertEquals("*/*", filter.toString());
                assertEquals("*", filter.toCQLString());
                assertFetchedQueried(true, true, filter, s1, s2, v2);
                assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
                assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
            }
            else
            {
                assertEquals("*/[v1]", filter.toString());
                assertEquals("v1", filter.toCQLString());
                assertFetchedQueried(true, false, filter, s1, s2, v2);
                assertCellFetchedQueried(true, false, filter, v2, path0, path1, path2, path3, path4);
                assertCellFetchedQueried(true, false, filter, s2, path0, path1, path2, path3, path4);
            }
        };

        check.accept(ColumnFilter.selection(metadata, PartitionColumns.builder().add(v1).build()));
        check.accept(ColumnFilter.allColumnsBuilder(metadata).add(v1).build());
    }

    @Test
    public void testSelectStaticColumnWithMetadata()
    {
        Consumer<ColumnFilter> check = filter -> {
            testRoundTrips(filter);
            assertFetchedQueried(true, true, filter, s1);
            if (anyNodeOn30)
            {
                assertEquals("*/*", filter.toString());
                assertEquals("*", filter.toCQLString());
                assertFetchedQueried(true, true, filter, v1, v2, s2);
                assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
                assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
            }
            else
            {
                assertEquals("*/[s1]", filter.toString());
                assertEquals("s1", filter.toCQLString());
                assertFetchedQueried(true, false, filter, v1, v2, s2);
                assertCellFetchedQueried(true, false, filter, v2, path0, path1, path2, path3, path4);
                assertCellFetchedQueried(false, false, filter, s2, path0, path1, path2, path3, path4);
            }
        };

        check.accept(ColumnFilter.selection(metadata, PartitionColumns.builder().add(s1).build()));
        check.accept(ColumnFilter.allColumnsBuilder(metadata).add(s1).build());
    }

    @Test
    public void testSelectCellWithMetadata()
    {
        ColumnFilter filter = ColumnFilter.allColumnsBuilder(metadata).select(v2, path1).build();
        testRoundTrips(filter);
        assertFetchedQueried(true, true, filter, v2);
        if (anyNodeOn30)
        {
            assertEquals("*/*", filter.toString());
            assertEquals("*", filter.toCQLString());
            assertFetchedQueried(true, true, filter, s1, s2, v1);
            assertCellFetchedQueried(true, true, filter, v2, path1);
            assertCellFetchedQueried(true, false, filter, v2, path0, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path0, path1, path2, path3, path4);
        }
        else
        {
            assertEquals("*/[v2[1]]", filter.toString());
            assertEquals("v2[1]", filter.toCQLString());
            assertFetchedQueried(true, false, filter, s1, s2, v1);
            assertCellFetchedQueried(true, true, filter, v2, path1);
            assertCellFetchedQueried(true, false, filter, v2, path0, path2, path3, path4);
            assertCellFetchedQueried(true, false, filter, s2, path0, path1, path2, path3, path4);
        }
    }

    @Test
    public void testSelectStaticColumnCellWithMetadata()
    {
        ColumnFilter filter = ColumnFilter.allColumnsBuilder(metadata).select(s2, path1).build();
        testRoundTrips(filter);
        assertFetchedQueried(true, true, filter, s2);
        if (anyNodeOn30)
        {
            assertEquals("*/*", filter.toString());
            assertEquals("*", filter.toCQLString());
            assertFetchedQueried(true, true, filter, v1, v2, s1);
            assertCellFetchedQueried(true, true, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path1);
            assertCellFetchedQueried(true, false, filter, s2, path0, path2, path3, path4);
        }
        else
        {
            assertEquals("*/[s2[1]]", filter.toString());
            assertEquals("s2[1]", filter.toCQLString());
            assertFetchedQueried(true, false, filter, v1, v2, s1);
            assertCellFetchedQueried(true, false, filter, v2, path0, path1, path2, path3, path4);
            assertCellFetchedQueried(true, true, filter, s2, path1);
            assertCellFetchedQueried(true, false, filter, s2, path0, path2, path3, path4);
        }
    }

    private void testRoundTrips(ColumnFilter cf)
    {
        testRoundTrip(cf, MessagingService.VERSION_30);
        testRoundTrip(cf, MessagingService.VERSION_3014);
    }

    private void testRoundTrip(ColumnFilter columnFilter, int version)
    {
        try
        {
            DataOutputBuffer output = new DataOutputBuffer();
            serializer.serialize(columnFilter, output, version);
            Assert.assertEquals(serializer.serializedSize(columnFilter, version), output.position());
            DataInputPlus input = new DataInputBuffer(output.buffer(), false);
            ColumnFilter deserialized = serializer.deserialize(input, version, metadata);
            Assert.assertEquals(deserialized, columnFilter);
        }
        catch (IOException e)
        {
            throw Throwables.propagate(e);
        }
    }

    private static void assertFetchedQueried(boolean expectedFetched,
                                             boolean expectedQueried,
                                             ColumnFilter filter,
                                             ColumnDefinition... columns)
    {
        for (ColumnDefinition column : columns)
        {
            assertEquals(String.format("Expected fetches(%s) to be %s", column.name, expectedFetched),
                         expectedFetched, filter.fetches(column));
            if (expectedFetched)
                assertEquals(String.format("Expected fetchedColumnIsQueried(%s) to be %s", column.name, expectedQueried),
                             expectedQueried, filter.fetchedColumnIsQueried(column));
        }
    }

    private static void assertCellFetchedQueried(boolean expectedFetched,
                                                 boolean expectedQueried,
                                                 ColumnFilter filter,
                                                 ColumnDefinition column,
                                                 CellPath... paths)
    {
        ColumnFilter.Tester tester = filter.newTester(column);

        for (CellPath path : paths)
        {
            int p = ByteBufferUtil.toInt(path.get(0));
            if (expectedFetched)
                assertEquals(String.format("Expected fetchedCellIsQueried(%s:%s) to be %s", column.name, p, expectedQueried),
                             expectedQueried, filter.fetchedCellIsQueried(column, path));

            if (tester != null)
            {
                assertEquals(String.format("Expected tester.fetches(%s:%s) to be %s", column.name, p, expectedFetched),
                             expectedFetched, tester.fetches(path));
                if (expectedFetched)
                    assertEquals(String.format("Expected tester.fetchedCellIsQueried(%s:%s) to be %s", column.name, p, expectedQueried),
                                 expectedQueried, tester.fetchedCellIsQueried(path));
            }
        }
    }
}
