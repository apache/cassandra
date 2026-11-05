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

import org.junit.Test;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ColumnFilterSchemaEvolutionTest
{
    @Test
    public void fixedFetchedColumnsSurviveSchemaChangeAndSerialization() throws IOException
    {
        TableMetadata beforeDrop = metadata();
        ColumnMetadata staleRegularSimple = column(beforeDrop, "stale_regular_simple");
        ColumnMetadata staleRegularComplex = column(beforeDrop, "stale_regular_complex");
        ColumnMetadata staleStaticSimple = column(beforeDrop, "stale_static_simple");
        ColumnMetadata staleStaticComplex = column(beforeDrop, "stale_static_complex");
        TableMetadata current = drop(beforeDrop,
                                     staleRegularSimple,
                                     staleRegularComplex,
                                     staleStaticSimple,
                                     staleStaticComplex);

        ColumnMetadata queriedRegularSimple = column(current, "queried_regular_simple");
        ColumnMetadata queriedRegularComplex = column(current, "queried_regular_complex");
        ColumnMetadata queriedStaticSimple = column(current, "queried_static_simple");
        ColumnMetadata queriedStaticComplex = column(current, "queried_static_complex");
        ColumnMetadata unqueriedRegularSimple = column(current, "unqueried_regular_simple");
        ColumnMetadata unqueriedRegularComplex = column(current, "unqueried_regular_complex");
        ColumnMetadata unqueriedStaticSimple = column(current, "unqueried_static_simple");
        ColumnMetadata unqueriedStaticComplex = column(current, "unqueried_static_complex");

        RegularAndStaticColumns queried = RegularAndStaticColumns.builder()
                                                                 .add(queriedRegularSimple)
                                                                 .add(queriedRegularComplex)
                                                                 .add(queriedStaticSimple)
                                                                 .add(queriedStaticComplex)
                                                                 .build();
        ColumnMetadata[] alwaysFetched = {
            queriedRegularSimple,
            queriedRegularComplex,
            queriedStaticSimple,
            queriedStaticComplex,
            unqueriedRegularSimple,
            unqueriedRegularComplex
        };
        ColumnMetadata[] fetchedOnlyWithAllStatics = { unqueriedStaticSimple, unqueriedStaticComplex };
        ColumnMetadata[] neverFetched = {
            staleRegularSimple,
            staleRegularComplex,
            staleStaticSimple,
            staleStaticComplex
        };

        for (boolean fetchAllStatics : new boolean[]{ false, true })
        {
            ColumnFilter filter = ColumnFilter.selection(current, queried, fetchAllStatics);
            assertFetchedSet(filter, fetchAllStatics, alwaysFetched, fetchedOnlyWithAllStatics, neverFetched);
            assertFetchedSet(roundTrip(filter, beforeDrop),
                             fetchAllStatics,
                             alwaysFetched,
                             fetchedOnlyWithAllStatics,
                             neverFetched);
        }
    }

    @Test
    public void allEverFetchesDroppedColumns() throws IOException
    {
        TableMetadata beforeDrop = metadata();
        ColumnMetadata staleRegularSimple = column(beforeDrop, "stale_regular_simple");
        ColumnMetadata staleRegularComplex = column(beforeDrop, "stale_regular_complex");
        ColumnMetadata staleStaticSimple = column(beforeDrop, "stale_static_simple");
        ColumnMetadata staleStaticComplex = column(beforeDrop, "stale_static_complex");
        TableMetadata current = drop(beforeDrop,
                                     staleRegularSimple,
                                     staleRegularComplex,
                                     staleStaticSimple,
                                     staleStaticComplex);

        ColumnFilter currentColumns = roundTrip(ColumnFilter.all(current), beforeDrop);
        assertFetches(currentColumns, false,
                      staleRegularSimple, staleRegularComplex, staleStaticSimple, staleStaticComplex);

        ColumnFilter currentAndDroppedColumns = roundTrip(ColumnFilter.allEver(current), beforeDrop);
        assertFetches(currentAndDroppedColumns, true,
                      staleRegularSimple, staleRegularComplex, staleStaticSimple, staleStaticComplex);
    }

    private static void assertFetchedSet(ColumnFilter filter,
                                         boolean fetchAllStatics,
                                         ColumnMetadata[] alwaysFetched,
                                         ColumnMetadata[] fetchedOnlyWithAllStatics,
                                         ColumnMetadata[] neverFetched)
    {
        assertFetches(filter, true, alwaysFetched);
        assertFetches(filter, fetchAllStatics, fetchedOnlyWithAllStatics);
        assertFetches(filter, false, neverFetched);
    }

    private static void assertFetches(ColumnFilter filter, boolean expected, ColumnMetadata... columns)
    {
        for (ColumnMetadata column : columns)
            assertEquals(column.toString(), expected, filter.fetches(column));
    }

    private static ColumnFilter roundTrip(ColumnFilter filter, TableMetadata metadata) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            ColumnFilter.serializer.serialize(filter, output, MessagingService.current_version);
            assertEquals(ColumnFilter.serializer.serializedSize(filter, MessagingService.current_version),
                         output.position());
            try (DataInputBuffer input = new DataInputBuffer(output.buffer(), false))
            {
                ColumnFilter deserialized =
                    ColumnFilter.serializer.deserialize(input, MessagingService.current_version, metadata);
                assertEquals(filter, deserialized);
                return deserialized;
            }
        }
    }

    private static TableMetadata metadata()
    {
        return TableMetadata.builder("ks", "table")
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("queried_regular_simple", Int32Type.instance)
                            .addRegularColumn("queried_regular_complex", SetType.getInstance(Int32Type.instance, true))
                            .addRegularColumn("unqueried_regular_simple", Int32Type.instance)
                            .addRegularColumn("unqueried_regular_complex",
                                              SetType.getInstance(Int32Type.instance, true))
                            .addRegularColumn("stale_regular_simple", Int32Type.instance)
                            .addRegularColumn("stale_regular_complex", SetType.getInstance(Int32Type.instance, true))
                            .addStaticColumn("queried_static_simple", Int32Type.instance)
                            .addStaticColumn("queried_static_complex", SetType.getInstance(Int32Type.instance, true))
                            .addStaticColumn("unqueried_static_simple", Int32Type.instance)
                            .addStaticColumn("unqueried_static_complex", SetType.getInstance(Int32Type.instance, true))
                            .addStaticColumn("stale_static_simple", Int32Type.instance)
                            .addStaticColumn("stale_static_complex", SetType.getInstance(Int32Type.instance, true))
                            .build();
    }

    private static TableMetadata drop(TableMetadata metadata, ColumnMetadata... columns)
    {
        TableMetadata.Builder builder = metadata.unbuild();
        for (ColumnMetadata column : columns)
        {
            builder.removeRegularOrStaticColumn(column.name);
            builder.recordColumnDrop(column, 1L);
        }
        return builder.build();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }
}
