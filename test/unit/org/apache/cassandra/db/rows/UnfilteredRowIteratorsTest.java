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

package org.apache.cassandra.db.rows;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class UnfilteredRowIteratorsTest
{
    static final TableMetadata metadata;
    static final ColumnMetadata v1Metadata;
    static final ColumnMetadata v2Metadata;

    static
    {
        metadata = TableMetadata.builder("", "")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .build();
        v1Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(0);
        v2Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(1);
    }


    @Test
    public void concatTest()
    {
        UnfilteredRowIterator iter1, iter2, iter3, concat;
        // simple concatenation
        iter1 = rows(metadata.regularAndStaticColumns(), 1,
                     row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                     row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));
        iter2 = rows(metadata.regularAndStaticColumns(), 1,
                     row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)),
                     row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)));
        concat = UnfilteredRowIterators.concat(iter1, iter2);
        Assert.assertEquals(concat.columns(), metadata.regularAndStaticColumns());
        assertRows(concat,
                   row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                   row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)),
                   row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)),
                   row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)));

        // concat with RHS empty iterator
        iter1 = rows(metadata.regularAndStaticColumns(), 1,
                     row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                     row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));
        Assert.assertEquals(concat.columns(), metadata.regularAndStaticColumns());
        assertRows(UnfilteredRowIterators.concat(iter1, EmptyIterators.unfilteredRow(metadata, dk(1), false, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE)),
                   row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                   row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));

        // concat with LHS empty iterator
        iter1 = rows(metadata.regularAndStaticColumns(), 1,
                     row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                     row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));
        Assert.assertEquals(concat.columns(), metadata.regularAndStaticColumns());
        assertRows(UnfilteredRowIterators.concat(EmptyIterators.unfilteredRow(metadata, dk(1), false, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE), iter1),
                   row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                   row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));

        // concat with different columns
        iter1 = rows(metadata.regularAndStaticColumns().without(v1Metadata), 1,
                     row(1, cell(v2Metadata, 1)), row(2, cell(v2Metadata, 2)));
        iter2 = rows(metadata.regularAndStaticColumns().without(v2Metadata), 1,
                     row(3, cell(v1Metadata, 3)), row(4, cell(v1Metadata, 4)));
        concat = UnfilteredRowIterators.concat(iter1, iter2);
        Assert.assertEquals(concat.columns(), RegularAndStaticColumns.of(v1Metadata).mergeTo(RegularAndStaticColumns.of(v2Metadata)));
        assertRows(concat,
                   row(1, cell(v2Metadata, 1)), row(2, cell(v2Metadata, 2)),
                   row(3, cell(v1Metadata, 3)), row(4, cell(v1Metadata, 4)));

        // concat with CQL limits
        iter1 = rows(metadata.regularAndStaticColumns(), 1,
                     row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                     row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));
        iter2 = rows(metadata.regularAndStaticColumns(), 1,
                     row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)),
                     row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)));
        concat = UnfilteredRowIterators.concat(DataLimits.cqlLimits(1).filter(iter1, FBUtilities.nowInSeconds(), true),
                                               DataLimits.cqlLimits(1).filter(iter2, FBUtilities.nowInSeconds(), true));
        Assert.assertEquals(concat.columns(), metadata.regularAndStaticColumns());
        assertRows(concat,
                   row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                   row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)));

        // concat concatenated iterators
        iter1 = rows(metadata.regularAndStaticColumns(), 1,
                     row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                     row(2, cell(v1Metadata, 2), cell(v2Metadata, 2)));
        iter2 = rows(metadata.regularAndStaticColumns(), 1,
                     row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)),
                     row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)));

        concat = UnfilteredRowIterators.concat(DataLimits.cqlLimits(1).filter(iter1, FBUtilities.nowInSeconds(), true),
                                               DataLimits.cqlLimits(1).filter(iter2, FBUtilities.nowInSeconds(), true));

        iter3 = rows(metadata.regularAndStaticColumns(), 1,
                     row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)),
                     row(5, cell(v1Metadata, 5), cell(v2Metadata, 5)));
        concat = UnfilteredRowIterators.concat(concat, DataLimits.cqlLimits(1).filter(iter3, FBUtilities.nowInSeconds(), true));

        Assert.assertEquals(concat.columns(), metadata.regularAndStaticColumns());
        assertRows(concat,
                   row(1, cell(v1Metadata, 1), cell(v2Metadata, 1)),
                   row(3, cell(v1Metadata, 3), cell(v2Metadata, 3)),
                   row(4, cell(v1Metadata, 4), cell(v2Metadata, 4)));
    }

    public static void assertRows(UnfilteredRowIterator iterator, Row... rows)
    {
        Iterator<Row> rowsIterator = Arrays.asList(rows).iterator();

        while (iterator.hasNext() && rowsIterator.hasNext())
            Assert.assertEquals(iterator.next(), rowsIterator.next());

        Assert.assertTrue(iterator.hasNext() == rowsIterator.hasNext());
    }

    public static DecoratedKey dk(int pk)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(pk), ByteBufferUtil.bytes(pk));
    }

    public static UnfilteredRowIterator rows(RegularAndStaticColumns columns, int pk, Row... rows)
    {
        Iterator<Row> rowsIterator = Arrays.asList(rows).iterator();
        return new AbstractUnfilteredRowIterator(metadata, dk(pk), DeletionTime.LIVE, columns, Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS) {
            protected Unfiltered computeNext()
            {
                return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
            }
        };
    }

    public Row row(int ck, Cell<?>... columns)
    {
        BTreeRow.Builder builder = new BTreeRow.Builder(true);
        builder.newRow(Util.clustering(metadata.comparator, ck));
        for (Cell<?> cell : columns)
            builder.addCell(cell);
        return builder.build();
    }

    public Cell<?> cell(ColumnMetadata metadata, int v)
    {
        return new BufferCell(metadata,
                              1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, ByteBufferUtil.bytes(v), null);
    }
}
