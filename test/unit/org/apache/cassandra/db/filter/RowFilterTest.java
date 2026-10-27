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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RowFilterTest
{
    @Test
    public void testRowFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter.Builder filter = RowFilter.builder(false);
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());

        filter = RowFilter.builder(false);
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());
    }

    @Test
    public void testMutableIntersections()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .addRegularColumn("t", UTF8Type.instance)
                                              .build();

        // Empty filter - no expressions
        RowFilter filter = RowFilter.builder(false).build();
        assertFalse(filter.isMutableIntersection());

        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        // Single expression on one column
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Two expressions on the same column
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Three expressions on two different columns - this is a mutable intersection
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(t, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertTrue(filter.isMutableIntersection());
    }

    /**
     * A LIKE pattern that can't be compiled into an automaton fails the filtering with an invalid pattern error naming
     * the restricted column, which the operator evaluating the pattern doesn't know.
     */
    @Test
    public void testInvalidLikePatternNamesColumn()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("v", UTF8Type.instance)
                                              .build();
        ColumnMetadata v = metadata.getColumn(new ColumnIdentifier("v", true));

        // a generic pattern too complex to determinize
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            pattern.append("a%");
        pattern.append('a');

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(v, Operator.LIKE_MATCHES, UTF8Type.instance.decompose(pattern.toString()));
        RowFilter filter = builder.build();
        Row row = BTreeRow.create(Clustering.EMPTY,
                                  LivenessInfo.EMPTY,
                                  Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(v, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, UTF8Type.instance.decompose("abc"), null)));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));

        assertThatThrownBy(() -> filter.isSatisfiedBy(metadata, key, row, 1))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");
    }

    @Test
    public void testRowFilterAll()
    {
        // RowFilter.ALL is the singleton unsatisfiable filter
        assertTrue(RowFilter.REJECT_ALL.isUnsatisfiable());
        assertTrue(RowFilter.REJECT_ALL.isEmpty());

        // isSatisfiedBy always returns false
        TableMetadata metadata = TableMetadata.builder("k", "t")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));
        ByteBuffer one = Int32Type.instance.decompose(1);
        Row row = BTreeRow.create(Clustering.EMPTY,
                                  LivenessInfo.EMPTY,
                                  Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
        assertFalse(RowFilter.REJECT_ALL.isSatisfiedBy(metadata, null, row, 1));

        // filter(UnfilteredPartitionIterator) closes the non-empty input and returns an empty iterator
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator unfiltereds = new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return DeletionTime.LIVE; }
            public EncodingStats stats() { return EncodingStats.NO_STATS; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return metadata.regularAndStaticColumns(); }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext() { return true; }   // non-empty partition
            public Unfiltered next() { return row; }
            public void close() { closed.set(true); }
        });
        assertTrue(unfiltereds.hasNext());
        assertFalse(RowFilter.REJECT_ALL.filter(unfiltereds, 1).hasNext());
        assertTrue(closed.get());

        // filter(PartitionIterator) closes the non-empty input and returns an empty iterator
        closed.set(false);
        PartitionIterator partitions = new PartitionIterator()
        {
            boolean hasNext = true;
            public boolean hasNext() { return hasNext; }
            public RowIterator next() {hasNext = false; return null; }
            public void close() { closed.set(true); }
        };
        assertTrue(partitions.hasNext());
        assertFalse(RowFilter.REJECT_ALL.filter(partitions, metadata, 1).hasNext());
        assertTrue(closed.get());
    }
}
