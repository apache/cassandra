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

package org.apache.cassandra.io.sstable.compaction;

import java.util.NoSuchElementException;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Wrapper that converts a cursor into an UnfilteredPartitionIterator for testing.
 */
public class IteratorFromCursor implements UnfilteredPartitionIterator
{
    final TableMetadata metadata;
    final SSTableCursor cursor;
    final Row.Builder rowBuilder;

    public IteratorFromCursor(TableMetadata metadata, SSTableCursor cursor)
    {
        this.metadata = metadata;
        this.cursor = cursor;
        this.rowBuilder = BTreeRow.sortedBuilder();
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public void close()
    {
        cursor.close();
    }

    public boolean hasNext()
    {
        return (advanceToNextPartition() == SSTableCursor.Type.PARTITION);
    }

    private SSTableCursor.Type advanceToNextPartition()
    {
        SSTableCursor.Type type = cursor.type();
        while (true)
        {
            switch (type)
            {
                case PARTITION:
                case EXHAUSTED:
                    return type;
                default:
                    type = cursor.advance();
            }
        }
    }

    public UnfilteredRowIterator next()
    {
        SSTableCursor.Type type = advanceToNextPartition();
        if (type == SSTableCursor.Type.EXHAUSTED)
            throw new NoSuchElementException();
        assert type == SSTableCursor.Type.PARTITION;
        switch (cursor.advance())
        {
            case PARTITION:
            case EXHAUSTED:
                return EmptyIterators.unfilteredRow(metadata,
                                                    cursor.partitionKey(),
                                                    false,
                                                    Rows.EMPTY_STATIC_ROW,
                                                    cursor.partitionLevelDeletion());
            case ROW:
            case RANGE_TOMBSTONE:
                return new RowIterator();
            default:
                throw new AssertionError();
        }
    }

    class RowIterator implements UnfilteredRowIterator
    {
        final DecoratedKey partitionKey;
        final Row staticRow;
        final DeletionTime partitionLevelDeletion;

        protected RowIterator()
        {
            this.partitionKey = cursor.partitionKey();
            this.partitionLevelDeletion = cursor.partitionLevelDeletion();
            if (Clustering.STATIC_CLUSTERING.equals(cursor.clusteringKey()))
            {
                staticRow = collectRow(cursor, rowBuilder);
            }
            else
            {
                staticRow = Rows.EMPTY_STATIC_ROW;
            }
        }

        public boolean hasNext()
        {
            return cursor.type().level == SSTableCursor.Type.ROW.level;
        }

        public Unfiltered next()
        {
            switch (cursor.type())
            {
                case ROW:
                    return collectRow(cursor, rowBuilder);
                case RANGE_TOMBSTONE:
                    return collectRangeTombstoneMarker(cursor);
                default:
                    throw new AssertionError();
            }
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean isReverseOrder()
        {
            return false;
        }

        public RegularAndStaticColumns columns()
        {
            return metadata.regularAndStaticColumns();
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public EncodingStats stats()
        {
            return EncodingStats.NO_STATS;
        }

        public void close()
        {
            // Nothing to do on row close
        }
    }

    public static RangeTombstoneMarker collectRangeTombstoneMarker(SSTableCursor cursor)
    {
        ClusteringPrefix key = cursor.clusteringKey();
        DeletionTime previous = cursor.activeRangeDeletion();
        DeletionTime next = cursor.rowLevelDeletion();
        cursor.advance();
        switch (key.kind())
        {
            case INCL_START_BOUND:
            case EXCL_START_BOUND:
                return new RangeTombstoneBoundMarker((ClusteringBound) key, next);
            case INCL_END_BOUND:
            case EXCL_END_BOUND:
                return new RangeTombstoneBoundMarker((ClusteringBound) key, previous);
            case EXCL_END_INCL_START_BOUNDARY:
            case INCL_END_EXCL_START_BOUNDARY:
                return new RangeTombstoneBoundaryMarker((ClusteringBoundary) key, previous, next);
            default:
                throw new AssertionError();
        }
    }

    public static Row collectRow(SSTableCursor cursor, Row.Builder builder)
    {
        builder.newRow((Clustering) cursor.clusteringKey());
        builder.addPrimaryKeyLivenessInfo(cursor.clusteringKeyLivenessInfo());
        builder.addRowDeletion(Row.Deletion.regular(cursor.rowLevelDeletion()));
        while (true)
        {
            switch (cursor.advance())
            {
                case COMPLEX_COLUMN_CELL:
                case SIMPLE_COLUMN:
                    builder.addCell(cursor.cell());
                    break;
                case COMPLEX_COLUMN:
                    // Note: we want to create complex deletion cell even if there is no deletion because this passes
                    // the correct version of the column metadata to the builder.
                    builder.addComplexDeletion(cursor.column(), cursor.complexColumnDeletion());
                    break;
                default:
                    return builder.build();
            }
        }
    }
}
