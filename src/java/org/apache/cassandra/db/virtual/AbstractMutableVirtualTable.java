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
package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An abstract virtual table implementation that builds the resultset on demand and allows fine-grained source
 * modification via INSERT/UPDATE, DELETE and TRUNCATE operations.
 * 
 * Virtual table implementation need to be thread-safe has they can be called from different threads.
 */
public abstract class AbstractMutableVirtualTable extends AbstractVirtualTable
{

    protected AbstractMutableVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @Override
    public final void apply(PartitionUpdate update)
    {
        ColumnValues partitionKey = ColumnValues.from(metadata(), update.partitionKey());

        if (update.deletionInfo().isLive())
            update.forEach(row ->
            {
                ColumnValues clusteringColumns = ColumnValues.from(metadata(), row.clustering());

                if (row.deletion().isLive())
                {
                    if (row.columnCount() == 0)
                    {
                        applyColumnUpdate(partitionKey, clusteringColumns, Optional.empty());
                    }
                    else
                    {
                        row.forEach(columnData ->
                        {
                            checkFalse(columnData.column().isComplex(), "Complex type columns are not supported by table %s", metadata);

                            Cell<?> cell = (Cell<?>) columnData;

                            if (cell.isTombstone())
                                applyColumnDeletion(partitionKey, clusteringColumns, columnName(cell));
                            else
                                applyColumnUpdate(partitionKey,
                                        clusteringColumns,
                                        Optional.of(ColumnValue.from(cell)));
                        });
                    }
                }
                else
                    applyRowDeletion(partitionKey, clusteringColumns);
            });
        else
        {
            // MutableDeletionInfo may have partition delete or range tombstone list or both
            if (update.deletionInfo().hasRanges())
                update.deletionInfo()
                        .rangeIterator(false)
                        .forEachRemaining(rt -> applyRangeTombstone(partitionKey, toRange(rt.deletedSlice())));

            if (!update.deletionInfo().getPartitionDeletion().isLive())
                applyPartitionDeletion(partitionKey);
        }
    }

    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        throw invalidRequest("Partition deletion is not supported by table %s", metadata);
    }

    private Range<ColumnValues> toRange(Slice slice)
    {
        ClusteringBound<?> startBound = slice.start();
        ClusteringBound<?> endBound = slice.end();

        if (startBound.isBottom())
        {
            if (endBound.isTop())
                return Range.all();

            return Range.upTo(ColumnValues.from(metadata(), endBound), boundType(endBound));
        }

        if (endBound.isTop())
            return Range.downTo(ColumnValues.from(metadata(), startBound), boundType(startBound));

        ColumnValues start = ColumnValues.from(metadata(), startBound);
        BoundType startType = boundType(startBound);

        ColumnValues end = ColumnValues.from(metadata(), endBound);
        BoundType endType = boundType(endBound);

        return Range.range(start, startType, end, endType);
    }

    private static BoundType boundType(ClusteringBound<?> bound)
    {
        return bound.isInclusive() ? BoundType.CLOSED : BoundType.OPEN;
    }

    protected void applyRangeTombstone(ColumnValues partitionKey, Range<ColumnValues> range)
    {
        throw invalidRequest("Range deletion is not supported by table %s", metadata);
    }

    protected void applyRowDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns)
    {
        throw invalidRequest("Row deletion is not supported by table %s", metadata);
    }

    protected void applyColumnDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns, String columnName)
    {
        throw invalidRequest("Column deletion is not supported by table %s", metadata);
    }

    protected void applyColumnUpdate(ColumnValues partitionKey,
                                     ColumnValues clusteringColumns,
                                     Optional<ColumnValue> columnValue)
    {
        throw invalidRequest("Column modification is not supported by table %s", metadata);
    }

    private static String columnName(Cell<?> cell)
    {
        return cell.column().name.toCQLString();
    }

    /**
     * A set of partition key or clustering column values.
     */
    public static final class ColumnValues implements Comparable<ColumnValues>
    {
        /**
         * An empty set of column values.
         */
        private static final ColumnValues EMPTY = new ColumnValues(ImmutableList.of(), ArrayUtils.EMPTY_OBJECT_ARRAY);

        /**
         * The column metadata for the set of columns.
         */
        private final ImmutableList<ColumnMetadata> metadata;

        /**
         * The column values. The number of values can be smaller than the number of values if only
         * a sub-set of the column values is specified (e.g. clustering prefix).
         */
        private final Object[] values;

        /**
         * Returns the set of column values corresponding to the specified partition key.
         *
         * @param metadata the table metadata
         * @param partitionKey the partition key
         * @return the set of columns values corresponding to the specified partition key
         */
        public static ColumnValues from(TableMetadata metadata, DecoratedKey partitionKey)
        {
            if (metadata.partitionKeyType instanceof CompositeType)
            {
                ByteBuffer[] buffers = ((CompositeType) metadata.partitionKeyType).split(partitionKey.getKey());
                return ColumnValues.from(metadata.partitionKeyColumns(), buffers);
            }

            return ColumnValues.from(metadata.partitionKeyColumns(), partitionKey.getKey());
        }

        /**
         * Returns the set of column values corresponding to the specified clustering prefix.
         *
         * @param metadata the table metadata
         * @param prefix the clustering prefix
         * @return the set of columns values corresponding to the specified clustering prefix
         */
        public static ColumnValues from(TableMetadata metadata, ClusteringPrefix<?> prefix)
        {
            if (prefix == Clustering.EMPTY)
                return EMPTY;

            return ColumnValues.from(metadata.clusteringColumns(), prefix.getBufferArray());
        }

        private static ColumnValues from(ImmutableList<ColumnMetadata> metadata, ByteBuffer... buffers)
        {
            return new ColumnValues(metadata, convert(metadata, buffers));
        }

        /**
         * Create a {@code ColumnValues} for the specified set of columns.
         *
         * @param metadata the partition or clustering columns metadata
         * @param values the partition or clustering column values
         */
        public ColumnValues(List<ColumnMetadata> metadata, Object... values)
        {
            this.metadata = ImmutableList.copyOf(metadata);
            this.values = values;
        }

        /**
         * Deserializes the column values.
         *
         * @param metadata the column metadata
         * @param buffers the serialized column values
         * @return the deserialized column values
         */
        private static Object[] convert(ImmutableList<ColumnMetadata> metadata, ByteBuffer[] buffers)
        {
            Object[] values = new Object[buffers.length];
            for (int i = 0; i < buffers.length; i++)
            {
                values[i] = metadata.get(i).type.compose(buffers[i]);
            }
            return values;
        }

        /**
         * Returns the name of the specified column
         *
         * @param i the column index
         * @return the column name
         */
        public String name(int i)
        {
            Preconditions.checkPositionIndex(i, values.length);
            return metadata.get(i).name.toCQLString();
        }

        /**
         * Returns the value for the specified column
         *
         * @param i the column index
         * @return the column value
         */
        @SuppressWarnings("unchecked")
        public <V> V value(int i)
        {
            Preconditions.checkPositionIndex(i, values.length);
            return (V) values[i];
        }

        /**
         * Returns the number of column values.
         *
         * @return the number of column values.
         */
        public int size()
        {
            return values.length;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            for (int i = 0, m = metadata.size(); i <m; i++)
            {
                if (i != 0)
                    builder.append(", ");

                builder.append(metadata.get(i).name.toCQLString())
                       .append(" : ");

                if (i < values.length)
                       builder.append(i < values.length ? values[i].toString() : "unspecified");
            }
            return builder.append(']').toString();
        }

        @Override
        public int compareTo(ColumnValues o)
        {
            assert metadata.equals(o.metadata);

            int s1 = size();
            int s2 = o.size();
            int minSize = Math.min(s1, s2);

            for (int i = 0; i < minSize; i++)
            {
                int cmp = compare(values[i], o.values[i]);
                if (cmp != 0)
                    return cmp;
            }

            return 0;
        }

        @SuppressWarnings("unchecked")
        private <T extends Comparable<T>> int compare(Object c1, Object c2)
        {
            return ((T) c1).compareTo((T) c2);
        }
    }

    /**
     * A regular column value.
     */
    public static final class ColumnValue
    {
        /**
         * The column metadata
         */
        private final ColumnMetadata metadata;

        /**
         * The column value
         */
        private final Object value;

        /**
         * Returns the column value corresponding to the specified cell.
         *
         * @param cell the column cell metadata
         * @return the column value corresponding to the specified cell
         */
        public static ColumnValue from(Cell<?> cell)
        {
            ColumnMetadata metadata = cell.column();
            return new ColumnValue(metadata, metadata.type.compose(cell.buffer()));
        }

        private ColumnValue(ColumnMetadata metadata, Object value)
        {
            this.metadata = metadata;
            this.value = value;
        }

        /**
         * Returns the column name.
         *
         * @return the column name
         */
        public String name()
        {
            return metadata.name.toCQLString();
        }

        /**
         * Returns the column value.
         *
         * @return the column value
         */
        @SuppressWarnings("unchecked")
        public <V> V value()
        {
            return (V) value;
        }

        @Override
        public String toString()
        {
            return String.format("%s : %s", name(), value());
        }
    }
}
