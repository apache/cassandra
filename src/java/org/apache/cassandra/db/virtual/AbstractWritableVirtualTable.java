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
import java.util.SortedMap;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An abstract virtual table implementation that builds the resultset on demand and allows fine-grained source modification.
 */
public abstract class AbstractWritableVirtualTable extends AbstractVirtualTable
{

    protected AbstractWritableVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        DecoratedKey partitionKey = update.partitionKey();

        if (update.deletionInfo().isLive())
            update.forEach(row ->
            {
                Clustering<?> clusteringColumns = row.clustering();

                if (row.deletion().isLive())
                    row.forEach(columnMetadata ->
                    {
                        if (columnMetadata.column().isComplex())
                            throw new InvalidRequestException("Complex type column deletes are not supported by table " + metadata);

                        Cell<?> cell = (Cell<?>) columnMetadata;

                        if (cell.isTombstone())
                            applyColumnDelete(partitionKey, clusteringColumns, cell);
                        else
                            applyColumnUpdate(partitionKey, clusteringColumns, cell);
                    });
                else
                    applyRowDelete(partitionKey, clusteringColumns);
            });
        else
        {
            // MutableDeletionInfo may have partition delete or range tombstone list or both
            if (update.deletionInfo().hasRanges())
                update.deletionInfo()
                        .rangeIterator(false)
                        .forEachRemaining(rt -> applyRangeTombstone(partitionKey, rt.deletedSlice()));

            if (!update.deletionInfo().getPartitionDeletion().isLive())
                applyPartitionDelete(partitionKey);
        }
    }

    protected void applyPartitionDelete(DecoratedKey partitionKey)
    {
        throw new InvalidRequestException("Partition deletion is not supported by table " + metadata);
    }

    protected void applyRangeTombstone(DecoratedKey partitionKey, Slice slice)
    {
        throw new InvalidRequestException("Range deletion is not supported by table " + metadata);
    }

    protected void applyRowDelete(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns)
    {
        throw new InvalidRequestException("Row deletion is not supported by table " + metadata);
    }

    protected void applyColumnDelete(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns, Cell<?> cell)
    {
        throw new InvalidRequestException("Column deletion is not supported by table " + metadata);
    }

    protected abstract void applyColumnUpdate(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns, Cell<?> cell);

    public static abstract class SimpleWritableVirtualTable extends AbstractWritableVirtualTable {

        protected SimpleWritableVirtualTable(TableMetadata metadata)
        {
            super(metadata);
        }

        @Override
        protected void applyPartitionDelete(DecoratedKey partitionKey)
        {
            applyPartitionDelete(extractPartitionKeyColumnValues(partitionKey));
        }

        protected void applyPartitionDelete(Object[] partitionKeyColumnValues)
        {
            throw new InvalidRequestException("Partition deletion is not supported by table " + metadata);

        }

        @Override
        protected void applyRangeTombstone(DecoratedKey partitionKey, Slice slice)
        {
            ClusteringBound<?> startClusteringColumns = slice.start();
            Object[] startClusteringColumnValues = extractClusteringColumnValues(startClusteringColumns);

            ClusteringBound<?> endClusteringColumns = slice.end();
            Object[] endClusteringColumnValues = extractClusteringColumnValues(endClusteringColumns);

            // It is a prefix of clustering columns that have equal condition. For example, if there are two clustering
            // columns c1 and c2, then it will have c1. In case of a single clustering column the prefix is empty.
            int clusteringColumnsPrefixLength = Math.max(startClusteringColumnValues.length, endClusteringColumnValues.length) - 1;
            Object[] clusteringColumnValuesPrefix = new Object[clusteringColumnsPrefixLength];
            System.arraycopy(startClusteringColumnValues, 0, clusteringColumnValuesPrefix, 0, clusteringColumnsPrefixLength);

            Object startClusteringColumnValue = startClusteringColumns.isBottom()
                    ? null : startClusteringColumnValues[startClusteringColumnValues.length - 1];
            boolean isStartClusteringColumnInclusive = startClusteringColumns.isInclusive();

            Object endClusteringColumnValue = endClusteringColumns.isBottom()
                    ? null : endClusteringColumnValues[endClusteringColumnValues.length - 1];
            boolean isEndClusteringColumnInclusive = endClusteringColumns.isInclusive();

            applyRangeTombstone(extractPartitionKeyColumnValues(partitionKey),
                    clusteringColumnValuesPrefix,
                    startClusteringColumnValue,
                    isStartClusteringColumnInclusive,
                    endClusteringColumnValue,
                    isEndClusteringColumnInclusive);
        }

        /**
         * This method is called for every range tombstone.
         *
         * @param partitionKeyColumnValues is non-empty
         * @param clusteringColumnValuesPrefix is empty if there is a single clustering column
         * @param startClusteringColumnValue is null if there is no "gt" or "gte" condition on the clustering column
         * @param isStartClusteringColumnInclusive distinguishes "gt" and "gte" conditions
         * @param endClusteringColumnValue is null if there is no "lt" or "lte" condition on the clustering column
         * @param isEndClusteringColumnInclusive distinguishes "lt" and "lte" conditions
         */
        protected void applyRangeTombstone(Object[] partitionKeyColumnValues,
                                           Object[] clusteringColumnValuesPrefix,
                                           @Nullable Object startClusteringColumnValue,
                                           boolean isStartClusteringColumnInclusive,
                                           @Nullable Object endClusteringColumnValue,
                                           boolean isEndClusteringColumnInclusive)
        {
            throw new InvalidRequestException("Range deletion is not supported by table " + metadata);
        }


        @Override
        protected void applyRowDelete(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns)
        {
            applyRowDelete(extractPartitionKeyColumnValues(partitionKey), extractClusteringColumnValues(clusteringColumns));
        }

        protected void applyRowDelete(Object[] partitionKeyColumnValues, Object[] clusteringColumnValues)
        {
            throw new InvalidRequestException("Row deletion is not supported by table " + metadata);
        }

        @Override
        protected void applyColumnDelete(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns, Cell<?> cell)
        {
            applyColumnDelete(extractPartitionKeyColumnValues(partitionKey),
                    extractClusteringColumnValues(clusteringColumns),
                    extractColumnName(cell));
        }

        protected void applyColumnDelete(Object[] partitionKeyColumnValues, Object[] clusteringColumnValues, String columnName)
        {
            throw new InvalidRequestException("Column deletion is not supported by table " + metadata);
        }

        @Override
        protected void applyColumnUpdate(DecoratedKey partitionKey, ClusteringPrefix<?> clusteringColumns, Cell<?> cell)
        {
            applyColumnUpdate(extractPartitionKeyColumnValues(partitionKey),
                    extractClusteringColumnValues(clusteringColumns),
                    extractColumnName(cell),
                    extractColumnValue(cell));
        }

        protected abstract void applyColumnUpdate(Object[] partitionKeyColumnValues, Object[] clusteringColumnValues,
                                                  String columnName, Object columnValue);

        protected <V> void filterStringKeySortedMap(SortedMap<String, V> clusteringColumnsMap,
                                                    @Nullable String startClusteringColumnValue,
                                                    boolean isStartClusteringColumnInclusive,
                                                    @Nullable String endClusteringColumnValue,
                                                    boolean isEndClusteringColumnInclusive)
        {
            V firstValuePair = startClusteringColumnValue != null
                    ? clusteringColumnsMap.get(startClusteringColumnValue) : null;

            if (startClusteringColumnValue != null && endClusteringColumnValue != null)
            {
                // remove values for startClusteringColumnValue <= c < endClusteringColumnValue range
                clusteringColumnsMap.subMap(startClusteringColumnValue, endClusteringColumnValue).clear();
            }
            else if (endClusteringColumnValue == null)
            {
                // remove values for c <= startClusteringColumnValue range
                clusteringColumnsMap.tailMap(startClusteringColumnValue).clear();
            }
            else if (startClusteringColumnValue == null)
            {
                // remove values for endClusteringColumnValue < c range
                clusteringColumnsMap.headMap(endClusteringColumnValue).clear();
            }

            // tailMap and subMap are inclusive for start key and we explicitly put start value back if needed
            if (firstValuePair != null && !isStartClusteringColumnInclusive)
                clusteringColumnsMap.put(startClusteringColumnValue, firstValuePair);

            // headMap and subMap are exclusive for end key and we explicitly remove start value if needed
            if (isEndClusteringColumnInclusive)
                clusteringColumnsMap.remove(endClusteringColumnValue);
        }

        private Object[] extractPartitionKeyColumnValues(DecoratedKey partitionKey)
        {
            if (metadata.partitionKeyType instanceof CompositeType)
            {
                ByteBuffer[] partitionKeyColumnBytes = ((CompositeType) metadata.partitionKeyType).split(partitionKey.getKey());
                Object[] partitionKeyColumnValues = new Object[partitionKeyColumnBytes.length];
                for (int i = 0; i < partitionKeyColumnValues.length; i++)
                {
                    partitionKeyColumnValues[i] = metadata.partitionKeyColumns().get(i).type.compose(partitionKeyColumnBytes[i]);
                }
                return partitionKeyColumnValues;

            }
            else
                return new Object[]{metadata.partitionKeyType.compose(partitionKey.getKey())};
        }

        private Object[] extractClusteringColumnValues(ClusteringPrefix<?> clusteringColumns)
        {
            // clusteringColumns.size() may be less than metadata.clusteringColumns().size() since not all clustering
            // columns have to be always specified
            Object[] clusteringColumnValues = new Object[clusteringColumns.size()];
            for (int i = 0; i < clusteringColumnValues.length; i++)
            {
                clusteringColumnValues[i] = metadata.clusteringColumns().get(i).type.compose(clusteringColumns.bufferAt(i));
            }
            return clusteringColumnValues;
        }

        private String extractColumnName(Cell<?> cell)
        {
            return cell.column().name.toCQLString();
        }

        private Object extractColumnValue(Cell<?> cell)
        {
            return metadata.getColumn(cell.column().name).cellValueType().compose(cell.buffer());
        }
    }
}
