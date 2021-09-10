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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * An abstract virtual table implementation that builds the resultset on demand and allows fine-grained source
 * modification via INSERT/UPDATE, DELETE and TRUNCATE operations.
 */
public abstract class AbstractMutableVirtualTable extends AbstractVirtualTable
{

    protected AbstractMutableVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        Object[] partitionKeyColumnValues = extractPartitionKeyColumnValues(update.partitionKey());

        if (update.deletionInfo().isLive())
            update.forEach(row ->
            {
                Comparable<?>[] clusteringColumnValues = extractClusteringColumnValues(row.clustering());

                if (row.deletion().isLive())
                {
                    if (row.columnCount() == 0)
                        applyRowWithoutRegularColumnsInsertion(partitionKeyColumnValues, clusteringColumnValues);
                     else
                     {
                        row.forEach(columnData ->
                        {
                            checkFalse(columnData.column().isComplex(), "Complex type columns are not supported by table " + metadata);

                            Cell<?> cell = (Cell<?>) columnData;
                            String columnName = extractColumnName(cell);

                            if (cell.isTombstone())
                                applyColumnDeletion(partitionKeyColumnValues, clusteringColumnValues, columnName);
                            else
                                applyColumnUpdate(partitionKeyColumnValues,
                                        clusteringColumnValues,
                                        columnName,
                                        extractColumnValue(cell));
                        });
                    }
                }
                else
                    applyRowDeletion(partitionKeyColumnValues, clusteringColumnValues);
            });
        else
        {
            // MutableDeletionInfo may have partition delete or range tombstone list or both
            if (update.deletionInfo().hasRanges())
                update.deletionInfo()
                        .rangeIterator(false)
                        .forEachRemaining(rt -> convertAndApplyRangeTombstone(partitionKeyColumnValues, rt.deletedSlice()));

            if (!update.deletionInfo().getPartitionDeletion().isLive())
                applyPartitionDeletion(partitionKeyColumnValues);
        }
    }

    protected void applyPartitionDeletion(Object[] partitionKeyColumnValues)
    {
        throw new InvalidRequestException("Partition deletion is not supported by table " + metadata);

    }

    private void convertAndApplyRangeTombstone(Object[] partitionKeyColumnValues, Slice slice)
    {
        ClusteringBound<?> startClusteringColumns = slice.start();
        Comparable<?>[] startClusteringColumnValues = extractClusteringColumnValues(startClusteringColumns);
        BoundType startClusteringColumnBoundType = startClusteringColumns.isInclusive() ? BoundType.CLOSED : BoundType.OPEN;

        ClusteringBound<?> endClusteringColumns = slice.end();
        Comparable<?>[] endClusteringColumnValues = extractClusteringColumnValues(endClusteringColumns);
        BoundType endClusteringColumnBoundType = endClusteringColumns.isInclusive() ? BoundType.CLOSED : BoundType.OPEN;

        int clusteringColumnsPrefixLength = Math.max(startClusteringColumnValues.length, endClusteringColumnValues.length) - 1;
        Comparable<?>[] clusteringColumnValuesPrefix = new Comparable<?>[clusteringColumnsPrefixLength];
        System.arraycopy(startClusteringColumnValues, 0, clusteringColumnValuesPrefix, 0, clusteringColumnsPrefixLength);

        Range<Comparable<?>> range;
        if (startClusteringColumnValues.length < endClusteringColumnValues.length)
            range = Range.upTo(endClusteringColumnValues[endClusteringColumnValues.length - 1], endClusteringColumnBoundType);
        else if (startClusteringColumnValues.length > endClusteringColumnValues.length)
            range = Range.downTo(startClusteringColumnValues[startClusteringColumnValues.length - 1], startClusteringColumnBoundType);
        else
            range = Range.range(startClusteringColumnValues[startClusteringColumnValues.length - 1], startClusteringColumnBoundType,
                    endClusteringColumnValues[endClusteringColumnValues.length - 1], endClusteringColumnBoundType);

        applyRangeTombstone(partitionKeyColumnValues, clusteringColumnValuesPrefix, range);
    }

    /**
     * This method accepts parsed parts of a corresponding range tombstone. There is a tricky logic for
     * {@code clusteringColumnValuesPrefix} calculation. It consists of clustering columns that have equality condition.
     * It is worth mentioning that it is only possible to have clusering columns from first levels. For example, if
     * there are three clustering columns specified in the range combstone: {@code c1='c1_1' AND c2='c2_1' AND c3>'c3_1'},
     * then {@code clusteringColumnValuesPrefix} will have ["c1_1", "c2_1"] values. In case of a single clustering column
     * the prefix will be empty.
     *
     * @param partitionKeyColumnValues is a non-empty array of partition key columns
     * @param clusteringColumnValuesPrefix is an array (it may be empty!) of clustering columns with equality condition
     * @param range is a range of values for the last clustering column
     */
    protected void applyRangeTombstone(Object[] partitionKeyColumnValues,
                                       Comparable<?>[] clusteringColumnValuesPrefix,
                                       Range<Comparable<?>> range)
    {
        throw new InvalidRequestException("Range deletion is not supported by table " + metadata);
    }

    protected void applyRowWithoutRegularColumnsInsertion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues)
    {
        throw new InvalidRequestException("Row insertion is not supported by table " + metadata);
    }

    protected void applyRowDeletion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues)
    {
        throw new InvalidRequestException("Row deletion is not supported by table " + metadata);
    }

    protected void applyColumnDeletion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues, String columnName)
    {
        throw new InvalidRequestException("Column deletion is not supported by table " + metadata);
    }

    protected void applyColumnUpdate(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues,
                                              String columnName, Object columnValue)
    {
        throw new InvalidRequestException("Column modification is not supported by table " + metadata);
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

    private Comparable<?>[] extractClusteringColumnValues(ClusteringPrefix<?> clusteringColumns)
    {
        // clusteringColumns.size() may be less than metadata.clusteringColumns().size() since not all clustering
        // columns have to be always specified
        Comparable<?>[] clusteringColumnValues = new Comparable<?>[clusteringColumns.size()];
        for (int i = 0; i < clusteringColumnValues.length; i++)
        {
            Object clusteringColumnValue = metadata.clusteringColumns().get(i).type.compose(clusteringColumns.bufferAt(i));
            checkTrue(clusteringColumnValue instanceof Comparable, "Non-comparable types are not supported as clustering columns by table " + metadata);
            clusteringColumnValues[i] = (Comparable<?>) clusteringColumnValue;
        }
        return clusteringColumnValues;
    }

    private String extractColumnName(Cell<?> cell)
    {
        return cell.column().name.toCQLString();
    }

    private Object extractColumnValue(Cell<?> cell)
    {
        return cell.column().cellValueType().compose(cell.buffer());
    }
}
