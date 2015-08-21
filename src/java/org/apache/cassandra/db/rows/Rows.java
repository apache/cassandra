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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    // TODO: we could have a that in a more generic place...
    private static final SearchIterator<ColumnDefinition, ColumnData> EMPTY_SEARCH_ITERATOR = new SearchIterator<ColumnDefinition, ColumnData>()
    {
        public boolean hasNext()
        {
            return false;
        }

        public ColumnData next(ColumnDefinition column)
        {
            return null;
        }
    };

    private Rows() {}

    public static final Row EMPTY_STATIC_ROW = BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING);

    public static Row.Builder copy(Row row, Row.Builder builder)
    {
        builder.newRow(row.clustering());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                builder.addCell((Cell)cd);
            }
            else
            {
                ComplexColumnData complexData = (ComplexColumnData)cd;
                builder.addComplexDeletion(complexData.column(), complexData.complexDeletion());
                for (Cell cell : complexData)
                    builder.addCell(cell);
            }
        }
        return builder;
    }

    /**
     * Collect statistics ont a given row.
     *
     * @param row the row for which to collect stats.
     * @param collector the stats collector.
     * @return the total number of cells in {@code row}.
     */
    public static int collectStats(Row row, PartitionStatisticsCollector collector)
    {
        assert !row.isEmpty();

        collector.update(row.primaryKeyLivenessInfo());
        collector.update(row.deletion());

        int columnCount = 0;
        int cellCount = 0;
        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                ++columnCount;
                ++cellCount;
                Cells.collectStats((Cell)cd, collector);
            }
            else
            {
                ComplexColumnData complexData = (ComplexColumnData)cd;
                collector.update(complexData.complexDeletion());
                if (complexData.hasCells())
                {
                    ++columnCount;
                    for (Cell cell : complexData)
                    {
                        ++cellCount;
                        Cells.collectStats(cell, collector);
                    }
                }
            }

        }
        collector.updateColumnSetPerRow(columnCount);
        return cellCount;
    }

    /**
     * Given the result ({@code merged}) of merging multiple {@code inputs}, signals the difference between
     * each input and {@code merged} to {@code diffListener}.
     *
     * @param merged the result of merging {@code inputs}.
     * @param columns a superset of all the columns in any of {@code merged}/{@code inputs}.
     * @param inputs the inputs whose merge yielded {@code merged}.
     * @param diffListener the listener to which to signal the differences between the inputs and the merged
     * result.
     */
    public static void diff(RowDiffListener diffListener, Row merged, Columns columns, Row...inputs)
    {
        Clustering clustering = merged.clustering();
        LivenessInfo mergedInfo = merged.primaryKeyLivenessInfo().isEmpty() ? null : merged.primaryKeyLivenessInfo();
        DeletionTime mergedDeletion = merged.deletion().isLive() ? null : merged.deletion();
        for (int i = 0; i < inputs.length; i++)
        {
            Row input = inputs[i];
            LivenessInfo inputInfo = input == null || input.primaryKeyLivenessInfo().isEmpty() ? null : input.primaryKeyLivenessInfo();
            DeletionTime inputDeletion = input == null || input.deletion().isLive() ? null : input.deletion();

            if (mergedInfo != null || inputInfo != null)
                diffListener.onPrimaryKeyLivenessInfo(i, clustering, mergedInfo, inputInfo);
            if (mergedDeletion != null || inputDeletion != null)
                diffListener.onDeletion(i, clustering, mergedDeletion, inputDeletion);
        }

        SearchIterator<ColumnDefinition, ColumnData> mergedIterator = merged.searchIterator();
        List<SearchIterator<ColumnDefinition, ColumnData>> inputIterators = new ArrayList<>(inputs.length);

        for (Row row : inputs)
            inputIterators.add(row == null ? EMPTY_SEARCH_ITERATOR : row.searchIterator());

        Iterator<ColumnDefinition> simpleColumns = columns.simpleColumns();
        while (simpleColumns.hasNext())
        {
            ColumnDefinition column = simpleColumns.next();
            Cell mergedCell = (Cell)mergedIterator.next(column);
            for (int i = 0; i < inputs.length; i++)
            {
                Cell inputCell = (Cell)inputIterators.get(i).next(column);
                if (mergedCell != null || inputCell != null)
                    diffListener.onCell(i, clustering, mergedCell, inputCell);
            }
        }

        Iterator<ColumnDefinition> complexColumns = columns.complexColumns();
        while (complexColumns.hasNext())
        {
            ColumnDefinition column = complexColumns.next();
            ComplexColumnData mergedData = (ComplexColumnData)mergedIterator.next(column);
            // Doing one input at a time is not the most efficient, but it's a lot simpler for now
            for (int i = 0; i < inputs.length; i++)
            {
                ComplexColumnData inputData = (ComplexColumnData)inputIterators.get(i).next(column);
                if (mergedData == null)
                {
                    if (inputData == null)
                        continue;

                    // Everything in inputData has been shadowed
                    if (!inputData.complexDeletion().isLive())
                        diffListener.onComplexDeletion(i, clustering, column, null, inputData.complexDeletion());
                    for (Cell inputCell : inputData)
                        diffListener.onCell(i, clustering, null, inputCell);
                }
                else if (inputData == null)
                {
                    // Everything in inputData is new
                    if (!mergedData.complexDeletion().isLive())
                        diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), null);
                    for (Cell mergedCell : mergedData)
                        diffListener.onCell(i, clustering, mergedCell, null);
                }
                else
                {
                    PeekingIterator<Cell> mergedCells = Iterators.peekingIterator(mergedData.iterator());
                    PeekingIterator<Cell> inputCells = Iterators.peekingIterator(inputData.iterator());
                    while (mergedCells.hasNext() && inputCells.hasNext())
                    {
                        int cmp = column.cellPathComparator().compare(mergedCells.peek().path(), inputCells.peek().path());
                        if (cmp == 0)
                            diffListener.onCell(i, clustering, mergedCells.next(), inputCells.next());
                        else if (cmp < 0)
                            diffListener.onCell(i, clustering, mergedCells.next(), null);
                        else // cmp > 0
                            diffListener.onCell(i, clustering, null, inputCells.next());
                    }
                    while (mergedCells.hasNext())
                        diffListener.onCell(i, clustering, mergedCells.next(), null);
                    while (inputCells.hasNext())
                        diffListener.onCell(i, clustering, null, inputCells.next());
                }
            }
        }
    }

    public static Row merge(Row row1, Row row2, int nowInSec)
    {
        Columns mergedColumns = row1.columns().mergeTo(row2.columns());
        Row.Builder builder = BTreeRow.sortedBuilder(mergedColumns);
        merge(row1, row2, mergedColumns, builder, nowInSec);
        return builder.build();
    }

    // Merge rows in memtable
    // Return the minimum timestamp delta between existing and update
    public static long merge(Row existing,
                             Row update,
                             Columns mergedColumns,
                             Row.Builder builder,
                             int nowInSec)
    {
        Clustering clustering = existing.clustering();
        builder.newRow(clustering);

        LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
        LivenessInfo updateInfo = update.primaryKeyLivenessInfo();
        LivenessInfo mergedInfo = existingInfo.supersedes(updateInfo) ? existingInfo : updateInfo;

        long timeDelta = Math.abs(existingInfo.timestamp() - mergedInfo.timestamp());

        DeletionTime deletion = existing.deletion().supersedes(update.deletion()) ? existing.deletion() : update.deletion();

        if (deletion.deletes(mergedInfo))
            mergedInfo = LivenessInfo.EMPTY;

        builder.addPrimaryKeyLivenessInfo(mergedInfo);
        builder.addRowDeletion(deletion);

        for (int i = 0; i < mergedColumns.simpleColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getSimple(i);
            Cell existingCell = existing.getCell(c);
            Cell updateCell = update.getCell(c);
            timeDelta = Math.min(timeDelta, Cells.reconcile(existingCell,
                                                            updateCell,
                                                            deletion,
                                                            builder,
                                                            nowInSec));
        }

        for (int i = 0; i < mergedColumns.complexColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getComplex(i);
            ComplexColumnData existingData = existing.getComplexColumnData(c);
            ComplexColumnData updateData = update.getComplexColumnData(c);

            DeletionTime existingDt = existingData == null ? DeletionTime.LIVE : existingData.complexDeletion();
            DeletionTime updateDt = updateData == null ? DeletionTime.LIVE : updateData.complexDeletion();
            DeletionTime maxDt = existingDt.supersedes(updateDt) ? existingDt : updateDt;
            if (maxDt.supersedes(deletion))
                builder.addComplexDeletion(c, maxDt);
            else
                maxDt = deletion;

            Iterator<Cell> existingCells = existingData == null ? null : existingData.iterator();
            Iterator<Cell> updateCells = updateData == null ? null : updateData.iterator();
            timeDelta = Math.min(timeDelta, Cells.reconcileComplex(c, existingCells, updateCells, maxDt, builder, nowInSec));
        }

        return timeDelta;
    }
}
