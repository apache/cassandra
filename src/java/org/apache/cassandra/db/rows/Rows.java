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

import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.WrappedInt;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
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
     * Creates a new simple row builder.
     *
     * @param metadata the metadata of the table this is a row of.
     * @param clusteringValues the value for the clustering columns of the row to add to this build. There may be no
     * values if either the table has no clustering column, or if you want to edit the static row. Note that as a
     * shortcut it is also allowed to pass a {@code Clustering} object directly, in which case that should be the
     * only argument.
     * @return a newly created builder.
     */
    public static Row.SimpleBuilder simpleBuilder(CFMetaData metadata, Object... clusteringValues)
    {
        return new SimpleBuilders.RowBuilder(metadata, clusteringValues);
    }

    /**
     * Collect statistics on a given row.
     *
     * @param row the row for which to collect stats.
     * @param collector the stats collector.
     * @return the total number of cells in {@code row}.
     */
    public static int collectStats(Row row, PartitionStatisticsCollector collector)
    {
        assert !row.isEmpty();

        collector.update(row.primaryKeyLivenessInfo());
        collector.update(row.deletion().time());

        //we have to wrap these for the lambda
        final WrappedInt columnCount = new WrappedInt(0);
        final WrappedInt cellCount = new WrappedInt(0);

        row.apply(cd -> {
            if (cd.column().isSimple())
            {
                columnCount.increment();
                cellCount.increment();
                Cells.collectStats((Cell) cd, collector);
            }
            else
            {
                ComplexColumnData complexData = (ComplexColumnData)cd;
                collector.update(complexData.complexDeletion());
                if (complexData.hasCells())
                {
                    columnCount.increment();
                    for (Cell cell : complexData)
                    {
                        cellCount.increment();
                        Cells.collectStats(cell, collector);
                    }
                }
            }
        }, false);

        collector.updateColumnSetPerRow(columnCount.get());
        return cellCount.get();
    }

    /**
     * Given the result ({@code merged}) of merging multiple {@code inputs}, signals the difference between
     * each input and {@code merged} to {@code diffListener}.
     * <p>
     * Note that this method doesn't only emit cells etc where there's a difference. The listener is informed
     * of every corresponding entity between the merged and input rows, including those that are equal.
     *
     * @param diffListener the listener to which to signal the differences between the inputs and the merged result.
     * @param merged the result of merging {@code inputs}.
     * @param inputs the inputs whose merge yielded {@code merged}.
     */
    public static void diff(RowDiffListener diffListener, Row merged, Row...inputs)
    {
        Clustering clustering = merged.clustering();
        LivenessInfo mergedInfo = merged.primaryKeyLivenessInfo().isEmpty() ? null : merged.primaryKeyLivenessInfo();
        Row.Deletion mergedDeletion = merged.deletion().isLive() ? null : merged.deletion();
        for (int i = 0; i < inputs.length; i++)
        {
            Row input = inputs[i];
            LivenessInfo inputInfo = input == null || input.primaryKeyLivenessInfo().isEmpty() ? null : input.primaryKeyLivenessInfo();
            Row.Deletion inputDeletion = input == null || input.deletion().isLive() ? null : input.deletion();

            if (mergedInfo != null || inputInfo != null)
                diffListener.onPrimaryKeyLivenessInfo(i, clustering, mergedInfo, inputInfo);
            if (mergedDeletion != null || inputDeletion != null)
                diffListener.onDeletion(i, clustering, mergedDeletion, inputDeletion);
        }

        List<Iterator<ColumnData>> inputIterators = new ArrayList<>(1 + inputs.length);
        inputIterators.add(merged.iterator());
        for (Row row : inputs)
            inputIterators.add(row == null ? Collections.emptyIterator() : row.iterator());

        Iterator<?> iter = MergeIterator.get(inputIterators, ColumnData.comparator, new MergeIterator.Reducer<ColumnData, Object>()
        {
            ColumnData mergedData;
            ColumnData[] inputDatas = new ColumnData[inputs.length];
            public void reduce(int idx, ColumnData current)
            {
                if (idx == 0)
                    mergedData = current;
                else
                    inputDatas[idx - 1] = current;
            }

            protected Object getReduced()
            {
                for (int i = 0 ; i != inputDatas.length ; i++)
                {
                    ColumnData input = inputDatas[i];
                    if (mergedData != null || input != null)
                    {
                        ColumnDefinition column = (mergedData != null ? mergedData : input).column;
                        if (column.isSimple())
                        {
                            diffListener.onCell(i, clustering, (Cell) mergedData, (Cell) input);
                        }
                        else
                        {
                            ComplexColumnData mergedData = (ComplexColumnData) this.mergedData;
                            ComplexColumnData inputData = (ComplexColumnData) input;
                            if (mergedData == null)
                            {
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

                                if (!mergedData.complexDeletion().isLive() || !inputData.complexDeletion().isLive())
                                    diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), inputData.complexDeletion());

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
                return null;
            }

            protected void onKeyChange()
            {
                mergedData = null;
                Arrays.fill(inputDatas, null);
            }
        });

        while (iter.hasNext())
            iter.next();
    }

    public static Row merge(Row row1, Row row2, int nowInSec)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        merge(row1, row2, builder, nowInSec);
        return builder.build();
    }

    /**
     * Merges two rows into the given builder, mainly for merging memtable rows. In addition to reconciling the cells
     * in each row, the liveness info, and deletion times for the row and complex columns are also merged.
     * <p>
     * Note that this method assumes that the provided rows can meaningfully be reconciled together. That is,
     * that the rows share the same clustering value, and belong to the same partition.
     *
     * @param existing
     * @param update
     * @param builder the row build to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     *
     * @return the smallest timestamp delta between corresponding rows from existing and update. A
     * timestamp delta being computed as the difference between the cells and DeletionTimes from {@code existing}
     * and those in {@code existing}.
     */
    public static long merge(Row existing,
                             Row update,
                             Row.Builder builder,
                             int nowInSec)
    {
        Clustering clustering = existing.clustering();
        builder.newRow(clustering);

        LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
        LivenessInfo updateInfo = update.primaryKeyLivenessInfo();
        LivenessInfo mergedInfo = existingInfo.supersedes(updateInfo) ? existingInfo : updateInfo;

        long timeDelta = Math.abs(existingInfo.timestamp() - mergedInfo.timestamp());

        Row.Deletion rowDeletion = existing.deletion().supersedes(update.deletion()) ? existing.deletion() : update.deletion();

        if (rowDeletion.deletes(mergedInfo))
            mergedInfo = LivenessInfo.EMPTY;
        else if (rowDeletion.isShadowedBy(mergedInfo))
            rowDeletion = Row.Deletion.LIVE;

        builder.addPrimaryKeyLivenessInfo(mergedInfo);
        builder.addRowDeletion(rowDeletion);

        DeletionTime deletion = rowDeletion.time();

        Iterator<ColumnData> a = existing.iterator();
        Iterator<ColumnData> b = update.iterator();
        ColumnData nexta = a.hasNext() ? a.next() : null, nextb = b.hasNext() ? b.next() : null;
        while (nexta != null | nextb != null)
        {
            int comparison = nexta == null ? 1 : nextb == null ? -1 : nexta.column.compareTo(nextb.column);
            ColumnData cura = comparison <= 0 ? nexta : null;
            ColumnData curb = comparison >= 0 ? nextb : null;
            ColumnDefinition column = getColumnDefinition(cura, curb);

            if (column.isSimple())
            {
                timeDelta = Math.min(timeDelta, Cells.reconcile((Cell) cura, (Cell) curb, deletion, builder, nowInSec));
            }
            else
            {
                ComplexColumnData existingData = (ComplexColumnData) cura;
                ComplexColumnData updateData = (ComplexColumnData) curb;

                DeletionTime existingDt = existingData == null ? DeletionTime.LIVE : existingData.complexDeletion();
                DeletionTime updateDt = updateData == null ? DeletionTime.LIVE : updateData.complexDeletion();
                DeletionTime maxDt = existingDt.supersedes(updateDt) ? existingDt : updateDt;
                if (maxDt.supersedes(deletion))
                    builder.addComplexDeletion(column, maxDt);
                else
                    maxDt = deletion;

                Iterator<Cell> existingCells = existingData == null ? null : existingData.iterator();
                Iterator<Cell> updateCells = updateData == null ? null : updateData.iterator();
                timeDelta = Math.min(timeDelta, Cells.reconcileComplex(column, existingCells, updateCells, maxDt, builder, nowInSec));
            }

            if (cura != null)
                nexta = a.hasNext() ? a.next() : null;
            if (curb != null)
                nextb = b.hasNext() ? b.next() : null;
        }
        return timeDelta;
    }

    /**
     * Returns a row that is obtained from the given existing row by removing everything that is shadowed by data in
     * the update row. In other words, produces the smallest result row such that
     * {@code merge(result, update, nowInSec) == merge(existing, update, nowInSec)} after filtering by rangeDeletion.
     *
     * @param existing source row
     * @param update shadowing row
     * @param rangeDeletion extra {@code DeletionTime} from covering tombstone
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     */
    public static Row removeShadowedCells(Row existing, Row update, DeletionTime rangeDeletion, int nowInSec)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        Clustering clustering = existing.clustering();
        builder.newRow(clustering);

        DeletionTime deletion = update.deletion().time();
        if (rangeDeletion.supersedes(deletion))
            deletion = rangeDeletion;

        LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
        if (!deletion.deletes(existingInfo))
            builder.addPrimaryKeyLivenessInfo(existingInfo);
        Row.Deletion rowDeletion = existing.deletion();
        if (!deletion.supersedes(rowDeletion.time()))
            builder.addRowDeletion(rowDeletion);

        Iterator<ColumnData> a = existing.iterator();
        Iterator<ColumnData> b = update.iterator();
        ColumnData nexta = a.hasNext() ? a.next() : null, nextb = b.hasNext() ? b.next() : null;
        while (nexta != null)
        {
            int comparison = nextb == null ? -1 : nexta.column.compareTo(nextb.column);
            if (comparison <= 0)
            {
                ColumnData cura = nexta;
                ColumnDefinition column = cura.column;
                ColumnData curb = comparison == 0 ? nextb : null;
                if (column.isSimple())
                {
                    Cells.addNonShadowed((Cell) cura, (Cell) curb, deletion, builder, nowInSec);
                }
                else
                {
                    ComplexColumnData existingData = (ComplexColumnData) cura;
                    ComplexColumnData updateData = (ComplexColumnData) curb;

                    DeletionTime existingDt = existingData.complexDeletion();
                    DeletionTime updateDt = updateData == null ? DeletionTime.LIVE : updateData.complexDeletion();

                    DeletionTime maxDt = updateDt.supersedes(deletion) ? updateDt : deletion;
                    if (existingDt.supersedes(maxDt))
                    {
                        builder.addComplexDeletion(column, existingDt);
                        maxDt = existingDt;
                    }

                    Iterator<Cell> existingCells = existingData.iterator();
                    Iterator<Cell> updateCells = updateData == null ? null : updateData.iterator();
                    Cells.addNonShadowedComplex(column, existingCells, updateCells, maxDt, builder, nowInSec);
                }
                nexta = a.hasNext() ? a.next() : null;
                if (curb != null)
                    nextb = b.hasNext() ? b.next() : null;
            }
            else
            {
                nextb = b.hasNext() ? b.next() : null;
            }
        }
        Row row = builder.build();
        return row != null && !row.isEmpty() ? row : null;
    }

    /**
     * Returns the {@code ColumnDefinition} to use for merging the columns.
     * If the 2 column definitions are different the latest one will be returned.
     */
    private static ColumnDefinition getColumnDefinition(ColumnData cura, ColumnData curb)
    {
        if (cura == null)
            return curb.column;

        if (curb == null)
            return cura.column;

        if (AbstractTypeVersionComparator.INSTANCE.compare(cura.column.type, curb.column.type) >= 0)
            return cura.column;

        return curb.column;
    }
}
