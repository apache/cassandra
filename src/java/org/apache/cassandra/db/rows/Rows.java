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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private static final Logger logger = LoggerFactory.getLogger(Rows.class);

    private Rows() {}

    public static final Row EMPTY_STATIC_ROW = new AbstractRow()
    {
        public Columns columns()
        {
            return Columns.NONE;
        }

        public LivenessInfo primaryKeyLivenessInfo()
        {
            return LivenessInfo.NONE;
        }

        public DeletionTime deletion()
        {
            return DeletionTime.LIVE;
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean hasComplexDeletion()
        {
            return false;
        }

        public Clustering clustering()
        {
            return Clustering.STATIC_CLUSTERING;
        }

        public Cell getCell(ColumnDefinition c)
        {
            return null;
        }

        public Cell getCell(ColumnDefinition c, CellPath path)
        {
            return null;
        }

        public Iterator<Cell> getCells(ColumnDefinition c)
        {
            return null;
        }

        public DeletionTime getDeletion(ColumnDefinition c)
        {
            return DeletionTime.LIVE;
        }

        public Iterator<Cell> iterator()
        {
            return Iterators.<Cell>emptyIterator();
        }

        public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
        {
            return new SearchIterator<ColumnDefinition, ColumnData>()
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
        }

        public Kind kind()
        {
            return Unfiltered.Kind.ROW;
        }

        public Row takeAlias()
        {
            return this;
        }
    };

    public interface SimpleMergeListener
    {
        public void onAdded(Cell newCell);
        public void onRemoved(Cell removedCell);
        public void onUpdated(Cell existingCell, Cell updatedCell);
    }

    public static void writeClustering(Clustering clustering, Row.Writer writer)
    {
        for (int i = 0; i < clustering.size(); i++)
            writer.writeClusteringValue(clustering.get(i));
    }

    public static void merge(Row row1, Row row2, Columns mergedColumns, Row.Writer writer, int nowInSec)
    {
        merge(row1, row2, mergedColumns, writer, nowInSec, SecondaryIndexManager.nullUpdater);
    }

    // Merge rows in memtable
    // Return the minimum timestamp delta between existing and update
    public static long merge(Row existing,
                             Row update,
                             Columns mergedColumns,
                             Row.Writer writer,
                             int nowInSec,
                             SecondaryIndexManager.Updater indexUpdater)
    {
        Clustering clustering = existing.clustering();
        writeClustering(clustering, writer);

        LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
        LivenessInfo updateInfo = update.primaryKeyLivenessInfo();
        LivenessInfo mergedInfo = existingInfo.mergeWith(updateInfo);

        long timeDelta = Math.abs(existingInfo.timestamp() - mergedInfo.timestamp());

        DeletionTime deletion = existing.deletion().supersedes(update.deletion()) ? existing.deletion() : update.deletion();

        if (deletion.deletes(mergedInfo))
            mergedInfo = LivenessInfo.NONE;

        writer.writePartitionKeyLivenessInfo(mergedInfo);
        writer.writeRowDeletion(deletion);

        indexUpdater.maybeIndex(clustering, mergedInfo.timestamp(), mergedInfo.ttl(), deletion);

        for (int i = 0; i < mergedColumns.simpleColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getSimple(i);
            Cell existingCell = existing.getCell(c);
            Cell updateCell = update.getCell(c);
            timeDelta = Math.min(timeDelta, Cells.reconcile(clustering,
                                                            existingCell,
                                                            updateCell,
                                                            deletion,
                                                            writer,
                                                            nowInSec,
                                                            indexUpdater));
        }

        for (int i = 0; i < mergedColumns.complexColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getComplex(i);
            DeletionTime existingDt = existing.getDeletion(c);
            DeletionTime updateDt = update.getDeletion(c);
            DeletionTime maxDt = existingDt.supersedes(updateDt) ? existingDt : updateDt;
            if (maxDt.supersedes(deletion))
                writer.writeComplexDeletion(c, maxDt);
            else
                maxDt = deletion;

            Iterator<Cell> existingCells = existing.getCells(c);
            Iterator<Cell> updateCells = update.getCells(c);
            timeDelta = Math.min(timeDelta, Cells.reconcileComplex(clustering, c, existingCells, updateCells, maxDt, writer, nowInSec, indexUpdater));
        }

        writer.endOfRow();
        return timeDelta;
    }
}
