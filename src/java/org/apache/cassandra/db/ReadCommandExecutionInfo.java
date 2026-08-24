/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.Index;

/**
 * A custom {@link Monitorable.ExecutionInfo} implementation for {@link ReadCommand}, to be used unless there is an
 * {@link Index.Searcher} with its own custom implementation.
 * </p>
 * It holds and prints the number of partitions, rows and tombstones fetched and returned by the command.
 * </p>
 * Deleted partitions are considered as a partition tombstone.
 * Deleted rows and range tombstone markers are considered as row tombstones.
 */
@NotThreadSafe
class ReadCommandExecutionInfo implements Monitorable.ExecutionInfo
{
    private long partitionsFetched = 0;
    private long partitionsReturned = 0;
    private long partitionTombstones = 0;
    private long rowsFetched = 0;
    private long rowsReturned = 0;
    private long rowTombstones = 0;
    private long cellsFetched = 0;
    private long cellsReturned = 0;

    /**
     * Counts the number of fetched partitions and rows in the specified iterator.
     *
     * @param partitions the iterator of fetched partitions to count
     * @param nowInSec the command's time in seconds, used to evaluate whether a partition/row is alive
     * @return the same iterator
     */
    UnfilteredPartitionIterator countFetched(UnfilteredPartitionIterator partitions, int nowInSec)
    {
        Transformation<UnfilteredRowIterator> rowCounter = new Transformation<>() {
            @Override
            protected Row applyToRow(Row row)
            {
                if (row.hasLiveData(nowInSec, false))
                    rowsFetched++;

                cellsFetched += row.cellsCount();

                return row;
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                cellsFetched += row.cellsCount();
                return row;
            }
        };

        return Transformation.apply(partitions, new Transformation<>() {
            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                if (!partition.partitionLevelDeletion().deletes(nowInSec))
                    partitionsFetched++;
                return Transformation.apply(partition, rowCounter);
            }
        });
    }

    /**
     * Counts the number of fetched partitions, rows and tombstones in the specified iterator.
     *
     * @param partitions the iterator of returned partitions to count
     * @param nowInSec the command's time in seconds, used to evaluate whether a partition/row is alive
     * @return the same iterator
     */
    UnfilteredPartitionIterator countReturned(UnfilteredPartitionIterator partitions, int nowInSec)
    {
        Transformation<UnfilteredRowIterator> rowCounter = new Transformation<>() {
            @Override
            protected Row applyToRow(Row row)
            {
                if (row.hasLiveData(nowInSec, false))
                    rowsReturned++;
                else
                    rowTombstones++;

                cellsReturned += row.cellsCount();

                return row;
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                cellsReturned += row.cellsCount();
                return row;
            }

            @Override
            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                rowTombstones++;
                return marker;
            }
        };
        return Transformation.apply(partitions, new Transformation<>() {
            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                if (partition.partitionLevelDeletion().deletes(nowInSec))
                    partitionTombstones++;
                else
                    partitionsReturned++;
                return Transformation.apply(partition, rowCounter);
            }
        });
    }

    @Override
    public String toLogString(boolean unique)
    {
        StringBuilder sb = new StringBuilder("\n");
        sb.append(INDENT);
        sb.append(unique ? "Fetched/returned/tombstones:" : "Slowest fetched/returned/tombstones:");
        append(sb, "partitions",
               partitionsFetched,
               partitionsReturned,
               partitionTombstones);
        append(sb, "rows",
               rowsFetched,
               rowsReturned,
               rowTombstones);
        append(sb, "cells",
               cellsFetched,
               cellsReturned,
               null);
        return sb.toString();
    }

    private static void append(StringBuilder sb, String name, long fetched, long returned, @Nullable Long tombstones)
    {
        sb.append('\n')
          .append(DOUBLE_INDENT)
          .append(name)
          .append(": ")
          .append(fetched)
          .append('/')
          .append(returned)
          .append('/');
        if (tombstones == null)
            sb.append('-');
        else
            sb.append(tombstones);
    }
}
