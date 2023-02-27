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

package org.apache.cassandra.tcm;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class Period
{
    private static final Logger logger = LoggerFactory.getLogger(Period.class);
    public static final long EMPTY = 0;
    public static final long FIRST = 1;

    /**
     * Last resort fallback to find where in the log table (either local or distributed) we can find a
     * given epoch. If the current ClusterMetadata.period > Period.FIRST (as should be the case normally),
     * we start there and walk backwards through the log table. Otherwise, we walk forwards.
     * Note, this method is only used (and should only be used) as a last resort in case the local index of
     * max epoch to period (in system.metadata_sealed_periods) is not available.
     * @param logTable which table to scan, system.local_metadata_log or cluster_metadata.distributed_metadata_log
     * @param since the target epoch
     * @return the period at which to begin reading when contstructing a list of log entries which follow the
     *         target epoch
     */
    public static long scanLogForPeriod(TableMetadata logTable, Epoch since)
    {
        long currentPeriod = ClusterMetadata.current().period;
        PeriodFinder visitor = currentPeriod > Period.FIRST
                               ? new ReversePeriodFinder(since, currentPeriod)
                               : new ForwardPeriodFinder(since);
        scan(logTable, visitor);
        return Math.max(visitor.readPeriod, Period.FIRST);
    }

    /**
     * Scan the log table (could be either local or distributed, but in practice only the local is used),
     * to find the n most recently sealed periods, beginning and including at a given start point.
     * Used when initialising the in-memory index of max epoch to sealed period at startup.
     * @param logTable which table to scan, system.local_metadata_log or cluster_metadata.distributed_metadata_log
     * @param startPeriod the period to begin reading from
     * @param max maximum number of sealed periods to collect
     * @return the list of most recently sealed periods, starting from & including startPeriod
     */
    public static List<Sealed> scanLogForRecentlySealed(TableMetadata logTable, long startPeriod, int max)
    {
        ReverseSealedCollector visitor = new ReverseSealedCollector(startPeriod, max);
        scan(logTable, visitor);
        return visitor.result;
    }

    private static void scan(TableMetadata logTable, Visitor visitor)
    {
        ColumnMetadata col = logTable.getColumn(ColumnIdentifier.getInterned("current_epoch", true));
        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(col));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.NONE, false);
        long startPeriod = visitor.readPeriod;
        long partitionsScanned = 0;

        boolean done = false;
        while (!done)
        {
            DecoratedKey key = logTable.partitioner.decorateKey(ByteBufferUtil.bytes(visitor.readPeriod));
            ReadCommand command = SinglePartitionReadCommand.create(logTable,
                                                                    FBUtilities.nowInSeconds(),
                                                                    key,
                                                                    columnFilter,
                                                                    sliceFilter);
            try (ReadExecutionController executionController = command.executionController();
                 PartitionIterator iterator = command.executeInternal(executionController))
            {
                if (!iterator.hasNext())
                    done = true;
                else
                {
                    ++partitionsScanned;
                    try (RowIterator partition = iterator.next())
                    {
                        visitor.accept(ByteBufferUtil.toLong(partition.staticRow().getCell(col).buffer()));
                    }
                    done = visitor.done;
                }
            }
        }
        logger.info("Performed partial local scan of {}.{}, tarted at period {}, {} partitions scanned",
                    logTable.keyspace, logTable.name, startPeriod, partitionsScanned);
    }

    private static abstract class Visitor implements LongConsumer
    {
        boolean done = false;
        long readPeriod;
        Visitor(long startPeriod)
        {
            readPeriod = startPeriod;
        }
    }

    private static abstract class PeriodFinder extends Visitor
    {
        long targetEpoch;
        PeriodFinder(Epoch target, long startPeriod)
        {
            super(startPeriod);
            this.readPeriod = startPeriod;
            this.targetEpoch = target.getEpoch();
        }
    }

    private static class ReversePeriodFinder extends PeriodFinder
    {
        ReversePeriodFinder(Epoch target, long startPeriod)
        {
            super(target, startPeriod);
        }

        @Override
        public void accept(long maxEpochInPeriod)
        {

            if (maxEpochInPeriod < targetEpoch)
            {
                readPeriod++;
                done = true;
            }
            else if (maxEpochInPeriod == targetEpoch)
                done = true;
            else
                --readPeriod;            // keep walking backwards
        }
    }

    private static class ForwardPeriodFinder extends PeriodFinder
    {
        ForwardPeriodFinder(Epoch target)
        {
            super(target, Period.FIRST);
        }

        @Override
        public void accept(long maxEpochInPeriod)
        {
            if (maxEpochInPeriod > targetEpoch)
                done = true;
            else
                ++readPeriod;           // keep walking forwards
        }
    }

    private static class ReverseSealedCollector extends Visitor
    {
        int max;
        List<Sealed> result;
        ReverseSealedCollector(long startPeriod, int max)
        {
            super(startPeriod);
            result = new ArrayList<>(max);
            this.max = max;
        }

        @Override
        public void accept(long maxEpochInPeriod)
        {
            if (result.size() < max)
                result.add(new Sealed(readPeriod, maxEpochInPeriod));

            --readPeriod;
            done = result.size() >= max;
        }
    }

}
