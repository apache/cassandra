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

package org.apache.cassandra.service.reads.tracked;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;

public class PartialTrackedRangeRead extends AbstractPartialTrackedRead
{
    private final PartitionRangeReadCommand command;
    private final SortedMap<DecoratedKey, SimpleBTreePartition> data = new TreeMap<>();
    private final UnfilteredPartitionIterator initialData;

    public PartialTrackedRangeRead(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
    {
        super(executionController, searcher, cfs, startTimeNanos);
        this.command = command;
        this.initialData = initialData;
    }

    public static PartialTrackedRangeRead create(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
    {
        PartialTrackedRangeRead read = new PartialTrackedRangeRead(executionController, searcher, cfs, startTimeNanos, command, initialData);
        try
        {
            read.prepare();
            return read;
        }
        catch (Throwable e)
        {
            read.close();
            throw e;
        }
    }

    @Override
    protected ReadCommand command()
    {
        return command;
    }

    UnfilteredRowIterator queryPartition(SimpleBTreePartition partition)
    {
        return partition.unfilteredIterator(command.columnFilter(),
                                            command.requestedSlices(),
                                            command.clusteringIndexFilter(partition.partitionKey()).isReversed());
    }

    @Override
    void freezeInitialData()
    {
        // memtable contents are frozen at read completion time, when the iterator is evaluated, not at the beginning
        // of the read, when references to memtables and sstables are collected. Because of this, replica coordinated
        // reads can cause read monotonicity to be broken by returning data that hasn't been replicated to at least
        // CL other nodes via reconciliation. To prevent this, the contents of the initial iterator are materialized
        // onto heap until the limits of the read are reached.

        UnfilteredPartitionIterator materializer = new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public TableMetadata metadata()
            {
                return initialData.metadata();
            }

            @Override
            public boolean hasNext()
            {
                return initialData.hasNext();
            }

            @Override
            public UnfilteredRowIterator next()
            {
                UnfilteredRowIterator rowIterator = initialData.next();
                DecoratedKey key = rowIterator.partitionKey();
                augmentResponse(PartitionUpdate.fromIterator(rowIterator, command.columnFilter()));
                return queryPartition(data.get(key));
            }

            @Override
            public void close()
            {
                super.close();
                initialData.close();
            }
        };

        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), false, command.selectsFullPartition(), command.metadata().enforceStrictLiveness());
        try (UnfilteredPartitionIterator iterator = counter.applyTo(materializer))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    @Override
    UnfilteredPartitionIterator initialData()
    {
        Iterator<SimpleBTreePartition> iterator = data.values().iterator();
        return new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public UnfilteredRowIterator next()
            {
                return queryPartition(iterator.next());
            }
        };
    }

    @Override
    UnfilteredPartitionIterator augmentedData()
    {
        return null;
    }

    @Override
    void augmentResponse(PartitionUpdate update)
    {
        SimpleBTreePartition partition = data.computeIfAbsent(update.partitionKey(), key -> new SimpleBTreePartition(key, update.metadata(), UpdateTransaction.NO_OP));
        partition.update(update);
    }
}
