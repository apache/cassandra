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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.UpdateTransaction;

public class PartialTrackedSinglePartitionRead extends AbstractPartialTrackedRead
{
    private final SinglePartitionReadCommand command;
    private final UnfilteredPartitionIterator initialData;
    private SimpleBTreePartition augmentedData;

    public PartialTrackedSinglePartitionRead(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, SinglePartitionReadCommand command, UnfilteredPartitionIterator initialData)
    {
        super(executionController, searcher, cfs, startTimeNanos);
        this.command = command;
        this.initialData = initialData;
    }

    public static PartialTrackedSinglePartitionRead create(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, SinglePartitionReadCommand command, UnfilteredPartitionIterator initialData)
    {
        PartialTrackedSinglePartitionRead read = new PartialTrackedSinglePartitionRead(executionController, searcher, cfs, startTimeNanos, command, initialData);
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
    void freezeInitialData()
    {
        // the iterators from queryStorage grabs sstable references and a
        // snapshot of the memtable partition so we don't need to do anything here
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    UnfilteredPartitionIterator initialData()
    {
        return initialData;
    }

    @Override
    UnfilteredPartitionIterator augmentedData()
    {
        if (augmentedData == null)
            return null;
        Slices slices = command.clusteringIndexFilter().getSlices(command.metadata());
        UnfilteredRowIterator augmented = augmentedData.unfilteredIterator(command.columnFilter(), slices, command.clusteringIndexFilter().isReversed());
        return new SingletonUnfilteredPartitionIterator(augmented);
    }

    @Override
    void augmentResponse(PartitionUpdate update)
    {
        Preconditions.checkArgument(update.partitionKey().equals(command.partitionKey()));
        if (augmentedData == null)
            augmentedData = new SimpleBTreePartition(command.partitionKey(), command.metadata(), UpdateTransaction.NO_OP);

        augmentedData.update(update);
    }

    @Override
    CompletedRead createResult(UnfilteredPartitionIterator iterator)
    {
        return CompletedRead.simple(iterator, command().nowInSec());
    }
}
