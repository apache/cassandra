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

import java.util.List;

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
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.UpdateTransaction;

import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP;

public class PartialTrackedSinglePartitionRead extends AbstractPartialTrackedRead
{
    private final Index.Searcher searcher;
    private final SinglePartitionReadCommand command;

    public PartialTrackedSinglePartitionRead(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, SinglePartitionReadCommand command)
    {
        super(executionController, cfs, startTimeNanos);
        this.searcher = searcher;
        this.command = command;
    }

    public static PartialTrackedSinglePartitionRead create(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, SinglePartitionReadCommand command, UnfilteredPartitionIterator initialData)
    {
        PartialTrackedSinglePartitionRead read = new PartialTrackedSinglePartitionRead(executionController, searcher, cfs, startTimeNanos, command);
        try
        {
            read.prepare(initialData);
            return read;
        }
        catch (Throwable e)
        {
            read.close();
            throw e;
        }
    }

    private class SinglePartitionPrepared extends Prepared
    {
        private final UnfilteredPartitionIterator initialData;
        private SimpleBTreePartition augmentedData;

        private SinglePartitionPrepared(UnfilteredPartitionIterator initialData)
        {
            this.initialData = initialData;
        }

        @Override
        public State augment(PartitionUpdate update)
        {
            Preconditions.checkArgument(update.partitionKey().equals(command.partitionKey()));
            if (augmentedData == null)
                augmentedData = new SimpleBTreePartition(command.partitionKey(), command.metadata(), UpdateTransaction.NO_OP);

            augmentedData.update(update);
            return this;
        }

        @Override
        Completed complete()
        {
            return new SinglePartitionCompleted(initialData, augmentedData);
        }
    }

    private class SinglePartitionCompleted extends Completed
    {
        private final UnfilteredPartitionIterator initialData;
        private final SimpleBTreePartition augmentedData;

        public SinglePartitionCompleted(UnfilteredPartitionIterator initialData, SimpleBTreePartition augmentedData)
        {
            this.initialData = initialData;
            this.augmentedData = augmentedData;
        }

        private UnfilteredPartitionIterator augmentedIterator()
        {
            if (augmentedData == null)
                return null;
            Slices slices = command.clusteringIndexFilter().getSlices(command.metadata());
            UnfilteredRowIterator augmentedPartition = augmentedData.unfilteredIterator(command.columnFilter(), slices, command.clusteringIndexFilter().isReversed());
            return new SingletonUnfilteredPartitionIterator(augmentedPartition);
        }

        @Override
        protected UnfilteredPartitionIterator iterator()
        {
            UnfilteredPartitionIterator augmentedIterator = augmentedIterator();
            if (augmentedIterator == null)
                return initialData;

            return UnfilteredPartitionIterators.merge(List.of(initialData, augmentedIterator), NOOP);
        }

        @Override
        protected CompletedRead createResult(UnfilteredPartitionIterator iterator)
        {
            return CompletedRead.simple(iterator, command);
        }
    }

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        return new SinglePartitionPrepared(initialData);
    }

    @Override
    public Index.Searcher searcher()
    {
        return searcher;
    }

    // TODO: delete (almost?) ever

    @Override
    public ReadCommand command()
    {
        return command;
    }
}
