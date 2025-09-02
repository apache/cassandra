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
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.transport.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PartialTrackedRead
{
    Logger logger = LoggerFactory.getLogger(PartialTrackedRead.class);
    
    interface CompletedRead extends AutoCloseable
    {
        TrackedDataResponse response(); // must be called from the read stage
        Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);

        @Override
        void close();

        static TrackedDataResponse createResponse(UnfilteredPartitionIterator partition, ReadCommand command)
        {
            PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, command.nowInSec());
            DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(),
                                                                     false,
                                                                     command.selectsFullPartition(),
                                                                     command.metadata().enforceStrictLiveness()).onlyCount();
            return TrackedDataResponse.create(counter.applyTo(iterator),
                                              command.columnFilter());
        }

        static CompletedRead simple(UnfilteredPartitionIterator partition, ReadCommand command, long nowInSec)
        {
            return new CompletedRead()
            {
                @Override
                public TrackedDataResponse response()
                {
                    return createResponse(partition, command);
                }

                @Override
                public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialRead, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
                {
                    return null;
                }

                @Override
                public void close()
                {
                    partition.close();
                }
            };
        }
    }

    CompletedRead complete();

    void augment(Mutation mutation);

    default void augment(Log2OffsetsMap<?> augmentingOffsets)
    {
        augmentingOffsets.forEach(this::augment);
    }

    default void augment(ShortMutationId mutationId)
    {
        Mutation mutation = MutationJournal.instance.read(mutationId);
        Preconditions.checkNotNull(mutation);
        if (!command().selectsKey(mutation.key()))
        {
            logger.trace("Skipping mutation {} - {} not in read range", mutationId, mutation.key());
            return;
        }
        augment(mutation);
    }

    ReadExecutionController executionController();

    Index.Searcher searcher();

    ColumnFamilyStore cfs();

    long startTimeNanos();

    ReadCommand command();

    void close();
}
