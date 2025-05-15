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

import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.index.Index;

public interface PartialTrackedRead
{
    interface CompletedRead extends AutoCloseable
    {
        PartitionIterator iterator();
        TrackedRead<?, ?> followupRead(ConsistencyLevel consistencyLevel, long expiresAtNanos);

        @Override
        void close();

        static CompletedRead simple(UnfilteredPartitionIterator partition, long nowInSec)
        {
            return new CompletedRead()
            {
                @Override
                public PartitionIterator iterator()
                {
                    return UnfilteredPartitionIterators.filter(partition, nowInSec);
                }

                @Override
                public TrackedRead<?, ?> followupRead(ConsistencyLevel consistencyLevel, long expiresAtNanos)
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

    default void augment(Collection<Mutation> mutations)
    {
        mutations.forEach(this::augment);
    }

    ReadExecutionController executionController();

    Index.Searcher searcher();

    ColumnFamilyStore cfs();

    long startTimeNanos();

    ReadCommand command();

    void close();
}
