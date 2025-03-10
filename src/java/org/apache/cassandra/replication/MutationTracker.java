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

package org.apache.cassandra.replication;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;

public interface MutationTracker
{
    /**
     * Hook for preventing races between reads and log write / memtable write.
     *
     * The write path applies mutations to the log, then applies them to the memtable, and this is not synchronized
     * with the read path. This means it's possible for a logged data read to get a mutation summary from the log
     * and have it include a mutation id that hasn't yet been applied to the memtable, and return a data response
     * without that mutation. This would cause a read to return with missing data.
     *
     * To prevent that, we keep track of writes that may have raced with a read, and add them to the data response
     * if they're included in the mutation summary
     */
    interface PendingWrite extends AutoCloseable
    {
        PendingWrite NOOP = () -> {};

        @Override
        void close();
    }

    PendingWrite startWrite(Mutation mutation);

    interface PendingRead extends AutoCloseable
    {
        PendingRead NOOP = new PendingRead()
        {
            @Override
            public void close() {}

            @Override
            public UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary)
            {
                return iterator;
            }
        };
        @Override
        void close();

        /**
         * Returns mutations contained in the mutation summary that may not have been
         * applied to the memtable when it was read.
         */
        UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary);
    }

    void start();

    PendingRead startRead(ReadCommand command);

    void add(Mutation mutation);

    MutationSummary summaryForKey(TableId tableId, DecoratedKey key);
    MutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range);
    default MutationSummary summaryForRange(TableId tableId, Range<Token> range)
    {
        return summaryForRange(tableId, Range.makeRowRange(range));
    }
    Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries);
    List<Mutation> mutations(Collection<MutationId> ids);
}
