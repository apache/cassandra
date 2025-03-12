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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

public class MutationTracker
{
    public interface PendingRead extends AutoCloseable
    {
        static PendingRead NOOP = new PendingRead()
        {
            @Override
            public void close()
            {
            }

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
         *
         * @param summary
         * @return
         */
        UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary);
    }

    public class ListeningPendingRead implements PendingRead
    {
        private final ReadCommand command;
        private final Map<MutationId, Mutation> pendingWrites = new ConcurrentHashMap<>();

        public ListeningPendingRead(ReadCommand command)
        {
            this.command = command;
        }

        public void onNewWrite(Mutation mutation)
        {
            if (command.readsMutationContents(mutation))
                pendingWrites.put(mutation.id(), mutation);
        }

        @Override
        public void close()
        {
            pendingReads.remove(this);
        }

        public Set<MutationId> mutationIds()
        {
            return pendingWrites.keySet();
        }

        @Override
        public UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary)
        {
            if (pendingWrites.isEmpty() || summary.isEmpty())
                return iterator;

            List<Mutation> augmentingMutations = new ArrayList<>(pendingWrites.size());
            for (Mutation mutation : pendingWrites.values())
            {
                if (summary.contains(mutation.id()))
                    augmentingMutations.add(mutation);
            }
            return command.augmentResultWithMutations(iterator, augmentingMutations);
        }
    }


    public interface PendingWrite extends AutoCloseable
    {
        static PendingWrite NOOP = new PendingWrite()
        {
            @Override
            public void close()
            {
            }
        };

        @Override
        void close();
    }

    private final Map<MutationId, Mutation> pendingMutations = new ConcurrentHashMap<MutationId, Mutation>();
    private final Set<ListeningPendingRead> pendingReads = Sets.newConcurrentHashSet();

    public void start()
    {
        // if we need to grab it earliier, go to tcm.Startup and add afterReplay() callbacks
        Shards.instance.load(ClusterMetadata.current());
    }


    public PendingWrite startWrite(Mutation mutation)
    {
        if (mutation.id().isNone())
            return PendingWrite.NOOP;

        pendingMutations.put(mutation.id(), mutation);
        pendingReads.forEach(read -> read.onNewWrite(mutation));

        return new PendingWrite()
        {
            @Override
            public void close()
            {
                pendingMutations.remove(mutation.id());
            }
        };
    }

    public PendingRead startRead(ReadCommand command)
    {
        if (!Schema.instance.getKeyspaceMetadata(command.metadata().keyspace).useMutationTracking())
            return PendingRead.NOOP;

        ListeningPendingRead pendingRead = new ListeningPendingRead(command);
        pendingReads.add(pendingRead);
        pendingMutations.values().forEach(pendingRead::onNewWrite);

        return pendingRead;
    }

    // TODO: ditch?
    public void add(Mutation mutation)
    {
    }

    public MutationSummary summaryForKey(TableId tableId, DecoratedKey key)
    {
        String keyspace = Schema.instance.getTableMetadata(tableId).keyspace;

        MutationSummary.Builder summaryBuilder = new MutationSummary.Builder(tableId);

        Shard shard = Shards.instance.lookUp(keyspace, key.getToken());
        shard.addSummaryForKey(summaryBuilder, key.getToken());

        return summaryBuilder.build();
    }

    public MutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range)
    {
        String keyspace = Schema.instance.getTableMetadata(tableId).keyspace;

        MutationSummary.Builder summaryBuilder = new MutationSummary.Builder(tableId);

        Shards.instance.forEachIntersectingShard(keyspace, range, shard -> shard.addSummaryForRange(summaryBuilder, range));

        return summaryBuilder.build();
    }

    public MutationSummary summaryForRange(TableId tableId, Range<Token> range)
    {
        return summaryForRange(tableId, Range.makeRowRange(range));
    }

    public Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        throw new UnsupportedOperationException();
    }
    public List<Mutation> mutations(Collection<MutationId> ids)
    {
        List<Mutation> result = new ArrayList<>(ids.size());
        MutationJournal.instance.readAll(ids, result);
        Preconditions.checkArgument(ids.size() == result.size());
        return result;
    }
}
