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

package org.apache.cassandra.replication.simple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationSummarizer;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTracker;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.schema.TableId;

public class SimpleMutationTracker implements MutationTracker
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleMutationTracker.class);

    public static class KeyIds
    {
        private final SortedSet<MutationId> mutationIds = new TreeSet<MutationId>();

        private void add(MutationId id)
        {
            mutationIds.add(id);
        }
    }

    public static class TableIds
    {
        private final Map<DecoratedKey, KeyIds> tableIds = new HashMap<DecoratedKey, KeyIds>();

        public void add(DecoratedKey key, MutationId mutationId)
        {
            tableIds.computeIfAbsent(key, k -> new KeyIds()).add(mutationId);
        }
    }

    private class SimpleAccumulator implements MutationSummarizer
    {
        private final TableId tableId;
        private List<DecoratedKey> keys;
        private List<AbstractBounds<PartitionPosition>> ranges;

        public SimpleAccumulator(TableId tableId)
        {
            this.tableId = tableId;
        }

        @Override
        public synchronized void addForKey(TableId table, DecoratedKey key)
        {
            Preconditions.checkArgument(table.equals(tableId));
            if (keys == null)
                keys = new ArrayList<>();
            keys.add(key);
        }

        @Override
        public void addForRange(TableId table, AbstractBounds<PartitionPosition> range)
        {
            Preconditions.checkArgument(table.equals(tableId));
            if (ranges == null)
                ranges = new ArrayList<>();
            ranges.add(range);
        }

        @Override
        public synchronized MutationSummary summary()
        {
            lock.readLock().lock();
            try
            {
                SimpleMutationSummary summary = SimpleMutationSummary.empty(tableId);
                if (keys != null)
                {
                    for (DecoratedKey key : keys)
                        summary = summary.merge(summaryForKey(tableId, key));
                }

                if (ranges != null)
                {
                    for (AbstractBounds<PartitionPosition> range : ranges)
                        summary = summary.merge(summaryForRange(tableId, range));
                }
                return summary;
            }
            finally
            {
                lock.readLock().unlock();
            }
        }

        @Override
        public synchronized void close()
        {
        }
    }

    public class SimplePendingRead implements PendingRead
    {
        private final ReadCommand command;
        private final Map<MutationId, Mutation> pendingWrites = new ConcurrentHashMap<>();

        public SimplePendingRead(ReadCommand command)
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
            SimpleMutationSummary mutationSummary = (SimpleMutationSummary) summary;
            if (pendingWrites.isEmpty() || mutationSummary.isEmpty())
                return iterator;

            List<Mutation> augmentingMutations = new ArrayList<>(pendingWrites.size());
            for (Mutation mutation : pendingWrites.values())
            {
                if (mutationSummary.allIds.contains(mutation.id()))
                    augmentingMutations.add(mutation);
            }
            return command.augmentResultWithMutations(iterator, augmentingMutations);
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<MutationId, Mutation> mutations = new HashMap<>();
    private final Map<TableId, TableIds> tableIds = new HashMap<TableId, TableIds>();

    private final Map<MutationId, Mutation> pendingMutations = new ConcurrentHashMap<MutationId, Mutation>();
    private final Set<SimplePendingRead> pendingReads = Sets.newConcurrentHashSet();

    @Override
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

    @Override
    public PendingRead startRead(ReadCommand command)
    {
        if (!command.metadata().hasLoggedReplication())
            return PendingRead.NOOP;

        SimplePendingRead pendingRead = new SimplePendingRead(command);
        pendingReads.add(pendingRead);
        pendingMutations.values().forEach(pendingRead::onNewWrite);

        return pendingRead;
    }

    @Override
    public void add(Mutation mutation)
    {
        if (mutation.id().isNone())
            return;

        lock.writeLock().lock();
        try
        {
            if (mutations.containsKey(mutation.id()))
                return;

            for (PartitionUpdate update : mutation.getPartitionUpdates())
                tableIds.computeIfAbsent(update.metadata().id, k -> new TableIds()).add(mutation.key(), mutation.id());

            mutations.put(mutation.id(), mutation);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized SimpleMutationSummary summaryForKey(TableId tableId, DecoratedKey key)
    {
        lock.readLock().lock();
        try
        {
            TableIds ids = tableIds.get(tableId);

            if (ids == null)
                return SimpleMutationSummary.empty(tableId);

            KeyIds keyIds = ids.tableIds.get(key);
            if (keyIds == null || keyIds.mutationIds.isEmpty())
                return SimpleMutationSummary.empty(tableId);

            return SimpleMutationSummary.of(tableId, key, keyIds.mutationIds);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized SimpleMutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range)
    {
        lock.readLock().lock();
        try
        {
            TableIds ids = tableIds.get(tableId);

            SimpleMutationSummary summary = SimpleMutationSummary.empty(tableId);
            if (ids == null)
                return summary;

            for (Map.Entry<DecoratedKey, KeyIds> entry : ids.tableIds.entrySet())
            {
                if (!range.contains(entry.getKey()))
                    continue;
                summary = summary.merge(SimpleMutationSummary.of(tableId, entry.getKey(), entry.getValue().mutationIds));
            }

            return summary;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public MutationSummarizer summarizer(TableId tableId)
    {
        return new SimpleAccumulator(tableId);
    }

    @Override
    public Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        return SimpleReconciliationPlan.calculateReconciliation(summaries);
    }

    @Override
    public List<Mutation> mutations(Collection<MutationId> ids)
    {
        List<Mutation> result = new ArrayList<Mutation>(ids.size());
        ids.forEach(id -> {
            Mutation mutation = mutations.get(id);
            Preconditions.checkArgument(mutation != null);
            result.add(mutation);
        });
        return result;
    }
}
