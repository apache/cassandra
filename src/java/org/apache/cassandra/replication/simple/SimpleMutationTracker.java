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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.replication.MutationSummarizer;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTracker;
import org.apache.cassandra.schema.TableId;

public class SimpleMutationTracker implements MutationTracker
{
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
        private SimpleMutationSummary summary = SimpleMutationSummary.empty();

        public SimpleAccumulator()
        {
            lock.readLock().lock();
        }

        @Override
        public synchronized void addForKey(TableId table, DecoratedKey key)
        {
            Preconditions.checkNotNull(summary);
            summary = summary.merge(summaryForKey(table, key));
        }

        @Override
        public synchronized MutationSummary summary()
        {
            Preconditions.checkNotNull(summary);
            SimpleMutationSummary result = summary;
            summary = null;
            return result;
        }

        @Override
        public synchronized void close()
        {
            try
            {
                summary = null;
            }
            finally
            {
                lock.readLock().unlock();
            }
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<MutationId, Mutation> mutations = new HashMap<>();
    private final Map<TableId, TableIds> tableIds = new HashMap<TableId, TableIds>();

    @Override
    public void add(Mutation mutation)
    {
        lock.writeLock().lock();
        try
        {
            if (mutation.id().isNone())
                return;

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
    public synchronized SimpleMutationSummary summaryForKey(TableId table, DecoratedKey key)
    {
        lock.readLock().lock();
        try
        {
            TableIds ids = tableIds.get(table);

            if (ids == null)
                return SimpleMutationSummary.empty();

            KeyIds keyIds = ids.tableIds.get(key);
            if (keyIds == null || keyIds.mutationIds.isEmpty())
                return SimpleMutationSummary.empty();

            return SimpleMutationSummary.of(key, keyIds.mutationIds);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public MutationSummarizer summarizer()
    {
        return new SimpleAccumulator();
    }
}
