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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.partitions.PartitionUpdate;
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

    private final Map<MutationId, Mutation> mutations = new HashMap<>();


    private final Map<TableId, TableIds> tableIds = new HashMap<TableId, TableIds>();

    @Override
    public synchronized void add(Mutation mutation)
    {
        if (mutation.id().isNone())
            return;

        if (mutations.containsKey(mutation.id()))
        {
            Preconditions.checkState(mutations.get(mutation.id()).equals(mutation));
            return;
        }

        for (PartitionUpdate update : mutation.getPartitionUpdates())
            tableIds.computeIfAbsent(update.metadata().id, k -> new TableIds()).add(mutation.key(), mutation.id());

        mutations.put(mutation.id(), mutation);
    }

    @Override
    public MutationSummary summaryForKey(TableId table, DecoratedKey key)
    {
        TableIds ids = tableIds.get(table);

        if (ids == null)
            return SimpleMutationSummary.empty();

        KeyIds keyIds = ids.tableIds.get(key);
        if (keyIds == null || keyIds.mutationIds.isEmpty())
            return SimpleMutationSummary.empty();

        return SimpleMutationSummary.of(key, keyIds.mutationIds);
    }
}
