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

package org.apache.cassandra.repair;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Participants;
import org.apache.cassandra.replication.Shard;

public class SyncTasks extends AbstractCollection<SyncTask>
{
    private final List<ShardedSyncTask> shardedTasks = new ArrayList<>();

    public static class ShardedSyncTask
    {
        public final String keyspace;
        public final Participants participants;
        public final SyncTask task;
        public final Range<Token> range;

        private ShardedSyncTask(String keyspace, Participants participants, SyncTask task, Range<Token> range)
        {
            this.keyspace = keyspace;
            this.participants = participants;
            this.task = task;
            this.range = range;
        }
    }

    static SyncTasks untracked(Collection<SyncTask> tasks)
    {
        SyncTasks syncTasks = new SyncTasks();
        tasks.forEach(t -> syncTasks.shardedTasks.add(new ShardedSyncTask(null, null, t, null)));
        return syncTasks;
    }

    /**
     * Mutation Tracking manages tracking metadata within shards that are each responsible for a piece of the owned
     * token space. Executing a full repair across an entire node's ownership will span multiple shards, so repair sync
     * tasks need to be split to each align within a single tracking shard.
     */
    static SyncTasks tracked(Keyspace keyspace, List<SyncTask> tasks)
    {
        return MutationTrackingService.instance.alignToShardBoundaries(keyspace, tasks);
    }

    public void add(Shard shard, SyncTask task)
    {
        // Narrow the ultimate scope of activation to the ranges in the sync tasks rather than the entire shard.
        Set<Range<Token>> ranges = new HashSet<>(task.rangesToSync);
        Range<Token> span = span(ranges);
        shardedTasks.add(new ShardedSyncTask(shard.keyspace, shard.participants, task, span));
    }

    public static Range<Token> span(Set<Range<Token>> ranges)
    {
        List<Range<Token>> normalized = Range.normalize(ranges);
        Token min = normalized.get(0).left;
        Token max = normalized.get(normalized.size() - 1).right;
        return new Range<>(min, max);
    }

    public void apply(Consumer<ShardedSyncTask> consumer)
    {
        shardedTasks.forEach(consumer);
    }

    @Override
    public Iterator<SyncTask> iterator()
    {
        return shardedTasks.stream().map(shardedSyncTask -> shardedSyncTask.task).iterator();
    }

    @Override
    public int size()
    {
        return shardedTasks.size();
    }
}
