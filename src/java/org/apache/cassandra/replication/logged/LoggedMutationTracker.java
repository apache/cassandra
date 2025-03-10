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

package org.apache.cassandra.replication.logged;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.tracking.Shard;
import org.apache.cassandra.service.tracking.Shards;
import org.apache.cassandra.tcm.ClusterMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggedMutationTracker implements MutationTracker
{
    private final Map<MutationId, Mutation> mutations = new ConcurrentHashMap<>();

    private final PendingWrites pendingWrites = new PendingWrites();

    @Override
    public void start()
    {
        // if we need to grab it earliier, go to tcm.Startup and add afterReplay() callbacks
        Shards.instance.load(ClusterMetadata.current());
    }

    @Override
    public PendingWrite startWrite(Mutation mutation)
    {
        return pendingWrites.startWrite(mutation);
    }

    @Override
    public PendingRead startRead(ReadCommand command)
    {
        return pendingWrites.startRead(command);
    }

    @Override
    public void add(Mutation mutation)
    {
        // FIXME (now): this should all be handled by the write path
        Shard shard = Shards.instance.lookUp(mutation.getKeyspaceName(), mutation.key().getToken());
        mutations.put(mutation.id(), mutation);
        shard.witnessedMutationLocal(mutation.id(), mutation);
    }

    @Override
    public MutationSummary summaryForKey(TableId tableId, DecoratedKey key)
    {
        String keyspace = Schema.instance.getTableMetadata(tableId).keyspace;

        LoggedMutationSummary.Builder summaryBuilder = new LoggedMutationSummary.Builder(tableId);

        Shard shard = Shards.instance.lookUp(keyspace, key.getToken());
        shard.addSummaryForKey(summaryBuilder, key.getToken());

        return summaryBuilder.build();
    }

    @Override
    public MutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range)
    {
        String keyspace = Schema.instance.getTableMetadata(tableId).keyspace;

        LoggedMutationSummary.Builder summaryBuilder = new LoggedMutationSummary.Builder(tableId);

        Shards.instance.forEachIntersectingShard(keyspace, range, shard -> shard.addSummaryForRange(summaryBuilder, range));

        return summaryBuilder.build();
    }

    @Override
    public Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        throw new UnsupportedOperationException();
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
