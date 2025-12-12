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
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Pair;

/**
 * Repair sync tasks (that stream SSTable contents) must be integrated with Mutation Tracking's bulk transfer handling
 * because any data that is not present on all instances must be expressed as unreconciled in the log, since read
 * reconciliations depend on the log state to guarantee monotonicity of subsequent reads. Streaming sessions for full
 * repair can complete on some instances before others, so we need to represent those completed sessions as unreconciled
 * in the log.
 * <p>
 * Tracked repairs mostly follow the default coordination process. They are notable in two ways. The first is that 
 * {@link #prepare} is essentially a no-op. Sync completion assumes that streamed SSTables have arrived at replicas.
 * The second is that activation is based on the {@link #bounds} implied by the {@link SyncTask} owned by the transfer.
 */
public class TrackedRepairTransfer extends CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedRepairTransfer.class);

    public TrackedRepairTransfer(SyncTasks.ShardedSyncTask shardedTask)
    {
        this(shardedTask.task.getTransferId(), shardedTask.participants, shardedTask.task, shardedTask.keyspace, shardedTask.range);
    }

    private TrackedRepairTransfer(ShortMutationId id, Participants participants, SyncTask task, String keyspace, Range<Token> range)
    {
        super(id, keyspace, range);

        Set<Integer> replicaNodeIds = participants.asSet();
        Set<InetAddressAndPort> participating = new HashSet<>();

        participating.add(task.nodePair().coordinator);
        participating.add(task.nodePair().peer);
        streamResults.put(Pair.create(task.nodePair().coordinator, task.nodePair().peer), SingleTransferResult.Init());

        ClusterMetadata cm = ClusterMetadata.current();
        for (Integer replicaNodeId : replicaNodeIds)
        {
            InetAddressAndPort addr = cm.directory.endpoint(new NodeId(replicaNodeId));

            // Need to activate on all replicas, not just ones with SyncTasks. For replicas that don't receive any data
            // as part of a repair, they still need to activate the transfer ID as a no-op, to allow read 
            // reconciliations to complete.
            if (!participating.contains(addr))
                streamResults.put(Pair.create(addr, addr), SingleTransferResult.EmptySync());
        }
    }

    /**
     * When every {@link SyncTask} for a repair has completed, follow the bulk transfer activation path to safely make
     * the new data live and tracked in the log. This needs to include all replicas of {@link #id()}, even those that
     * did not receive anything as part of the repair. Otherwise, any read reconciliation will fail to complete.
     */
    public void activate(SyncStat stat)
    {
        Preconditions.checkNotNull(stat.planId, "A complete sync should have a plan ID");

        logger.debug("{} Activating {}", logPrefix(), this);

        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(stat.nodes.coordinator, stat.nodes.peer);
        // Account for syncs where data streams both ways. (i.e. We need to activate both receivers.)
        streamResults.put(pair, SingleTransferResult.StreamComplete(stat.planId));
        streamResults.put(pair.reverse(), SingleTransferResult.StreamComplete(stat.planId));

        activate(streamResults.keySet());
    }

    @Override
    protected void prepare(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> targets)
    {
        // There should be no need to prepare on stream completion for repair, as not all activations will involve a
        // local pending transfer to begin with.
        for (Pair<InetAddressAndPort, InetAddressAndPort> target : targets)
            streamResults.computeIfPresent(target, (pair, result) -> result.preparing());
    }

    @Override
    protected ActivationRequest createActivation(Pair<InetAddressAndPort, InetAddressAndPort> pair, ActivationRequest.Phase phase)
    {
        return new ActivationRequest(StreamOperation.REPAIR, pair, phase, id(), ClusterMetadata.current().myNodeId(), range, keyspace, streamResults.get(pair).planId());
    }

    @Override
    public String toString()
    {
        return "TrackedRepairTransfer{" +
               "keyspace='" + keyspace + '\'' +
               ", range=" + range +
               ", streamResults=" + streamResults +
               '}';
    }
}
