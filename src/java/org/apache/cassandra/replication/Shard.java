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

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.jctools.maps.NonBlockingHashMapLong;

public class Shard
{
    private final String keyspace;
    private final Range<Token> tokenRange;
    private final int localHostId;
    private final Participants participants;
    private final Epoch sinceEpoch;
    private final NonBlockingHashMapLong<CoordinatorLog> logs;
    // TODO (expected): add support for log rotation
    private final CoordinatorLogPrimary currentLocalLog;

    Shard(String keyspace, Range<Token> tokenRange, int localHostId, Participants participants, Epoch sinceEpoch, IntSupplier logIdProvider)
    {
        Preconditions.checkArgument(participants.contains(localHostId));

        this.keyspace = keyspace;
        this.tokenRange = tokenRange;
        this.localHostId = localHostId;
        this.participants = participants;
        this.sinceEpoch = sinceEpoch;
        this.logs = new NonBlockingHashMapLong<>();
        this.currentLocalLog = startNewLog(localHostId, logIdProvider.getAsInt(), participants);
        CoordinatorLogId logId = currentLocalLog.logId;
        Preconditions.checkArgument(!logId.isNone());
        logs.put(logId.asLong(), currentLocalLog);
    }

    MutationId nextId()
    {
        return currentLocalLog.nextId();
    }

    void receivedWriteResponse(MutationId mutationId, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        getOrCreate(mutationId).receivedWriteResponse(mutationId, onHostId);
    }

    void updateReplicatedOffsets(List<? extends Offsets> offsets, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        for (Offsets logOffsets : offsets)
            getOrCreate(logOffsets.logId()).updateReplicatedOffsets(logOffsets, onHostId);
    }

    void startWriting(Mutation mutation)
    {
        getOrCreate(mutation.id()).startWriting(mutation);
    }

    void finishWriting(Mutation mutation)
    {
        getOrCreate(mutation.id()).finishWriting(mutation);
    }

    void addSummaryForKey(Token token, boolean includePending, MutationSummary.Builder builder)
    {
        logs.forEach((id, log) -> {
            MutationSummary.CoordinatorSummary.Builder summaryBuilder = builder.builderForLog(log.logId);
            log.collectOffsetsFor(token, builder.tableId, includePending, summaryBuilder.unreconciled, summaryBuilder.reconciled);
        });
    }

    void addSummaryForRange(AbstractBounds<PartitionPosition> range, boolean includePending, MutationSummary.Builder builder)
    {
        logs.forEach((id, log) -> {
            MutationSummary.CoordinatorSummary.Builder summaryBuilder = builder.builderForLog(log.logId);
            log.collectOffsetsFor(range, builder.tableId, includePending, summaryBuilder.unreconciled, summaryBuilder.reconciled);
        });
    }

    List<InetAddressAndPort> remoteReplicas()
    {
        List<InetAddressAndPort> replicas = new ArrayList<>(participants.size() - 1);
        for (int i = 0, size = participants.size(); i < size; ++i)
        {
            int hostId = participants.get(i);
            if (hostId != localHostId)
                replicas.add(ClusterMetadata.current().directory.endpoint(new NodeId(hostId)));
        }
        return replicas;
    }

    /**
     * Collects replicated offsets for the logs owned by this coordinator on this shard.
     */
    ShardReplicatedOffsets collectReplicatedOffsets()
    {
        List<Offsets.Immutable> offsets = new ArrayList<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable logOffsets = log.collectReplicatedOffsets();
            if (logOffsets != null)
                offsets.add(logOffsets);
        }

        return new ShardReplicatedOffsets(keyspace, tokenRange, offsets);
    }

    /**
     * Creates a new coordinator log for this host. Primarily on Shard init (node startup or topology change).
     * Also on keyspace creation.
     */
    private static CoordinatorLog.CoordinatorLogPrimary startNewLog(int localHostId, int hostLogId, Participants participants)
    {
        CoordinatorLogId logId = new CoordinatorLogId(localHostId, hostLogId);
        return new CoordinatorLog.CoordinatorLogPrimary(localHostId, logId, participants);
    }

    private CoordinatorLog getOrCreate(MutationId mutationId)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        return getOrCreate(mutationId.logId());
    }

    private CoordinatorLog getOrCreate(CoordinatorLogId logId)
    {
        return getOrCreate(logId.asLong());
    }

    private CoordinatorLog getOrCreate(long logId)
    {
        CoordinatorLog log = logs.get(logId);
        return log != null
             ? log : logs.computeIfAbsent(logId, ignore -> CoordinatorLog.create(localHostId, new CoordinatorLogId(logId), participants));
    }
}
