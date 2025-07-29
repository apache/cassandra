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
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
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
    private final BiConsumer<Shard, CoordinatorLog> onNewLog;
    private final NonBlockingHashMapLong<CoordinatorLog> logs;
    // TODO (expected): add support for log rotation
    private final CoordinatorLog.CoordinatorLogPrimary currentLocalLog;

    private final List<Subscriber> subscribers = new ArrayList<>();

    public interface Subscriber
    {
        default void onLogCreation(CoordinatorLog log) {}
        default void onSubscribe(CoordinatorLog currentLog) {}
    }

    Shard(String keyspace,
          Range<Token> tokenRange,
          int localHostId,
          Participants participants,
          Epoch sinceEpoch,
          IntSupplier logIdProvider,
          BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        Preconditions.checkArgument(participants.contains(localHostId));

        this.keyspace = keyspace;
        this.tokenRange = tokenRange;
        this.localHostId = localHostId;
        this.participants = participants;
        this.sinceEpoch = sinceEpoch;
        this.logs = new NonBlockingHashMapLong<>();
        this.onNewLog = onNewLog;
        this.currentLocalLog = startNewLog(localHostId, logIdProvider.getAsInt(), participants);
        CoordinatorLogId logId = currentLocalLog.logId;
        Preconditions.checkArgument(!logId.isNone());
        logs.put(logId.asLong(), currentLocalLog);
    }

    MutationId nextId()
    {
        return currentLocalLog.nextId();
    }

    void receivedWriteResponse(ShortMutationId mutationId, InetAddressAndPort fromHost)
    {
        int fromHostId = ClusterMetadata.current().directory.peerId(fromHost).id();
        getOrCreate(mutationId).receivedWriteResponse(mutationId, fromHostId);
    }

    void updateReplicatedOffsets(List<? extends Offsets> offsets, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        for (Offsets logOffsets : offsets)
            getOrCreate(logOffsets.logId()).updateReplicatedOffsets(logOffsets, onHostId);
    }

    boolean startWriting(Mutation mutation)
    {
        return getOrCreate(mutation).startWriting(mutation);
    }

    void finishWriting(Mutation mutation)
    {
        getOrCreate(mutation).finishWriting(mutation);
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

    void collectLocallyMissingMutations(Offsets remoteOffsets, Log2OffsetsMap.Mutable into)
    {
        CoordinatorLog log = get(remoteOffsets.logId());
        log.collectLocallyMissingMutations(remoteOffsets, into);
    }

    void collectRemotelyMissingMutations(Offsets localOffsets, IntArrayList remoteNodeIds, Node2OffsetsMap into)
    {
        CoordinatorLog log = get(localOffsets.logId());
        log.collectRemotelyMissingMutations(localOffsets, remoteNodeIds, into);
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
    BroadcastLogOffsets collectReplicatedOffsets()
    {
        List<Offsets.Immutable> offsets = new ArrayList<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable logOffsets = log.collectReplicatedOffsets();
            if (logOffsets != null)
                offsets.add(logOffsets);
        }

        return new BroadcastLogOffsets(keyspace, tokenRange, offsets);
    }

    /**
     * Creates a new coordinator log for this host. Primarily on Shard init (node startup or topology change).
     * Also on keyspace creation.
     */
    private CoordinatorLog.CoordinatorLogPrimary startNewLog(int localHostId, int hostLogId, Participants participants)
    {
        CoordinatorLogId logId = new CoordinatorLogId(localHostId, hostLogId);
        CoordinatorLog.CoordinatorLogPrimary log =
            new CoordinatorLog.CoordinatorLogPrimary(localHostId, logId, participants);
        onNewLog.accept(this, log);
        return log;
    }

    private CoordinatorLog getOrCreate(Mutation mutation)
    {
        return getOrCreate(mutation.id());
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

    @Nonnull
    private CoordinatorLog get(CoordinatorLogId logId)
    {
        return Preconditions.checkNotNull(logs.get(logId.asLong()));
    }

    private CoordinatorLog getOrCreate(long logId)
    {
        CoordinatorLog log = logs.get(logId);
        if (log != null)
            return log;
        CoordinatorLog newLog = logs.computeIfAbsent(logId, ignore -> CoordinatorLog.create(localHostId, new CoordinatorLogId(logId), participants));
        onNewLog.accept(this, newLog);
        for (Subscriber subscriber : subscribers)
            subscriber.onLogCreation(newLog);
        return newLog;
    }

    public void addSubscriber(Subscriber subscriber)
    {
        subscriber.onSubscribe(currentLocalLog);
        subscribers.add(subscriber);
    }

    private CoordinatorLog create(long logId)
    {
        CoordinatorLog log = CoordinatorLog.create(localHostId, new CoordinatorLogId(logId), participants);
        onNewLog.accept(this, log);
        for (Subscriber subscriber : subscribers)
            subscriber.onLogCreation(log);
        return log;
    }
}
