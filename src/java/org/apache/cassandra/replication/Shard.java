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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.jctools.maps.NonBlockingHashMapLong;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class Shard
{
    private static final Logger logger = LoggerFactory.getLogger(Shard.class);

    final int localNodeId;
    final String keyspace;
    final Range<Token> range;
    final Participants participants;

    private final LongSupplier logIdProvider;
    private final BiConsumer<Shard, CoordinatorLog> onNewLog;
    private final NonBlockingHashMapLong<CoordinatorLog> logs;
    private volatile CoordinatorLogPrimary currentLocalLog;

    Shard(int localNodeId,
          String keyspace,
          Range<Token> range,
          Participants participants,
          List<CoordinatorLog> logs,
          LongSupplier logIdProvider,
          BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        Preconditions.checkArgument(participants.contains(localNodeId));

        this.localNodeId = localNodeId;
        this.keyspace = keyspace;
        this.range = range;
        this.participants = participants;
        this.logIdProvider = logIdProvider;
        this.logs = new NonBlockingHashMapLong<>();
        this.onNewLog = onNewLog;
        for (CoordinatorLog log : logs)
        {
            this.logs.put(log.logId.asLong(), log);
            onNewLog.accept(Shard.this, log);
        }
        this.currentLocalLog = createNewPrimaryLog();
    }

    Shard(int localNodeId, String keyspace, Range<Token> range, Participants participants, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        this(localNodeId, keyspace, range, participants, Collections.emptyList(), logIdProvider, onNewLog);
    }

    MutationId nextId()
    {
        MutationId nextId = currentLocalLog.nextId();
        if (nextId != null)
            return nextId;
        return maybeRotateLocalLogAndGetNextId();
    }

    // if ids overflow, we need to rotate the local log
    synchronized private MutationId maybeRotateLocalLogAndGetNextId()
    {
        MutationId nextId = currentLocalLog.nextId();
        if (nextId != null) // another thread got to rotate before us
            return nextId;
        CoordinatorLogId oldLogId = currentLocalLog.logId;
        currentLocalLog = createNewPrimaryLog();
        logger.info("Rotated primary log for {}/{} from {} to {}", keyspace, range, oldLogId, currentLocalLog.logId);
        return nextId();
    }

    void receivedWriteResponse(ShortMutationId mutationId, InetAddressAndPort fromHost)
    {
        int fromHostId = ClusterMetadata.current().directory.peerId(fromHost).id();
        getOrCreate(mutationId).receivedWriteResponse(mutationId, fromHostId);
    }

    void updateReplicatedOffsets(List<? extends Offsets> offsets, boolean durable, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        for (Offsets logOffsets : offsets)
            getOrCreate(logOffsets.logId()).updateReplicatedOffsets(logOffsets, durable, onHostId);
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
            if (hostId != localNodeId)
                replicas.add(ClusterMetadata.current().directory.endpoint(new NodeId(hostId)));
        }
        return replicas;
    }

    boolean isDurablyReconciled(long logId, CoordinatorLogOffsets<?> logOffsets)
    {
        return logs.get(logId).isDurablyReconciled(logOffsets);
    }

    /**
     * Collects replicated offsets for the logs owned by this coordinator on this shard.
     */
    BroadcastLogOffsets collectReplicatedOffsets(boolean durable)
    {
        List<Offsets.Immutable> offsets = new ArrayList<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable logOffsets = log.collectReplicatedOffsets(durable);
            if (logOffsets != null)
                offsets.add(logOffsets);
        }

        return new BroadcastLogOffsets(keyspace, range, offsets, durable);
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
        return log != null ? log : createNewLog(logId);
    }

    /**
     * Creates a new coordinator log for this host. Primarily on Shard init (node startup or topology change) and on keyspace creation.
     */
    private CoordinatorLog createNewLog(long logId)
    {
        CoordinatorLog next = CoordinatorLog.create(keyspace, range, localNodeId, new CoordinatorLogId(logId), participants);
        CoordinatorLog prev = logs.putIfAbsent(logId, next);
        if (null == prev) onNewLog.accept(this, next);
        return null != prev ? prev : next;
    }

    private CoordinatorLogPrimary createNewPrimaryLog()
    {
        return (CoordinatorLogPrimary) createNewLog(logIdProvider.getAsLong());
    }

    /*
     * Persist to / load from system table.
     */

    private static final String INSERT_QUERY =
        format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, participants) VALUES (?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);

    void persistToSystemTables()
    {
        executeInternal(INSERT_QUERY, keyspace, range.left.toString(), range.right.toString(), participants.asSet());
        for (CoordinatorLog log : logs.values())
            log.persistToSystemTable();
    }

    void updateLogsInSystemTable()
    {
        for (CoordinatorLog log : logs.values())
            log.updateLogsInSystemTable();
    }

    private static final String SELECT_QUERY =
        format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);

    static ArrayList<Shard> loadFromSystemTables(int localNodeId, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        Token.TokenFactory factory = ClusterMetadata.current().partitioner.getTokenFactory();
        ArrayList<Shard> shards = new ArrayList<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_QUERY))
        {
            String keyspace = row.getString("keyspace_name");
            String rangeStart = row.getString("range_start");
            String rangeEnd = row.getString("range_end");
            Range<Token> range = new Range<>(factory.fromString(rangeStart), factory.fromString(rangeEnd));
            Set<Integer> participants = row.getFrozenSet("participants", Int32Type.instance);
            List<CoordinatorLog> logs = CoordinatorLog.loadFromSystemTable(keyspace, range, localNodeId);
            shards.add(new Shard(localNodeId, keyspace, range, new Participants(participants), logs, logIdProvider, onNewLog));
        }
        return shards;
    }
}
