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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.agrona.collections.IntArrayList;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class Shard
{
    private static final Logger logger = LoggerFactory.getLogger(Shard.class);

    /**
     * 3 possible shard states:
     * - ACTIVE: a shard that is allocating new mutation ids and participates in read reconciliations
     * - SEALING: a shard that is being sealed; doesn't allocate new mutation ids, but participates
     *     in read reconciliations
     * - SEALED: a shard that was fully sealed; no new mutation ids get allocated for it, and it doesn't
     *     get included into mutation summaries.
     * <p>
     * See {@link SealingCoordinator} for more context.
     */
    enum State
    {
        ACTIVE, SEALING, SEALED
    }

    final int localNodeId;
    public final String keyspace;
    public final long sinceEpoch;
    public final Range<Token> range;
    public final Participants participants;
    private final LongSupplier logIdProvider;

    private final BiConsumer<Shard, CoordinatorLog> onNewLog;
    private final NonBlockingHashMapLong<CoordinatorLog> logs;

    private volatile CoordinatorLogPrimary currentLocalLog;
    private volatile State state = State.ACTIVE;

    Shard(int localNodeId,
          String keyspace,
          long sinceEpoch,
          Range<Token> range,
          Participants participants,
          State state,
          List<CoordinatorLog> logs,
          LongSupplier logIdProvider,
          BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        Preconditions.checkArgument(participants.contains(localNodeId));

        this.localNodeId = localNodeId;
        this.keyspace = keyspace;
        this.sinceEpoch = sinceEpoch;
        this.range = range;
        this.participants = participants;
        this.state = state;
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

    Shard(int localNodeId, String keyspace, long sinceEpoch, Range<Token> range, Participants participants, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        this(localNodeId, keyspace, sinceEpoch, range, participants, State.ACTIVE, Collections.emptyList(), logIdProvider, onNewLog);
    }

    Shard(int localNodeId,
          String keyspace,
          long sinceEpoch,
          Range<Token> range,
          Participants participants,
          NonBlockingHashMapLong<CoordinatorLog> logs,
          CoordinatorLog.CoordinatorLogPrimary currentLocalLog,
          LongSupplier logIdProvider,
          BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        this.localNodeId = localNodeId;
        this.keyspace = keyspace;
        this.sinceEpoch = sinceEpoch;
        this.range = range;
        this.participants = participants;
        this.logIdProvider = logIdProvider;
        this.logs = logs;
        this.onNewLog = onNewLog;
        this.currentLocalLog = currentLocalLog;
    }

    /**
     * For rebuilding the MTS log->shard index after a topology change
     */
    void reportAllLogsToCallback()
    {
        logs.values().forEach(log -> onNewLog.accept(this, log));
    }

    void forEachLog(BiConsumer<Shard, CoordinatorLog> callback)
    {
        logs.values().forEach(log -> callback.accept(this, log));
    }

    /**
     * Incremented before this shard allocates a MutationId.
     * Decremented once the id has been allocated and added to the pending writes set.
     * Used by shard sealing logic for drain() step.
     */
    private final AtomicInteger pendingIdAllocations = new AtomicInteger();
    private final NonBlockingHashMap<MutationId, Boolean> pendingLocalWrites = new NonBlockingHashMap<>();

    @Nonnull
    MutationId nextMutationId()
    {
        pendingIdAllocations.incrementAndGet();
        try
        {
            MutationId id = nextId();
            pendingLocalWrites.put(id, true);
            return id;
        }
        finally
        {
            pendingIdAllocations.decrementAndGet();
        }
    }

    /*
     * TODO (expected): drain() should handle tracked transfers as well (later)
     */
    @Nonnull
    MutationId nextTransferId()
    {
        return nextId();
    }

    @Nonnull
    private MutationId nextId()
    {
        if (state != State.ACTIVE)
            throw new IllegalStateException(format("%s cannot assign next id, state: %s", this, state));
        MutationId nextId = currentLocalLog.nextId();
        if (nextId == null)
            nextId = maybeRotateLocalLogAndGetNextId();
        logger.trace("Issuing next id {}", nextId);
        return nextId;
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
        return currentLocalLog.nextId();
    }

    /**
     * Must be called at least once per {@code nextId()} invocation.
     */
    void completeLocalWrite(MutationId mutationId)
    {
        pendingLocalWrites.remove(mutationId);
    }

    void receivedWriteResponse(ShortMutationId mutationId, InetAddressAndPort fromHost)
    {
        int fromHostId = ClusterMetadata.current().directory.peerId(fromHost).id();
        getOrCreate(mutationId).receivedWriteResponse(mutationId, fromHostId);
    }

    void finishActivation(Bounds<Token> bounds, ActivationRequest activation)
    {
        getOrCreate(activation.transferId).finishActivation(bounds, activation);
    }

    void receivedActivationResponse(CoordinatedTransfer transfer, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        getOrCreate(transfer.id()).receivedActivationResponse(transfer, onHostId);
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
        pendingLocalWrites.remove(mutation.id());
    }

    void addSummaryForKey(Token token, boolean includePending, MutationSummary.Builder builder)
    {
        // TODO (expected): this is a temporary solutions, which is racy *during* a topology change (SEALING -> SEALED transition);
        //      instead, should be gating on epochs after transition, decided by read coordinator;
        //      some of those additional TCM transitions are currently missing however (pending for unhappy path)
        if (isSealed())
            return;

        logs.forEach((id, log) -> {
            MutationSummary.CoordinatorSummary.Builder summaryBuilder = builder.builderForLog(log.logId);
            log.collectOffsetsFor(token, builder.tableId, includePending, summaryBuilder.unreconciled, summaryBuilder.reconciled);
        });
    }

    void addSummaryForRange(AbstractBounds<PartitionPosition> range, boolean includePending, MutationSummary.Builder builder)
    {
        // TODO (expected): this is a temporary solutions, which is racy *during* a topology change (SEALING -> SEALED transition);
        //      instead, should be gating on epochs after transition, decided by read coordinator
        //      some of those additional TCM transitions are currently missing however (pending for unhappy path)
        if (isSealed())
            return;

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
            {
                InetAddressAndPort ep = ClusterMetadata.current().directory.endpoint(new NodeId(hostId));
                if (ep == null)
                {
                    // offset broadcasting can race with topology changes
                    // TODO (expected): consider adding a more sophisticated check so we don't die during normal topology operations, but still detect bugs
                    logger.warn("No endpoint found for hostId {}", hostId);
                    continue;
                }

                replicas.add(ep);
            }
        }
        return replicas;
    }

    boolean isDurablyReconciled(ShortMutationId id)
    {
        return logs.get(id.logId()).isDurablyReconciled(id);
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

        return new BroadcastLogOffsets(keyspace, sinceEpoch, range, participants, offsets, durable);
    }

    /**
     * @return the list of the collected locally missing offsets for the logs owned by this coordinator on
     * this shard
     */
    List<Offsets.Immutable> collectLocallyMissingOffsets()
    {
        List<Offsets.Immutable> result = new ArrayList<>(logs.size());
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable missing = log.collectLocallyMissingOffsets();
            if (missing != null)
                result.add(missing);
        }
        return result;
    }

    void collectDurablyReconciledOffsets(Log2OffsetsMap.Mutable into)
    {
        logs.values().forEach(log -> log.collectDurablyReconciledOffsets(into));
    }

    private CoordinatorLog getOrCreate(Mutation mutation)
    {
        return getOrCreate(mutation.id());
    }

    private CoordinatorLog getOrCreate(ShortMutationId mutationId)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        return getOrCreate(mutationId.logId());
    }

    private CoordinatorLog getOrCreate(CoordinatorLogId logId)
    {
        return getOrCreate(logId.asLong());
    }

    public long getUnreconciledCount()
    {
        long count = 0;
        for (CoordinatorLog log : logs.values())
        {
            count += log.getUnreconciledCount();
        }
        return count;
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
        CoordinatorLog next = CoordinatorLog.create(keyspace, sinceEpoch, range, localNodeId, new CoordinatorLogId(logId), participants);
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
        format("INSERT INTO %s.%s (keyspace_name, since_epoch, range_start, range_end, participants, state) VALUES (?, ?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);

    void persistToSystemTables()
    {
        executeInternal(INSERT_QUERY, keyspace, sinceEpoch, range.left.toString(), range.right.toString(), participants.asSet(), state.name());
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
        //noinspection DataFlowIssue
        for (UntypedResultSet.Row row : executeInternal(SELECT_QUERY))
        {
            String keyspace = row.getString("keyspace_name");
            long sinceEpoch = row.getLong("since_epoch");
            String rangeStart = row.getString("range_start");
            String rangeEnd = row.getString("range_end");
            Range<Token> range = new Range<>(factory.fromString(rangeStart), factory.fromString(rangeEnd));
            Set<Integer> participants = row.getFrozenSet("participants", Int32Type.instance);
            State state = row.has("state") ? State.valueOf(row.getString("state")) : State.ACTIVE;
            List<CoordinatorLog> logs = CoordinatorLog.loadFromSystemTable(keyspace, sinceEpoch, range, localNodeId);
            shards.add(new Shard(localNodeId, keyspace, sinceEpoch, range, new Participants(participants), state, logs, logIdProvider, onNewLog));
        }
        return shards;
    }

    /*
     * Sealing
     */

    void markSealing()
    {
        if (state == State.SEALED)
            throw new IllegalStateException(format("%s cannot transition to SEALING from %s", this, state));

        if (state != State.SEALING)
        {
            state = State.SEALING;
            persistToSystemTables();
        }
    }

    boolean isDrained()
    {
        return state != State.ACTIVE && pendingIdAllocations.get() == 0 && pendingLocalWrites.isEmpty();
    }

    /**
     * @return locally-applied offsets for each log of the shard
     */
    Log2OffsetsMap.Immutable collectLocallyWitnessedOffsets()
    {
        Log2OffsetsMap.Immutable.Builder builder = new Log2OffsetsMap.Immutable.Builder();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable witnessed = log.collectReplicatedOffsets(false);
            if (witnessed != null)
                builder.add(witnessed);
        }
        return builder.build();
    }

    /**
     * @return whether the shard has locally applied every offset in {@code offsets}.
     */
    boolean hasWitnessed(Log2OffsetsMap<?> offsets)
    {
        for (Offsets target : offsets.offsets())
        {
            CoordinatorLog log = logs.get(target.logId().asLong());
            if (log == null)
                return false;
            Offsets.Immutable local = log.collectReplicatedOffsets(false);
            if (local == null || !Offsets.Immutable.difference(target, local).isEmpty())
                return false;
        }
        return true;
    }

    void markSealed()
    {
        if (state == State.ACTIVE)
            throw new IllegalStateException(format("%s cannot transition to SEALED from %s", this, state));

        if (state != State.SEALED)
        {
            state = State.SEALED;
            persistToSystemTables();
        }
    }

    public boolean isSealed()
    {
        return state == State.SEALED;
    }

    public Range<Token> tokenRange()
    {
        return range;
    }

    /**
     * Returns the reconciled offsets for each coordinator log in this shard.
     * Reconciled offsets are the intersection of what all participants have.
     */
    public Map<CoordinatorLogId, Offsets.Immutable> collectReconciledOffsetsPerLog()
    {
        Map<CoordinatorLogId, Offsets.Immutable> result = new HashMap<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable reconciled = log.collectReconciledOffsets();
            if (!reconciled.isEmpty())
                result.put(log.logId, reconciled);
        }
        return result;
    }

    /**
     * Returns the intersection of witnessed offsets scoped to only the specified participant host IDs.
     * If liveHostIds is null, behaves the same as {@link #collectReconciledOffsetsPerLog()}.
     */
    public Map<CoordinatorLogId, Offsets.Immutable> collectReconciledOffsetsPerLog(Set<Integer> liveHostIds)
    {
        if (liveHostIds == null)
            return collectReconciledOffsetsPerLog();

        Map<CoordinatorLogId, Offsets.Immutable> result = new HashMap<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable reconciled = log.collectReconciledOffsets(liveHostIds);
            if (!reconciled.isEmpty())
                result.put(log.logId, reconciled);
        }
        return result;
    }

    /**
     * Returns the UNION of witnessed offsets from all participants for each coordinator log.
     * Union = all offsets that ANY replica has witnessed.
     */
    public Map<CoordinatorLogId, Offsets.Immutable> collectUnionOfWitnessedOffsetsPerLog()
    {
        Map<CoordinatorLogId, Offsets.Immutable> result = new HashMap<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable union = log.collectUnionOfWitnessedOffsets();
            if (!union.isEmpty())
                result.put(log.logId, union);
        }
        return result;
    }

    /**
     * Returns the UNION of witnessed offsets scoped to only the specified participant host IDs.
     * If liveHostIds is null, behaves the same as {@link #collectUnionOfWitnessedOffsetsPerLog()}.
     */
    public Map<CoordinatorLogId, Offsets.Immutable> collectUnionOfWitnessedOffsetsPerLog(Set<Integer> liveHostIds)
    {
        if (liveHostIds == null)
            return collectUnionOfWitnessedOffsetsPerLog();

        Map<CoordinatorLogId, Offsets.Immutable> result = new HashMap<>();
        for (CoordinatorLog log : logs.values())
        {
            Offsets.Immutable union = log.collectUnionOfWitnessedOffsets(liveHostIds);
            if (!union.isEmpty())
                result.put(log.logId, union);
        }
        return result;
    }

    @Override
    public String toString()
    {
        return "Shard{keyspace='" + keyspace + '\'' +
               ", sinceEpoch=" + sinceEpoch +
               ", range=" + range +
               ", participants=" + participants +
               ", state=" + state +
               '}';
    }

    public DebugInfo getDebugInfo()
    {
        SortedMap<CoordinatorLogId, CoordinatorLog.DebugInfo> logDebugState = new TreeMap<>(Comparator.comparing(CoordinatorLogId::asLong));
        for (CoordinatorLog log : logs.values())
            logDebugState.put(log.getLogId(), log.getDebugState());
        return new DebugInfo(keyspace, sinceEpoch, range, localNodeId, participants, logDebugState);
    }

    public static class DebugInfo
    {
        public final String keyspace;
        public final long sinceEpoch;
        public final Range<Token> range;
        public final int localNodeId;
        public final Participants participants;
        public final SortedMap<CoordinatorLogId, CoordinatorLog.DebugInfo> logs;

        private DebugInfo(
                String keyspace, long sinceEpoch, Range<Token> range, int localNodeId,
                Participants participants, SortedMap<CoordinatorLogId, CoordinatorLog.DebugInfo> logs)
        {
            this.keyspace = keyspace;
            this.sinceEpoch = sinceEpoch;
            this.range = range;
            this.localNodeId = localNodeId;
            this.participants = participants;
            this.logs = logs;
        }
    }

    static final Comparator<Shard> COMPARATOR = (a, b) -> {
        int c = a.range.left.compareTo(b.range.left);
        if (c == 0) c = Range.compareRightToken(a.range.right, b.range.right);
        if (c == 0) c = Long.compare(a.sinceEpoch, b.sinceEpoch);
        return c;
    };
}
