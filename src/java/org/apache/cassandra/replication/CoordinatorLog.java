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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public abstract class CoordinatorLog
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorLog.class);

    protected final int localHostId;
    protected final String keyspace;
    protected final Range<Token> range;
    protected final CoordinatorLogId logId;
    protected final Participants participants;

    protected final UnreconciledMutations unreconciledMutations;
    protected final Offsets.Mutable[] witnessedOffsets;
    protected final Offsets.Mutable[] durableOffsets;
    protected final Offsets.Mutable reconciledOffsets;

    protected final ReadWriteLock lock;

    abstract void receivedWriteResponse(ShortMutationId mutationId, int fromHostId);

    CoordinatorLog(String keyspace,
                   Range<Token> range,
                   int localHostId,
                   CoordinatorLogId logId,
                   Participants participants,
                   Offsets.Mutable[] witnessedOffsets,
                   Offsets.Mutable[] durableOffsets)
    {
        this.localHostId = localHostId;
        this.keyspace = keyspace;
        this.range = range;
        this.logId = logId;
        this.participants = participants;
        this.unreconciledMutations = new UnreconciledMutations();
        this.witnessedOffsets = witnessedOffsets;
        this.durableOffsets = durableOffsets;
        this.reconciledOffsets = Offsets.Mutable.intersection(witnessedOffsets);
        this.lock = new ReentrantReadWriteLock();
    }

    CoordinatorLog(String keyspace, Range<Token> range, int localHostId, CoordinatorLogId logId, Participants participants)
    {
        this(keyspace, range, localHostId, logId, participants, initOffsets(logId, participants), initOffsets(logId, participants));
    }

    private static Offsets.Mutable[] initOffsets(CoordinatorLogId logId, Participants participants)
    {
        Offsets.Mutable[] ids = new Offsets.Mutable[participants.size()];
        for (int i = 0; i < participants.size(); i++)
            ids[i] = new Offsets.Mutable(logId);
        return ids;
    }

    static CoordinatorLog create(String keyspace, Range<Token> range, int localHostId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(keyspace, range, localHostId, id, participants)
                                        : new CoordinatorLogReplica(keyspace, range, localHostId, id, participants);
    }

    // TODO (expected): recreate unreconciledMutations using journal
    static CoordinatorLog recreate(
        String keyspace, Range<Token> range, int localHostId, CoordinatorLogId id, Participants participants,
        Offsets.Mutable[] witnessedOffsets, Offsets.Mutable[] durableOffsets)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(keyspace, range, localHostId, id, participants, witnessedOffsets, durableOffsets)
                                        : new CoordinatorLogReplica(keyspace, range, localHostId, id, participants, witnessedOffsets, durableOffsets);
    }

    void updateReplicatedOffsets(Offsets offsets, boolean durable, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            if (durable)
                updateDurableReplicatedOffsets(offsets, onHostId);
            else
                updateTransientReplicatedOffsets(offsets, onHostId);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void updateTransientReplicatedOffsets(Offsets offsets, int onHostId)
    {
        getTransient(onHostId).addAll(offsets, (ignore, start, end) ->
        {
            for (int offset = start; offset <= end; ++offset)
            {
                // TODO (desired): use the fact that Offsets are ordered to optimise this look up
                if (othersWitnessed(offset, onHostId))
                {
                    reconciledOffsets.add(offset);
                    unreconciledMutations.remove(offset);
                }
            }
        });
    }

    private void updateDurableReplicatedOffsets(Offsets offsets, int onHostId)
    {
        getDurable(onHostId).addAll(offsets);
    }

    @Nullable
    Offsets.Immutable collectReplicatedOffsets(boolean durable)
    {
        lock.readLock().lock();
        try
        {
            int idx = participants.indexOf(localHostId);
            Offsets offsets = durable ? durableOffsets[idx] : witnessedOffsets[idx];
            return offsets.isEmpty() ? null : Offsets.Immutable.copy(offsets);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    boolean startWriting(Mutation mutation)
    {
        lock.writeLock().lock();
        try
        {
            if (getLocal().contains(mutation.id().offset()))
                return false; // already witnessed; shouldn't get to this path often (duplicate mutation)

            unreconciledMutations.startWriting(mutation);
            return true;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void finishWriting(Mutation mutation)
    {
        logger.trace("witnessed local mutation {}", mutation.id());

        lock.writeLock().lock();
        try
        {
            int offset = mutation.id().offset();
            // we've raced with another write, no need to do anything else
            if (!getLocal().add(offset))
                return;

            unreconciledMutations.finishWriting(mutation);

            if (remoteReplicasWitnessed(offset))
            {
                reconciledOffsets.add(offset);
                unreconciledMutations.remove(offset);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private boolean othersWitnessed(int offset, int exceptHostId)
    {
        for (int i = 0; i < participants.size(); ++i)
        {
            int hostId = participants.get(i);
            if (hostId != exceptHostId && !getTransient(hostId).contains(offset))
                return false;
        }
        return true;
    }

    protected boolean remoteReplicasWitnessed(int offset)
    {
        return othersWitnessed(offset, localHostId);
    }

    /**
     * Look up unreconciled sequence ids of mutations witnessed by this host in this coordinataor log.
     * Adds the ids to the supplied collection, so it can be reused to aggregate lookups for multiple logs.
     */
    boolean collectOffsetsFor(Token token, TableId tableId, boolean includePending, Offsets.OffsetReciever unreconciledInto, Offsets.OffsetReciever reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledOffsets);
            return unreconciledMutations.collect(token, tableId, includePending, unreconciledInto);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Look up unreconciled sequence ids of mutations witnessed by this host in this coordinataor log.
     * Adds the ids to the supplied collection, so it can be reused to aggregate lookups for multiple logs.
     */
    boolean collectOffsetsFor(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets.OffsetReciever unreconciledInto, Offsets.OffsetReciever reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledOffsets);
            return unreconciledMutations.collect(range, tableId, includePending, unreconciledInto);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Collect the offsets in {@code remoteOffsets} that are missing from the local log.
     */
    void collectLocallyMissingMutations(Offsets remoteOffsets, Log2OffsetsMap.Mutable into)
    {
        lock.readLock().lock();
        try
        {
            into.add(Offsets.Immutable.difference(remoteOffsets, getLocal()));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    void collectRemotelyMissingMutations(Offsets localOffsets, IntArrayList remoteNodeIds, Node2OffsetsMap into)
    {
        lock.readLock().lock();
        try
        {
            remoteNodeIds.forEachInt(remoteNodeId -> {
                Offsets missing = Offsets.Immutable.difference(getTransient(remoteNodeId), localOffsets);
                if (!missing.isEmpty()) into.add(remoteNodeId, missing);
            });
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected Offsets.Mutable getTransient(int hostId)
    {
        return witnessedOffsets[participants.indexOf(hostId)];
    }

    protected Offsets.Mutable getDurable(int hostId)
    {
        return durableOffsets[participants.indexOf(hostId)];
    }

    private int localHostIdx()
    {
        return participants.indexOf(localHostId);
    }

    // TODO (expected): wire up durably reconciled offsets
    boolean isDurablyReconciled(CoordinatorLogOffsets<?> logOffsets)
    {
        lock.readLock().lock();
        try
        {
            // TODO: reconciledOffsets not necessarily durable, update once durability is implemented
            Offsets.RangeIterator durablyReconciled = reconciledOffsets.rangeIterator();
            Offsets.RangeIterator difference = Offsets.difference(logOffsets.offsets(logId.asLong()).rangeIterator(), durablyReconciled);
            return !difference.tryAdvance();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected Offsets.Mutable getLocal()
    {
        return witnessedOffsets[participants.indexOf(localHostId)];
    }

    @Override
    public String toString()
    {
        return "CoordinatorLog{" +
               "logId=" + logId +
               ", localHostId=" + localHostId +
               ", participants=" + participants +
               '}';
    }

    static class CoordinatorLogPrimary extends CoordinatorLog
    {
        private final AtomicLong sequenceId = new AtomicLong(-1);

        CoordinatorLogPrimary(
            String keyspace, Range<Token> range, int localHostId, CoordinatorLogId logId, Participants participants,
            Offsets.Mutable[] witnessedOffsets, Offsets.Mutable[] durableOffsets)
        {
            super(keyspace, range, localHostId, logId, participants, witnessedOffsets, durableOffsets);
        }

        CoordinatorLogPrimary(String keyspace, Range<Token> range, int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(keyspace, range, localHostId, logId, participants);
        }

        @Override
        void receivedWriteResponse(ShortMutationId mutationId, int fromHostId)
        {
            Preconditions.checkArgument(!mutationId.isNone());
            Preconditions.checkArgument(!Objects.equals(fromHostId, ClusterMetadata.current().myNodeId().id()));
            logger.trace("witnessed remote mutation {} from {}", mutationId, fromHostId);
            lock.writeLock().lock();
            try
            {
                if (!getTransient(fromHostId).add(mutationId.offset()))
                    return; // already witnessed; very uncommon but possible path

                if (!getLocal().contains(mutationId.offset()))
                    return; // local host hasn't witnessed yet -> no cleanup needed

                if (remoteReplicasWitnessed(mutationId.offset()))
                {
                    logger.trace("marking mutation {} as fully reconciled", mutationId);
                    // if all replicas have now witnessed the id, remove it from the index
                    unreconciledMutations.remove(mutationId.offset());
                    reconciledOffsets.add(mutationId.offset());
                }
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }

        @Nullable
        MutationId nextId()
        {
            long nextSequenceId = nextSequenceId();
            return nextSequenceId >= 0
                 ? new MutationId(logId.asLong(), nextSequenceId)
                 : null;
        }

        private long nextSequenceId()
        {
            while (true)
            {
                long prev = sequenceId.get();
                int prevOffset = MutationId.offset(prev);
                int prevTimestamp = MutationId.timestamp(prev);

                // int overflow
                if (prevOffset == Integer.MAX_VALUE)
                    return -1;

                int nextOffset = prevOffset + 1;
                int nextTimestamp = Math.max(prevTimestamp + 1, (int) (currentTimeMillis() / 1000L));
                long next = MutationId.sequenceId(nextOffset, nextTimestamp);

                if (sequenceId.compareAndSet(prev, next))
                    return next;
            }
        }
    }

    static class CoordinatorLogReplica extends CoordinatorLog
    {
        CoordinatorLogReplica(
            String keyspace, Range<Token> range, int localHostId, CoordinatorLogId logId, Participants participants,
            Offsets.Mutable[] witnessedOffsets, Offsets.Mutable[] durableOffsets)
        {
            super(keyspace, range, localHostId, logId, participants, witnessedOffsets, durableOffsets);
        }

        CoordinatorLogReplica(String keyspace, Range<Token> range, int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(keyspace, range, localHostId, logId, participants);
        }

        @Override
        void receivedWriteResponse(ShortMutationId mutationId, int fromHostId)
        {
            // no-op
        }
    }

    /*
     * Persist to / load from system table.
     */

    private static final String INSERT_QUERY =
        format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, host_id, host_log_id, participants, witnessed_offsets, durable_offsets) "
               + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    void persistToSystemTable()
    {
        Map<Integer, List<Integer>> witnessed;
        Map<Integer, List<Integer>> durable;
        lock.readLock().lock();
        try
        {
            witnessed = formatOffsets(witnessedOffsets);
            durable = formatOffsets(durableOffsets);
        }
        finally
        {
            lock.readLock().unlock();
        }
        executeInternal(INSERT_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId,
                        logId.hostLogId, participants.asSet(), witnessed, durable);
    }

    void updateLogsInSystemTable()
    {
        Offsets.Mutable localWitnessed;

        Map<Integer, List<Integer>> witnessed;
        Map<Integer, List<Integer>> durable;

        int localIdx = localHostIdx();

        lock.readLock().lock();
        try
        {
            localWitnessed = Offsets.Mutable.copy(witnessedOffsets[localIdx]);

            witnessed = formatOffsets(witnessedOffsets);
            durable = formatOffsets(durableOffsets);

            durable.put(localIdx, witnessed.get(localIdx));
        }
        finally
        {
            lock.readLock().unlock();
        }

        executeInternal(INSERT_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId,
                        logId.hostLogId, participants.asSet(), witnessed, durable);

        lock.writeLock().lock();
        try
        {
            durableOffsets[localIdx] = localWitnessed;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private static Map<Integer, List<Integer>> formatOffsets(Offsets.Mutable[] offsets)
    {
        Int2ObjectHashMap<List<Integer>> formatted = new Int2ObjectHashMap<>();
        for (int i = 0; i < offsets.length; i++)
            formatted.put(i, offsets[i].asList());
        return formatted;
    }

    private static final String SELECT_QUERY =
        format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND range_start = ? AND range_end = ?",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    static List<CoordinatorLog> loadFromSystemTable(String keyspace, Range<Token> range, int localHostId)
    {
        ArrayList<CoordinatorLog> logs = new ArrayList<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_QUERY, keyspace, range.left.toString(), range.right.toString()))
        {
            int hostId = row.getInt("host_id");
            int hostLogId = row.getInt("host_log_id");
            CoordinatorLogId logId = new CoordinatorLogId(hostId, hostLogId);
            Set<Integer> participants = row.getFrozenSet("participants", Int32Type.instance);
            Map<Integer, List<Integer>> witnessedOffsets =
                row.getMap("witnessed_offsets", Int32Type.instance, ListType.getInstance(Int32Type.instance, false));
            Map<Integer, List<Integer>> durableOffsets =
                row.getMap("durable_offsets", Int32Type.instance, ListType.getInstance(Int32Type.instance, false));
            CoordinatorLog log =
                CoordinatorLog.recreate(keyspace, range, localHostId, logId, new Participants(participants),
                                        parseOffsets(logId, witnessedOffsets), parseOffsets(logId, durableOffsets));
            logs.add(log);
        }
        return logs;
    }

    private static Offsets.Mutable[] parseOffsets(CoordinatorLogId logId, Map<Integer, List<Integer>> rawOffsets)
    {
        Offsets.Mutable[] offsets = new Offsets.Mutable[rawOffsets.size()];
        for (Map.Entry<Integer, List<Integer>> entry : rawOffsets.entrySet())
        {
            int idx = entry.getKey();
            Preconditions.checkState(idx < offsets.length);
            offsets[idx] = Offsets.fromList(logId, entry.getValue());
        }
        return offsets;
    }

    private static final String DELETE_QUERY =
        format("DELETE FROM %s.%s WHERE keyspace_name = ? AND range_start = ? AND range_end = ? AND host_id = ? AND host_log_id = ?",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    void deleteFromSystemTable()
    {
        executeInternal(DELETE_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId, logId.hostLogId);
    }
}
