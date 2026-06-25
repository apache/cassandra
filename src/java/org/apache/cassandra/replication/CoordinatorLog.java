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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.MutationTrackingMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.replication.Node2OffsetsMap.forParticipants;
import static org.apache.cassandra.replication.Node2OffsetsMap.fromPrimitiveMap;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public abstract class CoordinatorLog
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorLog.class);

    protected final int localNodeId;
    protected final String keyspace;
    protected final Range<Token> range;
    protected final CoordinatorLogId logId;
    protected final Participants participants;

    protected final Node2OffsetsMap witnessedOffsets;
    protected final Node2OffsetsMap persistedOffsets;

    protected final UnreconciledMutations unreconciledMutations;
    protected final Offsets.Mutable reconciledOffsets;
    protected final Offsets.Mutable reconciledPersistedOffsets;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    abstract void receivedWriteResponse(ShortMutationId mutationId, int fromNodeId);

    CoordinatorLog(String keyspace,
                   Range<Token> range,
                   int localNodeId,
                   CoordinatorLogId logId,
                   Participants participants,
                   Node2OffsetsMap witnessedOffsets,
                   Node2OffsetsMap persistedOffsets,
                   UnreconciledMutations unreconciledMutations)
    {
        this.localNodeId = localNodeId;
        this.keyspace = keyspace;
        this.range = range;
        this.logId = logId;
        this.participants = participants;
        this.unreconciledMutations = unreconciledMutations;
        this.witnessedOffsets = witnessedOffsets;
        this.reconciledOffsets = witnessedOffsets.intersection();
        this.persistedOffsets = persistedOffsets;
        this.reconciledPersistedOffsets = persistedOffsets.intersection();
    }

    CoordinatorLog(String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId logId, Participants participants)
    {
        this(keyspace, range, localNodeId, logId, participants, forParticipants(logId, participants), forParticipants(logId, participants), new UnreconciledMutations());
    }

    static CoordinatorLog create(String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localNodeId ? new CoordinatorLogPrimary(keyspace, range, localNodeId, id, participants)
                                        : new CoordinatorLogReplica(keyspace, range, localNodeId, id, participants);
    }

    static CoordinatorLog recreate(
        String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId id, Participants participants,
        Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations)
    {
        return id.hostId == localNodeId ? new CoordinatorLogPrimary(keyspace, range, localNodeId, id, participants, witnessedOffsets, persistedOffsets, unreconciledMutations)
                                        : new CoordinatorLogReplica(keyspace, range, localNodeId, id, participants, witnessedOffsets, persistedOffsets, unreconciledMutations);
    }

    abstract CoordinatorLog withUpdatedParticipants(Participants newParticipants, Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations);

    CoordinatorLog withParticipants(Participants newParticipants)
    {
        if (participants.equals(newParticipants))
            return this;

        lock.readLock().lock();
        try
        {
            Node2OffsetsMap newWitnessedOffsets = new Node2OffsetsMap();
            Node2OffsetsMap newPersistedOffsets = new Node2OffsetsMap();
            Offsets passivelyReconciled = null;
            for (int newIndex = 0; newIndex < newParticipants.size(); newIndex++)
            {
                int participantId = newParticipants.get(newIndex);

                Offsets.Mutable offsets;
                if (participants.contains(participantId))
                {
                    offsets = witnessedOffsets.get(participantId);
                }
                else
                {
                    offsets = new Offsets.Mutable(logId);

                    // the new node doesn't actually have these reconciled offsets yet, but they will receive them
                    // as part of the topology change. We preemptively mark them as reconciled here to prevent so
                    // we don't stream journal entries that the new node will receive in sstables and to prevent
                    // retroactively un-reconciling previously reconciled offsets for the other replicas.
                    offsets.addAll(reconciledOffsets);
                }
                Offsets.Mutable persisted = participants.contains(participantId)
                                                     ? persistedOffsets.get(participantId)
                                                     : new Offsets.Mutable(logId);
                passivelyReconciled = passivelyReconciled != null
                                      ? Offsets.Immutable.intersection(passivelyReconciled, offsets)
                                      : offsets;
                newWitnessedOffsets.add(participantId, offsets);
                newPersistedOffsets.add(participantId, persisted);
            }

            UnreconciledMutations newUnreconciledMutations;
            passivelyReconciled = Offsets.Immutable.difference(passivelyReconciled, reconciledOffsets);
            if (!passivelyReconciled.isEmpty())
            {
                logger.debug("Toplogy change implicitly reconciled offsets: {}", passivelyReconciled);
                newUnreconciledMutations = unreconciledMutations.copy();
                passivelyReconciled.forEach(id -> newUnreconciledMutations.remove(id.offset));
            }
            else
            {
                newUnreconciledMutations = unreconciledMutations;
            }

            if (logger.isTraceEnabled())
                logger.trace("Updating coordinator log {} participants: {} -> {}. Passively reconciled: {}",
                             logId, participants, newParticipants, passivelyReconciled);

            return withUpdatedParticipants(newParticipants, newWitnessedOffsets, newPersistedOffsets, newUnreconciledMutations);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    void updateReplicatedOffsets(Offsets offsets, boolean persisted, int onNodeId)
    {
        lock.writeLock().lock();
        try
        {
            // there may have been a topology change we're not yet aware of
            if (!participants.contains(onNodeId))
                return;

            if (persisted)
                updatePersistedReplicatedOffsets(offsets, onNodeId);
            else
                updateWitnessedReplicatedOffsets(offsets, onNodeId);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void updateWitnessedReplicatedOffsets(Offsets offsets, int onNodeId)
    {
        // Track newly-witnessed offsets from broadcasts (use array for lambda)
        int[] newlyWitnessedCount = {0};

        witnessedOffsets.get(onNodeId).addAll(offsets, (ignore, start, end) ->
        {
            // Count the newly-witnessed offsets in this range
            newlyWitnessedCount[0] += (end - start + 1);

            for (int offset = start; offset <= end; ++offset)
            {
                // TODO (desired): use the fact that Offsets are ordered to optimise this look up
                if (othersWitnessed(offset, onNodeId))
                {
                    reconciledOffsets.add(offset);
                    unreconciledMutations.remove(offset);
                }
                logger.trace("done applying WRO, now {}", witnessedOffsets);
            }
        });

        // Record metric for newly witnessed offsets only
        MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.inc(newlyWitnessedCount[0]);
    }

    private void updatePersistedReplicatedOffsets(Offsets offsets, int onNodeId)
    {
        persistedOffsets.get(onNodeId).addAll(offsets);
        logger.debug("done applying PO, now {}", persistedOffsets);
        reconciledPersistedOffsets.addAll(persistedOffsets.intersection());
        logger.debug("done applying PRO, now {}", reconciledPersistedOffsets);
    }

    public void recordFullyReconciledOffsets(Offsets.Immutable reconciled)
    {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < participants.size(); ++i)
            {
                int participant = participants.get(i);
                updateWitnessedReplicatedOffsets(reconciled, participant);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Nullable
    Offsets.Immutable collectReplicatedOffsets(boolean persisted)
    {
        lock.readLock().lock();
        try
        {
            Offsets offsets = persisted
                            ? persistedOffsets.get(localNodeId)
                            : witnessedOffsets.get(localNodeId);
            return offsets.isEmpty() ? null : Offsets.Immutable.copy(offsets);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the computed union of remote-witnessed offsets minus local-witnessed offsets
     */
    @Nullable
    Offsets.Immutable collectLocallyMissingOffsets()
    {
        lock.readLock().lock();
        try
        {
            Offsets.Mutable local = witnessedOffsets.get(localNodeId);
            Offsets.Immutable.Builder missing = null;
            for (int i = 0; i < participants.size(); i++)
            {
                int nodeId = participants.get(i);
                if (nodeId == localNodeId) continue;
                Offsets.Immutable diff = Offsets.Immutable.difference(witnessedOffsets.get(nodeId), local);
                if (!diff.isEmpty())
                {
                    if (missing == null)
                    {
                        missing = new Offsets.Immutable.Builder(logId);
                    }
                    missing.addAll(diff);
                }
            }
            return missing != null ? missing.build() : null;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    Offsets.Immutable collectReconciledOffsets()
    {
        lock.readLock().lock();
        try
        {
            return Offsets.Immutable.copy(reconciledOffsets);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the UNION of all witnessed offsets from all participants.
     * This represents all offsets that ANY replica has witnessed.
     */
    Offsets.Immutable collectUnionOfWitnessedOffsets()
    {
        lock.readLock().lock();
        try
        {
            return Offsets.Immutable.copy(witnessedOffsets.union());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the UNION of witnessed offsets scoped to only the specified host IDs.
     */
    Offsets.Immutable collectUnionOfWitnessedOffsets(Set<Integer> liveHostIds)
    {
        Offsets.Mutable union = new Offsets.Mutable(logId);
        lock.readLock().lock();
        try
        {
            for (int hostId : liveHostIds)
            {
                if (!participants.contains(hostId))
                    continue;

                Offsets.Mutable nodeOffsets = witnessedOffsets.get(hostId);
                union.addAll(nodeOffsets);
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
        return Offsets.Immutable.copy(union);
    }

    /**
     * Returns the intersection of witnessed offsets scoped to only the specified host IDs.
     */
    Offsets.Immutable collectReconciledOffsets(Set<Integer> liveHostIds)
    {
        lock.readLock().lock();
        try
        {
            Offsets.Mutable intersection = null;
            for (int hostId : liveHostIds)
            {
                if (!participants.contains(hostId))
                    continue;

                Offsets.Mutable nodeOffsets = witnessedOffsets.get(hostId);
                if (intersection == null)
                    intersection = Offsets.Mutable.copy(nodeOffsets);
                else
                    intersection = Offsets.Mutable.intersection(intersection, nodeOffsets);
            }
            return intersection == null ? new Offsets.Immutable(logId) : Offsets.Immutable.copy(intersection);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public long getUnreconciledCount()
    {
        lock.readLock().lock();
        try
        {
            return unreconciledMutations.size();
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
            if (witnessedOffsets.get(localNodeId).contains(mutation.id().offset()))
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
            if (!witnessedOffsets.get(localNodeId).add(offset))
                return;

            // Track write-time discovery of newly-witnessed offset
            MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.inc();

            unreconciledMutations.finishWriting(mutation);
            maybeMoveOffset(offset);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /*
    - On local replicas after they've completed activation (onHostId == me)
     */
    void finishActivation(Bounds<Token> bounds, ActivationRequest activation)
    {
        logger.trace("witnessed local transfer {}", activation.id());

        lock.writeLock().lock();
        try
        {
            int offset = activation.id().offset();
            // we've raced with another write, no need to do anything else
            if (!witnessedOffsets.get(localNodeId).add(offset))
                return;

            unreconciledMutations.activatedTransfer(activation.id(), bounds);
            maybeMoveOffset(offset);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private boolean othersWitnessed(int offset, int exceptNodeId)
    {
        for (int i = 0; i < participants.size(); ++i)
        {
            int nodeId = participants.get(i);
            if (nodeId != exceptNodeId && !witnessedOffsets.get(nodeId).contains(offset))
                return false;
        }
        return true;
    }

    protected boolean remoteReplicasWitnessed(int offset)
    {
        return othersWitnessed(offset, localNodeId);
    }

    private void maybeMoveOffset(int offset)
    {
        if (remoteReplicasWitnessed(offset))
        {
            reconciledOffsets.add(offset);
            unreconciledMutations.remove(offset);
        }
    }

    /*
    - On transfer coordinators after they've received a completed activation from a peer (onHostId != me)
    - On local replicas after coordinators have propagated their replicated offsets
    */
    void receivedActivationResponse(CoordinatedTransfer transfer, int onHostId)
    {
        ShortMutationId transferId = transfer.id();
        Preconditions.checkArgument(!transferId.isNone());
        logger.trace("witnessed transfer activation ack {} from {}", transferId, onHostId);
        lock.writeLock().lock();
        try
        {
            if (!witnessedOffsets.get(onHostId).add(transferId.offset()))
                return; // already witnessed; very uncommon but possible path

            if (!witnessedOffsets.get(localNodeId).contains(transferId.offset()))
                return; // local host hasn't witnessed yet -> no cleanup needed

            if (remoteReplicasWitnessed(transferId.offset()))
            {
                logger.trace("marking transfer {} as fully reconciled", transferId);
                // if all replicas have now witnessed the id, remove it from the index
                unreconciledMutations.remove(transferId.offset());
                reconciledOffsets.add(transferId.offset());
            }
        }
        finally
        {
            logger.trace("after receivedActivationAck {} witnessed by: {}", transferId, witnessedOffsets);
            lock.writeLock().unlock();
        }
    }

    /**
     * Look up unreconciled sequence ids of mutations witnessed by this host in this coordinataor log.
     * Adds the ids to the supplied collection, so it can be reused to aggregate lookups for multiple logs.
     */
    void collectOffsetsFor(Token token, TableId tableId, boolean includePending, Offsets.OffsetReciever unreconciledInto, Offsets.OffsetReciever reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledOffsets);
            unreconciledMutations.collect(token, tableId, includePending, unreconciledInto);
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
    void collectOffsetsFor(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets.OffsetReciever unreconciledInto, Offsets.OffsetReciever reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledOffsets);
            unreconciledMutations.collect(range, tableId, includePending, unreconciledInto);
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
            into.add(Offsets.Immutable.difference(remoteOffsets, witnessedOffsets.get(localNodeId)));
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
            remoteNodeIds.forEachInt(remoteNodeId ->
            {
                Offsets missing = Offsets.Immutable.difference(witnessedOffsets.get(remoteNodeId), localOffsets);
                if (!missing.isEmpty())
                    into.add(remoteNodeId, missing);
            });
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    boolean isDurablyReconciled(ShortMutationId id)
    {
        lock.readLock().lock();
        try
        {
            boolean contains = reconciledPersistedOffsets.contains(id.offset);
            if (!contains)
                logger.debug("Offset {} is not contained in durably reconciled offsets {}", id.offset, reconciledPersistedOffsets);
            return contains;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private boolean isDurablyReconciled(Iterator<? extends ShortMutationId> ids)
    {
        if (ids == null)
            return true;
        while (ids.hasNext())
        {
            ShortMutationId id = ids.next();
            if (id.logId() != logId.asLong())
                continue;
            if (!isDurablyReconciled(id))
                return false;
        }
        return true;
    }

    boolean isDurablyReconciled(CoordinatorLogOffsets<?> logOffsets)
    {
        lock.readLock().lock();
        try
        {
            Offsets.RangeIterator durablyReconciled = reconciledPersistedOffsets.rangeIterator();
            // Mutations only
            Offsets.RangeIterator offsets = logOffsets.mutations().offsets(logId.asLong()).rangeIterator();
            Offsets.RangeIterator unreconciledMutations = Offsets.difference(offsets, durablyReconciled);

            // Transfers
            boolean transfersReconciled = isDurablyReconciled(logOffsets.transfers().iterator());
            return transfersReconciled && !unreconciledMutations.tryAdvance();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public CoordinatorLogId getLogId()
    {
        return logId;
    }

    public DebugInfo getDebugState()
    {
        Map<Integer, List<Integer>> witnessed = new Int2ObjectHashMap<>();
        Map<Integer, List<Integer>> persisted = new Int2ObjectHashMap<>();
        String reconciledStr;

        lock.readLock().lock();
        try
        {
            witnessedOffsets.convertToPrimitiveMap(witnessed);
            persistedOffsets.convertToPrimitiveMap(persisted);
            reconciledStr = reconciledOffsets.toString();
        }
        finally
        {
            lock.readLock().unlock();
        }

        return new DebugInfo(witnessed.toString(), reconciledStr, persisted.toString());
    }

    @Override
    public String toString()
    {
        return "CoordinatorLog{" +
               "logId=" + logId +
               ", localNodeId=" + localNodeId +
               ", participants=" + participants +
               '}';
    }

    public static class DebugInfo
    {
        public final String witnessedOffsets;
        public final String reconciledOffsets;
        public final String persistedOffsets;

        private DebugInfo(String witnessedOffsets, String reconciledOffsets, String persistedOffsets)
        {
            this.witnessedOffsets = witnessedOffsets;
            this.reconciledOffsets = reconciledOffsets;
            this.persistedOffsets = persistedOffsets;
        }
    }

    static class CoordinatorLogPrimary extends CoordinatorLog
    {
        private final AtomicLong sequenceId = new AtomicLong(-1);

        CoordinatorLogPrimary(
            String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId logId, Participants participants,
            Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations)
        {
            super(keyspace, range, localNodeId, logId, participants, witnessedOffsets, persistedOffsets, unreconciledMutations);
        }

        CoordinatorLogPrimary(String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId logId, Participants participants)
        {
            super(keyspace, range, localNodeId, logId, participants);
        }

        @Override
        CoordinatorLog withUpdatedParticipants(Participants newParticipants, Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations)
        {
            CoordinatorLogPrimary next = new CoordinatorLogPrimary(keyspace, range, localNodeId, logId, newParticipants, witnessedOffsets, persistedOffsets, unreconciledMutations);
            next.sequenceId.set(sequenceId.get());
            return next;
        }

        @Override
        void receivedWriteResponse(ShortMutationId mutationId, int fromNodeId)
        {
            Preconditions.checkArgument(!mutationId.isNone());
            Preconditions.checkArgument(!Objects.equals(fromNodeId, ClusterMetadata.current().myNodeId().id()));
            logger.trace("witnessed remote mutation {} from {}", mutationId, fromNodeId);
            lock.writeLock().lock();
            try
            {
                if (!witnessedOffsets.get(fromNodeId).add(mutationId.offset()))
                    return; // already witnessed; very uncommon but possible path

                if (!witnessedOffsets.get(localNodeId).contains(mutationId.offset()))
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
                if (prevOffset == MAX_OFFSET)
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
            String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId logId, Participants participants,
            Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations)
        {
            super(keyspace, range, localNodeId, logId, participants, witnessedOffsets, persistedOffsets, unreconciledMutations);
        }

        CoordinatorLogReplica(String keyspace, Range<Token> range, int localNodeId, CoordinatorLogId logId, Participants participants)
        {
            super(keyspace, range, localNodeId, logId, participants);
        }

        @Override
        CoordinatorLog withUpdatedParticipants(Participants newParticipants, Node2OffsetsMap witnessedOffsets, Node2OffsetsMap persistedOffsets, UnreconciledMutations unreconciledMutations)
        {
            return new CoordinatorLogReplica(keyspace, range, localNodeId, logId, newParticipants, witnessedOffsets, persistedOffsets, unreconciledMutations);
        }

        @Override
        void receivedWriteResponse(ShortMutationId mutationId, int fromNodeId)
        {
            // no-op
        }
    }

    /*
     * Persist to / load from system table.
     */

    private static final String INSERT_QUERY =
        format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, host_id, host_log_id, participants, witnessed_offsets, persisted_offsets) "
               + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    void persistToSystemTable()
    {
        Map<Integer, List<Integer>> witnessed = new Int2ObjectHashMap<>();
        Map<Integer, List<Integer>> persisted = new Int2ObjectHashMap<>();

        lock.readLock().lock();
        try
        {
            witnessedOffsets.convertToPrimitiveMap(witnessed);
            persistedOffsets.convertToPrimitiveMap(persisted);
        }
        finally
        {
            lock.readLock().unlock();
        }
        executeInternal(INSERT_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId,
                        logId.hostLogId, participants.asSet(), witnessed, persisted);
    }

    void updateLogsInSystemTable()
    {
        Offsets.Mutable localWitnessed;
        Map<Integer, List<Integer>> witnessed = new Int2ObjectHashMap<>();
        Map<Integer, List<Integer>> persisted = new Int2ObjectHashMap<>();

        lock.readLock().lock();
        try
        {
            localWitnessed = Offsets.Mutable.copy(witnessedOffsets.get(localNodeId));

            witnessedOffsets.convertToPrimitiveMap(witnessed);
            persistedOffsets.convertToPrimitiveMap(persisted);

            persisted.put(localNodeId, witnessed.get(localNodeId));
        }
        finally
        {
            lock.readLock().unlock();
        }

        executeInternal(INSERT_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId,
                        logId.hostLogId, participants.asSet(), witnessed, persisted);

        lock.writeLock().lock();
        try
        {
            persistedOffsets.set(localNodeId, localWitnessed);
            reconciledPersistedOffsets.addAll(persistedOffsets.intersection());
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private static final String SELECT_QUERY =
        format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND range_start = ? AND range_end = ?",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    static List<CoordinatorLog> loadFromSystemTable(String keyspace, Range<Token> range, int localNodeId)
    {
        ArrayList<CoordinatorLog> logs = new ArrayList<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_QUERY, keyspace, range.left.toString(), range.right.toString()))
        {
            int nodeId = row.getInt("host_id");
            int hostLogId = row.getInt("host_log_id");
            CoordinatorLogId logId = new CoordinatorLogId(nodeId, hostLogId);
            Set<Integer> participants = row.getFrozenSet("participants", Int32Type.instance);
            Map<Integer, List<Integer>> witnessedOffsets =
                row.getMap("witnessed_offsets", Int32Type.instance, ListType.getInstance(Int32Type.instance, false));
            Map<Integer, List<Integer>> persistedOffsets =
                row.getMap("persisted_offsets", Int32Type.instance, ListType.getInstance(Int32Type.instance, false));
            Node2OffsetsMap witnessed = fromPrimitiveMap(logId, witnessedOffsets);
            Node2OffsetsMap persisted = fromPrimitiveMap(logId, persistedOffsets);
            UnreconciledMutations unreconciled = UnreconciledMutations.loadFromJournal(witnessed, localNodeId);
            CoordinatorLog log =
                CoordinatorLog.recreate(keyspace, range, localNodeId, logId, new Participants(participants), witnessed, persisted, unreconciled);
            logs.add(log);
        }
        return logs;
    }

    private static final String DELETE_QUERY =
        format("DELETE FROM %s.%s WHERE keyspace_name = ? AND range_start = ? AND range_end = ? AND host_id = ? AND host_log_id = ?",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    void deleteFromSystemTable()
    {
        executeInternal(DELETE_QUERY, keyspace, range.left.toString(), range.right.toString(), logId.hostId, logId.hostLogId);
    }

    @VisibleForTesting
    static void overrideMaxOffsetForTesting(int nexMaxOffset)
    {
        MAX_OFFSET = nexMaxOffset;
    }
    // don't make volatile unless it genuinely is an issue for some test,
    // otherwise it should be *fine* as is, and slight overkill to make volatile
    private static int MAX_OFFSET = Integer.MAX_VALUE;
}
