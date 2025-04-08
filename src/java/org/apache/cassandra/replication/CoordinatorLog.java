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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public abstract class CoordinatorLog
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorLog.class);

    protected final int localHostId;
    protected final CoordinatorLogId logId;
    protected final Participants participants;

    /**
     * State machines and an Id <-> token index for unreconciled mutation ids that exist oh this host.
     */
    private final LocalMutationStates unreconciledMutations;

    protected final Offsets[] witnessedIds;
    protected final Offsets reconciledIds;
    protected final ReadWriteLock lock;

    CoordinatorLog(int localHostId, CoordinatorLogId logId, Participants participants)
    {
        this.localHostId = localHostId;
        this.logId = logId;
        this.participants = participants;
        this.unreconciledMutations = new LocalMutationStates();
        this.lock = new ReentrantReadWriteLock();

        Offsets[] ids = new Offsets[participants.size()];
        for (int i = 0; i < participants.size(); i++)
            ids[i] = new Offsets(logId);

        witnessedIds = ids;
        reconciledIds = new Offsets(logId);
    }

    static CoordinatorLog create(int localHostId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(localHostId, id, participants)
                                        : new CoordinatorLogReplica(localHostId, id, participants);
    }

    void witnessedRemoteMutation(MutationId mutationId, int onHostId)
    {
        logger.trace("witnessed remote mutation {} from {}", mutationId, onHostId);
        lock.writeLock().lock();
        try
        {
            if (!get(onHostId).add(mutationId.offset()))
                return; // already witnessed

            if (!getLocal().contains(mutationId.offset()))
                return; // local host hasn't witnessed -> no cleanup needed

            // see if any other replicas haven't witnessed the id yet
            boolean allOtherReplicasWitnessed = true;
            for (int i = 0; i < participants.size() && allOtherReplicasWitnessed; i++)
            {
                int hostId = participants.get(i);
                if (hostId != onHostId && hostId != localHostId && !get(hostId).contains(mutationId.offset()))
                    allOtherReplicasWitnessed = false;
            }

            if (allOtherReplicasWitnessed)
            {
                logger.trace("marking mutation {} as fully reconciled", mutationId);
                // if all replicas have now witnessed the id, remove it from the index
                unreconciledMutations.remove(mutationId.offset());
                reconciledIds.add(mutationId.offset());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void startWriting(Mutation mutation)
    {
        lock.writeLock().lock();
        try
        {
            if (getLocal().contains(mutation.id().offset()))
                return; // already witnessed; shouldn't get to this path often (duplicate mutation)

            unreconciledMutations.startWriting(mutation);
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
            if (!getLocal().add(mutation.id().offset()))
                throw new IllegalStateException("finishWriting() called on a reconciled mutation");

            // see if any other replicas haven't witnessed the id yet
            boolean allOtherReplicasWitnessed = true;
            for (int i = 0; i < participants.size() && allOtherReplicasWitnessed; i++)
            {
                int hostId = participants.get(i);
                if (hostId != localHostId && !get(hostId).contains(mutation.id().offset()))
                    allOtherReplicasWitnessed = false;
            }

            // if some replicas also haven't witnessed the mutation yet, we should update local mutation state;
            // otherwise we are the last node to witness this mutation, and can clean it up
            if (allOtherReplicasWitnessed)
                reconciledIds.add(mutation.id().offset());
            else
                unreconciledMutations.finishWriting(mutation);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Look up unreconciled sequence ids of mutations witnessed by this host in this coordinataor log.
     * Adds the ids to the supplied collection, so it can be reused to aggregate lookups for multiple logs.
     */
    boolean collectOffsetsFor(Token token, TableId tableId, boolean includePending, Offsets unreconciledInto, Offsets reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledIds);
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
    boolean collectOffsetsFor(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets unreconciledInto, Offsets reconciledInto)
    {
        lock.readLock().lock();
        try
        {
            reconciledInto.addAll(reconciledIds);
            return unreconciledMutations.collect(range, tableId, includePending, unreconciledInto);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected Offsets get(int hostId)
    {
        return witnessedIds[participants.indexOf(hostId)];
    }

    protected Offsets getLocal()
    {
        return witnessedIds[participants.indexOf(localHostId)];
    }

    public static class CoordinatorLogPrimary extends CoordinatorLog
    {
        AtomicLong sequenceId = new AtomicLong(-1);

        CoordinatorLogPrimary(int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(localHostId, logId, participants);
        }

        MutationId nextId()
        {
            return new MutationId(logId.asLong(), nextSequenceId());
        }

        private long nextSequenceId()
        {
            while (true)
            {
                long prev = sequenceId.get();
                int prevOffset = MutationId.offset(prev);
                int prevTimestamp = MutationId.timestamp(prev);

                int nextOffset = prevOffset + 1;
                int nextTimestamp = Math.max(prevTimestamp + 1, (int) (currentTimeMillis() / 1000L));
                long next = MutationId.sequenceId(nextOffset, nextTimestamp);

                if (sequenceId.compareAndSet(prev, next))
                    return next;
            }
        }
    }

    public static class CoordinatorLogReplica extends CoordinatorLog
    {
        CoordinatorLogReplica(int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(localHostId, logId, participants);
        }
    }
}
