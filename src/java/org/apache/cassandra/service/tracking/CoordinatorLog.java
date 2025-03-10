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
package org.apache.cassandra.service.tracking;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public abstract class CoordinatorLog
{
    protected final int localHostId;
    protected final CoordinatorLogId logId;
    protected final Participants participants;

    /**
     * Id <-> token index for unreconciled mutation ids.
     */
    private final IdTokenIndex index;

    protected final SequenceIds[] witnessedIds;
    protected final SequenceIds reconciledIds;
    protected final ReadWriteLock lock;

    CoordinatorLog(int localHostId, CoordinatorLogId logId, Participants participants)
    {
        this.localHostId = localHostId;
        this.logId = logId;
        this.participants = participants;
        this.index = new IdTokenIndex();
        this.lock = new ReentrantReadWriteLock();

        SequenceIds[] ids = new SequenceIds[participants.size()];
        for (int i = 0; i < participants.size(); i++)
            ids[i] = new SequenceIds();
        witnessedIds = ids;
        reconciledIds = new SequenceIds();
    }

    static CoordinatorLog create(int localHostId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(localHostId, id, participants)
                                        : new CoordinatorLogReplica(localHostId, id, participants);
    }

    void witnessedMutationsRemote(SequenceIds ranges, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            // TODO (expected): implement index update logic once we have positions broadcasting going
            get(onHostId).addAll(ranges, (start, end) -> {});
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void witnessedMutationRemote(MutationId mutationId, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            if (!get(onHostId).add(mutationId.sequenceId()))
                return; // already witnessed

            if (!get(localHostId).contains(mutationId.sequenceId()))
                return; // local host hasn't witnessed -> hasn't indexed -> no index cleanup needed

            // see if any other replicas haven't witnessed the id yet
            boolean allOtherReplicasWitnessed = true;
            for (int i = 0; i < participants.size() && allOtherReplicasWitnessed; i++)
            {
                int hostId = participants.get(i);
                if (hostId != onHostId && hostId != localHostId && !get(hostId).contains(mutationId.sequenceId()))
                    allOtherReplicasWitnessed = false;
            }

            if (allOtherReplicasWitnessed)
            {
                // if all replicas have now witnessed the id, remove in from the index
                index.invalidate(mutationId.sequenceId());
                reconciledIds.add(mutationId.sequenceId());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void witnessedMutationLocal(MutationId mutationId, Token token)
    {
        lock.writeLock().lock();
        try
        {
            if (!get(localHostId).add(mutationId.sequenceId()))
                return; // already witnessed

            // see if any other replicas haven't witnessed the id yet
            boolean allOtherReplicasWitnessed = true;
            for (int i = 0; i < participants.size() && allOtherReplicasWitnessed; i++)
            {
                int hostId = participants.get(i);
                if (hostId != localHostId && !get(hostId).contains(mutationId.sequenceId()))
                    allOtherReplicasWitnessed = false;
            }

            if (!allOtherReplicasWitnessed)
            {
                // if some replicas also haven't witnessed the mutation yet, we should update the token index;
                // otherwise we are the last node to witness this mutation, and don't need to update the index
                index.update(mutationId.sequenceId(), token);
            }
            else
            {
                reconciledIds.add(mutationId.sequenceId());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void witnessedMutationLocal(MutationId mutationId, Mutation mutation)
    {
        witnessedMutationLocal(mutationId, mutation.key().getToken());
    }

    /**
     * Look up unreconciled sequence ids of mutations witnessed by this host in this coordinataor log.
     * Adds the ids to the supplied collection, so it can be reused to aggregate lookups for multiple logs.
     */
    boolean lookUpUnreconciled(Token token, SequenceIdSet unreconciled, SequenceIds reconciled)
    {
        lock.readLock().lock();
        try
        {
            reconciled.addAll(reconciledIds, (s, e) -> {});
            return index.lookUp(token, unreconciled);
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
    boolean lookUpUnreconciled(Range<Token> range, SequenceIdSet into, SequenceIds reconciled)
    {
        lock.readLock().lock();
        try
        {
            reconciled.addAll(reconciled, (s, e) -> {});
            return index.lookUp(range, into);
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
    boolean lookUpUnreconciled(AbstractBounds<PartitionPosition> range, SequenceIdSet into, SequenceIds reconciled)
    {
        lock.readLock().lock();
        try
        {
            reconciled.addAll(reconciled, (s, e) -> {});
            return index.lookUp(range, into);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected SequenceIds get(int hostId)
    {
        return witnessedIds[participants.indexOf(hostId)];
    }

    public static class CoordinatorLogPrimary extends CoordinatorLog
    {
        AtomicLong sequenceId = new AtomicLong(0);

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
                int nextTimestamp = Math.max(prevTimestamp, (int) currentTimeMillis() / 1000);
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
