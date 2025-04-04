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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public abstract class CoordinatorLog
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorLog.class);

    protected final int localHostId;
    protected final CoordinatorLogId logId;
    protected final Participants participants;

    protected final Offsets.Mutable[] witnessedOffsets;
    protected final Offsets.Mutable reconciledOffsets;

    protected final ReadWriteLock lock;

    abstract UnreconciledMutations unreconciledMutations();

    CoordinatorLog(int localHostId, CoordinatorLogId logId, Participants participants)
    {
        this.localHostId = localHostId;
        this.logId = logId;
        this.participants = participants;
        this.lock = new ReentrantReadWriteLock();

        Offsets.Mutable[] ids = new Offsets.Mutable[participants.size()];
        for (int i = 0; i < participants.size(); i++)
            ids[i] = new Offsets.Mutable(logId);

        witnessedOffsets = ids;
        reconciledOffsets = new Offsets.Mutable(logId);
    }

    static CoordinatorLog create(int localHostId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(localHostId, id, participants)
                                        : new CoordinatorLogReplica(localHostId, id, participants);
    }

    void receivedWriteResponse(MutationId mutationId, int onHostId)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        logger.trace("witnessed remote mutation {} from {}", mutationId, onHostId);
        lock.writeLock().lock();
        try
        {
            if (!get(onHostId).add(mutationId.offset()))
                return; // already witnessed; very uncommon but possible path

            if (!getLocal().contains(mutationId.offset()))
                return; // local host hasn't witnessed yet -> no cleanup needed

            if (hasWrittenToRemoteReplicas(mutationId.offset()))
            {
                logger.trace("marking mutation {} as fully reconciled", mutationId);
                // if all replicas have now witnessed the id, remove it from the index
                unreconciledMutations().remove(mutationId.offset());
                reconciledOffsets.add(mutationId.offset());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void updateReplicatedOffsets(Offsets offsets, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            get(onHostId).addAll(offsets, (ignore, start, end) ->
            {
                for (int offset = start; offset <= end; ++offset)
                {
                    // TODO (desired): skip checking the host's offsets - all just added
                    // TODO (desired): use the fact that Offsets are ordered to optimise this look up
                    if (hasWrittenLocally(offset) && hasWrittenToRemoteReplicas(offset))
                    {
                        reconciledOffsets.add(offset);
                        unreconciledMutations().remove(offset);
                    }
                }
            });
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Nullable
    Offsets.Immutable collectReplicatedOffsets()
    {
        lock.readLock().lock();
        try
        {
            Offsets offsets = witnessedOffsets[participants.indexOf(localHostId)];
            return offsets.isEmpty() ? null : Offsets.Immutable.copy(offsets);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    void startWriting(Mutation mutation)
    {
        lock.writeLock().lock();
        try
        {
            if (getLocal().contains(mutation.id().offset()))
                return; // already witnessed; shouldn't get to this path often (duplicate mutation)

            unreconciledMutations().startWriting(mutation);
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
            if (!getLocal().add(offset))
                throw new IllegalStateException("finishWriting() called on a locally witnessed mutation " + mutation.id());

            unreconciledMutations().finishWriting(mutation);

            if (hasWrittenToRemoteReplicas(offset))
            {
                reconciledOffsets.add(offset);
                unreconciledMutations().remove(offset);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private boolean hasWrittenLocally(int offset)
    {
        return getLocal().contains(offset);
    }

    private boolean hasWrittenToRemoteReplicas(int offset)
    {
        for (int i = 0; i < participants.size(); ++i)
        {
            int hostId = participants.get(i);
            if (hostId != localHostId && !get(hostId).contains(offset))
                return false;
        }
        return true;
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
            return unreconciledMutations().collect(token, tableId, includePending, unreconciledInto);
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
            return unreconciledMutations().collect(range, tableId, includePending, unreconciledInto);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected Offsets.Mutable get(int hostId)
    {
        return witnessedOffsets[participants.indexOf(hostId)];
    }

    protected Offsets.Mutable getLocal()
    {
        return witnessedOffsets[participants.indexOf(localHostId)];
    }

    public static class CoordinatorLogPrimary extends CoordinatorLog
    {
        private final AtomicLong sequenceId = new AtomicLong(-1);
        private final UnreconciledMutationsReplica unreconciledMutations;

        CoordinatorLogPrimary(int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(localHostId, logId, participants);
            unreconciledMutations = new UnreconciledMutationsReplica();
        }

        @Override
        UnreconciledMutationsReplica unreconciledMutations()
        {
            return unreconciledMutations;
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
        private final UnreconciledMutationsReplica unreconciledMutations;

        CoordinatorLogReplica(int localHostId, CoordinatorLogId logId, Participants participants)
        {
            super(localHostId, logId, participants);
            this.unreconciledMutations = new UnreconciledMutationsReplica();
        }

        @Override
        UnreconciledMutationsReplica unreconciledMutations()
        {
            return unreconciledMutations;
        }
    }
}
