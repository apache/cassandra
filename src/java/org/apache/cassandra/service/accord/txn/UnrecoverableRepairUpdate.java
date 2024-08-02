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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Update;
import accord.api.Write;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.repair.BlockingReadRepair;

/**
 * This update is used to support blocking read repair from non-transactional Cassandra reads. Cassandra creates
 * a read repair mutation per node and this enables some partitiosn to be readable that would otherwise run into messages
 * size limits.
 *
 * This update is used during the `Execute` phase to apply the repair mutations directly in AccordInteropExecution similar
 * to how Accord applies read repair mutations for normal Accord transactions. It will always produce an empty update
 * for Accord to use in the Apply phase because Accord doesn't support a per replica Apply and adding it would be redundant
 * with the support that exists in AccordInteropExecution.
 *
 * The state for this update is always kept in memory and is never serialized. Only the Id is propagated so the cache
 * can evict the update and then load it back. We don't need to persist it or have it be recoverable because if the original
 * coordinator fails to complete the transaction then the dependent Cassandra read that triggered the read repair will
 * also fail and it doesn't matter if the read repair is partially applied or not applied at all.
 */
public class UnrecoverableRepairUpdate<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends AccordUpdate
{
    private static final ConcurrentHashMap<Key, UnrecoverableRepairUpdate> inflightUpdates = new ConcurrentHashMap<>();

    public static UnrecoverableRepairUpdate removeInflightUpdate(Key updateKey)
    {
        return inflightUpdates.remove(updateKey);
    }

    private static class Key
    {
        final int nodeId;
        final long counter;

        private Key(@Nonnull int nodeId, long counter)
        {
            this.nodeId = nodeId;
            this.counter = counter;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (nodeId != key.nodeId) return false;
            return counter == key.counter;
        }

        @Override
        public int hashCode()
        {
            int result = nodeId;
            result = 31 * result + (int) (counter ^ (counter >>> 32));
            return result;
        }
    }

    private static final AtomicLong nextCounter = new AtomicLong(0);

    public final BlockingReadRepair<E, P> parent;
    public final Seekables<?,?> keys;
    public final DecoratedKey dk;
    public final Map<Replica, Mutation> mutations;
    public final ReplicaPlan.ForWrite writePlan;
    public final Key updateKey;

    private UnrecoverableRepairUpdate(Node.Id nodeId, BlockingReadRepair<E, P> parent,
                                      Seekables<?, ?> keys, DecoratedKey dk, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        this.parent = parent;
        this.keys = keys;
        this.dk = dk;
        this.mutations = mutations;
        this.writePlan = writePlan;
        this.updateKey = new Key(nodeId.id, nextCounter.getAndIncrement());
    }

    public static <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> UnrecoverableRepairUpdate<E, P> create(Node.Id nodeId, BlockingReadRepair<E, P> parent,
                                                   Seekables<?, ?> keys, DecoratedKey dk, Map<Replica, Mutation> mutations,
                                                   ReplicaPlan.ForWrite writePlan)
    {
        UnrecoverableRepairUpdate<E, P> update = new UnrecoverableRepairUpdate<>(nodeId, parent, keys, dk, mutations, writePlan);
        inflightUpdates.put(update.updateKey, update);
        return update;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public Write apply(Timestamp executeAt, @Nullable Data data)
    {
        return TxnWrite.EMPTY_CONDITION_FAILED;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        return this;
    }

    @Override
    public Update intersecting(Participants<?> participants)
    {
        return this;
    }

    @Override
    public Update merge(Update other)
    {
        return this;
    }

    @Override
    public ConsistencyLevel cassandraCommitCL()
    {
        // Leads to standard async persist/commit which is fine since the repair mutations were applied
        // as part of execute/read
        return null;
    }

    @Override
    public Kind kind()
    {
        return Kind.UNRECOVERABLE_REPAIR;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        return 0;
    }

    public void runBRR(ReadCoordinator readCoordinator)
    {
        // This read repair is effectively running as a delegate of the read repair instance that did the reads
        // to generate the mutations, but since we already have the mutations we can go ahead and apply them
        // now that we are inside a transaction that guarantees that the contents of the mutations consist
        // of committed data everywhere we go to apply it
        parent.repairPartitionDirectly(readCoordinator, dk, mutations, writePlan);
    }

    public static final AccordUpdateSerializer<UnrecoverableRepairUpdate> serializer = new AccordUpdateSerializer<UnrecoverableRepairUpdate>()
    {
        @Override
        public void serialize(UnrecoverableRepairUpdate update, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(update.updateKey.nodeId);
            out.writeUnsignedVInt(update.updateKey.counter);
        }

        @Override
        public UnrecoverableRepairUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            return inflightUpdates.get(new Key(in.readUnsignedVInt32(), in.readUnsignedVInt()));
       }

        @Override
        public long serializedSize(UnrecoverableRepairUpdate update, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(update.updateKey.nodeId) + TypeSizes.sizeofUnsignedVInt(update.updateKey.counter);
        }
    };
}
