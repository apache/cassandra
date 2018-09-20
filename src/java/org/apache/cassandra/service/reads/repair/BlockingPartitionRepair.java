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

package org.apache.cassandra.service.reads.repair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.InOurDcTester;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.Tracing;

public class BlockingPartitionRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>>
        extends AbstractFuture<Object> implements IAsyncCallback<Object>
{
    private final DecoratedKey key;
    private final P replicaPlan;
    private final Map<Replica, Mutation> pendingRepairs;
    private final CountDownLatch latch;
    private final Predicate<InetAddressAndPort> shouldBlockOn;

    private volatile long mutationsSentTime;

    public BlockingPartitionRepair(DecoratedKey key, Map<Replica, Mutation> repairs, int maxBlockFor, P replicaPlan)
    {
        this(key, repairs, maxBlockFor, replicaPlan,
                replicaPlan.consistencyLevel().isDatacenterLocal() ? InOurDcTester.endpoints() : Predicates.alwaysTrue());
    }
    public BlockingPartitionRepair(DecoratedKey key, Map<Replica, Mutation> repairs, int maxBlockFor, P replicaPlan, Predicate<InetAddressAndPort> shouldBlockOn)
    {
        this.key = key;
        this.pendingRepairs = new ConcurrentHashMap<>(repairs);
        this.replicaPlan = replicaPlan;
        this.shouldBlockOn = shouldBlockOn;

        // here we remove empty repair mutations from the block for total, since
        // we're not sending them mutations
        int blockFor = maxBlockFor;
        for (Replica participant: replicaPlan.contacts())
        {
            // remote dcs can sometimes get involved in dc-local reads. We want to repair
            // them if they do, but they shouldn't interfere with blocking the client read.
            if (!repairs.containsKey(participant) && shouldBlockOn.test(participant.endpoint()))
                blockFor--;
        }

        // there are some cases where logically identical data can return different digests
        // For read repair, this would result in ReadRepairHandler being called with a map of
        // empty mutations. If we'd also speculated on either of the read stages, the number
        // of empty mutations would be greater than blockFor, causing the latch ctor to throw
        // an illegal argument exception due to a negative start value. So here we clamp it 0
        latch = new CountDownLatch(Math.max(blockFor, 0));
    }

    @VisibleForTesting
    long waitingOn()
    {
        return latch.getCount();
    }

    @VisibleForTesting
    void ack(InetAddressAndPort from)
    {
        if (shouldBlockOn.test(from))
        {
            pendingRepairs.remove(replicaPlan.getReplicaFor(from));
            latch.countDown();
        }
    }

    @Override
    public void response(MessageIn<Object> msg)
    {
        ack(msg.from);
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    private static PartitionUpdate extractUpdate(Mutation mutation)
    {
        return Iterables.getOnlyElement(mutation.getPartitionUpdates());
    }

    /**
     * Combine the contents of any unacked repair into a single update
     */
    private PartitionUpdate mergeUnackedUpdates()
    {
        // recombinate the updates
        List<PartitionUpdate> updates = Lists.newArrayList(Iterables.transform(pendingRepairs.values(), BlockingPartitionRepair::extractUpdate));
        return updates.isEmpty() ? null : PartitionUpdate.merge(updates);
    }

    @VisibleForTesting
    protected void sendRR(MessageOut<Mutation> message, InetAddressAndPort endpoint)
    {
        MessagingService.instance().sendRR(message, endpoint, this);
    }

    public void sendInitialRepairs()
    {
        mutationsSentTime = System.nanoTime();
        Replicas.assertFull(pendingRepairs.keySet());

        for (Map.Entry<Replica, Mutation> entry: pendingRepairs.entrySet())
        {
            Replica destination = entry.getKey();
            Preconditions.checkArgument(destination.isFull(), "Can't send repairs to transient replicas: %s", destination);
            Mutation mutation = entry.getValue();
            TableId tableId = extractUpdate(mutation).metadata().id;

            Tracing.trace("Sending read-repair-mutation to {}", destination);
            // use a separate verb here to avoid writing hints on timeouts
            sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR), destination.endpoint());
            ColumnFamilyStore.metricsFor(tableId).readRepairRequests.mark();

            if (!shouldBlockOn.test(destination.endpoint()))
                pendingRepairs.remove(destination);
            ReadRepairDiagnostics.sendInitialRepair(this, destination.endpoint(), mutation);
        }
    }

    public boolean awaitRepairs(long timeout, TimeUnit timeoutUnit)
    {
        long elapsed = System.nanoTime() - mutationsSentTime;
        long remaining = timeoutUnit.toNanos(timeout) - elapsed;

        try
        {
            return latch.await(remaining, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static int msgVersionIdx(int version)
    {
        return version - MessagingService.minimum_version;
    }

    /**
     * If it looks like we might not receive acks for all the repair mutations we sent out, combine all
     * the unacked mutations and send them to the minority of nodes not involved in the read repair data
     * read / write cycle. We will accept acks from them in lieu of acks from the initial mutations sent
     * out, so long as we receive the same number of acks as repair mutations transmitted. This prevents
     * misbehaving nodes from killing a quorum read, while continuing to guarantee monotonic quorum reads
     */
    public void maybeSendAdditionalWrites(long timeout, TimeUnit timeoutUnit)
    {
        if (awaitRepairs(timeout, timeoutUnit))
            return;

        E newCandidates = replicaPlan.uncontactedCandidates();
        if (newCandidates.isEmpty())
            return;

        PartitionUpdate update = mergeUnackedUpdates();
        if (update == null)
            // final response was received between speculate
            // timeout and call to get unacked mutation.
            return;

        ReadRepairMetrics.speculatedWrite.mark();

        Mutation[] versionedMutations = new Mutation[msgVersionIdx(MessagingService.current_version) + 1];

        for (Replica replica : newCandidates)
        {
            int versionIdx = msgVersionIdx(MessagingService.instance().getVersion(replica.endpoint()));

            Mutation mutation = versionedMutations[versionIdx];

            if (mutation == null)
            {
                mutation = BlockingReadRepairs.createRepairMutation(update, replicaPlan.consistencyLevel(), replica.endpoint(), true);
                versionedMutations[versionIdx] = mutation;
            }

            if (mutation == null)
            {
                // the mutation is too large to send.
                ReadRepairDiagnostics.speculatedWriteOversized(this, replica.endpoint());
                continue;
            }

            Tracing.trace("Sending speculative read-repair-mutation to {}", replica);
            sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR), replica.endpoint());
            ReadRepairDiagnostics.speculatedWrite(this, replica.endpoint(), mutation);
        }
    }

    Keyspace getKeyspace()
    {
        return replicaPlan.keyspace();
    }

    DecoratedKey getKey()
    {
        return key;
    }

    ConsistencyLevel getConsistency()
    {
        return replicaPlan.consistencyLevel();
    }
}
