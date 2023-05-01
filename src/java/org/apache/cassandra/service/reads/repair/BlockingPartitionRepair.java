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

import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.repair.BlockingReadRepair.PendingPartitionRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public class BlockingPartitionRepair
        extends AsyncFuture<Object> implements RequestCallback<Object>, PendingPartitionRepair
{
    private final ReadCoordinator coordinator;
    private final DecoratedKey key;
    private final ReplicaPlan.ForReadRepair repairPlan;
    private final Map<Replica, Mutation> pendingRepairs;
    private final CountDownLatch latch;
    private final Predicate<InetAddressAndPort> shouldBlockOn;
    private final int blockFor;
    private volatile long mutationsSentTime;

    public BlockingPartitionRepair(ReadCoordinator coordinator, DecoratedKey key, Map<Replica, Mutation> repairs, ReplicaPlan.ForReadRepair repairPlan)
    {
        this(coordinator, key, repairs, repairPlan,
             repairPlan.consistencyLevel().isDatacenterLocal() ? InOurDc.endpoints() : Predicates.alwaysTrue());
    }

    @VisibleForTesting
    public BlockingPartitionRepair(ReadCoordinator coordinator, DecoratedKey key, Map<Replica, Mutation> repairs, ReplicaPlan.ForReadRepair repairPlan, Predicate<InetAddressAndPort> shouldBlockOn)
    {
        this.coordinator = coordinator;
        this.key = key;
        this.pendingRepairs = new ConcurrentHashMap<>(repairs);
        // Remove empty repair mutations from the block for total, since we're not sending them.
        // Besides, remote dcs can sometimes get involved in dc-local reads. We want to repair them if they do, but we
        // they shouldn't block for them.
        this.repairPlan = repairPlan.skipBlockingFor((r) -> shouldBlockOn.test(r.endpoint()) && !repairs.containsKey(r));
        this.shouldBlockOn = shouldBlockOn;
        this.blockFor = this.repairPlan.writeQuorum();

        // there are some cases where logically identical data can return different digests
        // For read repair, this would result in ReadRepairHandler being called with a map of
        // empty mutations. If we'd also speculated on either of the read stages, the number
        // of empty mutations would be greater than blockFor, causing the latch ctor to throw
        // an illegal argument exception due to a negative start value. So here we clamp it 0
        latch = newCountDownLatch(Math.max(blockFor, 0));
    }

    @Override
    public ReplicaPlan.ForReadRepair repairPlan()
    {
        return repairPlan;
    }

    @Override
    public int blockFor()
    {
        return blockFor;
    }

    @VisibleForTesting
    @Override
    public int waitingOn()
    {
        return latch.count();
    }

    @VisibleForTesting
    void ack(InetAddressAndPort from)
    {
        if (shouldBlockOn.test(from))
        {
            pendingRepairs.remove(repairPlan.lookup(from));
            latch.decrement();
        }
    }

    @Override
    public void onResponse(Message<Object> msg)
    {
        repairPlan.collectSuccess(msg.from());
        ack(msg.from());
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
    protected void sendRR(Message<Mutation> message, InetAddressAndPort endpoint)
    {
        coordinator.sendReadRepairMutation(message, endpoint, this);
    }

    public void sendInitialRepairs()
    {
        mutationsSentTime = nanoTime();
        Replicas.assertFull(pendingRepairs.keySet());

        for (Map.Entry<Replica, Mutation> entry: pendingRepairs.entrySet())
        {
            Replica destination = entry.getKey();
            Preconditions.checkArgument(destination.isFull(), "Can't send repairs to transient replicas: %s", destination);
            Mutation mutation = coordinator.maybeAllowOutOfRangeMutations(entry.getValue());
            TableId tableId = extractUpdate(mutation).metadata().id;

            Tracing.trace("Sending read-repair-mutation to {}", destination);
            // use a separate verb here to avoid writing hints on timeouts
            sendRR(Message.out(READ_REPAIR_REQ, mutation), destination.endpoint());
            ColumnFamilyStore.metricsFor(tableId).readRepairRequests.mark();

            if (!shouldBlockOn.test(destination.endpoint()))
                pendingRepairs.remove(destination);

            ReadRepairDiagnostics.sendInitialRepair(this, destination.endpoint(), mutation);
        }
    }

    @Override
    public boolean awaitRepairsUntil(long timeoutAt, TimeUnit timeUnit)
    {
        long timeoutAtNanos = timeUnit.toNanos(timeoutAt);
        long remaining = timeoutAtNanos - nanoTime();
        try
        {
            return latch.await(remaining, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    @Override
    public boolean awaitRepairs(long remaining, TimeUnit timeUnit) throws InterruptedException
    {
        return latch.await(remaining, timeUnit);
    }

    private static int msgVersionIdx(int version)
    {
        return version - MessagingService.minimum_version;
    }

    @Override
    public void maybeSendAdditionalWrites(long timeout, TimeUnit timeoutUnit)
    {
        if (awaitRepairsUntil(timeout + timeoutUnit.convert(mutationsSentTime, TimeUnit.NANOSECONDS), timeoutUnit))
            return;

        EndpointsForToken newCandidates = repairPlan.liveUncontacted();
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
            int versionIdx = msgVersionIdx(MessagingService.instance().versions.get(replica.endpoint()));

            Mutation mutation = versionedMutations[versionIdx];

            if (mutation == null)
            {
                mutation = BlockingReadRepairs.createRepairMutation(update, repairPlan.consistencyLevel(), replica.endpoint(), true);
                versionedMutations[versionIdx] = mutation;
            }

            if (mutation == null)
            {
                // the mutation is too large to send.
                ReadRepairDiagnostics.speculatedWriteOversized(this, replica.endpoint());
                continue;
            }

            mutation = coordinator.maybeAllowOutOfRangeMutations(mutation);
            Tracing.trace("Sending speculative read-repair-mutation to {}", replica);
            sendRR(Message.out(READ_REPAIR_REQ, mutation), replica.endpoint());
            ReadRepairDiagnostics.speculatedWrite(this, replica.endpoint(), mutation);
        }
    }

    Keyspace getKeyspace()
    {
        return repairPlan.keyspace();
    }

    DecoratedKey getKey()
    {
        return key;
    }

    ConsistencyLevel getConsistency()
    {
        return repairPlan.consistencyLevel();
    }
}
