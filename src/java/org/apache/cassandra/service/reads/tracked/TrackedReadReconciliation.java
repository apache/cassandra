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
package org.apache.cassandra.service.reads.tracked;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.cassandra.replication.ReconciliationPlan.PeerReconciliation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class TrackedReadReconciliation<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> implements ReadRepair<E, P>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedReadReconciliation.class);

    private static class Data extends AsyncFuture<Void>
    {
        final long reconciliationId;
        final ReadCommand command;
        final private InetAddressAndPort dataNode;
        final private TrackedReadResponse.Data dataResponse;
        final int blockFor;
        final Set<ShortMutationId> outstandingMutations;
        final Consumer<PartitionIterator> resultConsumer;
        final Map<ShortMutationId, Mutation> mutations = new HashMap<>();

        public Data(long reconciliationId, ReadCommand command,
                    InetAddressAndPort dataNode,
                    TrackedReadResponse.Data dataResponse,
                    Set<ShortMutationId> outstandingMutations,
                    Consumer<PartitionIterator> resultConsumer)
        {
            this.reconciliationId = reconciliationId;
            this.command = command;
            this.dataNode = dataNode;
            this.dataResponse = dataResponse;
            this.blockFor = outstandingMutations.size();
            this.outstandingMutations = Sets.newConcurrentHashSet(outstandingMutations);
            this.resultConsumer = resultConsumer;

            maybeComplete();
        }

        boolean isComplete()
        {
            return outstandingMutations.isEmpty();
        }

        int received()
        {
            return blockFor - outstandingMutations.size();
        }

        public PartitionIterator partitionIterator()
        {
            UnfilteredPartitionIterator result = dataResponse.makeIterator(command);
            if (!mutations.isEmpty())
                result = command.augmentResultWithMutations(result, mutations.values());

            return UnfilteredPartitionIterators.filter(result, command.nowInSec());
        }

        private void maybeComplete()
        {
            if (outstandingMutations.isEmpty())
            {
                logger.trace("Data reconciliation complete for {}", reconciliationId);
                resultConsumer.accept(partitionIterator());
                trySuccess(null);
            }
        }

        void receiveMutation(Mutation mutation)
        {
            Preconditions.checkArgument(!mutation.id().isNone());
            if (mutations.containsKey(mutation.id()))
            {
                logger.info("Received duplicate mutation for {}", mutation.id());
            }
            else if (outstandingMutations.contains(mutation.id()))
            {
                mutations.put(mutation.id(), mutation);
                outstandingMutations.remove(mutation.id());
                logger.trace("Received outstanding mutation {} for reconciliation {}", mutation.id(), reconciliationId);
                maybeComplete();
            }
            else
            {
                logger.info("Received unexpected mutation {}", mutation.id());
            }
        }
    }

    private static abstract class State
    {
        static State INITIALIZED = new State()
        {
            @Override
            String name()
            {
                return "INITIALIZED";
            }
        };

        boolean isInitialized()
        {
            return this == INITIALIZED;
        }

        boolean isPending()
        {
            return false;
        }

        Pending asPending()
        {
            throw new IllegalStateException("State is " + name() + ", not Pending");
        }

        boolean isComplete()
        {
            return false;
        }

        abstract String name();

        private static class Pending extends State
        {
            final long reconciliationId;
            final Map<InetAddressAndPort, ReconciliationPlan> plans;

            // mutable state
            final Map<Integer, PendingSync> pendingSync = new ConcurrentHashMap<>();
            final AsyncPromise<Void> future = new AsyncPromise<>();
            final int blockFor;
            final Data data;

            private Pending(long reconciliationId, Map<InetAddressAndPort, ReconciliationPlan> plans, ReadCommand command, InetAddressAndPort dataNode, TrackedReadResponse.Data dataResponse, Consumer<PartitionIterator> resultConsumer)
            {
                this.reconciliationId = reconciliationId;
                this.plans = plans;

                Set<ShortMutationId> outstandingMutations = new HashSet<>();
                int syncs = 0;
                int nextSyncId = 0;
                for (Map.Entry<InetAddressAndPort, ReconciliationPlan> entry : plans.entrySet())
                {
                    InetAddressAndPort from = entry.getKey();
                    ReconciliationPlan plan = entry.getValue();
                    for (InetAddressAndPort to : plan.nodes())
                    {
                        int syncId = nextSyncId++;
                        PendingSync sync = new PendingSync(syncId, from, to, plan.peerReconciliation(to), to.equals(dataNode));
                        pendingSync.put(syncId, sync);
                        syncs++;
                        if (sync.mirrorToCoordinator)
                        {
                            // TODO: should we use offsets here?
                            outstandingMutations.addAll(plan.idsFor(to));
                        }
                    }
                }

                this.blockFor = syncs;
                this.data = new Data(reconciliationId, command, dataNode, dataResponse, outstandingMutations, resultConsumer);
            }

            @Override
            boolean isPending()
            {
                return true;
            }

            @Override
            Pending asPending()
            {
                return this;
            }

            @Override
            String name()
            {
                return "PENDING";
            }

            void sendSyncMessages()
            {
                Map<InetAddressAndPort, List<ReadReconcileSend.PeerSync>> peerSync = new HashMap<>();
                pendingSync.values().forEach(pending -> {
                    peerSync.computeIfAbsent(pending.from, node -> new ArrayList<>()).add(pending.toPeerSync());
                });

                for (Map.Entry<InetAddressAndPort, List<ReadReconcileSend.PeerSync>> entry : peerSync.entrySet())
                {
                    Message<ReadReconcileSend> message = Message.out(Verb.READ_RECONCILE_SEND, new ReadReconcileSend(reconciliationId, entry.getValue()));
                    logger.trace("Sending read reconciliation {} to {}", message.payload, entry.getKey());
                    MessagingService.instance().send(message, entry.getKey());
                }
            }

            boolean isComplete()
            {
                return pendingSync.isEmpty() && data.isComplete();
            }

            State maybeComplete()
            {
                if (isComplete())
                {
                    logger.trace("Reconciliation completed: {}", reconciliationId);
                    future.trySuccess(null);
                    return new Complete(data);
                }
                return this;
            }

            public State acknowledgeSync(int syncId)
            {
                logger.trace("Reconciliation sync {} received for {}", syncId, reconciliationId);
                pendingSync.remove(syncId);
                return maybeComplete();
            }

            int received()
            {
                return blockFor - pendingSync.size();
            }

            /**
             *
             * @param mutations
             * @return true if reconciliation is now complete
             */
            public State addMutationsToRead(List<Mutation> mutations)
            {
                mutations.forEach(data::receiveMutation);
                return maybeComplete();
            }
        }

        private static class Complete extends State
        {
            final Data data;

            public Complete(Data data)
            {
                this.data = data;
            }

            @Override
            String name()
            {
                return "COMPLETE";
            }

            @Override
            boolean isComplete()
            {
                return true;
            }
        }
    }

    private static class PendingSync
    {
        final int syncId;
        final InetAddressAndPort from;
        final InetAddressAndPort to;
        final PeerReconciliation plan;
        final boolean mirrorToCoordinator;

        public PendingSync(int syncId, InetAddressAndPort from, InetAddressAndPort to, PeerReconciliation plan, boolean mirrorToCoordinator)
        {
            this.syncId = syncId;
            this.from = from;
            this.to = to;
            this.plan = plan;
            this.mirrorToCoordinator = mirrorToCoordinator;
        }

        public ReadReconcileSend.PeerSync toPeerSync()
        {
            return new ReadReconcileSend.PeerSync(syncId, to, plan, mirrorToCoordinator);
        }
    }

    private final ReadCommand command;
    private final ReplicaPlan.Shared<E, P> replicaPlan;
    private final Dispatcher.RequestTime requestTime;

    private State state = State.INITIALIZED;

    public TrackedReadReconciliation(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.requestTime = requestTime;
    }

    @Override
    public void startRepair(ResponseResolver<E, P> resolver, Consumer<PartitionIterator> resultConsumer)
    {
        if (!state.isInitialized())
        {
            throw new IllegalStateException("State is " + state.name() + ", not Initialized");
        }

        InetAddressAndPort dataNode = null;
        TrackedReadResponse.Data dataResponse = null;

        Map<InetAddressAndPort, MutationSummary> summaries = new HashMap<>();
        for (Message<IReadResponse> message : resolver.getMessages().snapshot())
        {
            TrackedReadResponse response = TrackedReadResponse.fromResponse(message.payload);
            summaries.put(message.from(), response.summary);
            if (dataResponse == null && response.isDataResponse())
            {
                Preconditions.checkState(dataNode == null);
                dataNode = message.from();
                dataResponse = response.asDataResponse();
            }
        }
        Preconditions.checkState(dataNode != null);
        Preconditions.checkState(dataResponse != null);

        Map<InetAddressAndPort, ReconciliationPlan> plans = ReconciliationPlan.calculateReconciliation(summaries);

        // the summaries were different, but after looking at the union of reconciled ids, there is nothing to do
        if (plans.isEmpty())
        {
            Data data = new Data(0, command, dataNode, dataResponse, Collections.emptySet(), resultConsumer);
            Preconditions.checkState(data.isComplete());
            state = new State.Complete(data);
            return;
        }

        ReadRepairMetrics.trackedReconcile.mark();
        ColumnFamilyStore.metricsFor(command.metadata().id).readRepairRequests.mark();

        long expiresAt = requestTime.computeDeadline(command.getTimeout(TimeUnit.NANOSECONDS));
        long reconciliationId = MutationTrackingService.instance.reconciliations().newReconciliation(this, expiresAt);
        logger.trace("New reconciliation {} with timeout {}", reconciliationId, command.timeoutNanos());

        state = new State.Pending(reconciliationId, plans, command, dataNode, dataResponse, resultConsumer);
        state.asPending().sendSyncMessages();
    }

    /**
     *
     * @param syncId
     * @return true if reconciliation is now complete
     */
    public synchronized boolean acknowledgeSync(int syncId)
    {
        if (state.isComplete())
            return true;

        state = state.asPending().acknowledgeSync(syncId);

        return state.isComplete();
    }

    /**
     *
     * @param mutations
     * @return true if reconciliation is now complete
     */
    public synchronized boolean addMutationsToRead(List<Mutation> mutations)
    {
        if (state.isComplete())
            return true;

        state = state.asPending().addMutationsToRead(mutations);

        return state.isComplete();
    }

    public static <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
    TrackedReadReconciliation<E, P> create(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        return new TrackedReadReconciliation<>(command, replicaPlan, requestTime);
    }

    @Override
    public void maybeSendAdditionalReads()
    {
        // TODO: if we haven't received all of the mutations we need to make a complete read response, send additional requests out
    }

    @Override
    public void awaitReads() throws ReadTimeoutException
    {
        Data data = null;
        synchronized (this)
        {
            if (!state.isPending())
                return;
            data = state.asPending().data;
        }

        try
        {
            if(data.awaitUntil(requestTime.computeDeadline(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS))))
                return;
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        // FIXME: received / block for won't make any sense to user, since they're referring to mutations expected/received
        throw new ReadTimeoutException(replicaPlan.get().consistencyLevel(), data.received(), data.blockFor, true);
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        // TODO: if we're behind, transmit unacked mutations to enough nodes to meet CL
    }

    @Override
    public void awaitWrites()
    {
        Data data = null;
        State.Pending pending = null;

        synchronized (this)
        {
            if (!state.isPending())
                return;

            pending = state.asPending();
            data = pending.data;
        }

        try
        {
            long deadline = requestTime.computeDeadline(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS));
            if(data.awaitUntil(deadline)
               && pending.future.awaitUntil(deadline)
               && pending.isComplete())
                return;
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        // FIXME: received / block for won't make any sense to user, since they're referring to mutations expected/received
        throw new ReadTimeoutException(replicaPlan.get().consistencyLevel(), data.received(), data.blockFor, true);
    }
}
