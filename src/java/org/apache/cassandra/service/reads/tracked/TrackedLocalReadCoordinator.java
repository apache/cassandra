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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MultiOffsets;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TrackedLocalReadCoordinator extends AsyncPromise<TrackedDataResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedLocalReadCoordinator.class);

    private final TrackedRead.Id readId;

    private static class ReceivedSummary
    {
        final InetAddressAndPort from;
        final MutationSummary summary;

        ReceivedSummary(InetAddressAndPort from, MutationSummary summary)
        {
            this.from = from;
            this.summary = summary;
        }

        static ReceivedSummary create(InetAddressAndPort from, MutationSummary summary)
        {
            return new ReceivedSummary(from, summary);
        }
    }

    private static abstract class State
    {
        static final State INITIALIZED = new State()
        {
            @Override
            String name() { return "INITIALIZED"; }

            @Override
            boolean isInitialized() { return true; }

            @Override
            boolean isTimedOutOrComplete(long nanoTime) { return false; }

            @Override
            void abort() {}
        };

        static final State ABORTED = new State()
        {
            @Override
            String name() { return "ABORTED"; }

            @Override
            boolean isAborted() { return true; }

            @Override
            boolean isTimedOutOrComplete(long nanoTime) { return true; }

            @Override
            void abort() {}
        };

        static final State COMPLETED = new State()
        {
            @Override
            String name() { return "COMPLETED"; }

            @Override
            boolean isTimedOutOrComplete(long nanoTime) { return true; }

            @Override
            boolean isComplete() { return true; }

            @Override
            void abort() {}
        };

        abstract String name();
        abstract boolean isTimedOutOrComplete(long nanoTime);
        abstract void abort();

        boolean isInitialized()
        {
            return false;
        }

        boolean isAwaitingRead()
        {
            return false;
        }

        AwaitingRead asAwaitingRead()
        {
            throw new IllegalStateException("State is " + name() + ", not " + AwaitingRead.NAME);
        }

        boolean isReading()
        {
            return false;
        }

        Reading asReading()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Reading.NAME);
        }

        boolean isReconciling()
        {
            return false;
        }

        Reconciling asReconciling()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Reconciling.NAME);
        }

        boolean isComplete()
        {
            return false;
        }

        boolean isAborted()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return name();
        }
    }

    // if we start receiving summaries before we receive the read command, they're
    // collected here
    private class AwaitingRead extends State
    {
        private static final String NAME = "AWAITING_READ";
        private long lastUpdateNanos;
        private final List<ReceivedSummary> summaries = new ArrayList<>();

        public AwaitingRead()
        {
        }

        @Override
        String name()
        {
            return NAME;
        }

        @Override
        boolean isAwaitingRead()
        {
            return true;
        }

        @Override
        AwaitingRead asAwaitingRead()
        {
            return this;
        }

        @Override
        boolean isTimedOutOrComplete(long nanoTime)
        {
            return nanoTime - lastUpdateNanos > DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS);
        }

        public State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            summaries.add(ReceivedSummary.create(from, summary));
            lastUpdateNanos = Clock.Global.nanoTime();
            return this;
        }

        @Override
        void abort()
        {

        }
    }

    private class Reading extends State
    {
        private static final String NAME = "READING";

        private final ReadCommand command;
        private volatile PartialTrackedRead read;
        private final long expiresAtNanos;
        private final ReplicaPlan.AbstractForRead<?, ?> replicaPlan;
        private final Accumulator<ReceivedSummary> summaries;
        private final Set<InetAddressAndPort> summaryNodes; // for speculating when we haven't received enough summaries

        public Reading(ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, Set<InetAddressAndPort> summaryNodes, long expiresAtNanos)
        {
            this.expiresAtNanos = expiresAtNanos;
            this.command = command;
            this.replicaPlan = replicaPlan;
            this.summaries = new Accumulator<>(replicaPlan.readCandidates().size());
            this.summaryNodes = summaryNodes;
        }

        @Override
        boolean isReading()
        {
            return true;
        }

        @Override
        Reading asReading()
        {
            return this;
        }

        @Override
        String name()
        {
            return NAME;
        }

        @Override
        boolean isTimedOutOrComplete(long nanoTime)
        {
            return nanoTime - expiresAtNanos > 0;
        }

        private State maybeComplete()
        {
            if (read == null || summaries.size() < replicaPlan.readQuorum())
                return this;

            Map<InetAddressAndPort, MutationSummary> summaryMap = new HashMap<>();
            summaries.snapshot().forEach(rc -> summaryMap.put(rc.from, rc.summary));

            Map<InetAddressAndPort, ReconciliationPlan> reconciliations = ReconciliationPlan.calculateReconciliation(summaryMap);

            if (reconciliations.isEmpty())
            {
                logger.trace("Read complete for {}", readId);
                complete(read, command.columnFilter(), replicaPlan.consistencyLevel(), expiresAtNanos);
                return COMPLETED;
            }
            else
            {
                logger.trace("Beginning reconciliation for {}", readId);
                Reconciling reconciling = new Reconciling(command, read, replicaPlan.consistencyLevel(), expiresAtNanos, reconciliations);
                reconciling.start();  // TODO: don't do this until after the coordinator state is set to reconciling if converting to lock free
                return reconciling;
            }
        }

        public State receiveInProgressRead(PartialTrackedRead read, MutationSummary summary)
        {
            if (this.read != null)
                return this;

            logger.trace("In progress read received for {}", readId);
            this.read = read;
            summaries.add(ReceivedSummary.create(FBUtilities.getBroadcastAddressAndPort(), summary));

            return maybeComplete();
        }

        public State receiveSummary(ReceivedSummary summary)
        {
            logger.trace("Summary received from {} for {}", summary.from, readId);
            summaries.add(summary);
            return maybeComplete();
        }

        public State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            return receiveSummary(ReceivedSummary.create(from, summary));
        }

        @Override
        void abort()
        {
            logger.trace("Aborting read {}", readId);
            if (read != null)
                read.close();
        }
    }

    private static class PendingSync
    {
        final int syncId;
        final InetAddressAndPort from;
        final InetAddressAndPort to;
        final MultiOffsets.Immutable plan;

        public PendingSync(int syncId, InetAddressAndPort from, InetAddressAndPort to, MultiOffsets.Immutable plan)
        {
            this.syncId = syncId;
            this.from = from;
            this.to = to;
            this.plan = plan;
        }

        public ReadReconcileSend.PeerSync toPeerSync()
        {
            return new ReadReconcileSend.PeerSync(syncId, to, plan);
        }
    }

    private class Reconciling extends State
    {
        private static final String NAME = "RECONCILING";

        private final ReadCommand command;
        private final PartialTrackedRead read;
        private final ConsistencyLevel consistencyLevel;
        private final long expiresAtNanos;

        final Map<InetAddressAndPort, ReconciliationPlan> plans;
        final MultiOffsets.Mutable outstandingMutations = new MultiOffsets.Mutable();
        final Map<Integer, PendingSync> pendingSync = new ConcurrentHashMap<>();
        final int blockFor;

        public Reconciling(ReadCommand command, PartialTrackedRead read, ConsistencyLevel consistencyLevel, long expiresAtNanos, Map<InetAddressAndPort, ReconciliationPlan> plans)
        {
            this.command = command;
            this.consistencyLevel = consistencyLevel;
            Preconditions.checkNotNull(read);
            this.read = read;
            this.expiresAtNanos = expiresAtNanos;

            this.plans = plans;

            int syncs = 0;
            int nextSyncId = 0;
            for (Map.Entry<InetAddressAndPort, ReconciliationPlan> entry : plans.entrySet())
            {
                InetAddressAndPort from = entry.getKey();
                ReconciliationPlan plan = entry.getValue();
                for (InetAddressAndPort to : plan.nodes())
                {
                    int syncId = nextSyncId++;
                    PendingSync sync = new PendingSync(syncId, from, to, plan.peerReconciliation(to));
                    pendingSync.put(syncId, sync);
                    syncs++;
                    if (to.equals(FBUtilities.getBroadcastAddressAndPort()))
                    {
                        outstandingMutations.addAll(plan.offsetsFor(to));
                    }
                }
            }

            logger.trace("Reconciling {} syncs, {} mutations for {}", syncs, outstandingMutations.idCount(), readId);
            this.blockFor = syncs;
        }

        @Override
        String name()
        {
            return NAME;
        }

        @Override
        boolean isReconciling()
        {
            return true;
        }

        @Override
        Reconciling asReconciling()
        {
            return this;
        }

        @Override
        boolean isTimedOutOrComplete(long nanoTime)
        {
            return nanoTime - expiresAtNanos > 0;
        }

        void start()
        {
            ReadRepairMetrics.trackedReconcile.mark();
            ColumnFamilyStore.metricsFor(command.metadata().id).readRepairRequests.mark();

            Map<InetAddressAndPort, List<ReadReconcileSend.PeerSync>> peerSync = new HashMap<>();
            pendingSync.values().forEach(pending -> {
                peerSync.computeIfAbsent(pending.from, node -> new ArrayList<>()).add(pending.toPeerSync());
            });

            for (Map.Entry<InetAddressAndPort, List<ReadReconcileSend.PeerSync>> entry : peerSync.entrySet())
            {
                Message<ReadReconcileSend> message = Message.out(Verb.READ_RECONCILE_SEND, new ReadReconcileSend(readId, entry.getValue()));
                logger.trace("Sending read reconciliation for {} {} to {}", readId, message.payload, entry.getKey());
                MessagingService.instance().send(message, entry.getKey());
            }
        }

        private State maybeComplete()
        {
            if (!pendingSync.isEmpty() || !outstandingMutations.isEmpty())
                return this;

            if (logger.isTraceEnabled())
                logger.trace("Reconciliation completed for read {}", readId);

            complete(read, command.columnFilter(), consistencyLevel, expiresAtNanos);
            return COMPLETED;
        }

        public State acknowledgeSync(int syncId)
        {
            if (logger.isTraceEnabled())
                logger.trace("Reconciliation sync {} received for {}", syncId, readId);
            pendingSync.remove(syncId);
            return maybeComplete();
        }

        int received()
        {
            return blockFor - pendingSync.size();
        }

        State receiveMutations(List<Mutation> mutations)
        {
            MultiOffsets.Mutable received = new MultiOffsets.Mutable();
            mutations.forEach(mutation -> {
                if (logger.isTraceEnabled())
                    logger.trace("Received mutation {} for read {}", mutation.id(), readId);
                received.add(mutation.id());
            });
            outstandingMutations.removeAll(received);

            if (logger.isTraceEnabled())
                logger.trace("Received {} mutations, {} mutations outstanding for {}", mutations.size(), outstandingMutations.idCount(), readId);
            read.augment(mutations);
            return maybeComplete();
        }

        @Override
        void abort()
        {
            read.close();
        }
    }

    private State state = State.INITIALIZED;

    public TrackedLocalReadCoordinator(TrackedRead.Id readId)
    {
        this.readId = readId;
    }

    @Override
    public String toString()
    {
        return "TrackedLocalReadCoordinator{" + readId + ':' + state.name() + '}';
    }

    public TrackedRead.Id readId()
    {
        return readId;
    }

    @VisibleForTesting
    public static void processDelta(PartialTrackedRead read, MutationSummary initialSummary, MutationSummary secondarySummary)
    {
        // Compute any mutations that we could've missed during initial read execution.
        ArrayList<ShortMutationId> delta = new ArrayList<>();
        MutationSummary.difference(secondarySummary, initialSummary, delta);

        delta.forEach(mutationId -> {
            Mutation mutation = MutationJournal.instance.read(mutationId);
            Preconditions.checkNotNull(mutation);
            read.augment(mutation);
        });
    }

    public void startLocalRead(ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, Set<InetAddressAndPort> summaryNodes, long expiresAtNanos, Consumer<PartialTrackedRead> partialReadConsumer)
    {
        Reading reading;
        synchronized (this)
        {
            if (!state.isInitialized() && !state.isAwaitingRead())
                return;

            AwaitingRead awaitingRead = state.isAwaitingRead() ? state.asAwaitingRead() : null;
            reading = new Reading(command, replicaPlan, summaryNodes, expiresAtNanos);
            state = reading;
            if (awaitingRead != null)
            {
                for (ReceivedSummary summary : awaitingRead.summaries)
                {
                    if (state.isReading())
                        state = state.asReading().receiveSummary(summary);
                }
            }
        }

        PartialTrackedRead read;
        MutationSummary secondarySummary;

        MutationSummary initialSummary = command.createMutationSummary(false);
        ReadExecutionController controller = command.executionController(false);
        try
        {
            read = command.beginTrackedRead(controller);
            if (partialReadConsumer != null)
                partialReadConsumer.accept(read);
            // Create another summary once initial data has been read fully. We do this to catch
            // any mutations that may have arrived during initial read execution.
            secondarySummary = command.createMutationSummary(true);
            processDelta(read, initialSummary, secondarySummary);
        }
        catch (Exception e)
        {
            controller.close();
            abort();
            throw e;
        }

        synchronized (this)
        {
            // the read can't complete without data, but it could have been aborted in the mean time
            if (!state.isAborted())
                state = state.asReading().receiveInProgressRead(read, secondarySummary);
        }
    }

    public synchronized void receiveSummary(InetAddressAndPort from, MutationSummary summary)
    {
        if (logger.isTraceEnabled())
            logger.trace("Received summary {} from {}, for {}", summary, from, state);

        if (state.isReading())
        {
            state = state.asReading().receiveSummary(from, summary);
        }
        else if (state.isAwaitingRead())
        {
            state = state.asAwaitingRead().receiveSummary(from, summary);
        }
        else if (state.isInitialized())
        {
            state = new AwaitingRead().receiveSummary(from, summary);
        }
        else
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring summary from {} with state {} for {}", from, state.name(), readId);
        }
    }

    private void complete(PartialTrackedRead read, ColumnFilter selection, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        Stage.READ.submit(() -> {
            synchronized (this)
            {
                try (PartialTrackedRead.CompletedRead completedRead = read.complete())
                {
                    TrackedDataResponse response = completedRead.response();
                    Future<TrackedDataResponse> followUp = completedRead.followupRead(response, consistencyLevel, expiresAtNanos);

                    if (followUp != null)
                    {
                        followUp.addCallback((newResponse, error) -> {
                            if (error != null)
                            {
                                tryFailure(error);
                                return;
                            }
                            trySuccess(newResponse);
                        });
                    }
                    else
                    {
                        trySuccess(response);
                    }
                }
                catch (Exception e)
                {
                    tryFailure(e);
                    throw e;
                }
                finally
                {
                    read.close();
                }
            }
        });
    }

    public synchronized boolean acknowledgeSync(int syncId)
    {
        if (state.isReconciling())
            state = state.asReconciling().acknowledgeSync(syncId);

        return state.isComplete();
    }

    public synchronized boolean receiveMutations(List<Mutation> mutations)
    {
        if (state.isReconciling())
            state = state.asReconciling().receiveMutations(mutations);
        return state.isComplete();
    }

    boolean isTimedOutOrComplete(long nanoTime)
    {
        return state.isTimedOutOrComplete(nanoTime);
    }

    public synchronized void abort()
    {
        state.abort();
        state = State.ABORTED;
    }
}
