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
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AbstractFuture;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class TrackedLocalReadCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedLocalReadCoordinator.class);

    private final TrackedRead.Id readId;
    private final AsyncPromise<TrackedDataResponse> promise;

    private volatile State state;

    public TrackedLocalReadCoordinator(TrackedRead.Id readId)
    {
        this.readId = readId;
        this.promise = new AsyncPromise<>();
        this.state = INITIALIZED;
    }

    public AbstractFuture<TrackedDataResponse> addCallback(BiConsumer<TrackedDataResponse, Throwable> callback)
    {
        return promise.addCallback(callback);
    }

    enum Status { INITIALIZED, AWAITING_READ, READING, RECONCILING, ABORTED, COMPLETED }

    private static abstract class State
    {
        abstract Status status();

        State startLocalRead(TrackedRead.Id readId, AsyncPromise<TrackedDataResponse> promise, ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, int[] summaryNodes, long expiresAtNanos)
        {
            // TODO: validate permitted state instead of just ignoring events?
            return this;
        }

        State receiveInProgressRead(PartialTrackedRead read, MutationSummary summary)
        {
            throw new IllegalStateException("Received in-progress read with status " + status());
        }

        State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            logger.trace("Ignoring summary from {} with status {}", from, status());
            return this;
        }

        State acknowledgeSync(int syncId)
        {
            logger.trace("Ignoring sync ack {} with status {}", syncId, status());
            return this;
        }

        State receiveMutations(List<Mutation> mutations)
        {
            logger.trace("Ignoring {} mutations with status {}", mutations.size(), status());
            return this;
        }

        State abort()
        {
            return ABORTED;
        }

        boolean isPurgeable(long nanoTime)
        {
            return false;
        }

        boolean isReading()
        {
            return is(Status.READING);
        }

        boolean isCompleted()
        {
            return is(Status.COMPLETED);
        }

        boolean is(Status status)
        {
            return status() == status;
        }

        @Override
        public String toString()
        {
            return status().toString();
        }
    }

    static final State INITIALIZED = new State()
    {
        @Override
        Status status() { return Status.INITIALIZED; }

        @Override
        State startLocalRead(TrackedRead.Id readId, AsyncPromise<TrackedDataResponse> promise, ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, int[] summaryNodes, long expiresAtNanos)
        {
            return new Reading(readId, promise, command, replicaPlan, summaryNodes, expiresAtNanos);
        }

        @Override
        State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            return new AwaitingRead(from, summary);
        }
    };

    static final State ABORTED = new State()
    {
        @Override
        Status status() { return Status.ABORTED; }

        @Override
        State receiveInProgressRead(PartialTrackedRead read, MutationSummary summary)
        {
            return this; // the read can't complete without data, but it could have been aborted in the meantime
        }

        @Override
        boolean isPurgeable(long nanoTime) { return true; }
    };

    static final State COMPLETED = new State()
    {
        @Override
        Status status() { return Status.COMPLETED; }

        @Override
        boolean isPurgeable(long nanoTime) { return true; }
    };

    private static class ReceivedSummary
    {
        final InetAddressAndPort from;
        final MutationSummary summary;

        ReceivedSummary(InetAddressAndPort from, MutationSummary summary)
        {
            this.from = from;
            this.summary = summary;
        }
    }

    // if we start receiving summaries before we receive the read command, they're
    // collected here
    private static class AwaitingRead extends State
    {
        private long lastUpdateNanos;
        private final List<ReceivedSummary> summaries;

        AwaitingRead(InetAddressAndPort summaryFrom, MutationSummary summary)
        {
             summaries = new ArrayList<>();
             summaries.add(new ReceivedSummary(summaryFrom, summary));
        }

        @Override
        Status status()
        {
            return Status.AWAITING_READ;
        }

        @Override
        State startLocalRead(TrackedRead.Id readId, AsyncPromise<TrackedDataResponse> promise, ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, int[] summaryNodes, long expiresAtNanos)
        {
            return new Reading(readId, promise, command, replicaPlan, summaryNodes, summaries, expiresAtNanos);
        }

        @Override
        State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            summaries.add(new ReceivedSummary(from, summary));
            lastUpdateNanos = Clock.Global.nanoTime();
            return this;
        }

        @Override
        boolean isPurgeable(long nanoTime)
        {
            return nanoTime - lastUpdateNanos > DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS);
        }
    }

    private static class Reading extends State
    {
        private final TrackedRead.Id readId;
        private final AsyncPromise<TrackedDataResponse> promise;
        private final ReadCommand command;
        private volatile PartialTrackedRead read;
        private final long expiresAtNanos;
        private final ReplicaPlan.AbstractForRead<?, ?> replicaPlan;
        private final Accumulator<ReceivedSummary> summaries;
        private final int[] summaryNodes; // for speculating when we haven't received enough summaries

        Reading(
            TrackedRead.Id readId,
            AsyncPromise<TrackedDataResponse> promise,
            ReadCommand command,
            ReplicaPlan.AbstractForRead<?, ?> replicaPlan,
            int[] summaryNodes,
            long expiresAtNanos)
        {
            this.readId = readId;
            this.promise = promise;
            this.expiresAtNanos = expiresAtNanos;
            this.command = command;
            this.replicaPlan = replicaPlan;
            this.summaries = new Accumulator<>(replicaPlan.readCandidates().size());
            this.summaryNodes = summaryNodes;
        }

        Reading(
            TrackedRead.Id readId,
            AsyncPromise<TrackedDataResponse> promise,
            ReadCommand command,
            ReplicaPlan.AbstractForRead<?, ?> replicaPlan,
            int[] summaryNodes,
            List<ReceivedSummary> summaries,
            long expiresAtNanos)
        {
            this(readId, promise, command, replicaPlan, summaryNodes, expiresAtNanos);
            for (ReceivedSummary summary : summaries) this.summaries.add(summary);
        }

        @Override
        Status status()
        {
            return Status.READING;
        }

        @Override
        boolean isPurgeable(long nanoTime)
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
                complete(promise, read, command.columnFilter(), replicaPlan.consistencyLevel(), expiresAtNanos);
                return COMPLETED;
            }
            else
            {
                logger.trace("Beginning reconciliation for {}", readId);
                Reconciling reconciling = new Reconciling(readId, promise, command, read, replicaPlan.consistencyLevel(), expiresAtNanos, reconciliations);
                reconciling.start();  // TODO: don't do this until after the coordinator state is set to reconciling if converting to lock free
                return reconciling;
            }
        }

        @Override
        State receiveInProgressRead(PartialTrackedRead read, MutationSummary summary)
        {
            if (this.read != null)
                return this;

            logger.trace("In progress read received for {}", readId);
            this.read = read;
            summaries.add(new ReceivedSummary(FBUtilities.getBroadcastAddressAndPort(), summary));

            return maybeComplete();
        }

        @Override
        State receiveSummary(InetAddressAndPort from, MutationSummary summary)
        {
            logger.trace("Summary received from {} for {}", from, readId);
            summaries.add(new ReceivedSummary(from, summary));
            return maybeComplete();
        }

        @Override
        State abort()
        {
            logger.trace("Aborting read {}", readId);
            if (read != null)
                read.close();
            return ABORTED;
        }
    }

    private static class PendingSync
    {
        final int syncId;
        final InetAddressAndPort from;
        final InetAddressAndPort to;
        final Log2OffsetsMap.Immutable plan;

        PendingSync(int syncId, InetAddressAndPort from, InetAddressAndPort to, Log2OffsetsMap.Immutable plan)
        {
            this.syncId = syncId;
            this.from = from;
            this.to = to;
            this.plan = plan;
        }

        ReadReconcileSend.PeerSync toPeerSync()
        {
            return new ReadReconcileSend.PeerSync(syncId, to, plan);
        }
    }

    private static class Reconciling extends State
    {
        private final TrackedRead.Id readId;
        private final AsyncPromise<TrackedDataResponse> promise;
        private final ReadCommand command;
        private final PartialTrackedRead read;
        private final ConsistencyLevel consistencyLevel;
        private final long expiresAtNanos;

        final Map<InetAddressAndPort, ReconciliationPlan> plans;
        final Log2OffsetsMap.Mutable outstandingMutations = new Log2OffsetsMap.Mutable();
        final Map<Integer, PendingSync> pendingSync = new ConcurrentHashMap<>();
        final int blockFor;

        Reconciling(
            TrackedRead.Id readId,
            AsyncPromise<TrackedDataResponse> promise,
            ReadCommand command,
            PartialTrackedRead read,
            ConsistencyLevel consistencyLevel,
            long expiresAtNanos,
            Map<InetAddressAndPort, ReconciliationPlan> plans)
        {
            this.readId = readId;
            this.promise = promise;
            this.command = command;
            this.read = Preconditions.checkNotNull(read);
            this.consistencyLevel = consistencyLevel;
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

            if (logger.isTraceEnabled())
                logger.trace("Reconciling {} syncs, {} mutations for {}", syncs, outstandingMutations.idCount(), readId);

            this.blockFor = syncs;
        }

        @Override
        Status status()
        {
            return Status.RECONCILING;
        }

        @Override
        boolean isPurgeable(long nanoTime)
        {
            return nanoTime - expiresAtNanos > 0;
        }

        void start()
        {
            ReadRepairMetrics.trackedReconcile.mark();
            ColumnFamilyStore.metricsFor(command.metadata().id).readRepairRequests.mark();

            Map<InetAddressAndPort, List<ReadReconcileSend.PeerSync>> peerSync = new HashMap<>();
            pendingSync.values().forEach(pending ->
                peerSync.computeIfAbsent(pending.from, node -> new ArrayList<>()).add(pending.toPeerSync()));

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

            logger.trace("Reconciliation completed for read {}", readId);
            complete(promise, read, command.columnFilter(), consistencyLevel, expiresAtNanos);
            return COMPLETED;
        }

        @Override
        State acknowledgeSync(int syncId)
        {
            logger.trace("Reconciliation sync {} received for {}", syncId, readId);
            pendingSync.remove(syncId);
            return maybeComplete();
        }

        @Override
        State receiveMutations(List<Mutation> mutations)
        {
            Log2OffsetsMap.Mutable received = new Log2OffsetsMap.Mutable();
            mutations.forEach(mutation -> {
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
        State abort()
        {
            read.close();
            return ABORTED;
        }
    }

    @Override
    public String toString()
    {
        return "TrackedLocalReadCoordinator{" + readId + ':' + state.status() + '}';
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

    public void startLocalRead(TrackedRead.Id readId, ReadCommand command, ReplicaPlan.AbstractForRead<?, ?> replicaPlan, int[] summaryNodes, long expiresAtNanos)
    {
        synchronized (this)
        {
            if (!(state = state.startLocalRead(readId, promise, command, replicaPlan, summaryNodes, expiresAtNanos)).isReading())
                return;
        }

        PartialTrackedRead read;
        MutationSummary secondarySummary;

        MutationSummary initialSummary = command.createMutationSummary(false);
        ReadExecutionController controller = command.executionController(false);
        try
        {
            read = command.beginTrackedRead(controller);
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
            state = state.receiveInProgressRead(read, secondarySummary);
        }
    }

    private static void complete(AsyncPromise<TrackedDataResponse> promise, PartialTrackedRead read, ColumnFilter selection, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        Stage.READ.submit(() -> completeInternal(promise, read, selection, consistencyLevel, expiresAtNanos));
    }

    private static void completeInternal(AsyncPromise<TrackedDataResponse> promise, PartialTrackedRead read, ColumnFilter selection, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        try (PartialTrackedRead.CompletedRead completedRead = read.complete())
        {
            TrackedDataResponse response = TrackedDataResponse.create(completedRead.iterator(), selection);
            TrackedRead<?, ?> followUp = completedRead.followupRead(consistencyLevel, expiresAtNanos);

            if (followUp != null)
            {
                ReadCommand command = read.command();
                followUp.future().addCallback((iterator, error) -> {
                    if (error != null)
                    {
                        promise.tryFailure(error);
                        return;
                    }
                    PartitionIterator previous = response.makeIterator(command);
                    TrackedDataResponse newResponse = TrackedDataResponse.create(PartitionIterators.concat(List.of(previous, iterator)), selection);
                    promise.trySuccess(newResponse);
                });
            }
            else
            {
                promise.trySuccess(response);
            }
        }
        catch (Exception e)
        {
            promise.tryFailure(e);
            throw e;
        }
        finally
        {
            read.close();
        }
    }

    synchronized void receiveSummary(InetAddressAndPort from, MutationSummary summary)
    {
        if (logger.isTraceEnabled())
            logger.trace("Received summary {} from {}, for {}", summary, from, state);
        state = state.receiveSummary(from, summary);
    }

    synchronized boolean acknowledgeSync(int syncId)
    {
        return (state = state.acknowledgeSync(syncId)).isCompleted();
    }

    synchronized boolean receiveMutations(List<Mutation> mutations)
    {
        return (state = state.receiveMutations(mutations)).isCompleted();
    }

    synchronized void abort()
    {
        state = state.abort();
    }

    boolean isTimedOutOrComplete(long nanoTime)
    {
        return state.isPurgeable(nanoTime);
    }
}
