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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.COMMITTED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.COMMITTING;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.PREPARE_FAILED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.PREPARING;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.INIT;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.STREAM_COMPLETE;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.STREAM_FAILED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.STREAM_NOOP;
import static org.apache.cassandra.replication.TransferActivation.Phase;

/**
 * Orchestrates the lifecycle of a tracked bulk data transfer for a single replica set, where the current instance is
 * coordinating the transfer.
 * <p>
 * The transfer proceeds through these phases:
 * <ol>
 *   <li>
 *       <b>Streaming</b>
 *       The coordinator streams SSTables to all replicas in parallel. Replicas store received data in a "pending"
 *       location where it's persisted to disk but not yet visible to reads. Once sufficient replicas have received
 *       their streams to meet the requested {@link ConsistencyLevel}, the SSTables are activated using a two-phase
 *       commit protocol, making them part of the live set and visible to reads.
 *   </li>
 *   <li>
 *       <b>Activation {@link Phase#PREPARE}</b>
 *       The coordinator sends PREPARE messages to verify replicas have the data persisted on disk and are ready for
 *       activation.
 *   </li>
 *   <li>
 *       <b>Activation {@link Phase#COMMIT}</b>
 *       After successful PREPARE, the coordinator sends COMMIT messages to replicas. Replicas atomically move data from
 *       pending to live sets, making it visible to reads with the proper transfer ID in metadata. If commit succeeds
 *       on some replicas but not others, the transfer will be activated later on via existing the existing
 *       reconciliation processes (read reconciliation and background reconciliation).
 *   </li>
 * </ol>
 *
 * For simplicity, the coordinator streams to itself rather than using direct file copy. This ensures we can use the
 * same lifecycle management for crash-safety and atomic add.
 * <p>
 * If a tracked data read is executed on a replica that's missing an activation, the read reconciliation process will
 * apply the missing activation during reconciliation and a subsequent read will succeed. To minimize the gap between
 * activations across replicas, avoid expensive operations like file copies or index builds during
 * {@link TransferActivation#apply()}.
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    String logPrefix()
    {
        return String.format("[CoordinatedTransfer #%s]", id);
    }

    private final ShortMutationId id;
    private final String keyspace;
    private final Range<Token> range;
    private final ConsistencyLevel cl;
    final Collection<SSTableReader> sstables;
    final ConcurrentMap<InetAddressAndPort, SingleTransferResult> streamResults;

    @VisibleForTesting
    CoordinatedTransfer(Range<Token> range, MutationId id)
    {
        this.keyspace = null;
        this.range = range;
        this.sstables = Collections.emptyList();
        this.cl = null;
        this.id = id;
        this.streamResults = new ConcurrentHashMap<>();
    }

    CoordinatedTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables, ConsistencyLevel cl, Supplier<MutationId> nextId)
    {
        this.keyspace = keyspace;
        this.range = range;
        this.sstables = sstables;
        this.cl = cl;
        this.id = nextId.get();

        ClusterMetadata cm = ClusterMetadata.current();
        this.streamResults = new ConcurrentHashMap<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streamResults.put(addr, SingleTransferResult.Init());
        }
    }

    ShortMutationId id()
    {
        return id;
    }

    void execute()
    {
        logger.debug("{} Executing tracked bulk transfer {}", logPrefix(), this);
        LocalTransfers.instance().save(this);
        stream();
    }

    private void stream()
    {
        // TODO: Don't stream multiple copies over the WAN, send one copy and indicate forwarding
        List<Future<Void>> streaming = new ArrayList<>(streamResults.size());
        for (InetAddressAndPort to : streamResults.keySet())
        {
            Future<Void> stream = LocalTransfers.instance().executor.submit(() -> {
                stream(to);
                return null;
            });
            streaming.add(stream);
        }

        // Wait for all streams to complete, so we can clean up after failures. If we exit at the first failure, a
        // future stream can complete.
        LinkedList<Throwable> failures = null;
        for (Future<Void> stream : streaming)
        {
            try
            {
                stream.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                if (failures == null)
                    failures = new LinkedList<>();
                failures.add(e);
                logger.error("{} Failed transfer due to", logPrefix(), e);
            }
        }

        if (failures != null && !failures.isEmpty())
        {
            Throwable failure = failures.element();
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            maybeCleanupFailedStreams(cause);

            String msg = String.format("Failed streaming on %s instance(s): %s", failures.size(), failures);
            throw new RuntimeException(msg, Throwables.unchecked(cause));
        }

        logger.info("{} All streaming completed successfully", logPrefix());
    }

    private boolean sufficient()
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = cl.blockFor(ars);
        int responses = 0;
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            if (entry.getValue().state == STREAM_COMPLETE)
                responses++;
        }
        return responses >= blockFor;
    }

    void stream(InetAddressAndPort to)
    {
        SingleTransferResult result;
        try
        {
            result = streamTask(to);
        }
        catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            markStreamFailure(to, cause);
            throw Throwables.unchecked(cause);
        }

        try
        {
            streamComplete(to, result);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
    }

    private void notifyFailure() throws ExecutionException, InterruptedException
    {
        class NotifyFailure extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet(streamResults.size());

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                tryFailure(failure.failure);
            }
        }

        NotifyFailure notifyFailure = new NotifyFailure();
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort to = entry.getKey();
            // Coordinator cleans up CoordinatedTransfer and PendingLocalTransfer separately, does not need to notify
            if (FBUtilities.getBroadcastAddressAndPort().equals(to))
                continue;

            SingleTransferResult result = entry.getValue();
            if (result.planId == null)
            {
                logger.warn("{} Skipping notification of transfer failure to {} due to unknown planId", logPrefix(), to);
                continue;
            }

            logger.debug("{}, Notifying {} of transfer failure for plan {}", logPrefix(), to, result.planId);
            notifyFailure.responses.add(to);
            Message<TransferFailed> msg = Message.out(Verb.TRACKED_TRANSFER_FAILED_REQ, new TransferFailed(result.planId));
            MessagingService.instance().sendWithCallback(msg, to, notifyFailure);
        }
        notifyFailure.get();
    }

    private void markStreamFailure(InetAddressAndPort to, Throwable cause)
    {
        TimeUUID planId;
        if (cause instanceof StreamException)
            planId = ((StreamException) cause).finalState.planId;
        else
            planId = null;
        streamResults.computeIfPresent(to, (peer, result) -> result.streamFailed(planId));
    }

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            boolean purgeable = LocalTransfers.instance().purger.test(this);
            if (!purgeable)
                return;

            notifyFailure();
            LocalTransfers.instance().scheduleCleanup();
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }

    private void streamComplete(InetAddressAndPort to, SingleTransferResult result) throws ExecutionException, InterruptedException, TimeoutException
    {
        streamResults.put(to, result);
        logger.info("{} Completed streaming to {}, {}", logPrefix(), to, this);
        maybeActivate();
    }

    synchronized void maybeActivate()
    {
        // If any activations have already been sent out, send new activations to any received plans that have not yet
        // been activated
        boolean anyActivated = false;
        Set<InetAddressAndPort> awaitingActivation = new HashSet<>();
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort peer = entry.getKey();
            SingleTransferResult result = entry.getValue();
            if (result.state == COMMITTING || result.state == COMMITTED)
            {
                anyActivated = true;
            }
            else if (result.state == STREAM_COMPLETE)
                awaitingActivation.add(peer);
        }
        if (anyActivated && !awaitingActivation.isEmpty())
        {
            logger.debug("{} Transfer already activated on some peers, sending activations to remaining: {}", logPrefix(), awaitingActivation);
            activateOn(awaitingActivation);
            return;
        }
        // If no activations have been sent out, check whether we have enough planIds back to meet the required CL
        else if (sufficient())
        {
            Set<InetAddressAndPort> peers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
            {
                InetAddressAndPort peer = entry.getKey();
                SingleTransferResult result = entry.getValue();
                if (result.state == STREAM_COMPLETE)
                    peers.add(peer);
            }
            logger.debug("{} Transfer meets consistency level {}, sending activations to {}", logPrefix(), cl, peers);
            activateOn(peers);
            return;
        }

        logger.debug("{} Nothing to activate", logPrefix());
    }

    void activateOn(Collection<InetAddressAndPort> peers)
    {
        Preconditions.checkState(!peers.isEmpty());
        logger.debug("{} Activating {} on {}", logPrefix(), this, peers);
        LocalTransfers.instance().activating(this);

        // First phase ensures data is present on disk, then second phase does the actual import. This ensures that if
        // something goes wrong (like a topology change during import), we don't have divergence.
        class Prepare extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            public Prepare()
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                logger.debug("{} Got response from: {}", logPrefix(), msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("{} Got failure {} from {}", logPrefix(), failure, from);
                CoordinatedTransfer.this.streamResults.computeIfPresent(from, (peer, result) -> result.prepareFailed());
                tryFailure(new RuntimeException("Tracked import failed during PREPARE on " + from + " due to " + failure.reason));
            }
        }

        Prepare prepare = new Prepare();
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.PREPARE);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, prepare);
            CoordinatedTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.preparing());
        }
        try
        {
            prepare.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation prepare complete for {}", logPrefix(), peers);

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables
        // are now part of the live set, visible to reads.
        class Commit extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            private Commit(Collection<InetAddressAndPort> peers)
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<Void> msg)
            {
                logger.debug("{} Activation successfully applied on {}", logPrefix(), msg.from());
                CoordinatedTransfer.this.streamResults.computeIfPresent(msg.from(), (peer, result) -> result.committed());

                MutationTrackingService.instance.receivedActivationResponse(CoordinatedTransfer.this, msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                {
                    // All activations complete, schedule cleanup to purge pending SSTables
                    LocalTransfers.instance().scheduleCleanup();
                    trySuccess(null);
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.error("{} Failed activation on {} due to {}", logPrefix(), from, failure);
                MutationTrackingService.instance.retryFailedTransfer(CoordinatedTransfer.this, from, failure.failure);
                // TODO(expected): should only fail if we don't meet requested CL
                tryFailure(new RuntimeException("Tracked import failed during COMMIT on " + from + " due to " + failure.reason));
            }
        }

        Commit commit = new Commit(peers);
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.COMMIT);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, commit);
            CoordinatedTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.committing());
        }

        try
        {
            commit.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation commit complete for {}", logPrefix(), peers);
    }

    public boolean isCommitted()
    {
        for (SingleTransferResult result : streamResults.values())
        {
            if (result.state != COMMITTED)
                return false;
        }
        return true;
    }

    /**
     * Tracks the lifecycle of a transfer from the coordinator to a single replica, using a two-phase commit protocol:
     *
     * <ul>
     *   <li>{@link State#INIT}: Transfer created, not yet streaming.</li>
     *   <li>{@link State#STREAM_COMPLETE}: Streaming successful, SSTables received on replica in pending directory.</li>
     *   <li>{@link State#STREAM_NOOP}: No data streamed (e.g., SSTable contains no rows in target range).</li>
     *   <li>{@link State#STREAM_FAILED}: Streaming failed, may not have a streaming plan ID yet.</li>
     *   <li>{@link State#PREPARING}: Preparing for activation (first phase).</li>
     *   <li>{@link State#PREPARE_FAILED}: Prepare failed, aborting transfer.</li>
     *   <li>{@link State#COMMITTING}: Committing transferred SSTables from pending to live set (second phase).</li>
     *   <li>{@link State#COMMITTED}: Transfer commit acknowledged on coordinator. SSTables now live and visible to reads.</li>
     * </ul>
     *
     * <h3>Valid State Transitions:</h3>
     * <pre>
     *                                       ┌────────────────┐
     *                                       ↓                │
     *   INIT ──┬──→ STREAM_COMPLETE ──→ PREPARING ──┬──→ COMMITTING ──→ COMMITTED
     *          │                                    │
     *          ├──→ STREAM_NOOP                     └──→ PREPARE_FAILED
     *          │
     *          └──→ STREAM_FAILED
     * </pre>
     *
     * Failure states may be non-terminal if sufficient replicas reach successful states, depending on the transfer's
     * consistency level.
     */
    static class SingleTransferResult
    {
        enum State
        {
            INIT,
            STREAM_NOOP,
            STREAM_FAILED,
            STREAM_COMPLETE,
            PREPARING,
            PREPARE_FAILED,
            COMMITTING,
            COMMITTED;

            EnumSet<State> transitionFrom;

            static
            {
                INIT.transitionFrom = EnumSet.noneOf(State.class);
                STREAM_NOOP.transitionFrom = EnumSet.of(INIT);
                STREAM_FAILED.transitionFrom = EnumSet.of(INIT);
                STREAM_COMPLETE.transitionFrom = EnumSet.of(INIT);
                PREPARING.transitionFrom = EnumSet.of(STREAM_COMPLETE, COMMITTING);
                PREPARE_FAILED.transitionFrom = EnumSet.of(PREPARING);
                COMMITTING.transitionFrom = EnumSet.of(PREPARING);
                COMMITTED.transitionFrom = EnumSet.of(COMMITTING);
            }
        }

        final State state;
        private final TimeUUID planId;

        @VisibleForTesting
        SingleTransferResult(State state, TimeUUID planId)
        {
            this.state = state;
            this.planId = planId;
        }

        private boolean canTransition(SingleTransferResult.State to)
        {
            return to.transitionFrom.contains(state);
        }

        public static SingleTransferResult Init()
        {
            return new SingleTransferResult(INIT, null);
        }

        @VisibleForTesting
        static SingleTransferResult StreamComplete(TimeUUID planId)
        {
            return new SingleTransferResult(STREAM_COMPLETE, planId);
        }

        @VisibleForTesting
        static SingleTransferResult Noop()
        {
            return new SingleTransferResult(STREAM_NOOP, null);
        }

        @CheckReturnValue
        private SingleTransferResult transition(State to, TimeUUID planId)
        {
            if (!canTransition(to))
            {
                logger.error("Ignoring invalid transition from {} to {}", state, to);
                return this;
            }
            // Don't overwrite if the stream succeeded but PREPARE failed, so we can clean up later
            return new SingleTransferResult(to, planId == null ? this.planId : planId);
        }

        @CheckReturnValue
        public SingleTransferResult streamFailed(TimeUUID planId)
        {
            return transition(STREAM_FAILED, planId);
        }

        @CheckReturnValue
        public SingleTransferResult preparing()
        {
            return transition(PREPARING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult prepareFailed()
        {
            return transition(PREPARE_FAILED, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committing()
        {
            return transition(COMMITTING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committed()
        {
            return transition(COMMITTED, this.planId);
        }

        public TimeUUID planId()
        {
            return planId;
        }

        @Override
        public String toString()
        {
            return "SingleTransferResult{" +
                   "state=" + state +
                   ", planId=" + planId +
                   '}';
        }
    }

    private SingleTransferResult streamTask(InetAddressAndPort to) throws StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.TRACKED_TRANSFER);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (SSTableReader sstable : sstables)
        {
            List<Range<Token>> ranges = Collections.singletonList(range);
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.TRACKED_TRANSFER, sstable.ref(), positions, ranges, estimatedKeys);
            plan.transferStreams(to, Collections.singleton(stream));
        }

        long timeout = DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds();

        logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get(timeout, TimeUnit.MILLISECONDS);
            logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            logger.error("Stream session failed with error", e);
            throw e;
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new StreamException(state, "Stream failed due to failed or aborted sessions");

        // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
        if (state.sessions().isEmpty())
            return SingleTransferResult.Noop();

        return SingleTransferResult.StreamComplete(plan.planId());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatedTransfer transfer = (CoordinatedTransfer) o;
        return Objects.equals(keyspace, transfer.keyspace) && Objects.equals(range, transfer.range) && Objects.equals(streamResults, transfer.streamResults) && Objects.equals(sstables, transfer.sstables) && cl == transfer.cl && Objects.equals(id, transfer.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, range, streamResults, sstables, cl, id);
    }

    @Override
    public String toString()
    {
        return "CoordinatedTransfer{" +
               "id=" + id +
               ", keyspace='" + keyspace + '\'' +
               ", range=" + range +
               ", cl=" + cl +
               ", sstables=" + sstables +
               ", streamResults=" + streamResults +
               '}';
    }
}
