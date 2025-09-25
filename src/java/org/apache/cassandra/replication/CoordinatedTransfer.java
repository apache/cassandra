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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.CheckReturnValue;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.COMMITTED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.STREAM_COMPLETE;
import static org.apache.cassandra.replication.ActivationRequest.Phase;

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
 *       The coordinator optinally sends PREPARE messages to verify replicas have the data persisted on disk and are 
 *       ready for activation. Implementations may customize this via {@link #prepare}
 *   </li>
 *   <li>
 *       <b>Activation {@link Phase#COMMIT}</b>
 *       After successful PREPARE, the coordinator sends COMMIT messages to replicas. Replicas atomically move data from
 *       pending to live sets, making it visible to reads with the proper transfer ID in metadata. If commit succeeds
 *       on some replicas but not others, the transfer will be activated later on via existing the existing
 *       reconciliation processes (read reconciliation and background reconciliation).
 *   </li>
 * </ol>
  * <p>
 * If a tracked data read is executed on a replica that's missing an activation, the read reconciliation process will
 * apply the missing activation during reconciliation and a subsequent read will succeed. To minimize the gap between
 * activations across replicas, avoid expensive operations like file copies or index builds during
 * {@link ActivationRequest#apply()}.
 */
public abstract class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    protected final String keyspace;
    protected final Range<Token> range;

    String logPrefix()
    {
        return String.format("[%s #%s]", getClass().getSimpleName(), id);
    }

    private final ShortMutationId id;
    final ConcurrentMap<Pair<InetAddressAndPort, InetAddressAndPort>, SingleTransferResult> streamResults;

    public CoordinatedTransfer(ShortMutationId id, String keyspace, Range<Token> range)
    {
        this.id = id;
        this.streamResults = new ConcurrentHashMap<>();
        this.keyspace = keyspace;
        this.range = range;
    }

    public CoordinatedTransfer(ShortMutationId id, Participants participants, String keyspace, Range<Token> range)
    {
        this.id = id;
        this.streamResults = new ConcurrentHashMap<>(participants.size());
        this.keyspace = keyspace;
        this.range = range;
    }

    ShortMutationId id()
    {
        return id;
    }

    Bounds<Token> bounds()
    {
        return new Bounds<>(range.left.nextValidToken(), range.right);
    }

    public boolean isCommitted()
    {
        for (SingleTransferResult result : streamResults.values())
        {
            if (result.state != SingleTransferResult.State.COMMITTED)
                return false;
        }
        return true;
    }

    protected abstract ActivationRequest createActivation(Pair<InetAddressAndPort, InetAddressAndPort> pair, ActivationRequest.Phase phase);

    final void activate(InetAddressAndPort peer)
    {
        activate(streamResults.keySet().stream().filter(pair -> pair.right.equals(peer)).collect(Collectors.toList()));
    }

    final void activate(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> pairs)
    {
        // There's no reason to try to re-activate already COMMITTED peers...
        List<Pair<InetAddressAndPort, InetAddressAndPort>> uncommittedPairs = new ArrayList<>(pairs.size());

        for (Pair<InetAddressAndPort, InetAddressAndPort> pair : pairs)
            if (streamResults.get(pair).state != COMMITTED)
                uncommittedPairs.add(pair);

        if (uncommittedPairs.isEmpty())
        {
            logAlreadyCommitted(pairs);
            return;
        }

        activateInternal(uncommittedPairs);
    }

    private synchronized void activateInternal(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> targets)
    {
        logger.debug("{} Activating {} for {}", logPrefix(), this, targets);

        prepare(targets);
        logger.debug("{} Activation prepare complete for {}", logPrefix(), targets);

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables
        // are now part of the live set, visible to reads.
        class Commit extends AsyncFuture<Void> implements RequestCallbackWithFailure<ActivationResponse>
        {
            final AtomicInteger responses = new AtomicInteger(0);

            private Commit(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> pairs)
            {
                responses.addAndGet(pairs.size());
            }

            @Override
            public void onResponse(Message<ActivationResponse> msg)
            {
                logger.debug("{} Activation successfully applied on {}", logPrefix(), msg.from());
                streamResults.computeIfPresent(msg.payload.syncPair, (peer, result) -> result.committed());

                MutationTrackingService.instance().receivedActivationResponse(CoordinatedTransfer.this, msg.from());

                if (responses.decrementAndGet() == 0)
                {
                    // All activations complete, schedule cleanup to purge pending SSTables
                    TransferTrackingService.instance().scheduleCleanup();
                    trySuccess(null);
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.error("{} Failed activation commit on {} due to {}", logPrefix(), from, failure);
                // TODO(expected): should only fail if we don't meet requested CL
                tryFailure(new RuntimeException("Tracked transfer failed during COMMIT on " + from + " due to " + failure.reason));
            }
        }

        Commit commit = new Commit(targets);
        for (Pair<InetAddressAndPort, InetAddressAndPort> target : targets)
        {
            ActivationRequest activation = createActivation(target, Phase.COMMIT);
            Message<ActivationRequest> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            logger.debug("{} Sending commit {} to peer {}", logPrefix(), activation, target.right);
            MessagingService.instance().sendWithCallback(msg, target.right, commit);
            streamResults.computeIfPresent(target, (peer0, result) -> result.committing());
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
        logger.debug("{} Activation commit complete for {}", logPrefix(), targets);
    }

    protected abstract void prepare(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> targets);

    private void logAlreadyCommitted(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> pairs)
    {
        logger.debug("Transfer {} for {} is already committed. Skipping activation...", this, pairs);
    }

    /**
     * Notify all replicas that this transfer failed, triggering cleanup of pending SSTables.
     * This is used by both {@link TrackedRepairTransfer} and {@link TrackedImportTransfer}.
     */
    protected void notifyFailure() throws ExecutionException, InterruptedException
    {
        class NotifyFailure extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            // TODO: Does this actually work? What if there is a race between notification and callbacks where we decrement to zero before submitting a second request?
            // It seems like we should add the pair up-front that actally have a plan ID...?
            final AtomicInteger responses = new AtomicInteger(0);

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                if (responses.decrementAndGet() == 0)
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                // Log but don't fail - best effort cleanup
                logger.warn("{} Failed to notify {} of transfer failure: {}", logPrefix(), from, failure);
                if (responses.decrementAndGet() == 0)
                    trySuccess(null);
            }
        }

        NotifyFailure notifyFailure = new NotifyFailure();
        for (Map.Entry<Pair<InetAddressAndPort, InetAddressAndPort>, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort to = entry.getKey().right;
            // Coordinator cleans up CoordinatedTransfer and PendingLocalTransfer separately, does not need to notify
            if (FBUtilities.getBroadcastAddressAndPort().equals(to))
                continue;

            SingleTransferResult result = entry.getValue();
            if (result.planId() == null)
            {
                // No planId means streaming never completed, so there's nothing to clean up on the replica
                logger.debug("{} Skipping notification of transfer failure to {} - no planId", logPrefix(), to);
                continue;
            }

            logger.debug("{} Notifying {} of transfer failure for plan {}", logPrefix(), to, result.planId());
            notifyFailure.responses.incrementAndGet();
            Message<TransferFailed> msg = Message.out(Verb.TRACKED_TRANSFER_FAILED_REQ, new TransferFailed(result.planId()));
            MessagingService.instance().sendWithCallback(msg, to, notifyFailure);
        }

        // Only wait if we actually sent notifications
        if (notifyFailure.responses.get() > 0)
            notifyFailure.get();
    }

    /**
     * Tracks the lifecycle of a transfer from the coordinator to a single replica, using a two-phase commit protocol:
     *
     * <ul>
     *   <li>{@link State#INIT}: Transfer created, not yet streaming.</li>
     *   <li>{@link State#STREAM_COMPLETE}: Streaming successful, SSTables received on replica in pending directory.</li>
     *   <li>{@link State#STREAM_NOOP}: No data streamed (e.g., SSTable contains no rows in target range).</li>
     *   <li>{@link State#EMPTY_SYNC}: No repair sync (e.g., No Merkle tree disagreement).</li>
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
     *          │                            ↑       │
     *          ├──→ EMPTY_SYNC ─────────────┘       │
     *          │                                    └──→ PREPARE_FAILED
     *          ├──→ STREAM_NOOP
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
            COMMITTED,
            EMPTY_SYNC;

            EnumSet<State> transitionFrom;

            static
            {
                INIT.transitionFrom = EnumSet.noneOf(State.class);
                EMPTY_SYNC.transitionFrom = EnumSet.noneOf(State.class);
                STREAM_NOOP.transitionFrom = EnumSet.of(INIT);
                STREAM_FAILED.transitionFrom = EnumSet.of(INIT);
                STREAM_COMPLETE.transitionFrom = EnumSet.of(INIT);
                PREPARING.transitionFrom = EnumSet.of(STREAM_COMPLETE, COMMITTING, EMPTY_SYNC);
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

        boolean canTransition(State to)
        {
            return to.transitionFrom.contains(state);
        }

        public static SingleTransferResult Init()
        {
            return new SingleTransferResult(State.INIT, null);
        }

        @VisibleForTesting
        static SingleTransferResult StreamComplete(TimeUUID planId)
        {
            return new SingleTransferResult(STREAM_COMPLETE, planId);
        }

        @VisibleForTesting
        static SingleTransferResult Noop()
        {
            return new SingleTransferResult(State.STREAM_NOOP, null);
        }

        @VisibleForTesting
        static SingleTransferResult EmptySync()
        {
            return new SingleTransferResult(State.EMPTY_SYNC, null);
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
            return transition(State.STREAM_FAILED, planId);
        }

        @CheckReturnValue
        public SingleTransferResult preparing()
        {
            return transition(State.PREPARING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult prepareFailed()
        {
            return transition(State.PREPARE_FAILED, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committing()
        {
            return transition(State.COMMITTING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committed()
        {
            return transition(State.COMMITTED, this.planId);
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
}
