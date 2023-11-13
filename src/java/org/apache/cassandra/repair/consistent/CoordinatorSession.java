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

package org.apache.cassandra.repair.consistent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.CoordinatedRepairResult;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SomeRepairFailedException;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.repair.messages.RepairMessage.notDone;
import static org.apache.cassandra.repair.messages.RepairMessage.sendAck;
import static org.apache.cassandra.repair.messages.RepairMessage.sendFailureResponse;
import static org.apache.cassandra.repair.messages.RepairMessage.sendMessageWithRetries;

/**
 * Coordinator side logic and state of a consistent repair session. Like {@link ActiveRepairService.ParentRepairSession},
 * there is only one {@code CoordinatorSession} per user repair command, regardless of the number of tables and token
 * ranges involved.
 */
public class CoordinatorSession extends ConsistentSession
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSession.class);

    private final SharedContext ctx;
    private final Map<InetAddressAndPort, State> participantStates = new HashMap<>();
    private final AsyncPromise<Void> prepareFuture = AsyncPromise.uncancellable();
    private final AsyncPromise<Void> finalizeProposeFuture = AsyncPromise.uncancellable();

    private volatile long sessionStart = Long.MIN_VALUE;
    private volatile long repairStart = Long.MIN_VALUE;
    private volatile long finalizeStart = Long.MIN_VALUE;

    private final Consumer<CoordinatorSession> listener;

    public CoordinatorSession(Builder builder)
    {
        super(builder);
        this.listener = builder.listener;
        ctx = builder.ctx == null ? SharedContext.Global.instance : builder.ctx;
        for (InetAddressAndPort participant : participants)
        {
            participantStates.put(participant, State.PREPARING);
        }
    }

    public static class Builder extends AbstractBuilder
    {
        Consumer<CoordinatorSession> listener;
        private SharedContext ctx;

        public Builder(SharedContext ctx)
        {
            super(ctx);
        }

        public void withListener(Consumer<CoordinatorSession> listener)
        {
            this.listener = listener;
        }

        public Builder withContext(SharedContext ctx)
        {
            this.ctx = ctx;
            return this;
        }

        public CoordinatorSession build()
        {
            validate();
            return new CoordinatorSession(this);
        }
    }

    public static Builder builder(SharedContext ctx)
    {
        return new Builder(ctx);
    }

    public void setState(State state)
    {
        logger.trace("Setting coordinator state to {} for repair {}", state, sessionID);
        super.setState(state);
        if (listener != null)
            listener.accept(this);
    }

    @VisibleForTesting
    synchronized State getParticipantState(InetAddressAndPort participant)
    {
        return participantStates.get(participant);
    }

    public synchronized void setParticipantState(InetAddressAndPort participant, State state)
    {
        logger.trace("Setting participant {} to state {} for repair {}", participant, state, sessionID);
        Preconditions.checkArgument(participantStates.containsKey(participant),
                                    "Session %s doesn't include %s",
                                    sessionID, participant);
        Preconditions.checkArgument(participantStates.get(participant).canTransitionTo(state),
                                    "Invalid state transition %s -> %s",
                                    participantStates.get(participant), state);
        participantStates.put(participant, state);

        // update coordinator state if all participants are at the value being set
        if (Iterables.all(participantStates.values(), s -> s == state))
        {
            setState(state);
        }
    }

    synchronized void setAll(State state)
    {
        for (InetAddressAndPort participant : participants)
        {
            setParticipantState(participant, state);
        }
    }

    synchronized boolean allStates(State state)
    {
        return getState() == state && Iterables.all(participantStates.values(), v -> v == state);
    }

    synchronized boolean hasFailed()
    {
        return getState() == State.FAILED || Iterables.any(participantStates.values(), v -> v == State.FAILED);
    }

    public Future<Void> prepare()
    {
        Preconditions.checkArgument(allStates(State.PREPARING));

        logger.info("Beginning prepare phase of incremental repair session {}", sessionID);

        PrepareConsistentRequest request = new PrepareConsistentRequest(sessionID, coordinator, participants);
        for (final InetAddressAndPort participant : participants)
        {
            sendMessageWithRetries(ctx,
                                   notDone(prepareFuture),
                                   request,
                                   Verb.PREPARE_CONSISTENT_REQ,
                                   participant);
        }
        return prepareFuture;
    }

    public synchronized void handlePrepareResponse(Message<PrepareConsistentResponse> msg)
    {
        InetAddressAndPort participant = msg.payload.participant;
        boolean success = msg.payload.success;
        if (getState() == State.FAILED)
        {
            logger.trace("Incremental repair {} has failed, ignoring prepare response from {}", sessionID, participant);
            sendFailureResponse(ctx, msg);
            return;
        }
        sendAck(ctx, msg);
        if (getParticipantState(participant) != State.PREPARING)
            return;
        if (!success)
        {
            logger.warn("{} failed the prepare phase for incremental repair session {}", participant, sessionID);
            sendFailureMessageToParticipants();
            setParticipantState(participant, State.FAILED);
        }
        else
        {
            logger.trace("Successful prepare response received from {} for repair session {}", participant, sessionID);
            setParticipantState(participant, State.PREPARED);
        }

        // don't progress until we've heard from all replicas
        if(Iterables.any(participantStates.values(), v -> v == State.PREPARING))
            return;

        if (getState() == State.PREPARED)
        {
            logger.info("Incremental repair session {} successfully prepared.", sessionID);
            prepareFuture.trySuccess(null);
        }
        else
        {
            fail();
        }
    }

    public synchronized void setRepairing()
    {
        setAll(State.REPAIRING);
    }

    public synchronized Future<Void> finalizePropose()
    {
        Preconditions.checkArgument(allStates(State.REPAIRING));
        logger.info("Proposing finalization of repair session {}", sessionID);
        FinalizePropose request = new FinalizePropose(sessionID);
        for (final InetAddressAndPort participant : participants)
        {
            sendMessageWithRetries(ctx, notDone(finalizeProposeFuture), request, Verb.FINALIZE_PROPOSE_MSG, participant);
        }
        return finalizeProposeFuture;
    }

    public synchronized void handleFinalizePromise(Message<FinalizePromise> message)
    {
        InetAddressAndPort participant = message.payload.participant;
        boolean success = message.payload.promised;
        if (getState() == State.FAILED)
        {
            logger.trace("Incremental repair {} has failed, ignoring finalize promise from {}", sessionID, participant);
            sendFailureResponse(ctx, message);
            return;
        }
        sendAck(ctx, message);
        if (getParticipantState(participant) != State.REPAIRING)
        {
            // this message is a retry, or we failed the session; in either case there is nothing more to do than ack
            return;
        }
        else if (!success)
        {
            logger.warn("Finalization proposal of session {} rejected by {}. Aborting session", sessionID, participant);
            fail();
        }
        else
        {
            logger.trace("Successful finalize promise received from {} for repair session {}", participant, sessionID);
            setParticipantState(participant, State.FINALIZE_PROMISED);
            if (getState() == State.FINALIZE_PROMISED)
            {
                logger.info("Finalization proposal for repair session {} accepted by all participants.", sessionID);
                finalizeProposeFuture.trySuccess(null);
            }
        }
    }

    public synchronized void finalizeCommit()
    {
        Preconditions.checkArgument(allStates(State.FINALIZE_PROMISED));
        logger.info("Committing finalization of repair session {}", sessionID);
        FinalizeCommit payload = new FinalizeCommit(sessionID);
        for (final InetAddressAndPort participant : participants)
        {
            sendMessageWithRetries(ctx, payload, Verb.FINALIZE_COMMIT_MSG, participant);
        }
        setAll(State.FINALIZED);
        logger.info("Incremental repair session {} completed", sessionID);
    }

    private void sendFailureMessageToParticipants()
    {
        FailSession payload = new FailSession(sessionID);
        for (final InetAddressAndPort participant : participants)
        {
            if (participantStates.get(participant) != State.FAILED)
            {
                sendMessageWithRetries(ctx, payload, Verb.FAILED_SESSION_MSG, participant);
            }
        }
    }

    public synchronized void fail()
    {
        Set<Map.Entry<InetAddressAndPort, State>> cantFail = participantStates.entrySet()
                                                                              .stream()
                                                                              .filter(entry -> !entry.getValue().canTransitionTo(State.FAILED))
                                                                              .collect(Collectors.toSet());
        if (!cantFail.isEmpty())
        {
            logger.error("Can't transition endpoints {} to FAILED", cantFail, new RuntimeException());
            return;
        }
        logger.info("Incremental repair session {} failed", sessionID);
        sendFailureMessageToParticipants();
        setAll(State.FAILED);

        String exceptionMsg = String.format("Incremental repair session %s has failed", sessionID);
        finalizeProposeFuture.tryFailure(RepairException.warn(exceptionMsg));
        prepareFuture.tryFailure(RepairException.warn(exceptionMsg));
    }

    private static String formatDuration(long then, long now)
    {
        if (then == Long.MIN_VALUE || now == Long.MIN_VALUE)
        {
            // if neither of the times were initially set, don't return a non-sensical answer
            return "n/a";
        }
        return DurationFormatUtils.formatDurationWords(now - then, true, true);
    }

    /**
     * Runs the asynchronous consistent repair session. Actual repair sessions are scheduled via a submitter to make unit testing easier
     */
    public Future<CoordinatedRepairResult> execute(Supplier<Future<CoordinatedRepairResult>> sessionSubmitter)
    {
        logger.info("Beginning coordination of incremental repair session {}", sessionID);

        sessionStart = ctx.clock().currentTimeMillis();
        Future<Void> prepareResult = prepare();

        // run repair sessions normally
        Future<CoordinatedRepairResult> repairSessionResults = prepareResult.flatMap(ignore -> {
            repairStart = ctx.clock().currentTimeMillis();
            if (logger.isDebugEnabled())
                logger.debug("Incremental repair {} prepare phase completed in {}", sessionID, formatDuration(sessionStart, repairStart));
            setRepairing();
            return sessionSubmitter.get();
        });

        // if any session failed, then fail the future
        Future<CoordinatedRepairResult> onlySuccessSessionResults = repairSessionResults.flatMap(result -> {
            finalizeStart = ctx.clock().currentTimeMillis();
            if (result.hasFailed())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Incremental repair {} validation/stream phase completed in {}", sessionID, formatDuration(repairStart, finalizeStart));
                return ImmediateFuture.failure(SomeRepairFailedException.INSTANCE);
            }
            return ImmediateFuture.success(result);
        });

        // mark propose finalization and commit
        Future<CoordinatedRepairResult> proposeFuture = onlySuccessSessionResults.flatMap(results -> finalizePropose().map(ignore -> {
            if (logger.isDebugEnabled())
                logger.debug("Incremental repair {} finalization phase completed in {}", sessionID, formatDuration(finalizeStart, ctx.clock().currentTimeMillis()));
            finalizeCommit();
            if (logger.isDebugEnabled())
                logger.debug("Incremental repair {} phase completed in {}", sessionID, formatDuration(sessionStart, ctx.clock().currentTimeMillis()));
            return results;
        }));

        return proposeFuture.addCallback((ignore, failure) -> {
            if (failure != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Incremental repair {} phase failed in {}", sessionID, formatDuration(sessionStart, ctx.clock().currentTimeMillis()));
                fail();
            }
        }, ImmediateExecutor.INSTANCE);
    }
}
