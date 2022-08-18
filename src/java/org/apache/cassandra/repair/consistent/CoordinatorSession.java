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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.SomeRepairFailedException;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Coordinator side logic and state of a consistent repair session. Like {@link ActiveRepairService.ParentRepairSession},
 * there is only one {@code CoordinatorSession} per user repair command, regardless of the number of tables and token
 * ranges involved.
 */
public class CoordinatorSession extends ConsistentSession
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSession.class);

    private final Map<InetAddressAndPort, State> participantStates = new HashMap<>();
    private final SettableFuture<Boolean> prepareFuture = SettableFuture.create();
    private final SettableFuture<Boolean> finalizeProposeFuture = SettableFuture.create();

    private volatile long sessionStart = Long.MIN_VALUE;
    private volatile long repairStart = Long.MIN_VALUE;
    private volatile long finalizeStart = Long.MIN_VALUE;

    public CoordinatorSession(Builder builder)
    {
        super(builder);
        for (InetAddressAndPort participant : participants)
        {
            participantStates.put(participant, State.PREPARING);
        }
    }

    public static class Builder extends AbstractBuilder
    {
        public CoordinatorSession build()
        {
            validate();
            return new CoordinatorSession(this);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public void setState(State state)
    {
        logger.trace("Setting coordinator state to {} for repair {}", state, sessionID);
        super.setState(state);
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

    protected void sendMessage(InetAddressAndPort destination, Message<RepairMessage> message)
    {
        logger.trace("Sending {} to {}", message.payload, destination);
        MessagingService.instance().send(message, destination);
    }

    public ListenableFuture<Boolean> prepare()
    {
        Preconditions.checkArgument(allStates(State.PREPARING));

        logger.info("Beginning prepare phase of incremental repair session {}", sessionID);
        Message<RepairMessage> message =
            Message.out(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, coordinator, participants));
        for (final InetAddressAndPort participant : participants)
        {
            sendMessage(participant, message);
        }
        return prepareFuture;
    }

    public synchronized void handlePrepareResponse(InetAddressAndPort participant, boolean success)
    {
        if (getState() == State.FAILED)
        {
            logger.trace("Incremental repair {} has failed, ignoring prepare response from {}", sessionID, participant);
            return;
        }
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
            prepareFuture.set(true);
        }
        else
        {
            fail();
            prepareFuture.set(false);
        }
    }

    public synchronized void setRepairing()
    {
        setAll(State.REPAIRING);
    }

    public synchronized ListenableFuture<Boolean> finalizePropose()
    {
        Preconditions.checkArgument(allStates(State.REPAIRING));
        logger.info("Proposing finalization of repair session {}", sessionID);
        Message<RepairMessage> message = Message.out(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(sessionID));
        for (final InetAddressAndPort participant : participants)
        {
            sendMessage(participant, message);
        }
        return finalizeProposeFuture;
    }

    public synchronized void handleFinalizePromise(InetAddressAndPort participant, boolean success)
    {
        if (getState() == State.FAILED)
        {
            logger.trace("Incremental repair {} has failed, ignoring finalize promise from {}", sessionID, participant);
        }
        else if (!success)
        {
            logger.warn("Finalization proposal of session {} rejected by {}. Aborting session", sessionID, participant);
            fail();
            finalizeProposeFuture.set(false);
        }
        else
        {
            logger.trace("Successful finalize promise received from {} for repair session {}", participant, sessionID);
            setParticipantState(participant, State.FINALIZE_PROMISED);
            if (getState() == State.FINALIZE_PROMISED)
            {
                logger.info("Finalization proposal for repair session {} accepted by all participants.", sessionID);
                finalizeProposeFuture.set(true);
            }
        }
    }

    public synchronized void finalizeCommit()
    {
        Preconditions.checkArgument(allStates(State.FINALIZE_PROMISED));
        logger.info("Committing finalization of repair session {}", sessionID);
        Message<RepairMessage> message = Message.out(Verb.FINALIZE_COMMIT_MSG, new FinalizeCommit(sessionID));
        for (final InetAddressAndPort participant : participants)
        {
            sendMessage(participant, message);
        }
        setAll(State.FINALIZED);
        logger.info("Incremental repair session {} completed", sessionID);
    }

    private void sendFailureMessageToParticipants()
    {
        Message<RepairMessage> message = Message.out(Verb.FAILED_SESSION_MSG, new FailSession(sessionID));
        for (final InetAddressAndPort participant : participants)
        {
            if (participantStates.get(participant) != State.FAILED)
            {
                sendMessage(participant, message);
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
        finalizeProposeFuture.setException(new RuntimeException(exceptionMsg));
        prepareFuture.setException(new RuntimeException(exceptionMsg));
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
    public ListenableFuture execute(Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSubmitter, AtomicBoolean hasFailure)
    {
        logger.info("Beginning coordination of incremental repair session {}", sessionID);

        sessionStart = System.currentTimeMillis();
        ListenableFuture<Boolean> prepareResult = prepare();

        // run repair sessions normally
        ListenableFuture<List<RepairSessionResult>> repairSessionResults = Futures.transformAsync(prepareResult, new AsyncFunction<Boolean, List<RepairSessionResult>>()
        {
            public ListenableFuture<List<RepairSessionResult>> apply(Boolean success) throws Exception
            {
                if (success)
                {
                    repairStart = System.currentTimeMillis();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Incremental repair {} prepare phase completed in {}", sessionID, formatDuration(sessionStart, repairStart));
                    }
                    setRepairing();
                    return sessionSubmitter.get();
                }
                else
                {
                    return Futures.immediateFuture(null);
                }

            }
        }, MoreExecutors.directExecutor());

        // mark propose finalization
        ListenableFuture<Boolean> proposeFuture = Futures.transformAsync(repairSessionResults, new AsyncFunction<List<RepairSessionResult>, Boolean>()
        {
            public ListenableFuture<Boolean> apply(List<RepairSessionResult> results) throws Exception
            {
                if (results == null || results.isEmpty() || Iterables.any(results, r -> r == null))
                {
                    finalizeStart = System.currentTimeMillis();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Incremental repair {} validation/stream phase completed in {}", sessionID, formatDuration(repairStart, finalizeStart));

                    }
                    return Futures.immediateFailedFuture(SomeRepairFailedException.INSTANCE);
                }
                else
                {
                    return finalizePropose();
                }
            }
        }, MoreExecutors.directExecutor());

        // return execution result as set by following callback
        SettableFuture<Boolean> resultFuture = SettableFuture.create();

        // commit repaired data
        Futures.addCallback(proposeFuture, new FutureCallback<Boolean>()
        {
            public void onSuccess(@Nullable Boolean result)
            {
                try
                {
                    if (result != null && result)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Incremental repair {} finalization phase completed in {}", sessionID, formatDuration(finalizeStart, System.currentTimeMillis()));
                        }
                        finalizeCommit();
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Incremental repair {} phase completed in {}", sessionID, formatDuration(sessionStart, System.currentTimeMillis()));
                        }
                    }
                    else
                    {
                        hasFailure.set(true);
                        fail();
                    }
                    resultFuture.set(result);
                }
                catch (Exception e)
                {
                    resultFuture.setException(e);
                }
            }

            public void onFailure(Throwable t)
            {
                try
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Incremental repair {} phase failed in {}", sessionID, formatDuration(sessionStart, System.currentTimeMillis()));
                    }
                    hasFailure.set(true);
                    fail();
                }
                finally
                {
                    resultFuture.setException(t);
                }
            }
        }, MoreExecutors.directExecutor());

        return resultFuture;
    }
}
