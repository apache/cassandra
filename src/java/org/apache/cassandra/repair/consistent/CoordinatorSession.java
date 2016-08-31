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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairSessionResult;
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

    private final Map<InetAddress, State> participantStates = new HashMap<>();
    private final SettableFuture<Boolean> prepareFuture = SettableFuture.create();
    private final SettableFuture<Boolean> finalizeProposeFuture = SettableFuture.create();


    public CoordinatorSession(Builder builder)
    {
        super(builder);
        for (InetAddress participant : participants)
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
        logger.debug("Setting coordinator state to {} for repair {}", state, sessionID);
        super.setState(state);
    }

    public synchronized void setParticipantState(InetAddress participant, State state)
    {
        logger.debug("Setting participant {} to state {} for repair {}", participant, state, sessionID);
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
        for (InetAddress participant : participants)
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

    protected void sendMessage(InetAddress destination, RepairMessage message)
    {
        MessageOut<RepairMessage> messageOut = new MessageOut<RepairMessage>(MessagingService.Verb.REPAIR_MESSAGE, message, RepairMessage.serializer);
        MessagingService.instance().sendOneWay(messageOut, destination);
    }

    public ListenableFuture<Boolean> prepare(Executor executor)
    {
        Preconditions.checkArgument(allStates(State.PREPARING));

        logger.debug("Sending PrepareConsistentRequest message to {}", participants);
        PrepareConsistentRequest message = new PrepareConsistentRequest(sessionID, coordinator, participants);
        for (final InetAddress participant : participants)
        {
            executor.execute(() -> sendMessage(participant, message));
        }
        return prepareFuture;
    }

    public synchronized void handlePrepareResponse(InetAddress participant, boolean success)
    {
        if (getState() == State.FAILED)
        {
            logger.debug("Consistent repair {} has failed, ignoring prepare response from {}", sessionID, participant);
        }
        else if (!success)
        {
            logger.debug("Failed prepare response received from {} for session {}", participant, sessionID);
            fail();
            prepareFuture.set(false);
        }
        else
        {
            logger.debug("Successful prepare response received from {} for session {}", participant, sessionID);
            setParticipantState(participant, State.PREPARED);
            if (getState() == State.PREPARED)
            {
                prepareFuture.set(true);
            }
        }
    }

    public synchronized void setRepairing()
    {
        setAll(State.REPAIRING);
    }

    public synchronized ListenableFuture<Boolean> finalizePropose(Executor executor)
    {
        Preconditions.checkArgument(allStates(State.REPAIRING));
        logger.debug("Sending FinalizePropose message to {}", participants);
        FinalizePropose message = new FinalizePropose(sessionID);
        for (final InetAddress participant : participants)
        {
            executor.execute(() -> sendMessage(participant, message));
        }
        return finalizeProposeFuture;
    }

    public synchronized void handleFinalizePromise(InetAddress participant, boolean success)
    {
        if (getState() == State.FAILED)
        {
            logger.debug("Consistent repair {} has failed, ignoring finalize promise from {}", sessionID, participant);
        }
        else if (!success)
        {
            logger.debug("Failed finalize promise received from {} for session {}", participant, sessionID);
            fail();
            finalizeProposeFuture.set(false);
        }
        else
        {
            logger.debug("Successful finalize promise received from {} for session {}", participant, sessionID);
            setParticipantState(participant, State.FINALIZE_PROMISED);
            if (getState() == State.FINALIZE_PROMISED)
            {
                finalizeProposeFuture.set(true);
            }
        }
    }

    public synchronized void finalizeCommit(Executor executor)
    {
        Preconditions.checkArgument(allStates(State.FINALIZE_PROMISED));
        logger.debug("Sending FinalizeCommit message to {}", participants);
        FinalizeCommit message = new FinalizeCommit(sessionID);
        for (final InetAddress participant : participants)
        {
            executor.execute(() -> sendMessage(participant, message));
        }
        setAll(State.FINALIZED);
    }

    public void fail()
    {
        fail(MoreExecutors.directExecutor());
    }

    public synchronized void fail(Executor executor)
    {
        logger.debug("Failing session {}", sessionID);
        FailSession message = new FailSession(sessionID);
        for (final InetAddress participant : participants)
        {
            if (participantStates.get(participant) != State.FAILED)
            {
                executor.execute(() -> sendMessage(participant, message));
            }
        }
        setAll(State.FAILED);
    }

    /**
     * Runs the asynchronous consistent repair session. Actual repair sessions are scheduled via a submitter to make unit testing easier
     */
    public ListenableFuture execute(Executor executor, Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSubmitter, AtomicBoolean hasFailure)
    {
        logger.debug("Executing consistent repair {}", sessionID);

        ListenableFuture<Boolean> prepareResult = prepare(executor);

        // run repair sessions normally
        ListenableFuture<List<RepairSessionResult>> repairSessionResults = Futures.transform(prepareResult, new AsyncFunction<Boolean, List<RepairSessionResult>>()
        {
            public ListenableFuture<List<RepairSessionResult>> apply(Boolean success) throws Exception
            {
                if (success)
                {
                    setRepairing();
                    return sessionSubmitter.get();
                }
                else
                {
                    return Futures.immediateFuture(null);
                }

            }
        });

        // mark propose finalization
        ListenableFuture<Boolean> proposeFuture = Futures.transform(repairSessionResults, new AsyncFunction<List<RepairSessionResult>, Boolean>()
        {
            public ListenableFuture<Boolean> apply(List<RepairSessionResult> results) throws Exception
            {
                if (results == null || results.isEmpty() || Iterables.any(results, r -> r == null))
                {
                    return Futures.immediateFailedFuture(new RuntimeException());
                }
                else
                {
                    return finalizePropose(executor);
                }
            }
        });

        // commit repaired data
        Futures.addCallback(proposeFuture, new FutureCallback<Boolean>()
        {
            public void onSuccess(@Nullable Boolean result)
            {
                if (result != null && result)
                {
                    finalizeCommit(executor);
                }
                else
                {
                    hasFailure.set(true);
                    fail(executor);
                }
            }

            public void onFailure(Throwable t)
            {
                hasFailure.set(true);
                fail(executor);
            }
        });

        return proposeFuture;
    }
}
