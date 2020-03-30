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

package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_REQ;
import static org.apache.cassandra.service.paxos.PaxosPropose.Superseded.SideEffects.NO;
import static org.apache.cassandra.service.paxos.PaxosPropose.Superseded.SideEffects.MAYBE;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

/**
 * In waitForNoSideEffect mode, we will not return failure to the caller until
 * we have received a complete set of refusal responses, or at least one accept,
 * indicating (respectively) that we have had no side effect, or that we cannot
 * know if we our proposal produced a side effect.
 */
public class PaxosPropose<OnDone extends Consumer<? super PaxosPropose.Status>> extends PaxosRequestCallback<PaxosPropose.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPropose.class);

    public static final RequestHandler requestHandler = new RequestHandler();
    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    /**
     * Represents the current status of a propose action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        enum Outcome { SUCCESS, SUPERSEDED, MAYBE_FAILURE }
        final Outcome outcome;

        Status(Outcome outcome)
        {
            this.outcome = outcome;
        }
        Superseded superseded() { return (Superseded) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
        public String toString() { return "Success"; }
    }

    static class Superseded extends Status
    {
        enum SideEffects { NO, MAYBE }
        final Ballot by;
        final SideEffects hadSideEffects;
        Superseded(Ballot by, SideEffects hadSideEffects)
        {
            super(Outcome.SUPERSEDED);
            this.by = by;
            this.hadSideEffects = hadSideEffects;
        }

        public String toString() { return "Superseded(" + by + ',' + hadSideEffects + ')'; }
    }

    private static class MaybeFailure extends Status
    {
        final Paxos.MaybeFailure info;
        MaybeFailure(Paxos.MaybeFailure info)
        {
            super(Outcome.MAYBE_FAILURE);
            this.info = info;
        }

        public String toString() { return info.toString(); }
    }

    private static final Status success = new Status(Status.Outcome.SUCCESS);

    private static final AtomicLongFieldUpdater<PaxosPropose> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosPropose.class, "responses");
    private static final AtomicReferenceFieldUpdater<PaxosPropose, Ballot> supersededByUpdater = AtomicReferenceFieldUpdater.newUpdater(PaxosPropose.class, Ballot.class, "supersededBy");

    @VisibleForTesting public static final long ACCEPT_INCREMENT = 1;
    private static final int  REFUSAL_SHIFT = 21;
    @VisibleForTesting public static final long REFUSAL_INCREMENT = 1L << REFUSAL_SHIFT;
    private static final int  FAILURE_SHIFT = 42;
    @VisibleForTesting public static final long FAILURE_INCREMENT = 1L << FAILURE_SHIFT;
    private static final long MASK = (1L << REFUSAL_SHIFT) - 1L;

    private final Proposal proposal;
    /** Wait until we know if we may have had side effects */
    private final boolean waitForNoSideEffect;
    /** Number of contacted nodes */
    final int participants;
    /** Number of accepts required */
    final int required;
    /** Invoke on reaching a terminal status */
    final OnDone onDone;

    /**
     * bit 0-20:  accepts
     * bit 21-41: refusals/errors
     * bit 42-62: timeouts
     * bit 63:    ambiguous signal bit (i.e. those states that cannot be certain to signal uniquely flip this bit to take signal responsibility)
     *
     * {@link #accepts}
     * {@link #refusals}
     * {@link #failures}
     * {@link #notAccepts} (timeouts/errors+refusals)
     */
    private volatile long responses;

    /** The newest superseding ballot from a refusal; only returned to the caller if we fail to reach a quorum */
    private volatile Ballot supersededBy;

    private PaxosPropose(Proposal proposal, int participants, int required, boolean waitForNoSideEffect, OnDone onDone)
    {
        this.proposal = proposal;
        assert required > 0;
        this.waitForNoSideEffect = waitForNoSideEffect;
        this.participants = participants;
        this.required = required;
        this.onDone = onDone;
    }

    /**
     * Submit the proposal for commit with all replicas, and return an object that can be waited on synchronously for the result,
     * or for the present status if the time elapses without a final result being reached.
     * @param waitForNoSideEffect if true, on failure we will wait until we can say with certainty there are no side effects
     *                            or until we know we will never be able to determine this with certainty
     */
    static Paxos.Async<Status> propose(Proposal proposal, Paxos.Participants participants, boolean waitForNoSideEffect)
    {
        if (waitForNoSideEffect && proposal.update.isEmpty())
            waitForNoSideEffect = false; // by definition this has no "side effects" (besides linearizing the operation)

        // to avoid unnecessary object allocations we extend PaxosPropose to implements Paxos.Async
        class Async extends PaxosPropose<ConditionAsConsumer<Status>> implements Paxos.Async<Status>
        {
            private Async(Proposal proposal, int participants, int required, boolean waitForNoSideEffect)
            {
                super(proposal, participants, required, waitForNoSideEffect, newConditionAsConsumer());
            }

            public Status awaitUntil(long deadline)
            {
                try
                {
                    onDone.awaitUntil(deadline);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return new MaybeFailure(new Paxos.MaybeFailure(true, participants, required, 0, emptyMap()));
                }

                return status();
            }
        }

        Async propose = new Async(proposal, participants.sizeOfPoll(), participants.sizeOfConsensusQuorum, waitForNoSideEffect);
        propose.start(participants);
        return propose;
    }

    static <T extends Consumer<Status>> T propose(Proposal proposal, Paxos.Participants participants, boolean waitForNoSideEffect, T onDone)
    {
        if (waitForNoSideEffect && proposal.update.isEmpty())
            waitForNoSideEffect = false; // by definition this has no "side effects" (besides linearizing the operation)

        PaxosPropose<?> propose = new PaxosPropose<>(proposal, participants.sizeOfPoll(), participants.sizeOfConsensusQuorum, waitForNoSideEffect, onDone);
        propose.start(participants);
        return onDone;
    }

    void start(Paxos.Participants participants)
    {
        Message<Request> message = Message.out(PAXOS2_PROPOSE_REQ, new Request(proposal));

        boolean executeOnSelf = false;
        for (int i = 0, size = participants.sizeOfPoll(); i < size ; ++i)
        {
            InetAddressAndPort destination = participants.voter(i);
            logger.trace("{} to {}", proposal, destination);
            if (shouldExecuteOnSelf(destination)) executeOnSelf = true;
            else MessagingService.instance().sendWithCallback(message, destination, this);
        }

        if (executeOnSelf)
            PAXOS2_PROPOSE_REQ.stage.execute(() -> executeOnSelf(proposal));
    }

    /**
     * @return the result as of now; unless the result is definitive, it is only a snapshot of the present incomplete status
     */
    Status status()
    {
        long responses = this.responses;

        if (isSuccessful(responses))
            return success;

        if (!canSucceed(responses) && supersededBy != null)
        {
            Superseded.SideEffects sideEffects = hasNoSideEffects(responses) ? NO : MAYBE;
            return new Superseded(supersededBy, sideEffects);
        }

        return new MaybeFailure(new Paxos.MaybeFailure(participants, required, accepts(responses), failureReasonsAsMap()));
    }

    private void executeOnSelf(Proposal proposal)
    {
        executeOnSelf(proposal, RequestHandler::execute);
    }

    public void onResponse(Response response, InetAddressAndPort from)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} for {} from {}", response, proposal, from);

        Ballot supersededBy = response.supersededBy;
        if (supersededBy != null)
            supersededByUpdater.accumulateAndGet(this, supersededBy, (a, b) -> a == null ? b : b.uuidTimestamp() > a.uuidTimestamp() ? b : a);

        long increment = supersededBy == null
                ? ACCEPT_INCREMENT
                : REFUSAL_INCREMENT;

        update(increment);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} {} failure from {}", proposal, reason, from);

        super.onFailure(from, reason);
        update(FAILURE_INCREMENT);
    }

    private void update(long increment)
    {
        long responses = responsesUpdater.addAndGet(this, increment);
        if (shouldSignal(responses))
            signalDone();
    }

    // returns true at most once for a given PaxosPropose, so we do not propagate a signal more than once
    private boolean shouldSignal(long responses)
    {
        return shouldSignal(responses, required, participants, waitForNoSideEffect, responsesUpdater, this);
    }

    @VisibleForTesting
    public static <T> boolean shouldSignal(long responses, int required, int participants, boolean waitForNoSideEffect, AtomicLongFieldUpdater<T> responsesUpdater, T update)
    {
        if (responses <= 0L) // already signalled via ambiguous signal bit
            return false;

        if (!isSuccessful(responses, required))
        {
            if (canSucceed(responses, required, participants))
                return false;

            if (waitForNoSideEffect && !hasPossibleSideEffects(responses))
                return hasNoSideEffects(responses, participants);
        }

        return responsesUpdater.getAndUpdate(update, x -> x | Long.MIN_VALUE) >= 0L;
    }

    private void signalDone()
    {
        if (onDone != null)
            onDone.accept(status());
    }

    private boolean isSuccessful(long responses)
    {
        return isSuccessful(responses, required);
    }

    private static boolean isSuccessful(long responses, int required)
    {
        return accepts(responses) >= required;
    }

    private boolean canSucceed(long responses)
    {
        return canSucceed(responses, required, participants);
    }

    private static boolean canSucceed(long responses, int required, int participants)
    {
        return refusals(responses) == 0 && required <= participants - failures(responses);
    }

    // Note: this is only reliable if !failFast
    private boolean hasNoSideEffects(long responses)
    {
        return hasNoSideEffects(responses, participants);
    }

    private static boolean hasNoSideEffects(long responses, int participants)
    {
        return refusals(responses) == participants;
    }

    private static boolean hasPossibleSideEffects(long responses)
    {
        return accepts(responses) + failures(responses) > 0;
    }

    /** {@link #responses} */
    private static int accepts(long responses)
    {
        return (int) (responses & MASK);
    }

    /** {@link #responses} */
    private static int notAccepts(long responses)
    {
        return failures(responses) + refusals(responses);
    }

    /** {@link #responses} */
    private static int refusals(long responses)
    {
        return (int) ((responses >>> REFUSAL_SHIFT) & MASK);
    }

    /** {@link #responses} */
    private static int failures(long responses)
    {
        return (int) ((responses >>> FAILURE_SHIFT) & MASK);
    }

    /**
     * A Proposal to submit to another node
     */
    static class Request
    {
        final Proposal proposal;
        Request(Proposal proposal)
        {
            this.proposal = proposal;
        }

        public String toString()
        {
            return proposal.toString("Propose");
        }
    }

    /**
     * The response to a proposal, indicating success (if {@code supersededBy == null},
     * or failure, alongside the ballot that beat us
     */
    static class Response
    {
        final Ballot supersededBy;
        Response(Ballot supersededBy)
        {
            this.supersededBy = supersededBy;
        }
        public String toString() { return supersededBy == null ? "Accept" : "RejectProposal(supersededBy=" + supersededBy + ')'; }
    }

    /**
     * The proposal request handler, i.e. receives a proposal from a peer and responds with either acccept/reject
     */
    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(Message<Request> message)
        {
            Response response = execute(message.payload.proposal, message.from());
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        public static Response execute(Proposal proposal, InetAddressAndPort from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, proposal.update.partitionKey(), proposal.update.metadata(), false))
                return null;

            long start = nanoTime();
            try (PaxosState state = PaxosState.get(proposal))
            {
                return new Response(state.acceptIfLatest(proposal));
            }
            finally
            {
                Keyspace.openAndGetStore(proposal.update.metadata()).metric.casPropose.addNano(nanoTime() - start);
            }
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Proposal.serializer.serialize(request.proposal, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Proposal propose = Proposal.serializer.deserialize(in, version);
            return new Request(propose);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Proposal.serializer.serializedSize(request.proposal, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.supersededBy != null);
            if (response.supersededBy != null)
                response.supersededBy.serialize(out);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isSuperseded = in.readBoolean();
            return isSuperseded ? new Response(Ballot.deserialize(in)) : new Response(null);
        }

        public long serializedSize(Response response, int version)
        {
            return response.supersededBy != null
                    ? TypeSizes.sizeof(true) + Ballot.sizeInBytes()
                    : TypeSizes.sizeof(false);
        }
    }
}
