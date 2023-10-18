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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.paxos.Paxos.Participants;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.exceptions.RequestFailureReason.NODE_DOWN;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_REMOTE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.service.StorageProxy.shouldHint;
import static org.apache.cassandra.service.StorageProxy.submitHint;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

// Does not support EACH_QUORUM, as no such thing as EACH_SERIAL
public class PaxosCommit<OnDone extends Consumer<? super PaxosCommit.Status>> extends PaxosRequestCallback<NoPayload>
{
    public static final RequestHandler requestHandler = new RequestHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommit.class);

    private static volatile boolean ENABLE_DC_LOCAL_COMMIT = CassandraRelevantProperties.ENABLE_DC_LOCAL_COMMIT.getBoolean();

    public static boolean getEnableDcLocalCommit()
    {
        return ENABLE_DC_LOCAL_COMMIT;
    }

    public static void setEnableDcLocalCommit(boolean enableDcLocalCommit)
    {
        ENABLE_DC_LOCAL_COMMIT = enableDcLocalCommit;
    }

    /**
     * Represents the current status of a commit action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        private final Paxos.MaybeFailure maybeFailure;

        Status(Paxos.MaybeFailure maybeFailure)
        {
            this.maybeFailure = maybeFailure;
        }

        boolean isSuccess() { return maybeFailure == null; }
        Paxos.MaybeFailure maybeFailure() { return maybeFailure; }

        public String toString() { return maybeFailure == null ? "Success" : maybeFailure.toString(); }
    }

    private static final Status success = new Status(null);

    private static final AtomicLongFieldUpdater<PaxosCommit> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosCommit.class, "responses");

    final Agreed commit;
    final boolean allowHints;
    final ConsistencyLevel consistencyForConsensus;
    final ConsistencyLevel consistencyForCommit;

    final EndpointsForToken replicas;
    final int required;
    final OnDone onDone;

    /**
     * packs two 32-bit integers;
     * bit 00-31: accepts
     * bit 32-63: failures/timeouts
     * 
     * {@link #accepts} 
     * {@link #failures}
     */
    private volatile long responses;

    public PaxosCommit(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, Participants participants, OnDone onDone)
    {
        this.commit = commit;
        this.allowHints = allowHints;
        this.consistencyForConsensus = consistencyForConsensus;
        this.consistencyForCommit = consistencyForCommit;
        this.replicas = participants.all;
        this.onDone = onDone;
        this.required = participants.requiredFor(consistencyForCommit);
        if (required == 0)
            onDone.accept(status());
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static Paxos.Async<Status> commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints)
    {
        // to avoid unnecessary object allocations we extend PaxosPropose to implements Paxos.Async
        class Async extends PaxosCommit<ConditionAsConsumer<Status>> implements Paxos.Async<Status>
        {
            private Async(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, Participants participants)
            {
                super(commit, allowHints, consistencyForConsensus, consistencyForCommit, participants, newConditionAsConsumer());
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
                    return new Status(new Paxos.MaybeFailure(true, replicas.size(), required, 0, emptyMap()));
                }

                return status();
            }
        }

        Async async = new Async(commit, allowHints, consistencyForConsensus, consistencyForCommit, participants);
        async.start(participants, false);
        return async;
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static <T extends Consumer<Status>> T commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints, T onDone)
    {
        new PaxosCommit<>(commit, allowHints, consistencyForConsensus, consistencyForCommit, participants, onDone)
                .start(participants, true);
        return onDone;
    }

    /**
     * Send commit messages to peers (or self)
     */
    void start(Participants participants, boolean async)
    {
        boolean executeOnSelf = false;
        Message<Agreed> commitMessage = Message.out(PAXOS_COMMIT_REQ, commit);
        Message<Mutation> mutationMessage = ENABLE_DC_LOCAL_COMMIT && consistencyForConsensus.isDatacenterLocal()
                                            ? Message.out(PAXOS2_COMMIT_REMOTE_REQ, commit.makeMutation()) : null;

        for (int i = 0, mi = participants.allLive.size(); i < mi ; ++i)
            executeOnSelf |= isSelfOrSend(commitMessage, mutationMessage, participants.allLive.endpoint(i));

        for (int i = 0, mi = participants.allDown.size(); i < mi ; ++i)
            onFailure(participants.allDown.endpoint(i), NODE_DOWN);

        if (executeOnSelf)
        {
            ExecutorPlus executor = PAXOS_COMMIT_REQ.stage.executor();
            if (async) executor.execute(this::executeOnSelf);
            else executor.maybeExecuteImmediately(this::executeOnSelf);
        }
    }

    /**
     * If isLocal return true; otherwise if the destination is alive send our message, and if not mark the callback with failure
     */
    private boolean isSelfOrSend(Message<Agreed> commitMessage, Message<Mutation> mutationMessage, InetAddressAndPort destination)
    {
        if (shouldExecuteOnSelf(destination))
            return true;

        // don't send commits to remote dcs for local_serial operations
        if (mutationMessage != null && !isInLocalDc(destination))
            MessagingService.instance().sendWithCallback(mutationMessage, destination, this);
        else
            MessagingService.instance().sendWithCallback(commitMessage, destination, this);

        return false;
    }

    private static boolean isInLocalDc(InetAddressAndPort destination)
    {
        return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination));
    }

    /**
     * Record a failure or timeout, and maybe submit a hint to {@code from}
     */
    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} {} from {}", commit, reason, from);

        response(false, from);
        Replica replica = replicas.lookup(from);

        if (allowHints && shouldHint(replica))
            submitHint(commit.makeMutation(), replica, null);
    }

    /**
     * Record a success response
     */
    public void onResponse(Message<NoPayload> response)
    {
        logger.trace("{} Success from {}", commit, response.from());

        response(true, response.from());
    }

    /**
     * Execute locally and record response
     */
    public void executeOnSelf()
    {
        executeOnSelf(commit, RequestHandler::execute);
    }

    @Override
    public void onResponse(NoPayload response, InetAddressAndPort from)
    {
        response(response != null, from);
    }

    /**
     * Record a failure or success response if {@code from} contributes to our consistency.
     * If we have reached a final outcome of the commit, run {@code onDone}.
     */
    private void response(boolean success, InetAddressAndPort from)
    {
        if (consistencyForCommit.isDatacenterLocal() && !InOurDc.endpoints().test(from))
            return;

        long responses = responsesUpdater.addAndGet(this, success ? 0x1L : 0x100000000L);
        // next two clauses mutually exclusive to ensure we only invoke onDone once, when either failed or succeeded
        if (accepts(responses) == required) // if we have received _precisely_ the required accepts, we have succeeded
            onDone.accept(status());
        else if (replicas.size() - failures(responses) == required - 1) // if we are _unable_ to receive the required accepts, we have failed
            onDone.accept(status());
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    Status status()
    {
        long responses = this.responses;
        if (isSuccessful(responses))
            return success;

        return new Status(new Paxos.MaybeFailure(replicas.size(), required, accepts(responses), failureReasonsAsMap()));
    }

    private boolean isSuccessful(long responses)
    {
        return accepts(responses) >= required;
    }

    private static int accepts(long responses)
    {
        return (int) (responses & 0xffffffffL);
    }

    private static int failures(long responses)
    {
        return (int) (responses >>> 32);
    }

    public static class RequestHandler implements IVerbHandler<Agreed>
    {
        @Override
        public void doVerb(Message<Agreed> message)
        {
            NoPayload response = execute(message.payload, message.from());
            // NOTE: for correctness, this must be our last action, so that we cannot throw an error and send both a response and a failure response
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        private static NoPayload execute(Agreed agreed, InetAddressAndPort from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, agreed.update.partitionKey(), agreed.update.metadata(), false))
                return null;

            PaxosState.commitDirect(agreed);
            Tracing.trace("Enqueuing acknowledge to {}", from);
            return NoPayload.noPayload;
        }
    }

}
