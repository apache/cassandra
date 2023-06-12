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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_PAXOS_REPAIR_VERSION_VALIDATION;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_REPAIR_REQ;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.service.paxos.ContentionStrategy.Type.REPAIR;
import static org.apache.cassandra.service.paxos.ContentionStrategy.waitUntilForContention;
import static org.apache.cassandra.service.paxos.Paxos.*;
import static org.apache.cassandra.service.paxos.PaxosPrepare.*;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

/**
 * Facility to finish any in-progress paxos transaction, and ensure that a quorum of nodes agree on the most recent operation.
 * Semantically, we simply ensure that any side effects that were "decided" before repair was initiated have been committed
 * to a quorum of nodes.
 * This means:
 *   - any prepare that has _possibly_ reached a quorum of nodes will be invalidated
 *   - any proposal that has been accepted by at least one node, but not known to be committed to any, will be proposed again
 *   - any proposal that has been committed to at least one node, but not committed to all, will be committed to a quorum
 *
 * Note that once started, this continues to try to repair any ongoing operations for the partition up to 4 times.
 * In a functioning cluster this should always be possible, but during a network partition this might cause the repair
 * to fail.
 *
 * Requirements for correction:
 * - If performed during a range movement, we depend on a quorum (of the new topology) have been informed of the new
 *   topology _prior_ to initiating this repair, and this node to have been a member of a quorum of nodes verifying
 *    - If a quorum of nodes is unaware of the new topology prior to initiating repair, an operation could simply occur
 *      after repair completes that permits a linearization failure, such as with CASSANDRA-15745.
 *   their topology is up-to-date.
 * - Paxos prepare rounds must also verify the topology being used with their peers
 *    - If prepare rounds do not verify their topology, a node that is not a member of the quorum who have agreed
 *      the latest topology could still perform an operation without being aware of the topology change, and permit a
 *      linearization failure, such as with CASSANDRA-15745.
 *
 * With these issues addressed elsewhere, our algorithm is fairly simple.
 * In brief:
 *   1) Query all replicas for any promises or proposals they have witnessed that have not been committed,
 *      and their most recent commit. Wait for a quorum of responses.
 *   2) If this is the first time we have queried other nodes, we take a note of the most recent ballot we see;
 *      If this is not the first time we have queried other nodes, and we have committed a newer ballot than the one
 *      we previously recorded, we terminate (somebody else has done the work for us).
 *   3) If we see an in-progress operation that is very recent, we wait for it to complete and try again
 *   4) If we see a previously accepted operation, we attempt to complete it, or
 *      if we see a prepare with no proposal, we propose an empty update to invalidate it;
 *      otherwise we have nothing to do, as there is no operation that can have produced a side-effect before we began.
 *   5) We prepare a paxos round to agree the new commit using a higher ballot than the one witnessed,
 *      but a lower than one we would propose a new operation with. This permits newer operations to "beat" us so
 *      that we do not interfere with normal paxos operations.
 *   6) If we are "beaten" we start again (without delay, as (2) manages delays where necessary)
 */
public class PaxosRepair extends AbstractPaxosRepair
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRepair.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();
    public static final RequestHandler requestHandler = new RequestHandler();
    private static final long RETRY_TIMEOUT_NANOS = getRetryTimeoutNanos();

    private static final ScheduledExecutorPlus RETRIES = executorFactory().scheduled("PaxosRepairRetries");

    private static long getRetryTimeoutNanos()
    {
        long retryMillis = PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS.getLong();
        return TimeUnit.MILLISECONDS.toNanos(retryMillis);
    }

    private final TableMetadata table;
    private final ConsistencyLevel paxosConsistency;
    private Participants participants;

    private Ballot successCriteria;
    private Ballot prevSupersededBy;
    private int attempts;

    public String toString()
    {
        return "PaxosRepair{" +
               "key=" + partitionKey() +
               ", table=" + table.toString() +
               ", consistency=" + paxosConsistency +
               ", participants=" + participants.electorate +
               ", state=" + state() +
               ", startedMillis=" + MonotonicClock.Global.approxTime.translate().toMillisSinceEpoch(startedNanos()) +
               ", started=" + isStarted() +
               '}';
    }

    /**
     * Waiting for responses to PAXOS_REPAIR messages.
     *
     * This state may be entered multiple times; every time we fail for any reason, we restart from this state
     */
    private class Querying extends State implements RequestCallbackWithFailure<Response>, Runnable
    {
        private int successes;
        private int failures;

        private Ballot latestWitnessed;
        private @Nullable Accepted latestAccepted;
        private Committed latestCommitted;
        private Ballot oldestCommitted;
        private Ballot clashingPromise;

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
        {
            updateState(this, null, (i1, i2) -> i1.onFailure());
        }

        @Override
        public void onResponse(Message<Response> msg)
        {
            logger.trace("PaxosRepair {} from {}", msg.payload, msg.from());
            updateState(this, msg, Querying::onResponseInternal);
        }

        private State onFailure()
        {
            if (++failures + participants.sizeOfConsensusQuorum > participants.sizeOfPoll())
                return retry(this);
            return this;
        }

        private State onResponseInternal(Message<Response> msg)
        {
            latestWitnessed = latest(latestWitnessed, msg.payload.latestWitnessedOrLowBound);
            latestAccepted = latest(latestAccepted, msg.payload.acceptedButNotCommitted);
            latestCommitted = latest(latestCommitted, msg.payload.committed);
            if (oldestCommitted == null || isAfter(oldestCommitted, msg.payload.committed))
                oldestCommitted = msg.payload.committed.ballot;

            if (isAfter(latestWitnessed, clashingPromise))
                clashingPromise = null;
            if (timestampsClash(latestAccepted, msg.payload.latestWitnessedOrLowBound))
                clashingPromise = msg.payload.latestWitnessedOrLowBound;
            if (timestampsClash(latestAccepted, latestWitnessed))
                clashingPromise = latestWitnessed;

            // once we receive the requisite number, we can simply proceed, and ignore future responses
            if (++successes == participants.sizeOfConsensusQuorum)
                return execute();

            return this;
        }

        private State execute()
        {
            // if we have a timestamp clash, always prefer the accepted ballot
            latestWitnessed = latest(latestAccepted, latestWitnessed);
            Ballot latestPreviouslyWitnessed = latest(successCriteria, prevSupersededBy);

            // Save as success criteria the latest promise seen by our first round; if we ever see anything
            // newer committed, we know at least one paxos round has been completed since we started, which is all we need
            // or newer than this committed we know we're done, so to avoid looping indefinitely in competition
            // with others, we store this ballot for future retries so we can terminate based on other proposers' work
            if (successCriteria == null || timestampsClash(successCriteria, latestWitnessed))
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} setting success criteria to {}", partitionKey(), Ballot.toString(latestWitnessed));

                successCriteria = latestWitnessed;
            }

            boolean hasCommittedSuccessCriteria = isAfter(latestCommitted, successCriteria) || latestCommitted.hasBallot(successCriteria);
            boolean isPromisedButNotAccepted    = isAfter(latestWitnessed, latestAccepted); // not necessarily promised - may be lowBound
            boolean isAcceptedButNotCommitted   = isAfter(latestAccepted, latestCommitted);
            boolean reproposalMayBeRejected     = clashingPromise != null || !isAfter(latestWitnessed, latestPreviouslyWitnessed);

            if (hasCommittedSuccessCriteria)
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair witnessed {} newer than success criteria {} (oldest: {})", latestCommitted, Ballot.toString(successCriteria), Ballot.toString(oldestCommitted));

                // we have a new enough commit, but it might not have reached enough participants; make sure it has before terminating
                // note: we could send to only those we know haven't witnessed it, but this is a rare operation so a small amount of redundant work is fine
                return oldestCommitted.equals(latestCommitted.ballot)
                        ? DONE
                        : PaxosCommit.commit(latestCommitted, participants, paxosConsistency, commitConsistency(), true,
                                             new CommittingRepair());
            }
            else if (isAcceptedButNotCommitted && !isPromisedButNotAccepted && !reproposalMayBeRejected)
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} completing {}", partitionKey(), latestAccepted);
                // We need to complete this in-progress accepted proposal, which may not have been seen by a majority
                // However, since we have not sought any promises, we can simply complete the existing proposal
                // since this is an idempotent operation - both us and the original proposer (and others) can
                // all do it at the same time without incident

                // If ballots with same timestamp have been both accepted and rejected by different nodes,
                // to avoid a livelock we simply try to poison, knowing we will fail but use a new ballot
                // (note there are alternative approaches but this is conservative)

                return PaxosPropose.propose(latestAccepted, participants, false,
                        new ProposingRepair(latestAccepted));
            }
            else if (isAcceptedButNotCommitted || isPromisedButNotAccepted || latestWitnessed.compareTo(latestPreviouslyWitnessed) < 0)
            {
                Ballot ballot = staleBallotNewerThan(latest(latestWitnessed, latestPreviouslyWitnessed), paxosConsistency);
                // We need to propose a no-op > latestPromised, to ensure we don't later discover
                // that latestPromised had already been accepted (by a minority) and repair it
                // This means starting a new ballot, but we choose to use one that is likely to lose a contention battle
                // Since this operation is not urgent, and we can piggy-back on other paxos operations
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} found incomplete promise or proposal; preparing stale ballot {}", partitionKey(), Ballot.toString(ballot));

                return prepareWithBallot(ballot, participants, partitionKey(), table, false, false,
                        new PoisonProposals());
            }
            else
            {
                logger.error("PaxosRepair illegal state latestWitnessed={}, latestAcceptedButNotCommitted={}, latestCommitted={}, oldestCommitted={}", latestWitnessed, latestAccepted, latestCommitted, oldestCommitted);
                throw new IllegalStateException(); // should be logically impossible
            }
        }

        public void run()
        {
            Message<Request> message = Message.out(PAXOS2_REPAIR_REQ, new Request(partitionKey(), table));
            for (int i = 0, size = participants.sizeOfPoll(); i < size ; ++i)
                MessagingService.instance().sendWithCallback(message, participants.voter(i), this);
        }
    }

    /**
     * We found either an incomplete promise or proposal, so we need to start a new paxos round to complete them
     */
    private class PoisonProposals extends ConsumerState<Status>
    {
        @Override
        public State execute(Status input) throws Throwable
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                    return retry(this);

                case READ_PERMITTED:
                case SUPERSEDED:
                    prevSupersededBy = latest(prevSupersededBy, input.retryWithAtLeast());
                    return retry(this);

                case FOUND_INCOMPLETE_ACCEPTED:
                {
                    // finish the in-progress proposal
                    // cannot simply restart, as our latest promise is newer than the proposal
                    // so we require a promise before we decide which proposal to complete
                    // (else an "earlier" operation can sneak in and invalidate us while we're proposing
                    // with a newer ballot)
                    FoundIncompleteAccepted incomplete = input.incompleteAccepted();
                    Proposal propose = new Proposal(incomplete.ballot, incomplete.accepted.update);
                    logger.trace("PaxosRepair of {} found incomplete {}", partitionKey(), incomplete.accepted);
                    return PaxosPropose.propose(propose, participants, false,
                            new ProposingRepair(propose)); // we don't know if we're done, so we must restart
                }

                case FOUND_INCOMPLETE_COMMITTED:
                {
                    // finish the in-progress commit
                    FoundIncompleteCommitted incomplete = input.incompleteCommitted();
                    logger.trace("PaxosRepair of {} found in progress {}", partitionKey(), incomplete.committed);
                    return PaxosCommit.commit(incomplete.committed, participants, paxosConsistency, commitConsistency(), true,
                                              new CommitAndRestart()); // we don't know if we're done, so we must restart
                }

                case PROMISED:
                {
                    // propose the empty ballot
                    logger.trace("PaxosRepair of {} submitting empty proposal", partitionKey());
                    Proposal proposal = Proposal.empty(input.success().ballot, partitionKey(), table);
                    return PaxosPropose.propose(proposal, participants, false,
                            new ProposingRepair(proposal));
                }

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private class ProposingRepair extends ConsumerState<PaxosPropose.Status>
    {
        final Proposal proposal;
        private ProposingRepair(Proposal proposal)
        {
            this.proposal = proposal;
        }

        @Override
        public State execute(PaxosPropose.Status input)
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                    return retry(this);

                case SUPERSEDED:
                    if (isAfter(input.superseded().by, prevSupersededBy))
                        prevSupersededBy = input.superseded().by;
                    return retry(this);

                case SUCCESS:
                    if (proposal.update.isEmpty())
                    {
                        logger.trace("PaxosRepair of {} complete after successful empty proposal", partitionKey());
                        return DONE;
                    }

                    logger.trace("PaxosRepair of {} committing successful proposal {}", partitionKey(), proposal);
                    return PaxosCommit.commit(proposal.agreed(), participants, paxosConsistency, commitConsistency(), true,
                                              new CommittingRepair());

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private class CommittingRepair extends ConsumerState<PaxosCommit.Status>
    {
        @Override
        public State execute(PaxosCommit.Status input)
        {
            logger.trace("PaxosRepair of {} {}", partitionKey(), input);
            return input.isSuccess() ? DONE : retry(this);
        }
    }

    private class CommitAndRestart extends ConsumerState<PaxosCommit.Status>
    {
        @Override
        public State execute(PaxosCommit.Status input)
        {
            return restart(this);
        }
    }

    private PaxosRepair(DecoratedKey partitionKey, Ballot incompleteBallot, TableMetadata table, ConsistencyLevel paxosConsistency)
    {
        super(partitionKey, incompleteBallot);
        // TODO: move precondition into super ctor
        Preconditions.checkArgument(paxosConsistency.isSerialConsistency());
        this.table = table;
        this.paxosConsistency = paxosConsistency;
        this.successCriteria = incompleteBallot;
    }

    public static PaxosRepair create(ConsistencyLevel consistency, DecoratedKey partitionKey, Ballot incompleteBallot, TableMetadata table)
    {
        return new PaxosRepair(partitionKey, incompleteBallot, table, consistency);
    }

    private State retry(State state)
    {
        Preconditions.checkState(isStarted());
        if (isResult(state))
            return state;

        return restart(state, waitUntilForContention(++attempts, table, partitionKey(), paxosConsistency, REPAIR));
    }

    @Override
    public State restart(State state, long waitUntil)
    {
        if (isResult(state))
            return state;

        participants = Participants.get(table, partitionKey(), paxosConsistency);

        if (waitUntil > Long.MIN_VALUE && waitUntil - startedNanos() > RETRY_TIMEOUT_NANOS)
            return new Failure(null);

        try
        {
            participants.assureSufficientLiveNodesForRepair();
        }
        catch (UnavailableException e)
        {
            return new Failure(e);
        }

        Querying querying = new Querying();
        long now;
        if (waitUntil == Long.MIN_VALUE || waitUntil - (now = nanoTime()) < 0) querying.run();
        else RETRIES.schedule(querying, waitUntil - now, NANOSECONDS);

        return querying;
    }

    private ConsistencyLevel commitConsistency()
    {
        Preconditions.checkState(paxosConsistency.isSerialConsistency());
        return paxosConsistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
    }

    static class Request
    {
        final DecoratedKey partitionKey;
        final TableMetadata table;
        Request(DecoratedKey partitionKey, TableMetadata table)
        {
            this.partitionKey = partitionKey;
            this.table = table;
        }
    }

    /**
     * The response to a proposal, indicating success (if {@code supersededBy == null},
     * or failure, alongside the ballot that beat us
     */
    static class Response
    {
        @Nonnull final Ballot latestWitnessedOrLowBound;
        @Nullable final Accepted acceptedButNotCommitted;
        @Nonnull final Committed committed;

        Response(Ballot latestWitnessedOrLowBound, @Nullable Accepted acceptedButNotCommitted, Committed committed)
        {
            this.latestWitnessedOrLowBound = latestWitnessedOrLowBound;
            this.acceptedButNotCommitted = acceptedButNotCommitted;
            this.committed = committed;
        }

        public String toString()
        {
            return String.format("Response(%s, %s, %s", latestWitnessedOrLowBound, acceptedButNotCommitted, committed);
        }
    }

    private static Map<String, Set<InetAddressAndPort>> mapToDc(Collection<InetAddressAndPort> endpoints, Function<InetAddressAndPort, String> dcFunc)
    {
        Map<String, Set<InetAddressAndPort>> map = new HashMap<>();
        endpoints.forEach(e -> map.computeIfAbsent(dcFunc.apply(e), k -> new HashSet<>()).add(e));
        return map;
    }

    private static boolean hasQuorumOrSingleDead(Collection<InetAddressAndPort> all, Collection<InetAddressAndPort> live, boolean requireQuorum)
    {
        Preconditions.checkArgument(all.size() >= live.size());
        return live.size() >= (all.size() / 2) + 1 || (!requireQuorum && live.size() >= all.size() - 1);
    }

    @VisibleForTesting
    static boolean hasSufficientLiveNodesForTopologyChange(Collection<InetAddressAndPort> allEndpoints, Collection<InetAddressAndPort> liveEndpoints, Function<InetAddressAndPort, String> dcFunc, boolean onlyQuorumRequired, boolean strictQuorum)
    {

        Map<String, Set<InetAddressAndPort>> allDcMap = mapToDc(allEndpoints, dcFunc);
        Map<String, Set<InetAddressAndPort>> liveDcMap = mapToDc(liveEndpoints, dcFunc);

        if (!hasQuorumOrSingleDead(allEndpoints, liveEndpoints, strictQuorum))
            return false;

        if (onlyQuorumRequired)
            return true;

        for (Map.Entry<String, Set<InetAddressAndPort>> entry : allDcMap.entrySet())
        {
            Set<InetAddressAndPort> all = entry.getValue();
            Set<InetAddressAndPort> live = liveDcMap.getOrDefault(entry.getKey(), Collections.emptySet());
            if (!hasQuorumOrSingleDead(all, live, strictQuorum))
                return false;
        }
        return true;
    }

    /**
     * checks if we have enough live nodes to perform a paxos repair for topology repair. Generally, this means that we need enough
     * live participants to reach EACH_QUORUM, with a few exceptions. The EACH_QUORUM requirement is meant to support workload using either
     * SERIAL or LOCAL_SERIAL
     *
     * if paxos_topology_repair_strict_each_quorum is set to false (the default), we will accept either a quorum or n-1 live nodes
     * in the cluster and per dc. If paxos_topology_repair_no_dc_checks is true, we only check the live nodes in the cluster,
     * and do not do any per-dc checks.
     */
    public static boolean hasSufficientLiveNodesForTopologyChange(Keyspace keyspace, Range<Token> range, Collection<InetAddressAndPort> liveEndpoints)
    {
        return hasSufficientLiveNodesForTopologyChange(keyspace.getReplicationStrategy().getNaturalReplicasForToken(range.right).endpoints(),
                                                       liveEndpoints,
                                                       DatabaseDescriptor.getEndpointSnitch()::getDatacenter,
                                                       DatabaseDescriptor.paxoTopologyRepairNoDcChecks(),
                                                       DatabaseDescriptor.paxoTopologyRepairStrictEachQuorum());
    }

    /**
     * The proposal request handler, i.e. receives a proposal from a peer and responds with either acccept/reject
     */
    public static class RequestHandler implements IVerbHandler<PaxosRepair.Request>
    {
        @Override
        public void doVerb(Message<PaxosRepair.Request> message)
        {
            PaxosRepair.Request request = message.payload;
            if (!isInRangeAndShouldProcess(message.from(), request.partitionKey, request.table, false))
            {
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
                return;
            }

            Ballot latestWitnessed;
            Accepted acceptedButNotCommited;
            Committed committed;
            long nowInSec = FBUtilities.nowInSeconds();
            try (PaxosState state = PaxosState.get(request.partitionKey, request.table))
            {
                PaxosState.Snapshot snapshot = state.current(nowInSec);
                latestWitnessed = snapshot.latestWitnessedOrLowBound();
                acceptedButNotCommited = snapshot.accepted;
                committed = snapshot.committed;
            }

            Response response = new Response(latestWitnessed, acceptedButNotCommited, committed);
            MessagingService.instance().respond(response, message);
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            request.table.id.serialize(out);
            DecoratedKey.serializer.serialize(request.partitionKey, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            TableMetadata table = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, table.partitioner, version);
            return new Request(partitionKey, table);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return request.table.id.serializedSize()
                   + DecoratedKey.serializer.serializedSize(request.partitionKey, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            response.latestWitnessedOrLowBound.serialize(out);
            serializeNullable(Accepted.serializer, response.acceptedButNotCommitted, out, version);
            Committed.serializer.serialize(response.committed, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot latestWitnessed = Ballot.deserialize(in);
            Accepted acceptedButNotCommitted = deserializeNullable(Accepted.serializer, in, version);
            Committed committed = Committed.serializer.deserialize(in, version);
            return new Response(latestWitnessed, acceptedButNotCommitted, committed);
        }

        public long serializedSize(Response response, int version)
        {
            return Ballot.sizeInBytes()
                   + serializedSizeNullable(Accepted.serializer, response.acceptedButNotCommitted, version)
                   + Committed.serializer.serializedSize(response.committed, version);
        }
    }

    private static volatile boolean SKIP_VERSION_VALIDATION = SKIP_PAXOS_REPAIR_VERSION_VALIDATION.getBoolean();

    public static void setSkipPaxosRepairCompatibilityCheck(boolean v)
    {
        SKIP_VERSION_VALIDATION = v;
    }

    public static boolean getSkipPaxosRepairCompatibilityCheck()
    {
        return SKIP_VERSION_VALIDATION;
    }

    static boolean validateVersionCompatibility(CassandraVersion version)
    {
        if (SKIP_VERSION_VALIDATION)
            return true;

        if (version == null)
            return false;

        // assume 4.0 is ok
        return (version.major == 4 && version.minor > 0) || version.major > 4;
    }

    static String getPeerVersion(InetAddressAndPort peer)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(peer);
        if (epState == null)
            return null;

        VersionedValue value = epState.getApplicationState(ApplicationState.RELEASE_VERSION);
        if (value == null)
            return null;

        try
        {
            return value.value;
        }
        catch (IllegalArgumentException e)
        {
            return null;
        }
    }

    static boolean validatePeerCompatibility(Replica peer)
    {
        String versionString = getPeerVersion(peer.endpoint());
        CassandraVersion version = versionString != null ? new CassandraVersion(versionString) : null;
        boolean result = validateVersionCompatibility(version);
        if (!result)
            logger.info("PaxosRepair isn't supported by {} on version {}", peer, versionString);
        return result;
    }

    static boolean validatePeerCompatibility(TableMetadata table, Range<Token> range)
    {
        Participants participants = Participants.get(table, range.right, ConsistencyLevel.SERIAL);
        return Iterables.all(participants.all, PaxosRepair::validatePeerCompatibility);
    }

    public static boolean validatePeerCompatibility(TableMetadata table, Collection<Range<Token>> ranges)
    {
        return Iterables.all(ranges, range -> validatePeerCompatibility(table, range));
    }

    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, units, RETRIES);
    }
}
