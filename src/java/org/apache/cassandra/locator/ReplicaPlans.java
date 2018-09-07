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

package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Collection;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.limit;

public class ReplicaPlans
{

    /**
     * Construct a ReplicaPlan for writing to exactly one node, with CL.ONE. This node is *assumed* to be alive.
     */
    public static ReplicaPlan.ForTokenWrite forSingleReplicaWrite(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken one = EndpointsForToken.of(token, replica);
        EndpointsForToken empty = EndpointsForToken.empty(token);
        return new ReplicaPlan.ForTokenWrite(keyspace, ConsistencyLevel.ONE, empty, one, one, one);
    }

    /**
     * A forwarding counter write is always sent to a single owning coordinator for the range, by the original coordinator
     * (if it is not itself an owner)
     */
    public static ReplicaPlan.ForTokenWrite forForwardingCounterWrite(Keyspace keyspace, Token token, Replica replica)
    {
        return forSingleReplicaWrite(keyspace, token, replica);
    }

    /**
     * Requires that the provided endpoints are alive.  Converts them to their relevant system replicas.
     * Note that the liveAndDown collection and live are equal to the provided endpoints.
     *
     * The semantics are a bit weird, in that CL=ONE iff we have one node provided, and otherwise is equal to TWO.
     * How these CL were chosen, and why we drop the CL if only one live node is available, are both unclear.
     */
    public static ReplicaPlan.ForTokenWrite forBatchlogWrite(Keyspace keyspace, Collection<InetAddressAndPort> endpoints) throws UnavailableException
    {
        // A single case we write not for range or token, but multiple mutations to many tokens
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWrite(
                SystemReplicas.getSystemReplicas(endpoints).forToken(token),
                EndpointsForToken.empty(token)
        );
        ConsistencyLevel consistencyLevel = liveAndDown.all().size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO;

        // assume that we have already been given live endpoints, and skip applying the failure detector
        return forWrite(keyspace, consistencyLevel, liveAndDown, liveAndDown, writeAll);
    }

    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, Selector selector) throws UnavailableException
    {
        return forWrite(keyspace, consistencyLevel, ReplicaLayout.forTokenWriteLiveAndDown(keyspace, token), selector);
    }

    @VisibleForTesting
    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken natural, EndpointsForToken pending, Predicate<Replica> isAlive, Selector selector) throws UnavailableException
    {
        return forWrite(keyspace, consistencyLevel, ReplicaLayout.forTokenWrite(natural, pending), isAlive, selector);
    }

    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, Selector selector) throws UnavailableException
    {
        return forWrite(keyspace, consistencyLevel, liveAndDown, FailureDetector.isReplicaAlive, selector);
    }

    @VisibleForTesting
    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, Predicate<Replica> isAlive, Selector selector) throws UnavailableException
    {
        ReplicaLayout.ForTokenWrite live = liveAndDown.filter(isAlive);
        return forWrite(keyspace, consistencyLevel, liveAndDown, live, selector);
    }

    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, ReplicaLayout.ForTokenWrite live, Selector selector) throws UnavailableException
    {
        EndpointsForToken contacts = selector.select(keyspace, consistencyLevel, liveAndDown, live);
        ReplicaPlan.ForTokenWrite result = new ReplicaPlan.ForTokenWrite(keyspace, consistencyLevel, liveAndDown.pending(), liveAndDown.all(), live.all(), contacts);
        result.assureSufficientReplicas();
        return result;
    }

    public interface Selector
    {
        <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L live);
    }

    /**
     * Select all nodes, transient or otherwise, as targets for the operation.
     *
     * This is may no longer be useful until we finish implementing transient replication support, however
     * it can be of value to stipulate that a location writes to all nodes without regard to transient status.
     */
    public static final Selector writeAll = new Selector()
    {
        @Override
        public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L live)
        {
            return liveAndDown.all();
        }
    };

    /**
     * Select all full nodes, live or down, as write targets.  If there are insufficient nodes to complete the write,
     * but there are live transient nodes, select a sufficient number of these to reach our consistency level.
     *
     * Pending nodes are always contacted, whether or not they are full.  When a transient replica is undergoing
     * a pending move to a new node, if we write (transiently) to it, this write would not be replicated to the
     * pending transient node, and so when completing the move, the write could effectively have not reached the
     * promised consistency level.
     */
    public static final Selector writeNormal = new Selector()
    {
        @Override
        public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L live)
        {
            if (!any(liveAndDown.all(), Replica::isTransient))
                return liveAndDown.all();

            assert consistencyLevel != ConsistencyLevel.EACH_QUORUM;

            ReplicaCollection.Mutable<E> contacts = liveAndDown.all().newMutable(liveAndDown.all().size());
            contacts.addAll(filter(liveAndDown.natural(), Replica::isFull));
            contacts.addAll(liveAndDown.pending());

            // TODO: this doesn't correctly handle LOCAL_QUORUM (or EACH_QUORUM at all)
            int liveCount = contacts.count(live.all()::contains);
            int requiredTransientCount = consistencyLevel.blockForWrite(keyspace, liveAndDown.pending()) - liveCount;
            if (requiredTransientCount > 0)
                contacts.addAll(limit(filter(live.natural(), Replica::isTransient), requiredTransientCount));
            return contacts.asSnapshot();
        }
    };

    /**
     * Construct the plan for a paxos round - NOT the write or read consistency level for either the write or comparison,
     * but for the paxos linearisation agreement.
     *
     * This will select all live nodes as the candidates for the operation.  Only the required number of participants
     */
    public static ReplicaPlan.ForPaxosWrite forPaxos(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        Token tk = key.getToken();
        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(keyspace, tk);

        Replicas.temporaryAssertFull(liveAndDown.all()); // TODO CASSANDRA-14547

        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // TODO: we should cleanup our semantics here, as we're filtering ALL nodes to localDC which is unexpected for ReplicaPlan
            // Restrict natural and pending to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            Predicate<Replica> isLocalDc = replica -> localDc.equals(snitch.getDatacenter(replica));

            liveAndDown = liveAndDown.filter(isLocalDc);
        }

        ReplicaLayout.ForTokenWrite live = liveAndDown.filter(FailureDetector.isReplicaAlive);

        // TODO: this should use assureSufficientReplicas
        int participants = liveAndDown.all().size();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833

        EndpointsForToken contacts = live.all();
        if (contacts.size() < requiredParticipants)
            throw UnavailableException.create(consistencyForPaxos, requiredParticipants, contacts.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (liveAndDown.pending().size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", liveAndDown.all().size()),
                    consistencyForPaxos,
                    participants + 1,
                    contacts.size());

        return new ReplicaPlan.ForPaxosWrite(keyspace, consistencyForPaxos, liveAndDown.pending(), liveAndDown.all(), live.all(), contacts, requiredParticipants);
    }

    /**
     * Construct a plan for reading from a single node - this permits no speculation or read-repair
     */
    public static ReplicaPlan.ForTokenRead forSingleReplicaRead(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken one = EndpointsForToken.of(token, replica);
        return new ReplicaPlan.ForTokenRead(keyspace, ConsistencyLevel.ONE, one, one);
    }

    /**
     * Construct a plan for reading from a single node - this permits no speculation or read-repair
     */
    public static ReplicaPlan.ForRangeRead forSingleReplicaRead(Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica)
    {
        // TODO: this is unsafe, as one.range() may be inconsistent with our supplied range; should refactor Range/AbstractBounds to single class
        EndpointsForRange one = EndpointsForRange.of(replica);
        return new ReplicaPlan.ForRangeRead(keyspace, ConsistencyLevel.ONE, range, one, one);
    }

    /**
     * Construct a plan for reading the provided token at the provided consistency level.  This translates to a collection of
     *   - candidates who are: alive, replicate the token, and are sorted by their snitch scores
     *   - contacts who are: the first blockFor + (retry == ALWAYS ? 1 : 0) candidates
     *
     * The candidate collection can be used for speculation, although at present it would break
     * LOCAL_QUORUM and EACH_QUORUM to do so without further filtering
     */
    public static ReplicaPlan.ForTokenRead forRead(Keyspace keyspace, Token token, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry)
    {
        ReplicaLayout.ForTokenRead candidates = ReplicaLayout.forTokenReadLiveSorted(keyspace, token);
        EndpointsForToken contacts = consistencyLevel.filterForQuery(keyspace, candidates.natural(),
                retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE));

        ReplicaPlan.ForTokenRead result = new ReplicaPlan.ForTokenRead(keyspace, consistencyLevel, candidates.natural(), contacts);
        result.assureSufficientReplicas(); // Throw UAE early if we don't have enough replicas.
        return result;
    }

    /**
     * Construct a plan for reading the provided range at the provided consistency level.  This translates to a collection of
     *   - candidates who are: alive, replicate the range, and are sorted by their snitch scores
     *   - contacts who are: the first blockFor candidates
     *
     * There is no speculation for range read queries at present, so we never 'always speculate' here, and a failed response fails the query.
     */
    public static ReplicaPlan.ForRangeRead forRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range)
    {
        ReplicaLayout.ForRangeRead candidates = ReplicaLayout.forRangeReadLiveSorted(keyspace, range);
        EndpointsForRange contacts = consistencyLevel.filterForQuery(keyspace, candidates.natural());

        ReplicaPlan.ForRangeRead result = new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, range, candidates.natural(), contacts);
        result.assureSufficientReplicas();
        return result;
    }

    /**
     * Take two range read plans for adjacent ranges, and check if it is OK (and worthwhile) to combine them into a single plan
     */
    public static ReplicaPlan.ForRangeRead maybeMerge(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaPlan.ForRangeRead left, ReplicaPlan.ForRangeRead right)
    {
        // TODO: should we be asserting that the ranges are adjacent?
        AbstractBounds<PartitionPosition> newRange = left.range().withNewRight(right.range().right);
        EndpointsForRange mergedCandidates = left.candidates().keep(right.candidates().endpoints());

        // Check if there are enough shared endpoints for the merge to be possible.
        if (!consistencyLevel.isSufficientLiveReplicasForRead(keyspace, mergedCandidates))
            return null;

        EndpointsForRange contacts = consistencyLevel.filterForQuery(keyspace, mergedCandidates);

        // Estimate whether merging will be a win or not
        if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(contacts, left.contacts(), right.contacts()))
            return null;

        // If we get there, merge this range and the next one
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, newRange, mergedCandidates, contacts);
    }
}
