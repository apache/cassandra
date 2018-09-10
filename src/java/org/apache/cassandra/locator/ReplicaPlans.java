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

    public static ReplicaPlan.ForTokenWrite forSingleReplicaWrite(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken one = EndpointsForToken.of(token, replica);
        EndpointsForToken empty = EndpointsForToken.empty(token);
        return new ReplicaPlan.ForTokenWrite(keyspace, ConsistencyLevel.ONE, empty, one, one, one);
    }

    public static ReplicaPlan.ForTokenWrite forForwardingCounterWrite(Keyspace keyspace, Token token, Replica replica)
    {
        return forSingleReplicaWrite(keyspace, token, replica);
    }

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
        ReplicaLayout.ForTokenWrite liveOnly = liveAndDown.filter(isAlive);
        return forWrite(keyspace, consistencyLevel, liveAndDown, liveOnly, selector);
    }

    public static ReplicaPlan.ForTokenWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, ReplicaLayout.ForTokenWrite liveOnly, Selector selector) throws UnavailableException
    {
        EndpointsForToken contact = selector.select(keyspace, consistencyLevel, liveAndDown, liveOnly);
        ReplicaPlan.ForTokenWrite result = new ReplicaPlan.ForTokenWrite(keyspace, consistencyLevel, liveAndDown.pending(), liveAndDown.all(), liveOnly.all(), contact);
        result.assureSufficientReplicas();
        return result;
    }

    public interface Selector
    {
        <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly);
    }

    public static final Selector writeAll = new Selector()
    {
        @Override
        public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly)
        {
            return liveAndDown.all();
        }
    };

    public static final Selector writeNormal = new Selector()
    {
        @Override
        public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly)
        {
            if (!any(liveAndDown.all(), Replica::isTransient))
                return liveAndDown.all();

            assert consistencyLevel != ConsistencyLevel.EACH_QUORUM;

            ReplicaCollection.Mutable<E> contact = liveAndDown.all().newMutable(liveAndDown.all().size());
            contact.addAll(filter(liveAndDown.natural(), Replica::isFull));
            contact.addAll(liveAndDown.pending());

            int liveCount = contact.count(liveOnly.all()::contains);
            int requiredTransientCount = consistencyLevel.blockForWrite(keyspace, liveAndDown.pending()) - liveCount;
            if (requiredTransientCount > 0)
                contact.addAll(limit(filter(liveOnly.natural(), Replica::isTransient), requiredTransientCount));
            return contact.asSnapshot();
        }
    };

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

        ReplicaLayout.ForTokenWrite liveOnly = liveAndDown.filter(FailureDetector.isReplicaAlive);

        // TODO: this should use assureSufficientReplicas
        int participants = liveAndDown.all().size();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833

        EndpointsForToken contact = liveOnly.all();
        if (contact.size() < requiredParticipants)
            throw UnavailableException.create(consistencyForPaxos, requiredParticipants, contact.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (liveAndDown.pending().size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", liveAndDown.all().size()),
                    consistencyForPaxos,
                    participants + 1,
                    contact.size());

        return new ReplicaPlan.ForPaxosWrite(keyspace, consistencyForPaxos, liveAndDown.pending(), liveAndDown.all(), liveOnly.all(), contact, requiredParticipants);
    }

    public static ReplicaPlan.ForTokenRead forSingleReplicaRead(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken one = EndpointsForToken.of(token, replica);
        return new ReplicaPlan.ForTokenRead(keyspace, ConsistencyLevel.ONE, one, one);
    }

    public static ReplicaPlan.ForRangeRead forSingleReplicaRead(Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica)
    {
        // TODO: this is unsafe, as one.range() may be inconsistent with our supplied range; should refactor Range/AbstractBounds to single class
        EndpointsForRange one = EndpointsForRange.of(replica);
        return new ReplicaPlan.ForRangeRead(keyspace, ConsistencyLevel.ONE, range, one, one);
    }

    public static ReplicaPlan.ForTokenRead forRead(Keyspace keyspace, Token token, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry)
    {
        ReplicaLayout.ForTokenRead candidates = ReplicaLayout.forTokenReadLiveSorted(keyspace, token);
        EndpointsForToken contact = consistencyLevel.filterForQuery(keyspace, candidates.natural(),
                retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE));

        ReplicaPlan.ForTokenRead result = new ReplicaPlan.ForTokenRead(keyspace, consistencyLevel, candidates.natural(), contact);
        result.assureSufficientReplicas(); // Throw UAE early if we don't have enough replicas.
        return result;
    }

    public static ReplicaPlan.ForRangeRead forRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range)
    {
        ReplicaLayout.ForRangeRead candidates = ReplicaLayout.forRangeReadLiveSorted(keyspace, range);
        EndpointsForRange contact = consistencyLevel.filterForQuery(keyspace, candidates.natural());

        int blockFor = consistencyLevel.blockFor(keyspace);
        if (blockFor < contact.size())
            contact = contact.subList(0, blockFor);

        ReplicaPlan.ForRangeRead result = new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, candidates.range(), candidates.all(), contact);
        result.assureSufficientReplicas();
        return result;
    }

    public static ReplicaPlan.ForRangeRead maybeMerge(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaPlan.ForRangeRead left, ReplicaPlan.ForRangeRead right)
    {
        AbstractBounds<PartitionPosition> newRange = left.range().withNewRight(right.range().right);
        EndpointsForRange mergedCandidates = left.candidates().keep(right.candidates().endpoints());

        // Check if there is enough endpoint for the merge to be possible.
        if (!consistencyLevel.isSufficientReplicasForRead(keyspace, mergedCandidates))
            return null;

        EndpointsForRange contact = consistencyLevel.filterForQuery(keyspace, mergedCandidates);
        int blockFor = consistencyLevel.blockFor(keyspace);
        if (blockFor < contact.size())
            contact = contact.subList(0, blockFor);

        // Estimate whether merging will be a win or not
        if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(contact, left.contact(), right.contact()))
            return null;

        // If we get there, merge this range and the next one
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, newRange, mergedCandidates, contact);
    }
}
