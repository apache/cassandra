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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;

import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;
import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.eachQuorumForRead;
import static org.apache.cassandra.db.ConsistencyLevel.eachQuorumForWrite;
import static org.apache.cassandra.db.ConsistencyLevel.localQuorumFor;
import static org.apache.cassandra.db.ConsistencyLevel.localQuorumForOurDc;
import static org.apache.cassandra.locator.Replicas.addToCountPerDc;
import static org.apache.cassandra.locator.Replicas.countInOurDc;
import static org.apache.cassandra.locator.Replicas.countPerDc;

public class ReplicaPlans
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicaPlans.class);

    public static boolean isSufficientLiveReplicasForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> liveReplicas)
    {
        switch (consistencyLevel)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countInOurDc(liveReplicas).hasAtleast(1, 1);
            case LOCAL_QUORUM:
                return countInOurDc(liveReplicas).hasAtleast(localQuorumForOurDc(keyspace), 1);
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    int fullCount = 0;
                    Collection<String> dcs = ((NetworkTopologyStrategy) keyspace.getReplicationStrategy()).getDatacenters();
                    for (ObjectObjectCursor<String, Replicas.ReplicaCount> entry : countPerDc(dcs, liveReplicas))
                    {
                        Replicas.ReplicaCount count = entry.value;
                        if (!count.hasAtleast(localQuorumFor(keyspace, entry.key), 0))
                            return false;
                        fullCount += count.fullReplicas();
                    }
                    return fullCount > 0;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return liveReplicas.size() >= consistencyLevel.blockFor(keyspace)
                        && Replicas.countFull(liveReplicas) > 0;
        }
    }

    static void assureSufficientLiveReplicasForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> liveReplicas) throws UnavailableException
    {
        assureSufficientLiveReplicas(keyspace, consistencyLevel, liveReplicas, consistencyLevel.blockFor(keyspace), 1);
    }
    static void assureSufficientLiveReplicasForWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> allLive, Endpoints<?> pendingWithDown) throws UnavailableException
    {
        assureSufficientLiveReplicas(keyspace, consistencyLevel, allLive, consistencyLevel.blockForWrite(keyspace, pendingWithDown), 0);
    }
    static void assureSufficientLiveReplicas(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> allLive, int blockFor, int blockForFullReplicas) throws UnavailableException
    {
        switch (consistencyLevel)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                break;
            case LOCAL_ONE:
            {
                Replicas.ReplicaCount localLive = countInOurDc(allLive);
                if (!localLive.hasAtleast(blockFor, blockForFullReplicas))
                    throw UnavailableException.create(consistencyLevel, 1, blockForFullReplicas, localLive.allReplicas(), localLive.fullReplicas());
                break;
            }
            case LOCAL_QUORUM:
            {
                Replicas.ReplicaCount localLive = countInOurDc(allLive);
                if (!localLive.hasAtleast(blockFor, blockForFullReplicas))
                {
                    if (logger.isTraceEnabled())
                    {
                        logger.trace(String.format("Local replicas %s are insufficient to satisfy LOCAL_QUORUM requirement of %d live replicas and %d full replicas in '%s'",
                                allLive.filter(InOurDcTester.replicas()), blockFor, blockForFullReplicas, DatabaseDescriptor.getLocalDataCenter()));
                    }
                    throw UnavailableException.create(consistencyLevel, blockFor, blockForFullReplicas, localLive.allReplicas(), localLive.fullReplicas());
                }
                break;
            }
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    int total = 0;
                    int totalFull = 0;
                    Collection<String> dcs = ((NetworkTopologyStrategy) keyspace.getReplicationStrategy()).getDatacenters();
                    for (ObjectObjectCursor<String, Replicas.ReplicaCount> entry : countPerDc(dcs, allLive))
                    {
                        int dcBlockFor = localQuorumFor(keyspace, entry.key);
                        Replicas.ReplicaCount dcCount = entry.value;
                        if (!dcCount.hasAtleast(dcBlockFor, 0))
                            throw UnavailableException.create(consistencyLevel, entry.key, dcBlockFor, dcCount.allReplicas(), 0, dcCount.fullReplicas());
                        totalFull += dcCount.fullReplicas();
                        total += dcCount.allReplicas();
                    }
                    if (totalFull < blockForFullReplicas)
                        throw UnavailableException.create(consistencyLevel, blockFor, total, blockForFullReplicas, totalFull);
                    break;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                int live = allLive.size();
                int full = Replicas.countFull(allLive);
                if (live < blockFor || full < blockForFullReplicas)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(allLive), blockFor);
                    throw UnavailableException.create(consistencyLevel, blockFor, blockForFullReplicas, live, full);
                }
                break;
        }
    }

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

    public static ReplicaPlan.ForTokenWrite forLocalBatchlogWrite()
    {
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Keyspace systemKeypsace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        Replica localSystemReplica = SystemReplicas.getSystemReplica(FBUtilities.getBroadcastAddressAndPort());

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWrite(
                EndpointsForToken.of(token, localSystemReplica),
                EndpointsForToken.empty(token)
        );

        return forWrite(systemKeypsace, ConsistencyLevel.ONE, liveAndDown, liveAndDown, writeAll);
    }

    /**
     * Requires that the provided endpoints are alive.  Converts them to their relevant system replicas.
     * Note that the liveAndDown collection and live are equal to the provided endpoints.
     *
     * @param isAny if batch consistency level is ANY, in which case a local node will be picked
     */
    public static ReplicaPlan.ForTokenWrite forBatchlogWrite(boolean isAny) throws UnavailableException
    {
        // A single case we write not for range or token, but multiple mutations to many tokens
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();

        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        Multimap<String, InetAddressAndPort> localEndpoints = HashMultimap.create(topology.getDatacenterRacks()
                                                                                          .get(snitch.getLocalDatacenter()));
        // Replicas are picked manually:
        //  - replicas should be alive according to the failure detector
        //  - replicas should be in the local datacenter
        //  - choose min(2, number of qualifying candiates above)
        //  - allow the local node to be the only replica only if it's a single-node DC
        Collection<InetAddressAndPort> chosenEndpoints = filterBatchlogEndpoints(snitch.getLocalRack(), localEndpoints);

        if (chosenEndpoints.isEmpty() && isAny)
            chosenEndpoints = Collections.singleton(FBUtilities.getBroadcastAddressAndPort());

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWrite(
                SystemReplicas.getSystemReplicas(chosenEndpoints).forToken(token),
                EndpointsForToken.empty(token)
        );

        // Batchlog is hosted by either one node or two nodes from different racks.
        ConsistencyLevel consistencyLevel = liveAndDown.all().size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO;

        Keyspace systemKeypsace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);

        // assume that we have already been given live endpoints, and skip applying the failure detector
        return forWrite(systemKeypsace, consistencyLevel, liveAndDown, liveAndDown, writeAll);
    }

    private static Collection<InetAddressAndPort> filterBatchlogEndpoints(String localRack,
                                                                          Multimap<String, InetAddressAndPort> endpoints)
    {
        return filterBatchlogEndpoints(localRack,
                                       endpoints,
                                       Collections::shuffle,
                                       FailureDetector.isEndpointAlive,
                                       ThreadLocalRandom.current()::nextInt);
    }

    // Collect a list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
    @VisibleForTesting
    public static Collection<InetAddressAndPort> filterBatchlogEndpoints(String localRack,
                                                                         Multimap<String, InetAddressAndPort> endpoints,
                                                                         Consumer<List<?>> shuffle,
                                                                         Predicate<InetAddressAndPort> isAlive,
                                                                         Function<Integer, Integer> indexPicker)
    {
        // special case for single-node data centers
        if (endpoints.values().size() == 1)
            return endpoints.values();

        // strip out dead endpoints and localhost
        ListMultimap<String, InetAddressAndPort> validated = ArrayListMultimap.create();
        for (Map.Entry<String, InetAddressAndPort> entry : endpoints.entries())
        {
            InetAddressAndPort addr = entry.getValue();
            if (!addr.equals(FBUtilities.getBroadcastAddressAndPort()) && isAlive.test(addr))
                validated.put(entry.getKey(), entry.getValue());
        }

        if (validated.size() <= 2)
            return validated.values();

        if (validated.size() - validated.get(localRack).size() >= 2)
        {
            // we have enough endpoints in other racks
            validated.removeAll(localRack);
        }

        if (validated.keySet().size() == 1)
        {
            /*
             * we have only 1 `other` rack to select replicas from (whether it be the local rack or a single non-local rack)
             * pick two random nodes from there; we are guaranteed to have at least two nodes in the single remaining rack
             * because of the preceding if block.
             */
            List<InetAddressAndPort> otherRack = Lists.newArrayList(validated.values());
            shuffle.accept(otherRack);
            return otherRack.subList(0, 2);
        }

        // randomize which racks we pick from if more than 2 remaining
        Collection<String> racks;
        if (validated.keySet().size() == 2)
        {
            racks = validated.keySet();
        }
        else
        {
            racks = Lists.newArrayList(validated.keySet());
            shuffle.accept((List<?>) racks);
        }

        // grab a random member of up to two racks
        List<InetAddressAndPort> result = new ArrayList<>(2);
        for (String rack : Iterables.limit(racks, 2))
        {
            List<InetAddressAndPort> rackMembers = validated.get(rack);
            result.add(rackMembers.get(indexPicker.apply(rackMembers.size())));
        }

        return result;
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
        EndpointsForToken contacts = selector.select(keyspace, liveAndDown, live);
        assureSufficientLiveReplicasForWrite(keyspace, consistencyLevel, live.all(), liveAndDown.pending());
        return new ReplicaPlan.ForTokenWrite(keyspace, consistencyLevel, liveAndDown.pending(), liveAndDown.all(), live.all(), contacts);
    }

    public interface Selector
    {
        <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, L liveAndDown, L live);
    }

    /**
     * Select all nodes, transient or otherwise, as targets for the operation.
     *
     * This is may no longer be useful once we finish implementing transient replication support, however
     * it can be of value to stipulate that a location writes to all nodes without regard to transient status.
     */
    public static final Selector writeAll = new Selector()
    {
        @Override
        public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(Keyspace keyspace, L liveAndDown, L live)
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
        E select(Keyspace keyspace, L liveAndDown, L live)
        {
            if (!any(liveAndDown.all(), Replica::isTransient))
                return liveAndDown.all();

            ReplicaCollection.Builder<E> contacts = liveAndDown.all().newBuilder(liveAndDown.all().size());
            contacts.addAll(filter(liveAndDown.natural(), Replica::isFull));
            contacts.addAll(liveAndDown.pending());

            /**
             * Per CASSANDRA-14768, we ensure we write to at least a QUORUM of nodes in every DC,
             * regardless of how many responses we need to wait for and our requested consistencyLevel.
             * This is to minimally surprise users with transient replication; with normal writes, we
             * soft-ensure that we reach QUORUM in all DCs we are able to, by writing to every node;
             * even if we don't wait for ACK, we have in both cases sent sufficient messages.
              */
            ObjectIntOpenHashMap<String> requiredPerDc = eachQuorumForWrite(keyspace, liveAndDown.pending());
            addToCountPerDc(requiredPerDc, live.natural().filter(Replica::isFull), -1);
            addToCountPerDc(requiredPerDc, live.pending(), -1);

            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            for (Replica replica : filter(live.natural(), Replica::isTransient))
            {
                String dc = snitch.getDatacenter(replica);
                if (requiredPerDc.addTo(dc, -1) >= 0)
                    contacts.add(replica);
            }
            return contacts.build();
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
            liveAndDown = liveAndDown.filter(InOurDcTester.replicas());
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


    private static <E extends Endpoints<E>> E candidatesForRead(ConsistencyLevel consistencyLevel, E liveNaturalReplicas)
    {
        return consistencyLevel.isDatacenterLocal()
                ? liveNaturalReplicas.filter(InOurDcTester.replicas())
                : liveNaturalReplicas;
    }

    private static <E extends Endpoints<E>> E contactForEachQuorumRead(Keyspace keyspace, E candidates)
    {
        assert keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy;
        ObjectIntOpenHashMap<String> perDc = eachQuorumForRead(keyspace);

        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return candidates.filter(replica -> {
            String dc = snitch.getDatacenter(replica);
            return perDc.addTo(dc, -1) >= 0;
        });
    }

    private static <E extends Endpoints<E>> E contactForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, boolean alwaysSpeculate, E candidates)
    {
        /*
         * If we are doing an each quorum query, we have to make sure that the endpoints we select
         * provide a quorum for each data center. If we are not using a NetworkTopologyStrategy,
         * we should fall through and grab a quorum in the replication strategy.
         *
         * We do not speculate for EACH_QUORUM.
         *
         * TODO: this is still very inconistently managed between {LOCAL,EACH}_QUORUM and other consistency levels - should address this in a follow-up
         */
        if (consistencyLevel == EACH_QUORUM && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
            return contactForEachQuorumRead(keyspace, candidates);

        int count = consistencyLevel.blockFor(keyspace) + (alwaysSpeculate ? 1 : 0);
        return candidates.subList(0, Math.min(count, candidates.size()));
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
     * The candidate collection can be used for speculation, although at present
     * it would break EACH_QUORUM to do so without further filtering
     */
    public static ReplicaPlan.ForTokenRead forRead(Keyspace keyspace, Token token, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry)
    {
        EndpointsForToken candidates = candidatesForRead(consistencyLevel, ReplicaLayout.forTokenReadLiveSorted(keyspace, token).natural());
        EndpointsForToken contacts = contactForRead(keyspace, consistencyLevel, retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE), candidates);

        assureSufficientLiveReplicasForRead(keyspace, consistencyLevel, contacts);
        return new ReplicaPlan.ForTokenRead(keyspace, consistencyLevel, candidates, contacts);
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
        EndpointsForRange candidates = candidatesForRead(consistencyLevel, ReplicaLayout.forRangeReadLiveSorted(keyspace, range).natural());
        EndpointsForRange contacts = contactForRead(keyspace, consistencyLevel, false, candidates);

        assureSufficientLiveReplicasForRead(keyspace, consistencyLevel, contacts);
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, range, candidates, contacts);
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
        if (!isSufficientLiveReplicasForRead(keyspace, consistencyLevel, mergedCandidates))
            return null;

        EndpointsForRange contacts = contactForRead(keyspace, consistencyLevel, false, mergedCandidates);

        // Estimate whether merging will be a win or not
        if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(contacts, left.contacts(), right.contacts()))
            return null;

        // If we get there, merge this range and the next one
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel, newRange, mergedCandidates, contacts);
    }
}
