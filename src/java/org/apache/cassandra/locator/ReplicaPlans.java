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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;

import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.FBUtilities;

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

    private static final Range<Token> FULL_TOKEN_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(), DatabaseDescriptor.getPartitioner().getMinimumToken());

    private static final int REQUIRED_BATCHLOG_REPLICA_COUNT
            = Math.max(1, Math.min(2, CassandraRelevantProperties.REQUIRED_BATCHLOG_REPLICA_COUNT.getInt()));

    static
    {
        int batchlogReplicaCount = CassandraRelevantProperties.REQUIRED_BATCHLOG_REPLICA_COUNT.getInt();
        if (batchlogReplicaCount < 1 || 2 < batchlogReplicaCount)
            logger.warn("System property {} was set to {} but must be 1 or 2. Running with {}", CassandraRelevantProperties.REQUIRED_BATCHLOG_REPLICA_COUNT.getKey(), batchlogReplicaCount, REQUIRED_BATCHLOG_REPLICA_COUNT);
    }

    public static boolean isSufficientLiveReplicasForRead(Locator locator, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, Endpoints<?> liveReplicas)
    {
        switch (consistencyLevel)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countInOurDc(liveReplicas).hasAtleast(1, 1);
            case LOCAL_QUORUM:
                return countInOurDc(liveReplicas).hasAtleast(localQuorumForOurDc(replicationStrategy), 1);
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    int fullCount = 0;
                    Collection<String> dcs = ((NetworkTopologyStrategy) replicationStrategy).getDatacenters();
                    for (ObjectObjectCursor<String, Replicas.ReplicaCount> entry : countPerDc(locator, dcs, liveReplicas))
                    {
                        Replicas.ReplicaCount count = entry.value;
                        if (!count.hasAtleast(localQuorumFor(replicationStrategy, entry.key), 0))
                            return false;
                        fullCount += count.fullReplicas();
                    }
                    return fullCount > 0;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return liveReplicas.size() >= consistencyLevel.blockFor(replicationStrategy)
                        && Replicas.countFull(liveReplicas) > 0;
        }
    }

    static void assureSufficientLiveReplicasForRead(Locator locator, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, Endpoints<?> liveReplicas) throws UnavailableException
    {
        assureSufficientLiveReplicas(locator, replicationStrategy, consistencyLevel, liveReplicas, consistencyLevel.blockFor(replicationStrategy), 1);
    }

    static void assureSufficientLiveReplicasForWrite(Locator locator, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, Endpoints<?> allLive, Endpoints<?> pendingWithDown) throws UnavailableException
    {
        assureSufficientLiveReplicas(locator, replicationStrategy, consistencyLevel, allLive, consistencyLevel.blockForWrite(replicationStrategy, pendingWithDown), 0);
    }

    static void assureSufficientLiveReplicas(Locator locator, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, Endpoints<?> allLive, int blockFor, int blockForFullReplicas) throws UnavailableException
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
                        logger.trace("Local replicas {} are insufficient to satisfy LOCAL_QUORUM requirement of {} live replicas and {} full replicas in '{}'",
                                     allLive.filter(InOurDc.replicas()), blockFor, blockForFullReplicas, DatabaseDescriptor.getLocalDataCenter());
                    throw UnavailableException.create(consistencyLevel, blockFor, blockForFullReplicas, localLive.allReplicas(), localLive.fullReplicas());
                }
                break;
            }
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    int total = 0;
                    int totalFull = 0;
                    Collection<String> dcs = ((NetworkTopologyStrategy) replicationStrategy).getDatacenters();
                    for (ObjectObjectCursor<String, Replicas.ReplicaCount> entry : countPerDc(locator, dcs, allLive))
                    {
                        int dcBlockFor = localQuorumFor(replicationStrategy, entry.key);
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
    public static ReplicaPlan.ForWrite forSingleReplicaWrite(ClusterMetadata metadata, Keyspace keyspace, Token token, Function<ClusterMetadata, Replica> replicaSupplier)
    {
        EndpointsForToken one = EndpointsForToken.of(token, replicaSupplier.apply(metadata));
        EndpointsForToken empty = EndpointsForToken.empty(token);

        return new ReplicaPlan.ForWrite(keyspace, keyspace.getReplicationStrategy(), ConsistencyLevel.ONE, empty, one, one, one,
                                        (newClusterMetadata) -> forSingleReplicaWrite(newClusterMetadata, keyspace, token, replicaSupplier),
                                        metadata.epoch);
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     *
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    public static Replica findCounterLeaderReplica(ClusterMetadata metadata, String keyspaceName, DecoratedKey key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();

        EndpointsForToken replicas = metadata.placements.get(keyspace.getMetadata().params.replication).reads.forToken(key.getToken()).get();

        // CASSANDRA-13043: filter out those endpoints not accepting clients yet, maybe because still bootstrapping
        // TODO: replace this with JOINED state.
        // TODO don't forget adding replicas = replicas.filter(replica -> FailureDetector.instance.isAlive(replica.endpoint())); after rebase (from CASSANDRA-17411)
        replicas = replicas.filter(replica -> StorageService.instance.isRpcReady(replica.endpoint()));

        // TODO have a way to compute the consistency level
        if (replicas.isEmpty())
            throw UnavailableException.create(cl, cl.blockFor(replicationStrategy), 0);

        List<Replica> localReplicas = new ArrayList<>(replicas.size());

        for (Replica replica : replicas)
            if (metadata.locator.location(replica.endpoint()).datacenter.equals(localDataCenter))
                localReplicas.add(replica);

        if (localReplicas.isEmpty())
        {
            // If the consistency required is local then we should not involve other DCs
            if (cl.isDatacenterLocal())
                throw UnavailableException.create(cl, cl.blockFor(replicationStrategy), 0);

            // No endpoint in local DC, pick the closest endpoint according to the configured proximity measures
            replicas = proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
            return replicas.get(0);
        }

        return localReplicas.get(ThreadLocalRandom.current().nextInt(localReplicas.size()));
    }

    /**
     * A forwarding counter write is always sent to a single owning coordinator for the range, by the original coordinator
     * (if it is not itself an owner)
     */
    public static ReplicaPlan.ForWrite forForwardingCounterWrite(ClusterMetadata metadata, Keyspace keyspace, Token token, Function<ClusterMetadata, Replica> replica)
    {
        return forSingleReplicaWrite(metadata, keyspace, token, replica);
    }

    public static ReplicaPlan.ForWrite forLocalBatchlogWrite()
    {
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Keyspace systemKeyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        Replica localSystemReplica = SystemReplicas.getSystemReplica(FBUtilities.getBroadcastAddressAndPort());

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWrite(
               systemKeyspace.getReplicationStrategy(),
               EndpointsForToken.of(token, localSystemReplica),
               EndpointsForToken.empty(token)
        );
        return forWrite(systemKeyspace, ConsistencyLevel.ONE, (cm) -> liveAndDown, (cm) -> true, writeAll);
    }

    /**
     * Requires that the provided endpoints are alive.  Converts them to their relevant system replicas.
     * Note that the liveAndDown collection and live are equal to the provided endpoints.
     *
     * @param isAny if batch consistency level is ANY, in which case a local node will be picked
     */
    public static ReplicaPlan.ForWrite forBatchlogWrite(boolean isAny) throws UnavailableException
    {
        return forBatchlogWrite(ClusterMetadata.current(), isAny);
    }

    private static ReplicaLayout.ForTokenWrite liveAndDownForBatchlogWrite(Token token, ClusterMetadata metadata, boolean isAny)
    {
        Directory directory = metadata.directory;
        Location local = metadata.locator.local();
        Multimap<String, InetAddressAndPort> localEndpoints = HashMultimap.create(directory.allDatacenterRacks()
                                                                                           .get(local.datacenter));
        // Replicas are picked manually:
        //  - replicas should be alive according to the failure detector
        //  - replicas should be in the local datacenter
        //  - choose min(2, number of qualifying candiates above)
        //  - allow the local node to be the only replica only if it's a single-node DC
        Collection<InetAddressAndPort> chosenEndpoints = filterBatchlogEndpoints(false,
                                                                                 local.rack,
                                                                                 localEndpoints,
                                                                                 Collections::shuffle,
                                                                                 (r) -> FailureDetector.isEndpointAlive.test(r) && metadata.directory.peerState(r) == NodeState.JOINED,
                                                                                 ThreadLocalRandom.current()::nextInt);

        // Batchlog is hosted by either one node or two nodes from different racks.
        ConsistencyLevel consistencyLevel = chosenEndpoints.size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO;

        if (chosenEndpoints.isEmpty())
        {
            if (isAny)
                chosenEndpoints = Collections.singleton(FBUtilities.getBroadcastAddressAndPort());
            else
                // UnavailableException instead of letting the batchlog write unnecessarily timeout
                throw new UnavailableException("Cannot achieve consistency level " + consistencyLevel
                        + " for batchlog in local DC, required:" + REQUIRED_BATCHLOG_REPLICA_COUNT
                        + ", available:" + 0,
                        consistencyLevel, REQUIRED_BATCHLOG_REPLICA_COUNT, 0);
        }

        return ReplicaLayout.forTokenWrite(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getReplicationStrategy(),
                                           SystemReplicas.getSystemReplicas(chosenEndpoints).forToken(token),
                                           EndpointsForToken.empty(token));
    }

    public static ReplicaPlan.ForWrite forBatchlogWrite(ClusterMetadata metadata, boolean isAny) throws UnavailableException
    {
        // A single case we write not for range or token, but multiple mutations to many tokens
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Keyspace systemKeyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);

        ReplicaLayout.ForTokenWrite liveAndDown = liveAndDownForBatchlogWrite(token, metadata, isAny);
        // Batchlog is hosted by either one node or two nodes from different racks.
        ConsistencyLevel consistencyLevel = liveAndDown.all().size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO;

        AbstractReplicationStrategy replicationStrategy = liveAndDown.replicationStrategy();
        EndpointsForToken contacts = writeAll.select(consistencyLevel, liveAndDown, liveAndDown);
        assureSufficientLiveReplicasForWrite(metadata.locator, replicationStrategy, consistencyLevel, liveAndDown.all(), liveAndDown.pending());
        return new ReplicaPlan.ForWrite(systemKeyspace,
                                        replicationStrategy,
                                        consistencyLevel,
                                        liveAndDown.pending(),
                                        liveAndDown.all(),
                                        liveAndDown.all().filter(FailureDetector.isReplicaAlive),
                                        contacts,
                                        (newMetadata) -> forBatchlogWrite(newMetadata, isAny),
                                        metadata.epoch) {
            @Override
            public boolean stillAppliesTo(ClusterMetadata newMetadata)
            {
                if (liveAndDown.stream().allMatch(r -> newMetadata.directory.peerState(r.endpoint()) == NodeState.JOINED))
                    return true;

                return super.stillAppliesTo(newMetadata);
            }
        };
    }

    // Collect a list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
    @VisibleForTesting
    public static Collection<InetAddressAndPort> filterBatchlogEndpoints(boolean preferLocalRack,
                                                                         String localRack,
                                                                         Multimap<String, InetAddressAndPort> endpoints,
                                                                         Consumer<List<?>> shuffle,
                                                                         Predicate<InetAddressAndPort> include,
                                                                         Function<Integer, Integer> indexPicker)
    {
        return DatabaseDescriptor.getBatchlogEndpointStrategy().useDynamicSnitchScores && DatabaseDescriptor.isDynamicEndpointSnitch()
                ? filterBatchlogEndpointsDynamic(preferLocalRack, localRack, endpoints,  FailureDetector.isEndpointAlive)
                : filterBatchlogEndpointsRandom(preferLocalRack, localRack, endpoints, Collections::shuffle, FailureDetector.isEndpointAlive, ThreadLocalRandom.current()::nextInt);
    }

    private static ListMultimap<String, InetAddressAndPort> validate(boolean preferLocalRack, String localRack,
                                                                     Multimap<String, InetAddressAndPort> endpoints,
                                                                     Predicate<InetAddressAndPort> include)
    {
        int endpointCount = endpoints.values().size();
        // special case for single-node data centers
        if (endpointCount <= REQUIRED_BATCHLOG_REPLICA_COUNT)
            return ArrayListMultimap.create(endpoints);

        // strip out dead endpoints and localhost
        int rackCount = endpoints.keySet().size();
        ListMultimap<String, InetAddressAndPort> validated = ArrayListMultimap.create(rackCount, endpointCount / rackCount);
        for (Map.Entry<String, InetAddressAndPort> entry : endpoints.entries())
        {
            InetAddressAndPort addr = entry.getValue();
            if (!addr.equals(FBUtilities.getBroadcastAddressAndPort()) && include.test(addr))
                validated.put(entry.getKey(), entry.getValue());
        }

        // return early if no more than 2 nodes:
        if (validated.size() <= REQUIRED_BATCHLOG_REPLICA_COUNT)
            return validated;

        // if the local rack is not preferred and there are enough nodes in other racks, remove it:
        if (!(DatabaseDescriptor.getBatchlogEndpointStrategy().preferLocalRack || preferLocalRack)
                && validated.size() - validated.get(localRack).size() >= REQUIRED_BATCHLOG_REPLICA_COUNT)
        {
            // we have enough endpoints in other racks
            validated.removeAll(localRack);
        }

        return validated;
    }

    // Collect a list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
    // Replicas are picked manually:
    //  - replicas should be alive according to the failure detector
    //  - replicas should be in the local datacenter
    //  - choose min(2, number of qualifying candiates above)
    //  - allow the local node to be the only replica only if it's a single-node DC
    @VisibleForTesting
    public static Collection<InetAddressAndPort> filterBatchlogEndpointsRandom(boolean preferLocalRack, String localRack,
                                                                               Multimap<String, InetAddressAndPort> endpoints,
                                                                               Consumer<List<?>> shuffle,
                                                                               Predicate<InetAddressAndPort> include,
                                                                               Function<Integer, Integer> indexPicker)
    {
        ListMultimap<String, InetAddressAndPort> validated = validate(preferLocalRack, localRack, endpoints, include);

        // return early if no more than 2 nodes:
        if (validated.size() <= REQUIRED_BATCHLOG_REPLICA_COUNT)
            return validated.values();

        /*
         * if we have only 1 `other` rack to select replicas from (whether it be the local rack or a single non-local rack),
         * pick two random nodes from there and return early;
         * we are guaranteed to have at least two nodes in the single remaining rack because of the above if block.
         */
        if (validated.keySet().size() == 1)
        {
            /*
             * we have only 1 `other` rack to select replicas from (whether it be the local rack or a single non-local rack)
             * pick two random nodes from there; we are guaranteed to have at least two nodes in the single remaining rack
             * because of the preceding if block.
             */
            List<InetAddressAndPort> otherRack = Lists.newArrayList(validated.values());
            shuffle.accept(otherRack);
            return otherRack.subList(0, REQUIRED_BATCHLOG_REPLICA_COUNT);
        }

        // randomize which racks we pick from if more than 2 remaining
        Collection<String> racks;
        if (validated.keySet().size() == REQUIRED_BATCHLOG_REPLICA_COUNT)
        {
            racks = validated.keySet();
        }
        else if (preferLocalRack || DatabaseDescriptor.getBatchlogEndpointStrategy().preferLocalRack)
        {
            List<String> nonLocalRacks = Lists.newArrayList(Sets.difference(validated.keySet(), ImmutableSet.of(localRack)));
            racks = new LinkedHashSet<>();
            racks.add(localRack);
            racks.add(nonLocalRacks.get(indexPicker.apply(nonLocalRacks.size())));
        }
        else
        {
            racks = Lists.newArrayList(validated.keySet());
            shuffle.accept((List<?>) racks);
        }

        // grab two random nodes from two different racks

        List<InetAddressAndPort> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
        for (String rack : Iterables.limit(racks, REQUIRED_BATCHLOG_REPLICA_COUNT))
        {
            List<InetAddressAndPort> rackMembers = validated.get(rack);
            result.add(rackMembers.get(indexPicker.apply(rackMembers.size())));
        }

        return result;
    }

    @VisibleForTesting
    public static Collection<InetAddressAndPort> filterBatchlogEndpointsDynamic(boolean preferLocalRack, String localRack,
                                                                                Multimap<String, InetAddressAndPort> endpoints,
                                                                                Predicate<InetAddressAndPort> include)
    {
        ListMultimap<String, InetAddressAndPort> validated = validate(preferLocalRack, localRack, endpoints, include);

        // return early if no more than 2 nodes:
        if (validated.size() <= REQUIRED_BATCHLOG_REPLICA_COUNT)
            return validated.values();

        // sort _all_ nodes to pick the best racks
        List<InetAddressAndPort> sorted = sortByProximity(validated.values());

        List<InetAddressAndPort> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
        Set<String> racks = new HashSet<>();

        while (result.size() < REQUIRED_BATCHLOG_REPLICA_COUNT)
        {
            for (InetAddressAndPort endpoint : sorted)
            {
                if (result.size() == REQUIRED_BATCHLOG_REPLICA_COUNT)
                    break;

                if (racks.isEmpty())
                    racks.addAll(validated.keySet());

                String rack = DatabaseDescriptor.getLocator().location(endpoint).rack;
                if (!racks.remove(rack))
                    continue;
                if (result.contains(endpoint))
                    continue;

                result.add(endpoint);
            }
        }

        return result;
    }

    @VisibleForTesting
    public static List<InetAddressAndPort> sortByProximity(Collection<InetAddressAndPort> endpoints)
    {
        EndpointsForRange endpointsForRange = SystemReplicas.getSystemReplicas(endpoints);
        return DatabaseDescriptor.getNodeProximity()
                .sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), endpointsForRange)
                .endpointList();
    }


    public static ReplicaPlan.ForWrite forReadRepair(ReplicaPlan<?, ?> forRead, ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, Predicate<Replica> isAlive) throws UnavailableException
    {
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        Selector selector = writeReadRepair(forRead);

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspace, token);
        ReplicaLayout.ForTokenWrite live = liveAndDown.filter(isAlive);

        EndpointsForToken contacts = selector.select(consistencyLevel, liveAndDown, live);
        assureSufficientLiveReplicasForWrite(metadata.locator, replicationStrategy, consistencyLevel, live.all(), liveAndDown.pending());
        return new ReplicaPlan.ForWrite(keyspace,
                                        replicationStrategy,
                                        consistencyLevel,
                                        liveAndDown.pending(),
                                        liveAndDown.all(),
                                        live.all(),
                                        contacts,
                                        (newClusterMetadata) -> forReadRepair(forRead, newClusterMetadata, keyspace, consistencyLevel, token, isAlive),
                                        metadata.epoch);
    }

    public static ReplicaPlan.ForWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, Selector selector) throws UnavailableException
    {
        return forWrite(ClusterMetadata.current(), keyspace, consistencyLevel, token, selector);
    }

    public static ReplicaPlan.ForWrite forWrite(ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, Selector selector) throws UnavailableException
    {
        return forWrite(metadata, keyspace, consistencyLevel, (newClusterMetadata) -> ReplicaLayout.forTokenWriteLiveAndDown(newClusterMetadata, keyspace, token), selector);
    }

    @VisibleForTesting
    public static ReplicaPlan.ForWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Function<ClusterMetadata, EndpointsForToken> natural, Function<ClusterMetadata, EndpointsForToken> pending, Epoch lastModified, Predicate<Replica> isAlive, Selector selector) throws UnavailableException
    {
        return forWrite(keyspace, consistencyLevel, (newClusterMetadata) -> ReplicaLayout.forTokenWrite(keyspace.getReplicationStrategy(), natural.apply(newClusterMetadata), pending.apply(newClusterMetadata)), isAlive, selector);
    }

    public static ReplicaPlan.ForWrite forWrite(ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel consistencyLevel, Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDown, Selector selector) throws UnavailableException
    {
        return forWrite(metadata, keyspace, consistencyLevel, liveAndDown, FailureDetector.isReplicaAlive, selector);
    }

    public static ReplicaPlan.ForWrite forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDownSupplier, Predicate<Replica> isAlive, Selector selector) throws UnavailableException
    {
        return forWrite(ClusterMetadata.current(), keyspace, consistencyLevel, liveAndDownSupplier, isAlive, selector);
    }

    public static ReplicaPlan.ForWrite forWrite(ClusterMetadata metadata,
                                                Keyspace keyspace,
                                                ConsistencyLevel consistencyLevel,
                                                Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDownSupplier,
                                                Predicate<Replica> isAlive,
                                                Selector selector) throws UnavailableException
    {
        ReplicaLayout.ForTokenWrite liveAndDown = liveAndDownSupplier.apply(metadata);
        ReplicaLayout.ForTokenWrite live = liveAndDown.filter(isAlive);

        AbstractReplicationStrategy replicationStrategy = liveAndDown.replicationStrategy();
        EndpointsForToken contacts = selector.select(consistencyLevel, liveAndDown, live);
        assureSufficientLiveReplicasForWrite(metadata.locator, replicationStrategy, consistencyLevel, live.all(), liveAndDown.pending());

        return new ReplicaPlan.ForWrite(keyspace,
                                        replicationStrategy,
                                        consistencyLevel,
                                        liveAndDown.pending(),
                                        liveAndDown.all(),
                                        live.all(),
                                        contacts,
                                        (newClusterMetadata) -> forWrite(newClusterMetadata, keyspace, consistencyLevel, liveAndDownSupplier, isAlive, selector),
                                        metadata.epoch);
    }

    public interface Selector
    {
        /**
         * Select the {@code Endpoints} from {@param liveAndDown} and {@param live} to contact according to the consistency level.
         */
        <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
        E select(ConsistencyLevel consistencyLevel, L liveAndDown, L live);
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
        E select(ConsistencyLevel consistencyLevel, L liveAndDown, L live)
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
        E select(ConsistencyLevel consistencyLevel, L liveAndDown, L live)
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
            Locator locator = ClusterMetadata.current().locator;
            ObjectIntHashMap<String> requiredPerDc = eachQuorumForWrite(locator, liveAndDown.replicationStrategy(), liveAndDown.pending());
            addToCountPerDc(locator, requiredPerDc, live.natural().filter(Replica::isFull), -1);
            addToCountPerDc(locator, requiredPerDc, live.pending(), -1);

            for (Replica replica : filter(live.natural(), Replica::isTransient))
            {

                String dc = locator.location(replica.endpoint()).datacenter;
                if (requiredPerDc.addTo(dc, -1) >= 0)
                    contacts.add(replica);
            }
            return contacts.build();
        }
    };

    /**
     * TODO: Transient Replication C-14404/C-14665
     * TODO: We employ this even when there is no monotonicity to guarantee,
     *          e.g. in case of CL.TWO, CL.ONE with speculation, etc.
     *
     * Construct a read-repair write plan to provide monotonicity guarantees on any data we return as part of a read.
     *
     * Since this is not a regular write, this is just to guarantee future reads will read this data, we select only
     * the minimal number of nodes to meet the consistency level, and prefer nodes we contacted on read to minimise
     * data transfer.
     */
    public static Selector writeReadRepair(ReplicaPlan<?, ?> readPlan)
    {
        return new Selector()
        {

            @Override
            public <E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>>
            E select(ConsistencyLevel consistencyLevel, L liveAndDown, L live)
            {
                assert !any(liveAndDown.all(), Replica::isTransient);

                ReplicaCollection.Builder<E> contacts = live.all().newBuilder(live.all().size());
                // add all live nodes we might write to that we have already contacted on read
                contacts.addAll(filter(live.all(), r -> readPlan.contacts().endpoints().contains(r.endpoint())));

                // finally, add sufficient nodes to achieve our consistency level
                if (consistencyLevel != EACH_QUORUM)
                {
                    int add = consistencyLevel.blockForWrite(liveAndDown.replicationStrategy(), liveAndDown.pending()) - contacts.size();
                    if (add > 0)
                    {
                        E all = consistencyLevel.isDatacenterLocal() ? live.all().filter(InOurDc.replicas()) : live.all();
                        for (Replica replica : filter(all, r -> !contacts.contains(r)))
                        {
                            contacts.add(replica);
                            if (--add == 0)
                                break;
                        }
                    }
                }
                else
                {
                    Locator locator = ClusterMetadata.current().locator;
                    ObjectIntHashMap<String> requiredPerDc = eachQuorumForWrite(locator, liveAndDown.replicationStrategy(), liveAndDown.pending());
                    addToCountPerDc(locator, requiredPerDc, contacts.snapshot(), -1);
                    for (Replica replica : filter(live.all(), r -> !contacts.contains(r)))
                    {
                        String dc = locator.location(replica.endpoint()).datacenter;
                        if (requiredPerDc.addTo(dc, -1) >= 0)
                            contacts.add(replica);
                    }
                }
                return contacts.build();
            }
        };
    }

    /**
     * Construct the plan for a paxos round - NOT the write or read consistency level for either the write or comparison,
     * but for the paxos linearisation agreement.
     *
     * This will select all live nodes as the candidates for the operation.  Only the required number of participants
     */
    public static ReplicaPlan.ForPaxosWrite forPaxos(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        return forPaxos(ClusterMetadata.current(), keyspace, key, consistencyForPaxos, true);
    }

    public static ReplicaPlan.ForPaxosWrite forPaxos(ClusterMetadata metadata, Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistencyForPaxos, boolean throwOnInsufficientLiveReplicas) throws UnavailableException
    {
        Token tk = key.getToken();

        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspace, tk);

        Replicas.temporaryAssertFull(liveAndDown.all()); // TODO CASSANDRA-14547

        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // TODO: we should cleanup our semantics here, as we're filtering ALL nodes to localDC which is unexpected for ReplicaPlan
            // Restrict natural and pending to node in the local DC only
            liveAndDown = liveAndDown.filter(InOurDc.replicas());
        }

        ReplicaLayout.ForTokenWrite live = liveAndDown.filter(FailureDetector.isReplicaAlive);

        // TODO: this should use assureSufficientReplicas
        int participants = liveAndDown.all().size();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833

        if (throwOnInsufficientLiveReplicas)
        {
            if (live.all().size() < requiredParticipants)
                throw UnavailableException.create(consistencyForPaxos, requiredParticipants, live.all().size());

            // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
            // Note that we fake an impossible number of required nodes in the unavailable exception
            // to nail home the point that it's an impossible operation no matter how many nodes are live.
            if (liveAndDown.pending().size() > 1)
                throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", liveAndDown.all().size()),
                                               consistencyForPaxos,
                                               participants + 1,
                                               live.all().size());
        }

        return new ReplicaPlan.ForPaxosWrite(keyspace,
                                             consistencyForPaxos,
                                             liveAndDown.pending(),
                                             liveAndDown.all(),
                                             live.all(),
                                             live.all(),
                                             requiredParticipants,
                                             (newClusterMetadata) -> forPaxos(newClusterMetadata, keyspace, key, consistencyForPaxos, false),
                                             metadata.epoch);
    }

    private static <E extends Endpoints<E>> E candidatesForRead(Keyspace keyspace,
                                                                @Nullable Index.QueryPlan indexQueryPlan,
                                                                ConsistencyLevel consistencyLevel,
                                                                E liveNaturalReplicas)
    {
        E replicas = consistencyLevel.isDatacenterLocal() ? liveNaturalReplicas.filter(InOurDc.replicas()) : liveNaturalReplicas;

        return indexQueryPlan != null ? IndexStatusManager.instance.filterForQuery(replicas, keyspace, indexQueryPlan, consistencyLevel) : replicas;
    }

    private static <E extends Endpoints<E>> E contactForEachQuorumRead(Locator locator, NetworkTopologyStrategy replicationStrategy, E candidates)
    {
        ObjectIntHashMap<String> perDc = eachQuorumForRead(replicationStrategy);
        return candidates.filter(replica -> {
            String dc = locator.location(replica.endpoint()).datacenter;
            return perDc.addTo(dc, -1) >= 0;
        });
    }

    private static <E extends Endpoints<E>> E contactForRead(Locator locator, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, boolean alwaysSpeculate, E candidates)
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
        if (consistencyLevel == EACH_QUORUM && replicationStrategy instanceof NetworkTopologyStrategy)
            return contactForEachQuorumRead(locator, (NetworkTopologyStrategy) replicationStrategy, candidates);

        int count = consistencyLevel.blockFor(replicationStrategy) + (alwaysSpeculate ? 1 : 0);
        return candidates.subList(0, Math.min(count, candidates.size()));
    }


    /**
     * Construct a plan for reading from a single node - this permits no speculation or read-repair
     */
    public static ReplicaPlan.ForTokenRead forSingleReplicaRead(Keyspace keyspace, Token token, Replica replica)
    {
        return forSingleReplicaRead(ClusterMetadata.current(), keyspace, token, replica);
    }

    private static ReplicaPlan.ForTokenRead forSingleReplicaRead(ClusterMetadata metadata, Keyspace keyspace, Token token, Replica replica)
    {
        // todo; replica does not always contain token, figure out why
//        if (!metadata.placements.get(keyspace.getMetadata().params.replication).reads.forToken(token).contains(replica))
//            throw UnavailableException.create(ConsistencyLevel.ONE, 1, 1, 0, 0);

        EndpointsForToken one = EndpointsForToken.of(token, replica);

        return new ReplicaPlan.ForTokenRead(keyspace, keyspace.getReplicationStrategy(), ConsistencyLevel.ONE, one, one,
                                            (newClusterMetadata) -> forSingleReplicaRead(newClusterMetadata, keyspace, token, replica),
                                            (self) -> {
                                                throw new IllegalStateException("Read repair is not supported for short read/replica filtering protection.");
                                            },
                                            metadata.epoch);
    }

    /**
     * Construct a plan for reading from a single node - this permits no speculation or read-repair
     */
    public static ReplicaPlan.ForRangeRead forSingleReplicaRead(Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica, int vnodeCount)
    {
        return forSingleReplicaRead(ClusterMetadata.current(), keyspace, range, replica, vnodeCount);
    }

    private static ReplicaPlan.ForRangeRead forSingleReplicaRead(ClusterMetadata metadata, Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica, int vnodeCount)
    {
        // TODO: this is unsafe, as one.range() may be inconsistent with our supplied range; should refactor Range/AbstractBounds to single class
        EndpointsForRange one = EndpointsForRange.of(replica);

        return new ReplicaPlan.ForRangeRead(keyspace, keyspace.getReplicationStrategy(), ConsistencyLevel.ONE, range, one, one, vnodeCount,
                                            (newClusterMetadata) -> forSingleReplicaRead(metadata, keyspace, range, replica, vnodeCount),
                                            (self, token) -> {
                                                throw new IllegalStateException("Read repair is not supported for short read/replica filtering protection.");
                                            },
                                            metadata.epoch);
    }

    /**
     * Construct a plan for reading the provided token at the provided consistency level.  This translates to a collection of
     *   - candidates who are: alive, replicate the token, and are sorted by their snitch scores
     *   - contacts who are: the first blockFor + (retry == ALWAYS ? 1 : 0) candidates
     *
     * The candidate collection can be used for speculation, although at present
     * it would break EACH_QUORUM to do so without further filtering
     */
    public static ReplicaPlan.ForTokenRead forRead(Keyspace keyspace,
                                                   Token token,
                                                   @Nullable Index.QueryPlan indexQueryPlan,
                                                   ConsistencyLevel consistencyLevel,
                                                   SpeculativeRetryPolicy retry)
    {
        return forRead(ClusterMetadata.current(), keyspace, token, indexQueryPlan, consistencyLevel, retry, false);
    }

    public static ReplicaPlan.ForTokenRead forRead(ClusterMetadata metadata,
                                                   Keyspace keyspace,
                                                   Token token,
                                                   @Nullable Index.QueryPlan indexQueryPlan,
                                                   ConsistencyLevel consistencyLevel,
                                                   SpeculativeRetryPolicy retry)
    {
        return forRead(metadata, keyspace, token, indexQueryPlan, consistencyLevel, retry, true);
    }

    private static ReplicaPlan.ForTokenRead forRead(ClusterMetadata metadata, Keyspace keyspace, Token token, @Nullable Index.QueryPlan indexQueryPlan, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry,  boolean throwOnInsufficientLiveReplicas)
    {
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        ReplicaLayout.ForTokenRead forTokenRead = ReplicaLayout.forTokenReadLiveSorted(metadata, keyspace, replicationStrategy, token);
        EndpointsForToken candidates = candidatesForRead(keyspace, indexQueryPlan, consistencyLevel, forTokenRead.natural());
        EndpointsForToken contacts = contactForRead(metadata.locator, replicationStrategy, consistencyLevel, retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE), candidates);

        if (throwOnInsufficientLiveReplicas)
            assureSufficientLiveReplicasForRead(metadata.locator, replicationStrategy, consistencyLevel, contacts);

        return new ReplicaPlan.ForTokenRead(keyspace, replicationStrategy, consistencyLevel, candidates, contacts,
                                            (newClusterMetadata) -> forRead(newClusterMetadata, keyspace, token, indexQueryPlan, consistencyLevel, retry, false),
                                            (self) -> forReadRepair(self, metadata, keyspace, consistencyLevel, token, FailureDetector.isReplicaAlive),
                                            metadata.epoch);
    }

    /**
     * Construct a plan for reading the provided range at the provided consistency level.  This translates to a collection of
     *   - candidates who are: alive, replicate the range, and are sorted by their snitch scores
     *   - contacts who are: the first blockFor candidates
     *
     * There is no speculation for range read queries at present, so we never 'always speculate' here, and a failed response fails the query.
     */
    public static ReplicaPlan.ForRangeRead forRangeRead(Keyspace keyspace,
                                                        @Nullable Index.QueryPlan indexQueryPlan,
                                                        ConsistencyLevel consistencyLevel,
                                                        AbstractBounds<PartitionPosition> range,
                                                        int vnodeCount)
    {
        return forRangeRead(ClusterMetadata.current(), keyspace, indexQueryPlan, consistencyLevel, range, vnodeCount, true);
    }

    public static ReplicaPlan.ForRangeRead forRangeRead(ClusterMetadata metadata,
                                                        Keyspace keyspace,
                                                        @Nullable Index.QueryPlan indexQueryPlan,
                                                        ConsistencyLevel consistencyLevel,
                                                        AbstractBounds<PartitionPosition> range,
                                                        int vnodeCount,
                                                        boolean throwOnInsufficientLiveReplicas)
    {
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        ReplicaLayout.ForRangeRead forRangeRead = ReplicaLayout.forRangeReadLiveSorted(metadata, keyspace, replicationStrategy, range);
        EndpointsForRange candidates = candidatesForRead(keyspace, indexQueryPlan, consistencyLevel, forRangeRead.natural());
        EndpointsForRange contacts = contactForRead(metadata.locator, replicationStrategy, consistencyLevel, false, candidates);

        if (throwOnInsufficientLiveReplicas)
            assureSufficientLiveReplicasForRead(metadata.locator, replicationStrategy, consistencyLevel, contacts);

        return new ReplicaPlan.ForRangeRead(keyspace,
                                            replicationStrategy,
                                            consistencyLevel,
                                            range,
                                            candidates,
                                            contacts,
                                            vnodeCount,
                                            (newClusterMetadata) -> forRangeRead(newClusterMetadata, keyspace, indexQueryPlan, consistencyLevel, range, vnodeCount, false),
                                            (self, token) -> forReadRepair(self, metadata, keyspace, consistencyLevel, token, FailureDetector.isReplicaAlive),
                                            metadata.epoch);
    }

    /**
     * Construct a plan for reading the provided range at the provided consistency level on given endpoints.
     *
     * Note that:
     *   - given range may span multiple vnodes
     *   - endpoints should be alive and satifies consistency requirement.
     *   - each endpoint will be considered as replica of entire token ring, so coordinator can execute request with given range
     */
    public static ReplicaPlan.ForRangeRead forFullRangeRead(Keyspace keyspace,
                                                            ConsistencyLevel consistencyLevel,
                                                            AbstractBounds<PartitionPosition> range,
                                                            Set<InetAddressAndPort> endpointsToContact,
                                                            int vnodeCount)
    {
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        EndpointsForRange.Builder builder = EndpointsForRange.Builder.builder(FULL_TOKEN_RANGE);
        for (InetAddressAndPort endpoint : endpointsToContact)
            builder.add(Replica.fullReplica(endpoint, FULL_TOKEN_RANGE), ReplicaCollection.Builder.Conflict.NONE);

        EndpointsForRange contacts = builder.build();

        ClusterMetadata metadata = ClusterMetadata.current();
        return new ReplicaPlan.ForFullRangeRead(keyspace, replicationStrategy, consistencyLevel, range, contacts, contacts, vnodeCount, metadata.epoch);
    }

    /**
     * Take two range read plans for adjacent ranges, and check if it is OK (and worthwhile) to combine them into a single plan
     */
    public static ReplicaPlan.ForRangeRead maybeMerge(ClusterMetadata metadata,
                                                      Keyspace keyspace,
                                                      ConsistencyLevel consistencyLevel,
                                                      ReplicaPlan.ForRangeRead left,
                                                      ReplicaPlan.ForRangeRead right)
    {
        assert left.range.right.equals(right.range.left);

        if (!left.epoch.equals(right.epoch))
            return null;

        EndpointsForRange mergedCandidates = left.readCandidates().keep(right.readCandidates().endpoints());
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        EndpointsForRange contacts = contactForRead(metadata.locator, replicationStrategy, consistencyLevel, false, mergedCandidates);

        // Estimate whether merging will be a win or not
        if (!DatabaseDescriptor.getNodeProximity().isWorthMergingForRangeQuery(contacts, left.contacts(), right.contacts()))
            return null;

        AbstractBounds<PartitionPosition> newRange = left.range().withNewRight(right.range().right);

        // Check if there are enough shared endpoints for the merge to be possible.
        if (!isSufficientLiveReplicasForRead(metadata.locator, replicationStrategy, consistencyLevel, mergedCandidates))
            return null;

        int newVnodeCount = left.vnodeCount() + right.vnodeCount();

        // If we get there, merge this range and the next one
        return new ReplicaPlan.ForRangeRead(keyspace,
                                            replicationStrategy,
                                            consistencyLevel,
                                            newRange,
                                            mergedCandidates,
                                            contacts,
                                            newVnodeCount,
                                            (newClusterMetadata) -> forRangeRead(newClusterMetadata,
                                                                                 keyspace,
                                                                                 null, // TODO (TCM) - we only use the recomputed ForRangeRead to check stillAppliesTo - make sure passing null here is ok
                                                                                 consistencyLevel,
                                                                                 newRange,
                                                                                 newVnodeCount,
                                                                                 false),
                                            (self, token) -> {
                                                // It might happen that the ring has moved forward since the operation has started, but because we'll be recomputing a quorum
                                                // after the operation is complete, we will catch inconsistencies either way.
                                                return forReadRepair(self, ClusterMetadata.current(), keyspace, consistencyLevel, token, FailureDetector.isReplicaAlive);
                                            },
                                            left.epoch);
    }
}
