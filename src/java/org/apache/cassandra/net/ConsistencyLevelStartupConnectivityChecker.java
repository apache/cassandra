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
package org.apache.cassandra.net;

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static java.util.stream.Collectors.groupingBy;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;


/**
 * A {@link StartupConnectivityChecker} implementation that based on the configured {@link ConsistencyLevel} to
 * determine how many peers in the local datacenter or all datacenters to wait for before it is ready to advertise to clients.
 * The lowest non-zero replication factor among all user keyspaces is used to determine the quorum.
 *
 * <p>The checker breaks the check into 2 phases.
 * 1. In phase one, it checks if the primary set of peers has connected.
 * 2. In phase two, it sends additional ping messages to the rest of the peers and check if the connected peers can satisfy
 *    the specified consistency level at the end.
 *
 * <p>Supported values for {@code blockForPeersConsistencyLevel} are:
 * <ul>
 *     <li>ALL
 *     <li>QUORUM
 *     <li>LOCAL_QUORUM
 *     <li>EACH_QUORUM
 * </ul>
 *
 * <p>The checker only supports the {@link NetworkTopologyStrategy}. The checker is not suitable when using vnodes.
 */
public class ConsistencyLevelStartupConnectivityChecker extends AbstractStartupConnectivityChecker
{
    public static final String DEFAULT_CONSISTENCY_LEVEL = "LOCAL_QUORUM";

    // Only used to help group peers from multi-dc when using QUORUM
    private static final String FAKE_DC_NAME_FOR_CL_QUORUM = "FAKE_DC_NAME_FOR_CL_QUORUM";

    private static final EnumSet<ConsistencyLevel> SUPPORTED_OPTIONS = EnumSet.of(ConsistencyLevel.ALL,
                                                                                  ConsistencyLevel.QUORUM,
                                                                                  ConsistencyLevel.LOCAL_QUORUM,
                                                                                  ConsistencyLevel.EACH_QUORUM);
    private final ConsistencyLevel consistencyLevel;
    private final long phaseOneTimeoutNanos;
    private final Map<String, CountDownLatch> peersToBlockPerDc = new HashMap<>();
    // Send a ping message to this group of peers first
    private final Set<InetAddressAndPort> primaryPeers = new HashSet<>();
    // Keeps a list of peers as backup if not enough peers respond in half the timeout
    private final Set<InetAddressAndPort> secondaryPeers = new HashSet<>();
    // Keeps a map of peers to their natural replicas
    private final ArrayListMultimap<InetAddressAndPort, InetAddressAndPort> replicaSets = ArrayListMultimap.create();
    // Create and initialize the dcInfos to help determine if the specified CL is satisfied
    private final Map<String, ReplicationInfo> infoByDc = new HashMap<>();

    @VisibleForTesting /* Only used for testing */
    boolean shouldRunSecondPhaseCheck = true;

    public ConsistencyLevelStartupConnectivityChecker()
    {
        this(DatabaseDescriptor.getBlockForPeersTimeoutInSeconds(), TimeUnit.SECONDS, DatabaseDescriptor.getBlockForPeersConsistencyLevel());
    }

    @VisibleForTesting
    ConsistencyLevelStartupConnectivityChecker(long timeout, TimeUnit unit, ConsistencyLevel consistencyLevel)
    {
        super(timeout, unit);
        Preconditions.checkArgument(SUPPORTED_OPTIONS.contains(consistencyLevel),
                                    "%s is not supported by ConsistencyLevelStartupConnectivityChecker", consistencyLevel);
        this.consistencyLevel = consistencyLevel;
        this.phaseOneTimeoutNanos = timeoutNanos / 2;
    }

    @Override
    protected Map<String, CountDownLatch> calculatePeersToBlockPerDc(Multimap<String, InetAddressAndPort> peersByDatacenter)
    {
        Preconditions.checkState(peersToBlockPerDc.isEmpty(), "peersToBlockPerDc should not be setup yet");

        boolean hasUserKeyspace = initializeInfoByDc(peersByDatacenter);
        // no need to proceed, return the empty map, i.e. wait for no peers
        if (!hasUserKeyspace)
            return peersToBlockPerDc;

        // blocks for all nodes in all datacenters
        if (consistencyLevel == ConsistencyLevel.ALL)
        {
            for (String datacenter : peersByDatacenter.keys())
            {
                int peersToBlock = peersByDatacenter.get(datacenter).size();
                peersToBlockPerDc.put(datacenter, newCountDownLatch(peersToBlock));
            }
        }
        else
        {
            // Build a map of token to endpoint sorted by token and remove unnecessary endpoints
            Map<Token, InetAddressAndPort> normalTokenToEndpoint = StorageService.instance.getTokenMetadata().getNormalTokenToEndpointMap();
            TreeMap<Token, InetAddressAndPort> sortedTokenToEndpoint = new TreeMap<>();
            normalTokenToEndpoint.forEach((token, endpoint) -> {
                String datacenter = getDatacenterSource().apply(endpoint);
                ReplicationInfo replicationInfo = infoByDc.get(datacenter);
                // Exclude the endpoints should not be considered, e.g. remote DC peers when using LOCAL_QUORUM.
                if (replicationInfo != null)
                {
                    sortedTokenToEndpoint.put(token, endpoint);
                }
            });

            initializeReplicaSets(sortedTokenToEndpoint);
            arrangeContacts(sortedTokenToEndpoint);

            if (consistencyLevel == ConsistencyLevel.QUORUM)
            {
                // Block for the primary peers, and group all peers as if they are in the same datacenter
                peersToBlockPerDc.put(FAKE_DC_NAME_FOR_CL_QUORUM, newCountDownLatch(primaryPeers.size()));
            }
            else
            {
                Map<String, List<InetAddressAndPort>> primaryPeersByDc = primaryPeers.stream()
                                                                                     .collect(groupingBy(getDatacenterSource()));
                for (String datacenter : peersByDatacenter.keySet())
                {
                    peersToBlockPerDc.put(datacenter, newCountDownLatch(primaryPeersByDc.get(datacenter).size()));
                }
            }
        }
        return peersToBlockPerDc;
    }

    @Override
    protected Multimap<String, InetAddressAndPort> filterNodes(Multimap<String, InetAddressAndPort> nodesByDatacenter)
    {
        nodesByDatacenter = super.filterNodes(nodesByDatacenter);
        if (consistencyLevel == ConsistencyLevel.LOCAL_QUORUM)
        {

            String localDc = getDatacenterSource().apply(localAddress);
            nodesByDatacenter.keySet().retainAll(Collections.singleton(localDc));
            logger.info("Blocking coordination until nodes in the local datacenter can satisfy {}, timeout={}s",
                        consistencyLevel,
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        else
        {
            logger.info("Blocking coordination until all nodes can satisfy {}, timeout={}s",
                        consistencyLevel,
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        return nodesByDatacenter;
    }

    @Override
    protected Function<InetAddressAndPort, String> getDatacenterSource()
    {
        // Since QUORUM considers DCs all together, override to group the endpoins in the same DC 'logically'
        if (consistencyLevel == ConsistencyLevel.QUORUM)
        {
            return ep -> FAKE_DC_NAME_FOR_CL_QUORUM;
        }
        else
        {
            return super.getDatacenterSource();
        }
    }

    @Override
    protected boolean executeInternal(Multimap<String, InetAddressAndPort> peersByDatacenter)
    {
        if (consistencyLevel == ConsistencyLevel.ALL)
        {
            // send out a ping message to open up the non-gossip connections to all peers. Note that this sends the
            // ping messages to _all_ peers, not just the ones we block for in dcToRemainingPeers.
            sendPingMessages(getAckTracker(), getAckTracker().getPeers(), getDatacenterSource(), peersToBlockPerDc);

            return waitForNodesUntilTimeout(peersToBlockPerDc.values(), timeoutNanos, getElaspedTimeNanos());
        }
        // consistency levels that need 2 phase check
        else if (consistencyLevel == ConsistencyLevel.LOCAL_QUORUM
                 || consistencyLevel == ConsistencyLevel.EACH_QUORUM
                 || consistencyLevel == ConsistencyLevel.QUORUM)
        {
            // send out a ping message to open up the non-gossip connections to all peers. Note that this sends the
            // ping messages to _all_ peers, not just the ones we block for in dcToRemainingPeers.
            sendPingMessages(getAckTracker(), primaryPeers, getDatacenterSource(), peersToBlockPerDc);

            // we now wait until we have heard back from all the peers required for the startup, or until the primary
            // timeout whichever comes first
            boolean succeeded = waitForNodesUntilTimeout(peersToBlockPerDc.values(), phaseOneTimeoutNanos, getElaspedTimeNanos());
            return startSecondPhaseCheckMaybe(succeeded);
        }
        else
        {
            // It should not reach here. Added for completeness.
            throw new IllegalStateException("Unsupported consistency level specified for the checker: " + consistencyLevel);
        }
    }

    private boolean startSecondPhaseCheckMaybe(boolean succeededFromPhaseOne)
    {
        // Only used for testing!
        if (!shouldRunSecondPhaseCheck)
            return succeededFromPhaseOne;

        boolean succeeded = succeededFromPhaseOne;
        if (!succeeded && (!secondaryPeers.isEmpty() && getRemainingTimeNanos() > 0))
        {
            logger.info("Sending out ping messages to {} secondary peers", secondaryPeers.size());
            // send out a ping message to open up the non-gossip connections to secondary peers
            // omit passing dcToRemainingPeers to the sendPingMessages method to prevent secondary peers
            // decrementing the primary peers latches
            sendPingMessages(getAckTracker(), secondaryPeers, getDatacenterSource(), /* dcToRemainingPeers */ null);
            succeeded = waitForNodesUntilTimeout(peersToBlockPerDc.values(), this.timeoutNanos, getElaspedTimeNanos());

            // regardless of the succeeded value, we want to check whether we have enough nodes
            // available to fulfill the consistency level when we still have missing peers
            if (!succeeded)
            {
                Set<InetAddressAndPort> missingPeers = getAckTracker().getMissingPeers();
                succeeded = isConsistencyLevelSatisfied(replicaSets, getDatacenterSource(), infoByDc, missingPeers);
            }
        }
        return succeeded;
    }

    /**
     * Initialize the dcInfos to help determine if the specified CL can be satisfied. A replicationInfo is associated with each datacenter.
     * If consistencyLevel is QUORUM, associate the replicationInfo with {@link #FAKE_DC_NAME_FOR_CL_QUORUM}
     *
     * @return true, if infoByDc is not empty, i.e. there is user keyspace; otherwise, return false, meaning there is no user keyspace.
     */
    private boolean initializeInfoByDc(Multimap<String, InetAddressAndPort> peersByDatacenter)
    {
        Preconditions.checkState(infoByDc.isEmpty(), "infoByDc should not be setup yet.");

        if (consistencyLevel == ConsistencyLevel.QUORUM)
        {
            NetworkTopologyStrategy miniRfReplicationStrategy = getReplicationStrategyWithMinRf(null);
            if (miniRfReplicationStrategy != null)
            {
                ReplicationInfo replicationInfo = new ReplicationInfo();
                replicationInfo.replicationStrategy = miniRfReplicationStrategy;
                replicationInfo.quorum = ConsistencyLevel.quorumFor(miniRfReplicationStrategy);
                replicationInfo.maxFailurePerReplicaSet = miniRfReplicationStrategy.getReplicationFactor().allReplicas - replicationInfo.quorum;
                infoByDc.put(FAKE_DC_NAME_FOR_CL_QUORUM, replicationInfo);
            }
        }
        else
        {
            for (String datacenter : peersByDatacenter.keySet())
            {
                NetworkTopologyStrategy miniRfReplicationStrategy = getReplicationStrategyWithMinRf(datacenter);
                if (miniRfReplicationStrategy != null)
                {
                    ReplicationInfo info = new ReplicationInfo();
                    info.replicationStrategy = getReplicationStrategyWithMinRf(datacenter);
                    info.quorum = ConsistencyLevel.localQuorumFor(info.replicationStrategy, datacenter);
                    info.maxFailurePerReplicaSet = info.replicationStrategy.getReplicationFactor(datacenter).allReplicas - info.quorum;
                    infoByDc.put(datacenter, info);
                }
            }
        }

        return !infoByDc.isEmpty();
    }

    /**
     * Initialize the replicaset of each endpoint. By setting filterByDc true, each replicaset only includes DC-local nodes
     */
    private void initializeReplicaSets(TreeMap<Token, InetAddressAndPort> sortedTokenToEndpoint)
    {
        sortedTokenToEndpoint.forEach((token, peer) -> {
            String datacenter = getDatacenterSource().apply(peer);
            ReplicationInfo replicationInfo = infoByDc.get(datacenter);

            Stream<InetAddressAndPort> replicasStream = replicationInfo.replicationStrategy
                .getNaturalReplicasForToken(token)
                .endpointList()
                .stream();

            if (consistencyLevel == ConsistencyLevel.LOCAL_QUORUM
                || consistencyLevel == ConsistencyLevel.EACH_QUORUM)
            {
                // Only include the nodes in the local DC
                replicasStream = replicasStream.filter(endpoint -> datacenter.equals(getDatacenterSource().apply(endpoint)));
            }
            replicasStream.forEach(ep -> replicaSets.put(peer, ep));
        });
    }

    /**
     * Arrange the primary and secondary groups of peers to contact.
     * The primary contacts can satisfy quorum for all replicasets.
     * The secondary contacts are the rest of the nodes.
     */
    private void arrangeContacts(TreeMap<Token, InetAddressAndPort> sortedTokenToEndpoint)
    {
        sortedTokenToEndpoint.forEach((token, peer) -> {
            String datacenter = getDatacenterSource().apply(peer);
            ReplicationInfo replicationInfo = infoByDc.get(datacenter);
            // For each replicas list,
            // 1. Check whether the current primaryPeers reaches to quorum; if not, goto 2
            // 2. Add from the end of the replicas list until the contacts count reaches to quorum.
            List<InetAddressAndPort> replicaList = replicaSets.get(peer);
            int contacts = 0;
            for (InetAddressAndPort replica : replicaList)
            {
                if (primaryPeers.contains(replica))
                    contacts++;
            }
            for (int i = replicaList.size() - 1; i >= 0; i--)
            {
                // reached quorum for this replica set and stop
                if (contacts >= replicationInfo.quorum)
                    return;

                if (primaryPeers.add(replicaList.get(i)))
                    contacts++;
            }
        });
        // add the rest of the peers to the secondary
        sortedTokenToEndpoint.values().forEach(ep -> {
            if (!primaryPeers.contains(ep))
                secondaryPeers.add(ep);
        });

        // The self node is required for counting quorum in the step above
        // Now it should be removed since it is surely connected
        primaryPeers.remove(localAddress);
        secondaryPeers.remove(localAddress);
    }

    /**
     * Makes sure that the consistency level has been satisfied in the ring by ensuring that each of the natural
     * replicas for every token have sufficient number of connections to peers.
     *
     * @return true if the consistency level is satisfied, false otherwise
     */
    private static boolean isConsistencyLevelSatisfied(Multimap<InetAddressAndPort, InetAddressAndPort> replicaSets,
                                                       Function<InetAddressAndPort, String> datacenterSource,
                                                       Map<String, ReplicationInfo> dcInfos,
                                                       Set<InetAddressAndPort> missingPeers)
    {
        for (InetAddressAndPort node : replicaSets.keys())
        {
            String datacenter = datacenterSource.apply(node);
            ReplicationInfo replicationInfo = dcInfos.get(datacenter);
            // dcInfo will be null when DC is remote for the LOCAL_QUORUM CL
            if (replicationInfo == null || replicationInfo.replicationStrategy == null)
                continue;

            long missingCount = replicaSets.get(node).stream().filter(missingPeers::contains).count();
            if (missingCount > replicationInfo.maxFailurePerReplicaSet)
                return false;
        }
        return true;
    }

    /**
     * Returns the {@link NetworkTopologyStrategy} that has the minimum number of
     * {@link org.apache.cassandra.locator.ReplicationFactor#allReplicas replicas} for all user keyspaces in
     * the given {@code datacenter}.
     *
     * @param datacenter the datacenter. If null, get the aggregated RF.
     * @return the {@link NetworkTopologyStrategy} with the minimum number of replicas
     */
    private static NetworkTopologyStrategy getReplicationStrategyWithMinRf(String datacenter)
    {
        // Obtain the minimum replication factor to determine the peers
        // that we require connecting to in the ring.
        Schema schema = Schema.instance;
        Set<String> userKeyspaces = new HashSet<>(schema.getNonSystemKeyspaces());

        if (userKeyspaces.isEmpty())
            return null;

        ToIntFunction<NetworkTopologyStrategy> allReplicas = rs -> {
            if (datacenter == null)
                return rs.getReplicationFactor().allReplicas;
            else
                return rs.getReplicationFactor(datacenter).allReplicas;
        };

        return schema.snapshot()
                     .stream()
                     .filter(ksMeta -> userKeyspaces.contains(ksMeta.name)
                                       // Only applies to NetworkTopologyStrategy
                                       && ksMeta.params.replication.klass.equals(NetworkTopologyStrategy.class))
                     .map(ksMeta -> (NetworkTopologyStrategy) ksMeta.createReplicationStrategy())
                     .filter(rs -> allReplicas.applyAsInt(rs) != 0)
                     .min(Comparator.comparingInt(allReplicas))
                     .orElse(null); // null is still possible when no user keyspace is replicated in the datacenter
    }

    /**
     * A class that holds DC information required for the checker
     */
    private static class ReplicationInfo
    {
        NetworkTopologyStrategy replicationStrategy;
        int quorum;
        int maxFailurePerReplicaSet;
    }
}
