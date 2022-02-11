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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.Verb.PING_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractStartupConnectivityChecker implements StartupConnectivityChecker
{
    private static final int DEFAULT_ACK_THRESHOLD = 3;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final long timeoutNanos;
    protected final InetAddressAndPort localAddress;

    private AliveListener gossipAliveListener;
    private AckTracker ackTracker;
    private long startTimeNanos;
    private Function<InetAddressAndPort, String> datacenterSource;

    protected AbstractStartupConnectivityChecker(long timeout, TimeUnit unit)
    {
        this.timeoutNanos = unit.toNanos(timeout);
        this.localAddress = FBUtilities.getBroadcastAddressAndPort();
    }

    protected abstract Map<String, CountDownLatch> calculatePeersToBlockPerDc(Multimap<String, InetAddressAndPort> peersByDatacenter);

    /**
     * The actual implementation of checking connections with peers
     * @return true if the requested percentage of peers are marked ALIVE in gossip and have their connections established;
     *         otherwise, return false.
     */
    protected abstract boolean executeInternal(Multimap<String, InetAddressAndPort> peersByDatacenter);

    /**
     * Checkers generally do those steps:
     * 1. Calculate the peers to block per datacenter, i.e. determine the threshold
     * 2. Register gossiper listener on node alive and send ping messages
     * 3. Count acks until reaches the threshold or timeout.
     * 4. Log the result and unregister gossiper listener
     *
     * @param nodes               The currently known nodes in the cluster, including self.
     * @param datacenterSource    A function for mapping peers to their datacenter.
     * @return true if the requested percentage of peers are marked ALIVE in gossip and have their connections opened;
     * else false.
     */
    @Override
    public boolean execute(ImmutableSet<InetAddressAndPort> nodes, Function<InetAddressAndPort, String> datacenterSource)
    {
        if (canSkipCheck(nodes, timeoutNanos))
            return true;

        // Create a nodeToDatacenter BiMultiValMap for 2 purposes
        // 1. Its inverse is the view of nodes by datacenters
        // 2. Cache the datacenter source is a local map in order to avoid calling datacenterSource, which is backed by snitch
        BiMultiValMap<InetAddressAndPort, String> nodeToDatacenterMap = new BiMultiValMap<>();
        nodes.forEach(node -> nodeToDatacenterMap.put(node, datacenterSource.apply(node)));

        this.datacenterSource = nodeToDatacenterMap::get;

        Multimap<String, InetAddressAndPort> peersByDatacenter = filterNodes(nodeToDatacenterMap.inverse());

        ackTracker = new AckTracker(getAckThreshold(), peersByDatacenter.values());

        Map<String, CountDownLatch> peersToBlockPerDc = calculatePeersToBlockPerDc(peersByDatacenter);

        if (peersToBlockPerDc.isEmpty())
            return true; // nothing to check so we return early

        // Start checking via gossip and internode message connections
        try
        {
            startTimeNanos = nanoTime();
            // Check liveness of nodes becoming alive (in gossip), and account for all the nodes that are already alive
            checkLivenessWithGossip(ackTracker, peersToBlockPerDc);

            boolean succeeded = executeInternal(peersByDatacenter);

            if (succeeded)
            {
                logger.info("Ensured sufficient healthy connections with {} after {} milliseconds",
                            peersByDatacenter.keySet(), TimeUnit.NANOSECONDS.toMillis(getElaspedTimeNanos()));
            }
            else
            {
                logMissingPeers(datacenterSource, getAckTracker().getMissingPeers());
            }
            return succeeded;
        }
        finally
        {
            if (gossipAliveListener != null)
                Gossiper.instance.unregister(gossipAliveListener);
        }
    }

    /**
     * Filter the nodes to exclude the ones that are not needed in the checker.
     *
     * @param nodesByDatacenter groups of peers by their datacenters
     * @return a trimmed copy of the nodesByDatacenter multimap.
     *         The self node is removed, so it is effectively peers by datacenter.
     */
    protected Multimap<String, InetAddressAndPort> filterNodes(Multimap<String, InetAddressAndPort> nodesByDatacenter)
    {
        nodesByDatacenter = HashMultimap.create(nodesByDatacenter);
        String localDc = getDatacenterSource().apply(localAddress);
        nodesByDatacenter.remove(localDc, localAddress);
        return nodesByDatacenter;
    }

    protected AckTracker getAckTracker()
    {
        return ackTracker;
    }

    protected long getElaspedTimeNanos()
    {
        return nanoTime() - startTimeNanos;
    }

    protected long getRemainingTimeNanos()
    {
        return Math.max(0, this.timeoutNanos - getElaspedTimeNanos());
    }

    /**
     * The threshold is 3 because for each peer we want to have 3 acks,
     * one for small message connection, one for large message connnection and one for alive event from gossip.
     * @return 3, the default, see {@link AbstractStartupConnectivityChecker#DEFAULT_ACK_THRESHOLD}
     */
    protected int getAckThreshold()
    {
        return DEFAULT_ACK_THRESHOLD;
    }

    protected Function<InetAddressAndPort, String> getDatacenterSource()
    {
        return datacenterSource;
    }

    /**
     * Returns a map of datacenters to {@link InetAddressAndPort} grouped by datacenter. When {@code excludeRemoteDcs}
     * is true, only return the group for the provided {@code localDc}.
     *
     * @param peers            a set of peers to map to a datacenter
     * @param peerToDatacenter a map of peers to datacenter
     * @param localDc          the name of the local datacenter
     * @param excludeRemoteDcs true to exclude remote datacenters, false includes all data centers
     * @return a map of datacenters to {@link InetAddressAndPort} grouped by datacenter
     */
    protected Map<String, Set<InetAddressAndPort>> mapDatacenterToPeers(Set<InetAddressAndPort> peers,
                                                                        Map<InetAddressAndPort, String> peerToDatacenter,
                                                                        String localDc, boolean excludeRemoteDcs)
    {
        Stream<Map.Entry<InetAddressAndPort, String>> stream = peerToDatacenter.entrySet().stream()
                                                                               .filter(entry -> peers.contains(entry.getKey()));
        // In the case where we do not want to block startup on remote datacenters (e.g. because clients only use
        // LOCAL_X consistency levels), we remove all other datacenter hosts from the mapping, and we only wait
        // on the remaining local datacenter.
        if (excludeRemoteDcs)
        {
            stream = stream.filter(entry -> entry.getValue().equals(localDc));
            logger.info("Blocking coordination in the local datacenter, timeout={}s", TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        else
        {
            logger.info("Blocking coordination in each datacenter, timeout={}s", TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }

        // create a mapping of datacenters to a set of peers
        return stream.collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toSet())));
    }

    protected void logMissingPeers(Function<InetAddressAndPort, String> datacenterSource,
                                   Collection<InetAddressAndPort> missingPeers)
    {
        // dc -> missing peer host addresses
        Map<String, List<String>> peersDown = missingPeers.stream()
                                                          .collect(groupingBy(peer -> StringUtils.defaultString(datacenterSource.apply(peer), "unknown"),
                                                                              mapping(InetAddressAndPort::getHostAddressAndPort,
                                                                                      toList())));
        logger.warn("Timed out after {} milliseconds, was waiting for the remaining peers to connect: {}",
                    TimeUnit.NANOSECONDS.toMillis(getElaspedTimeNanos()),
                    peersDown);
    }

    private void checkLivenessWithGossip(AckTracker ackTracker, Map<String, CountDownLatch> peersToBlockPerDc)
    {
        // set up a listener to react to new nodes becoming alive (in gossip), and account for all the nodes that are already alive
        Set<InetAddressAndPort> alivePeers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        gossipAliveListener = new AliveListener(alivePeers, peersToBlockPerDc, ackTracker, getDatacenterSource());
        Gossiper.instance.register(gossipAliveListener);

        // mark the peers that are already known alive to the local Gossiper
        for (InetAddressAndPort peer : ackTracker.getPeers())
        {
            if (Gossiper.instance.isAlive(peer) && alivePeers.add(peer))
                acksIncrementAndCheck(ackTracker, peersToBlockPerDc, peer, getDatacenterSource());
        }
    }

    /**
     * Returns {@code true} if the connectivity checks should be skipped, {@code false} otherwise.
     *
     * @param peers the list of peers in the cluster
     * @return {@code true} if the connectivity checks should be skipped, {@code false} otherwise
     */
    private boolean canSkipCheck(Set<InetAddressAndPort> peers, long checkTimeoutNanos)
    {
        // true if the peers set is null or if timeout nanos has been configured to a negative number
        if (peers == null || checkTimeoutNanos < 0)
            return true;

        // skip if the peers is empty or if it only contains this peer
        return peers.isEmpty() || (peers.size() == 1 && peers.contains(localAddress));
    }

    protected static boolean waitForNodesUntilTimeout(Collection<CountDownLatch> values, long timeoutNanos, long elapsedTimeNanos)
    {
        boolean succeeded = true;
        for (CountDownLatch countDownLatch : values)
        {
            long remainingNanos = Math.max(1, timeoutNanos - elapsedTimeNanos);
            //noinspection UnstableApiUsage
            succeeded &= countDownLatch.awaitUninterruptibly(remainingNanos, TimeUnit.NANOSECONDS);
        }
        return succeeded;
    }

    private static void acksIncrementAndCheck(AckTracker acks,
                                              Map<String, CountDownLatch> dcToRemainingPeers,
                                              InetAddressAndPort peer,
                                              Function<InetAddressAndPort, String> datacenterSource)
    {
        if (acks.incrementAndCheck(peer) && dcToRemainingPeers != null)
        {
            String datacenter = datacenterSource.apply(peer);
            // We have to check because we might only have the local DC in the map
            CountDownLatch remainingPeers = dcToRemainingPeers.get(datacenter);
            if (remainingPeers != null)
                remainingPeers.decrement();
        }
    }

    /**
     * Sends a "connection warmup" message to each peer in the collection, on
     * {@link ConnectionType#SMALL_MESSAGES small} and {@link ConnectionType#LARGE_MESSAGES large} connections used
     * for internode messaging (that is not gossip).
     */
    protected static void sendPingMessages(AckTracker ackTracker,
                                           Collection<InetAddressAndPort> peers,
                                           Function<InetAddressAndPort, String> datacenterSource,
                                           Map<String, CountDownLatch> dcToRemainingPeers)
    {
        RequestCallback<?> responseHandler = msg -> acksIncrementAndCheck(ackTracker, dcToRemainingPeers, msg.from(), datacenterSource);

        Message<PingRequest> small = Message.out(PING_REQ, PingRequest.forSmall);
        Message<PingRequest> large = Message.out(PING_REQ, PingRequest.forLarge);
        for (InetAddressAndPort peer : peers)
        {
            MessagingService.instance().sendWithCallback(small, peer, responseHandler, SMALL_MESSAGES);
            MessagingService.instance().sendWithCallback(large, peer, responseHandler, LARGE_MESSAGES);
        }
    }

    /**
     * A trivial implementation of {@link IEndpointStateChangeSubscriber} that really only cares about
     * {@link #onAlive(InetAddressAndPort, EndpointState)} invocations.
     */
    protected static final class AliveListener implements IEndpointStateChangeSubscriber
    {
        private final Map<String, CountDownLatch> dcToRemainingPeers;
        private final Set<InetAddressAndPort> livePeers;
        private final Function<InetAddressAndPort, String> datacenterSource;
        private final AckTracker acks;

        AliveListener(Set<InetAddressAndPort> livePeers, Map<String, CountDownLatch> dcToRemainingPeers,
                      AckTracker acks, Function<InetAddressAndPort, String> datacenterSource)
        {
            this.livePeers = livePeers;
            this.dcToRemainingPeers = dcToRemainingPeers;
            this.acks = acks;
            this.datacenterSource = datacenterSource;
        }

        @Override
        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
            if (livePeers.add(endpoint))
            {
                acksIncrementAndCheck(acks, dcToRemainingPeers, endpoint, datacenterSource);
            }
        }
    }

    /**
     * A tracker that tracks the number of acknowledges received from peers and checks if it has reached to the threshold.
     * It always excludes the self node, since it is surely connected.
     */
    protected static final class AckTracker
    {
        private final int threshold;
        private final Map<InetAddressAndPort, AtomicInteger> acks;
        private final InetAddressAndPort localAddress;

        AckTracker(int threshold, Iterable<InetAddressAndPort> initialNodes)
        {
            this.threshold = threshold;
            this.acks = new ConcurrentHashMap<>();
            this.localAddress = FBUtilities.getBroadcastAddressAndPort();
            for (InetAddressAndPort peer : initialNodes)
                initOrGetCounter(peer);
        }

        boolean incrementAndCheck(InetAddressAndPort address)
        {
            AtomicInteger counter = initOrGetCounter(address);
            if (counter == null)
                return false;
            return counter.incrementAndGet() == threshold;
        }

        /**
         * Get the peers that have not fully ack'd, i.e. not reaching threshold acks
         */
        Set<InetAddressAndPort> getMissingPeers()
        {
            Set<InetAddressAndPort> missingPeers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, AtomicInteger> entry : acks.entrySet())
            {
                if (entry.getValue().get() < threshold)
                    missingPeers.add(entry.getKey());
            }
            return missingPeers;
        }

        Collection<InetAddressAndPort> getPeers()
        {
            return new ArrayList<>(acks.keySet());
        }

        // init the counter for the peer just in case
        private AtomicInteger initOrGetCounter(InetAddressAndPort address)
        {
            if (localAddress.equals(address))
                return null;
            return acks.computeIfAbsent(address, addr -> new AtomicInteger(0));
        }
    }
}
