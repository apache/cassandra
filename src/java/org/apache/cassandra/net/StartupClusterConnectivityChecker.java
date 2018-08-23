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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MessagingService.Verb.PING;
import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

public class StartupClusterConnectivityChecker
{
    private static final Logger logger = LoggerFactory.getLogger(StartupClusterConnectivityChecker.class);

    private final boolean blockForRemoteDcs;
    private final long timeoutNanos;

    public static StartupClusterConnectivityChecker create(long timeoutSecs, boolean blockForRemoteDcs)
    {
        if (timeoutSecs > 100)
            logger.warn("setting the block-for-peers timeout (in seconds) to {} might be a bit excessive, but using it nonetheless", timeoutSecs);
        long timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSecs);

        return new StartupClusterConnectivityChecker(timeoutNanos, blockForRemoteDcs);
    }

    @VisibleForTesting
    StartupClusterConnectivityChecker(long timeoutNanos, boolean blockForRemoteDcs)
    {
        this.blockForRemoteDcs = blockForRemoteDcs;
        this.timeoutNanos = timeoutNanos;
    }

    /**
     * @param peers The currently known peers in the cluster; argument is not modified.
     * @param getDatacenterSource A function for mapping peers to their datacenter.
     * @return true if the requested percentage of peers are marked ALIVE in gossip and have their connections opened;
     * else false.
     */
    public boolean execute(Set<InetAddressAndPort> peers, Function<InetAddressAndPort, String> getDatacenterSource)
    {
        if (peers == null || this.timeoutNanos < 0)
            return true;

        // make a copy of the set, to avoid mucking with the input (in case it's a sensitive collection)
        peers = new HashSet<>(peers);
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        String localDc = getDatacenterSource.apply(localAddress);

        peers.remove(localAddress);
        if (peers.isEmpty())
            return true;

        // make a copy of the datacenter mapping (in case gossip updates happen during this method or some such)
        Map<InetAddressAndPort, String> peerToDatacenter = new HashMap<>();
        SetMultimap<String, InetAddressAndPort> datacenterToPeers = HashMultimap.create();

        for (InetAddressAndPort peer : peers)
        {
            String datacenter = getDatacenterSource.apply(peer);
            peerToDatacenter.put(peer, datacenter);
            datacenterToPeers.put(datacenter, peer);
        }

        // In the case where we do not want to block startup on remote datacenters (e.g. because clients only use
        // LOCAL_X consistency levels), we remove all other datacenter hosts from the mapping and we only wait
        // on the remaining local datacenter.
        if (!blockForRemoteDcs)
        {
            datacenterToPeers.keySet().retainAll(Collections.singleton(localDc));
            logger.info("Blocking coordination until only a single peer is DOWN in the local datacenter, timeout={}s",
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        else
        {
            logger.info("Blocking coordination until only a single peer is DOWN in each datacenter, timeout={}s",
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }

        AckMap acks = new AckMap(3);
        Map<String, CountDownLatch> dcToRemainingPeers = new HashMap<>(datacenterToPeers.size());
        for (String datacenter: datacenterToPeers.keys())
        {
            dcToRemainingPeers.put(datacenter,
                                   new CountDownLatch(Math.max(datacenterToPeers.get(datacenter).size() - 1, 0)));
        }

        long startNanos = System.nanoTime();

        // set up a listener to react to new nodes becoming alive (in gossip), and account for all the nodes that are already alive
        Set<InetAddressAndPort> alivePeers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AliveListener listener = new AliveListener(alivePeers, dcToRemainingPeers, acks, peerToDatacenter::get);
        Gossiper.instance.register(listener);

        // send out a ping message to open up the non-gossip connections to all peers. Note that this sends the
        // ping messages to _all_ peers, not just the ones we block for in dcToRemainingPeers.
        sendPingMessages(peers, dcToRemainingPeers, acks, peerToDatacenter::get);

        for (InetAddressAndPort peer : peers)
        {
            if (Gossiper.instance.isAlive(peer) && alivePeers.add(peer) && acks.incrementAndCheck(peer))
            {
                String datacenter = peerToDatacenter.get(peer);
                // We have to check because we might only have the local DC in the map
                if (dcToRemainingPeers.containsKey(datacenter))
                    dcToRemainingPeers.get(datacenter).countDown();
            }
        }

        boolean succeeded = true;
        for (String datacenter: dcToRemainingPeers.keySet())
        {
            long remainingNanos = Math.max(1, timeoutNanos - (System.nanoTime() - startNanos));
            succeeded &= Uninterruptibles.awaitUninterruptibly(dcToRemainingPeers.get(datacenter),
                                                               remainingNanos, TimeUnit.NANOSECONDS);
        }

        Gossiper.instance.unregister(listener);

        Map<String, Long> numDown = dcToRemainingPeers.entrySet().stream()
                                                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                e -> e.getValue().getCount()));

        if (succeeded)
        {
            logger.info("Ensured sufficient healthy connections with {} after {} milliseconds",
                        numDown.keySet(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
        else
        {
            logger.warn("Timed out after {} milliseconds, was waiting for remaining peers to connect: {}",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos), numDown);
        }

        return succeeded;
    }

    /**
     * Sends a "connection warmup" message to each peer in the collection, on every {@link ConnectionType}
     * used for internode messaging (that is not gossip).
     */
    private void sendPingMessages(Set<InetAddressAndPort> peers, Map<String, CountDownLatch> dcToRemainingPeers,
                                  AckMap acks, Function<InetAddressAndPort, String> getDatacenter)
    {
        IAsyncCallback responseHandler = new IAsyncCallback()
        {
            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void response(MessageIn msg)
            {
                if (acks.incrementAndCheck(msg.from))
                {
                    String datacenter = getDatacenter.apply(msg.from);
                    // We have to check because we might only have the local DC in the map
                    if (dcToRemainingPeers.containsKey(datacenter))
                        dcToRemainingPeers.get(datacenter).countDown();
                }
            }
        };

        MessageOut<PingMessage> smallChannelMessageOut = new MessageOut<>(PING, PingMessage.smallChannelMessage,
                                                                          PingMessage.serializer, SMALL_MESSAGE);
        MessageOut<PingMessage> largeChannelMessageOut = new MessageOut<>(PING, PingMessage.largeChannelMessage,
                                                                          PingMessage.serializer, LARGE_MESSAGE);
        for (InetAddressAndPort peer : peers)
        {
            MessagingService.instance().sendRR(smallChannelMessageOut, peer, responseHandler);
            MessagingService.instance().sendRR(largeChannelMessageOut, peer, responseHandler);
        }
    }

    /**
     * A trivial implementation of {@link IEndpointStateChangeSubscriber} that really only cares about
     * {@link #onAlive(InetAddressAndPort, EndpointState)} invocations.
     */
    private static final class AliveListener implements IEndpointStateChangeSubscriber
    {
        private final Map<String, CountDownLatch> dcToRemainingPeers;
        private final Set<InetAddressAndPort> livePeers;
        private final Function<InetAddressAndPort, String> getDatacenter;
        private final AckMap acks;

        AliveListener(Set<InetAddressAndPort> livePeers, Map<String, CountDownLatch> dcToRemainingPeers,
                      AckMap acks, Function<InetAddressAndPort, String> getDatacenter)
        {
            this.livePeers = livePeers;
            this.dcToRemainingPeers = dcToRemainingPeers;
            this.acks = acks;
            this.getDatacenter = getDatacenter;
        }

        public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
        {
        }

        public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
        }

        public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
        {
        }

        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
            if (livePeers.add(endpoint) && acks.incrementAndCheck(endpoint))
            {
                String datacenter = getDatacenter.apply(endpoint);
                if (dcToRemainingPeers.containsKey(datacenter))
                    dcToRemainingPeers.get(datacenter).countDown();
            }
        }

        public void onDead(InetAddressAndPort endpoint, EndpointState state)
        {
        }

        public void onRemove(InetAddressAndPort endpoint)
        {
        }

        public void onRestart(InetAddressAndPort endpoint, EndpointState state)
        {
        }
    }

    private static final class AckMap
    {
        private final int threshold;
        private final Map<InetAddressAndPort, AtomicInteger> acks;

        AckMap(int threshold)
        {
            this.threshold = threshold;
            acks = new ConcurrentHashMap<>();
        }

        boolean incrementAndCheck(InetAddressAndPort address)
        {
            return acks.computeIfAbsent(address, addr -> new AtomicInteger(0)).incrementAndGet() == threshold;
        }
    }
}
