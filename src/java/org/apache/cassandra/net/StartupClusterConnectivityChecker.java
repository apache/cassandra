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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
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

    private final int targetPercent;
    private final long timeoutNanos;

    public static StartupClusterConnectivityChecker create(int targetPercent, int timeoutSecs)
    {
        timeoutSecs = Math.max(1, timeoutSecs);
        if (timeoutSecs > 100)
            logger.warn("setting the block-for-peers timeout (in seconds) to {} might be a bit excessive, but using it nonetheless", timeoutSecs);
        long timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSecs);

        return new StartupClusterConnectivityChecker(targetPercent, timeoutNanos);
    }

    @VisibleForTesting
    StartupClusterConnectivityChecker(int targetPercent, long timeoutNanos)
    {
        this.targetPercent = Math.min(100, Math.max(0, targetPercent));
        this.timeoutNanos = timeoutNanos;
    }

    /**
     * @param peers The currently known peers in the cluster; argument is not modified.
     * @return true if the requested percentage of peers are marked ALIVE in gossip and have their connections opened;
     * else false.
     */
    public boolean execute(Set<InetAddressAndPort> peers)
    {
        if (targetPercent == 0 || peers == null)
            return true;

        // make a copy of the set, to avoid mucking with the input (in case it's a sensitive collection)
        peers = new HashSet<>(peers);
        peers.remove(FBUtilities.getBroadcastAddressAndPort());

        if (peers.isEmpty())
            return true;

        logger.info("choosing to block until {}% of the {} known peers are marked alive and connections are established; max time to wait = {} seconds",
                    targetPercent, peers.size(), TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));

        long startNanos = System.nanoTime();

        AckMap acks = new AckMap(3);
        int target = (int) ((targetPercent / 100.0) * peers.size());
        CountDownLatch latch = new CountDownLatch(target);

        // set up a listener to react to new nodes becoming alive (in gossip), and account for all the nodes that are already alive
        Set<InetAddressAndPort> alivePeers = Sets.newSetFromMap(new ConcurrentHashMap<>());
        AliveListener listener = new AliveListener(alivePeers, latch, acks);
        Gossiper.instance.register(listener);

        // send out a ping message to open up the non-gossip connections
        sendPingMessages(peers, latch, acks);

        for (InetAddressAndPort peer : peers)
            if (Gossiper.instance.isAlive(peer) && alivePeers.add(peer) && acks.incrementAndCheck(peer))
                latch.countDown();

        boolean succeeded = Uninterruptibles.awaitUninterruptibly(latch, timeoutNanos, TimeUnit.NANOSECONDS);
        Gossiper.instance.unregister(listener);

        int connected = peers.size() - (int) latch.getCount();
        logger.info("After waiting/processing for {} milliseconds, {} out of {} peers ({}%) have been marked alive and had connections established",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                    connected,
                    peers.size(),
                    connected / (peers.size()) * 100.0);
        return succeeded;
    }

    /**
     * Sends a "connection warmup" message to each peer in the collection, on every {@link ConnectionType}
     * used for internode messaging (that is not gossip).
     */
    private void sendPingMessages(Set<InetAddressAndPort> peers, CountDownLatch latch, AckMap acks)
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
                    latch.countDown();
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
        private final CountDownLatch latch;
        private final Set<InetAddressAndPort> livePeers;
        private final AckMap acks;

        AliveListener(Set<InetAddressAndPort> livePeers, CountDownLatch latch, AckMap acks)
        {
            this.latch = latch;
            this.livePeers = livePeers;
            this.acks = acks;
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
                latch.countDown();
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
