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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MessagingService.Verb.PING;

public class StartupClusterConnectivityChecker
{
    private static final Logger logger = LoggerFactory.getLogger(StartupClusterConnectivityChecker.class);

    enum State { CONTINUE, FINISH_SUCCESS, FINISH_TIMEOUT }

    private final int targetPercent;
    private final int timeoutSecs;
    private final Predicate<InetAddressAndPort> gossipStatus;

    public StartupClusterConnectivityChecker(int targetPercent, int timeoutSecs, Predicate<InetAddressAndPort> gossipStatus)
    {
        if (targetPercent < 0)
        {
            targetPercent = 0;
        }
        else if (targetPercent > 100)
        {
            targetPercent = 100;
        }
        this.targetPercent = targetPercent;

        if (timeoutSecs < 0)
        {
            timeoutSecs = 1;
        }
        else if (timeoutSecs > 100)
        {
            logger.warn("setting the block-for-peers timeout (in seconds) to {} might be a bit excessive, but using it nonetheless", timeoutSecs);
        }
        this.timeoutSecs = timeoutSecs;

        this.gossipStatus = gossipStatus;
    }

    public void execute(Set<InetAddressAndPort> peers)
    {
        if (peers == null || targetPercent == 0)
            return;

        // remove current node from the set
        peers = peers.stream()
                     .filter(peer -> !peer.equals(FBUtilities.getBroadcastAddressAndPort()))
                     .collect(Collectors.toSet());

        // don't block if there's no other nodes in the cluster (or we don't know about them)
        if (peers.size() <= 0)
            return;

        logger.info("choosing to block until {}% of peers are marked alive and connections are established; max time to wait = {} seconds",
                    targetPercent, timeoutSecs);

        // first, send out a ping message to open up the non-gossip connections
        final AtomicInteger connectedCount = sendPingMessages(peers);

        final long startNanos = System.nanoTime();
        final long expirationNanos = startNanos + TimeUnit.SECONDS.toNanos(timeoutSecs);
        int completedRounds = 0;
        while (checkStatus(peers, connectedCount, startNanos, expirationNanos < System.nanoTime(), completedRounds) == State.CONTINUE)
        {
            completedRounds++;
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MICROSECONDS);
        }
    }

    State checkStatus(Set<InetAddressAndPort> peers, AtomicInteger connectedCount, final long startNanos, boolean beyondExpiration, final int completedRounds)
    {
        long currentAlive = peers.stream().filter(gossipStatus).count();
        float currentAlivePercent = ((float) currentAlive / (float) peers.size()) * 100;

        // assume two connections to remote host that we care to track here (small msg & large msg)
        final int totalConnectionsSize = peers.size() * 2;
        final int connectionsCount = connectedCount.get();
        float currentConnectedPercent = ((float) connectionsCount / (float) totalConnectionsSize) * 100;

        if (currentAlivePercent >= targetPercent && currentConnectedPercent >= targetPercent)
        {
            logger.info("after {} milliseconds, found {}% ({} / {}) of peers as marked alive, " +
                        "and {}% ({} / {}) of peers as connected, " +
                        "both of which are above the desired threshold of {}%",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                        currentAlivePercent, currentAlive, peers.size(),
                        currentConnectedPercent, connectionsCount, totalConnectionsSize,
                        targetPercent);
            return State.FINISH_SUCCESS;
        }

        // perform at least two rounds of checking, else this is kinda useless (and the operator set the aliveTimeoutSecs too low)
        if (completedRounds >= 2 && beyondExpiration)
        {
            logger.info("after {} milliseconds, found {}% ({} / {}) of peers as marked alive, " +
                        "and {}% ({} / {}) of peers as connected, " +
                        "one or both of which is below the desired threshold of {}%",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                        currentAlivePercent, currentAlive, peers.size(),
                        currentConnectedPercent, connectionsCount, totalConnectionsSize,
                        targetPercent);
            return State.FINISH_TIMEOUT;
        }
        return State.CONTINUE;
    }

    /**
     * Sends a "connection warmup" message to each peer in the collection, on every {@link OutboundConnectionIdentifier.ConnectionType}
     * used for internode messaging.
     */
    private AtomicInteger sendPingMessages(Set<InetAddressAndPort> peers)
    {
        AtomicInteger connectedCount = new AtomicInteger(0);
        IAsyncCallback responseHandler = new IAsyncCallback()
        {
            @Override
            public boolean isLatencyForSnitch()
            {
                return false;
            }

            @Override
            public void response(MessageIn msg)
            {
                connectedCount.incrementAndGet();
            }
        };

        MessageOut<PingMessage> smallChannelMessageOut = new MessageOut<>(PING, PingMessage.smallChannelMessage, PingMessage.serializer);
        MessageOut<PingMessage> largeChannelMessageOut = new MessageOut<>(PING, PingMessage.largeChannelMessage, PingMessage.serializer);
        for (InetAddressAndPort peer : peers)
        {
            MessagingService.instance().sendRR(smallChannelMessageOut, peer, responseHandler);
            MessagingService.instance().sendRR(largeChannelMessageOut, peer, responseHandler);
        }

        return connectedCount;
    }
}
