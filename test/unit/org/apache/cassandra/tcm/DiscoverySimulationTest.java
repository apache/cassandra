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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.utils.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiscoverySimulationTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setup()
    {
        ClusterMetadata initial = new ClusterMetadata(Murmur3Partitioner.instance);
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withInitialState(initial)
                               .createLog();

        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                MetadataSnapshots.NO_OP,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                false);
        ClusterMetadataService.setInstance(cms);
        log.readyUnchecked();
    }

    @Test
    public void discoveryTest() throws Throwable
    {
        Map<InetAddressAndPort, Discovery> nodes = new HashMap<>();
        Map<InetAddressAndPort, FakeMessageDelivery> cluster = new HashMap<>();

        Set<InetAddressAndPort> seeds = new HashSet<>();
        seeds.add(InetAddressAndPort.getByName("127.0.100.1"));
        seeds.add(InetAddressAndPort.getByName("127.0.100.100")); // add an unreachable node
        // Thread per-node to try and avoid nodes which start first finishing
        // before later nodes have made first contact with the seed.
        Executor executor = Executors.newFixedThreadPool(10);
        for (int i = 1; i <= 10; i++)
        {
            InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.100." + i);
            FakeMessageDelivery messaging = new FakeMessageDelivery(cluster, addr);
            cluster.put(addr, messaging);
            Discovery discovery = new Discovery(() -> messaging, () -> Collections.unmodifiableSet(seeds), addr);
            nodes.put(addr, discovery);
            messaging.handlers.put(Verb.TCM_DISCOVER_REQ, discovery.requestHandler);
        }

        Map<InetAddressAndPort, CompletableFuture<Discovery.DiscoveredNodes>> futures = new HashMap<>();
        nodes.forEach((addr, discovery) -> {
            futures.put(addr, CompletableFuture.supplyAsync(() -> discovery.discover(5), executor));
        });

        Map<InetAddressAndPort, Discovery.DiscoveredNodes> discovered = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, CompletableFuture<Discovery.DiscoveredNodes>> future : futures.entrySet())
            discovered.put(future.getKey(), future.getValue().get());

        // It's possible that some node(s) in the cluster were completely unable to contact any seed. Therefore, these
        // nodes are undiscoverable by the rest of the cluster so we exclude them from the expected results. We should
        // also expect those partitioned nodes to have failed to discover any of their peers.
        Set<InetAddressAndPort> connected = new HashSet<>();
        cluster.forEach((addr, messaging) -> {
            if (messaging.hasSentAtLeastOneRequest)
                connected.add(addr);
        });

        for (Map.Entry<InetAddressAndPort, Discovery.DiscoveredNodes> result : discovered.entrySet())
        {
            InetAddressAndPort node = result.getKey();
            Set<InetAddressAndPort> peersDiscovered = result.getValue().nodes();
            if (connected.contains(node))
                assertEquals(node + " was able to contact seed, but didn't discover expected peers", connected, peersDiscovered);
            else
                assertTrue(node + " was unable to contact seed, but discovered " + peersDiscovered, peersDiscovered.isEmpty());
        }
    }

    /**
     * A simple fake message delivery mechanism: holds all other nodes in a map, delivers messages to
     * registered callbacks via this map, and times out messages to unknown nodes.
     *
     * Responses are delivered by checking if the verb is a response and finishing a callback.
     */
    public static class FakeMessageDelivery implements MessageDelivery
    {
        private final AtomicInteger counter = new AtomicInteger();

        private final Map<InetAddressAndPort, FakeMessageDelivery> cluster;
        private final Map<Long, RequestCallback<?>> callbacks;
        private final Map<Verb, IVerbHandler<?>> handlers;
        private final InetAddressAndPort addr;
        // If we're unlucky, every attempt by this node to contact the single live peer may
        // be randomly chosen to fail. If this happens, no other node is able to discover
        // that this one exists and so it should be excluded from the expected set when
        // checking results.
        public boolean hasSentAtLeastOneRequest = false;

        public FakeMessageDelivery(Map<InetAddressAndPort, FakeMessageDelivery> cluster,
                                   InetAddressAndPort addr)
        {
            this.cluster = cluster;
            this.addr = addr;
            this.callbacks = new ConcurrentHashMap<>();
            this.handlers = new HashMap<>();
        }

        public <RSP> void deliverResponse(Message<RSP> msg)
        {
            RequestCallback<RSP> callback = (RequestCallback<RSP>) callbacks.remove(msg.id());
            callback.onResponse(msg);
        }

        public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
        {
            if (message.verb().isResponse())
            {
                logger.info("{} sending response to {}", addr, to);
                cluster.get(to).deliverResponse(Message.forgeIdentityForTests(message, addr));
                return;
            }

            throw new NotImplementedException();
        }

        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
        {
            try
            {
                message = Message.forgeIdentityForTests(message, addr);
                FakeMessageDelivery node = cluster.get(to);
                if (node != null &&
                    // Inject some failures
                    counter.incrementAndGet() % 5 != 0)
                {
                    hasSentAtLeastOneRequest = true;
                    logger.info("{} sending request to {}", addr, to);
                    callbacks.put(message.id(), cb);
                    IVerbHandler<REQ> handler = (IVerbHandler<REQ>) node.handlers.get(message.verb());
                    handler.doVerb(message);
                }
                else
                {
                    logger.info("{} simulating failure sending request to {}", addr, to);
                    cb.onFailure(to, RequestFailureReason.TIMEOUT);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
        {
            throw new NotImplementedException();
        }

        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
        {
            throw new NotImplementedException();
        }

        public <V> void respond(V response, Message<?> message)
        {
            throw new NotImplementedException();
        }
    }
}