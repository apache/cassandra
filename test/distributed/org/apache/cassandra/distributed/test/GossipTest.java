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

package org.apache.cassandra.distributed.test;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class GossipTest extends DistributedTestBase
{

    @Test
    public void nodeDownDuringMove() throws Throwable
    {
        int liveCount = 1;
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        try (Cluster cluster = Cluster.build(2 + liveCount)
                                      .withConfig(config -> config.with(NETWORK).with(GOSSIP))
                                      .createWithoutStarting())
        {
            int fail = liveCount + 1;
            int late = fail + 1;
            for (int i = 1 ; i <= liveCount ; ++i)
                cluster.get(i).startup();
            cluster.get(fail).startup();
            Collection<String> expectTokens = cluster.get(fail).callsOnInstance(() ->
                StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddress())
                                       .stream().map(Object::toString).collect(Collectors.toList())
            ).call();

            InetAddress failAddress = cluster.get(fail).broadcastAddressAndPort().address;
            // wait for NORMAL state
            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetAddress endpoint) -> {
                    EndpointState ep;
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || ep.getApplicationState(ApplicationState.STATUS) == null
                           || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("NORMAL"))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                }).accept(failAddress);
            }

            // set ourselves to MOVING, and wait for it to propagate
            cluster.get(fail).runOnInstance(() -> {

                Token token = Iterables.getFirst(StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddress()), null);
                Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.moving(token));
            });

            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetAddress endpoint) -> {
                    EndpointState ep;
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || (ep.getApplicationState(ApplicationState.STATUS) == null
                               || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("MOVING")))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                }).accept(failAddress);
            }

            cluster.get(fail).shutdown(false).get();
            cluster.get(late).startup();
            cluster.get(late).acceptsOnInstance((InetAddress endpoint) -> {
                EndpointState ep;
                while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                       || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("MOVING"))
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }).accept(failAddress);

            Collection<String> tokens = cluster.get(late).appliesOnInstance((InetAddress endpoint) ->
                StorageService.instance.getTokenMetadata().getTokens(failAddress)
                                       .stream().map(Object::toString).collect(Collectors.toList())
            ).apply(failAddress);

            Assert.assertEquals(expectTokens, tokens);
        }
    }
    
}
