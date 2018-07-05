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

package org.apache.cassandra.service;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;
import static org.apache.cassandra.service.StorageServiceTest.assertMultimapEqualsIgnoreOrder;

/**
 * This is also fairly effectively testing source retrieval for bootstrap as well since RangeStreamer
 * is used to calculate the endpoints to fetch from and check they are alive for both RangeRelocator (move) and
 * bootstrap (RangeRelocator).
 */
public class BootstrapTransientTest
{
    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
    }

    private final List<InetAddressAndPort> downNodes = new ArrayList<>();
    Predicate<Replica> alivePredicate = replica -> !downNodes.contains(replica.endpoint());

    private final List<InetAddressAndPort> sourceFilterDownNodes = new ArrayList<>();
    private final Collection<Predicate<Replica>> sourceFilters = Collections.singleton(replica -> !sourceFilterDownNodes.contains(replica.endpoint()));

    @After
    public void clearDownNode()
    {
        downNodes.clear();
        sourceFilterDownNodes.clear();
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    Token tenToken = new OrderPreservingPartitioner.StringToken("00010");
    Token twentyToken = new OrderPreservingPartitioner.StringToken("00020");
    Token thirtyToken = new OrderPreservingPartitioner.StringToken("00030");
    Token fourtyToken = new OrderPreservingPartitioner.StringToken("00040");

    Range<Token> aRange = new Range<>(thirtyToken, tenToken);
    Range<Token> bRange = new Range<>(tenToken, twentyToken);
    Range<Token> cRange = new Range<>(twentyToken, thirtyToken);
    Range<Token> dRange = new Range<>(thirtyToken, fourtyToken);

    RangesAtEndpoint toFetch = RangesAtEndpoint.of(new Replica(dAddress, dRange, true),
                                                   new Replica(dAddress, cRange, true),
                                                   new Replica(dAddress, bRange, false));

    @Test
    public void testRangeStreamerRangesToFetch() throws Exception
    {
        EndpointsByReplica expectedResult = new EndpointsByReplica(ImmutableMap.of(
        fullReplica(dAddress, dRange), EndpointsForRange.builder(aRange).add(fullReplica(bAddress, aRange)).add(transientReplica(cAddress, aRange)).build(),
        fullReplica(dAddress, cRange), EndpointsForRange.builder(cRange).add(fullReplica(cAddress, cRange)).add(transientReplica(bAddress, cRange)).build(),
        transientReplica(dAddress, bRange), EndpointsForRange.builder(bRange).add(transientReplica(aAddress, bRange)).build()));

        invokeCalculateRangesToFetchWithPreferredEndpoints(toFetch, constructTMDs(), expectedResult);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDs()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(aRange.right, aAddress);
        tmd.updateNormalToken(bRange.right, bAddress);
        tmd.updateNormalToken(cRange.right, cAddress);
        TokenMetadata updated = tmd.cloneOnlyTokenMap();
        updated.updateNormalToken(dRange.right, dAddress);

        return Pair.create(tmd, updated);
    }

    private void invokeCalculateRangesToFetchWithPreferredEndpoints(ReplicaCollection<?> toFetch,
                                                                    Pair<TokenMetadata, TokenMetadata> tmds,
                                                                    EndpointsByReplica expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        EndpointsByReplica result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> replicas,
                                                                                                                   simpleStrategy(tmds.left),
                                                                                                                   toFetch,
                                                                                                                   true,
                                                                                                                   tmds.left,
                                                                                                                   tmds.right,
                                                                                                                   alivePredicate,
                                                                                                                   "OldNetworkTopologyStrategyTest",
                                                                                                                   sourceFilters);
        result.asMap().forEach((replica, list) -> System.out.printf("Replica %s, sources %s%n", replica, list));
        assertMultimapEqualsIgnoreOrder(expectedResult, result);

    }

    private AbstractReplicationStrategy simpleStrategy(TokenMetadata tmd)
    {
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return "R1";
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        };

        return new SimpleStrategy("MoveTransientTest",
                                  tmd,
                                  snitch,
                                  com.google.common.collect.ImmutableMap.of("replication_factor", "3/1"));
    }

}
