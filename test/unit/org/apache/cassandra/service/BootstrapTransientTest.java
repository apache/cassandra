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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
    static InetAddressAndPort address02;
    static InetAddressAndPort address03;
    static InetAddressAndPort address04;
    static InetAddressAndPort address05;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        address02 = InetAddressAndPort.getByName("127.0.0.2");
        address03 = InetAddressAndPort.getByName("127.0.0.3");
        address04 = InetAddressAndPort.getByName("127.0.0.4");
        address05 = InetAddressAndPort.getByName("127.0.0.5");
    }

    private final List<InetAddressAndPort> downNodes = new ArrayList<>();

    final RangeStreamer.SourceFilter alivePredicate = new RangeStreamer.SourceFilter()
    {
        public boolean apply(Replica replica)
        {
            return !downNodes.contains(replica.endpoint());
        }

        public String message(Replica replica)
        {
            return "Down nodes: " + downNodes;
        }
    };

    final RangeStreamer.SourceFilter sourceFilterDownNodesPredicate = new RangeStreamer.SourceFilter()
    {
        public boolean apply(Replica replica)
        {
            return !sourceFilterDownNodes.contains(replica.endpoint());
        }

        public String message(Replica replica)
        {
            return "Source filter down nodes" + sourceFilterDownNodes;
        }
    };

    private final List<InetAddressAndPort> sourceFilterDownNodes = new ArrayList<>();

    private final Collection<RangeStreamer.SourceFilter> sourceFilters = Arrays.asList(alivePredicate,
                                                                                       sourceFilterDownNodesPredicate,
                                                                                       new RangeStreamer.ExcludeLocalNodeFilter()
                                                                                       );

    @After
    public void clearDownNode()
    {
        // TODO: actually use these
        downNodes.clear();
        sourceFilterDownNodes.clear();
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    Token tenToken    = new OrderPreservingPartitioner.StringToken("00010");
    Token twentyToken = new OrderPreservingPartitioner.StringToken("00020");
    Token thirtyToken = new OrderPreservingPartitioner.StringToken("00030");
    Token fourtyToken = new OrderPreservingPartitioner.StringToken("00040");

    Range<Token> range30_10 = new Range<>(thirtyToken, tenToken);
    Range<Token> range10_20 = new Range<>(tenToken, twentyToken);
    Range<Token> range20_30 = new Range<>(twentyToken, thirtyToken);
    Range<Token> range30_40 = new Range<>(thirtyToken, fourtyToken);

    RangesAtEndpoint toFetch = RangesAtEndpoint.of(new Replica(address05, range30_40, true),
                                                   new Replica(address05, range20_30, true),
                                                   new Replica(address05, range10_20, false));



    public EndpointsForRange endpoints(Replica... replicas)
    {
        assert replicas.length > 0;

        Range<Token> range = replicas[0].range();
        EndpointsForRange.Builder builder = EndpointsForRange.builder(range);
        for (Replica r : replicas)
        {
            assert r.range().equals(range);
            builder.add(r);
        }

        return builder.build();
    }
    @Test
    public void testRangeStreamerRangesToFetch() throws Exception
    {
        EndpointsByReplica expectedResult = new EndpointsByReplica(ImmutableMap.of(
        transientReplica(address05, range10_20), endpoints(transientReplica(address02, range10_20)),
        fullReplica(address05, range20_30), endpoints(transientReplica(address03, range20_30), fullReplica(address04, range20_30)),
        fullReplica(address05, range30_40), endpoints(transientReplica(address04, range30_10), fullReplica(address02, range30_10))));

        invokeCalculateRangesToFetchWithPreferredEndpoints(toFetch, constructTMDs(), expectedResult);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDs()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(range30_10.right, address02);
        tmd.updateNormalToken(range10_20.right, address03);
        tmd.updateNormalToken(range20_30.right, address04);
        TokenMetadata updated = tmd.cloneOnlyTokenMap();
        updated.updateNormalToken(range30_40.right, address05);

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
                                                                                               "TestKeyspace",
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
