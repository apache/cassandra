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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
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
    static final String KEYSPACE = "TestKeyspace";
    static InetAddressAndPort address02;
    static InetAddressAndPort address03;
    static InetAddressAndPort address04;
    static InetAddressAndPort address05;

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

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(OrderPreservingPartitioner.instance);
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        address02 = InetAddressAndPort.getByName("127.0.0.2");
        address03 = InetAddressAndPort.getByName("127.0.0.3");
        address04 = InetAddressAndPort.getByName("127.0.0.4");
        address05 = InetAddressAndPort.getByName("127.0.0.5");
    }

    @Before
    public void setup()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
        ClusterMetadataTestHelper.addEndpoint(address02, range30_10.right);
        ClusterMetadataTestHelper.addEndpoint(address03, range10_20.right);
        ClusterMetadataTestHelper.addEndpoint(address04, range20_30.right);
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple("3/1"));
        SchemaTestUtil.addOrUpdateKeyspace(ksm);
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
    public void testRangeStreamerRangesToFetch()
    {
        EndpointsByReplica expectedResult = new EndpointsByReplica(ImmutableMap.of(
            transientReplica(address05, range10_20), endpoints(transientReplica(address02, range10_20)),
            fullReplica(address05, range20_30), endpoints(transientReplica(address03, range20_30), fullReplica(address02, range20_30)),
            fullReplica(address05, range30_40), endpoints(transientReplica(address04, range30_40), fullReplica(address03, range30_40))));

        /*
         * Pre-TCM expected result, there are a couple of differences worth explaining...
         *
         * transientReplica(address05, range10_20), endpoints(transientReplica(address02, range10_20)),
         * fullReplica(address05, range20_30), endpoints(transientReplica(address03, range20_30), fullReplica(address04, range20_30)),
         * fullReplica(address05, range30_40), endpoints(transientReplica(address04, range30_10), fullReplica(address02, range30_10))));

         * First, the ranges of the source replicas now exactly match the dest replicas. This is because we split
         * existing ranges without modifying ownership at the prepare stage.
         *
         * Second, the full replicas of the ranges for which the new node is becoming a full replica are different from
         * before. (20,30] used to be sourced from address04, but now comes from address02. Similarly, (30, 40] used to
         * be sourced from address02, but now is taken from address03. The reason for this is that we now identify the
         * strict sources slightly differently, but with semantically equivalent outcomes.
         *
         * We have 3/1 replicas for a given range with a new node is bootstrapping as a Full replica. This causes an
         * existing Full to become Transient and the existing Transient to give up the range entirely. The old and new
         * implementations work slightly differently in this case:
         *
         * Post CEP-21, the new node will pick the replica going from Full to Transient as the stream source.
         * In earlier versions, the Transient replica giving up the range will be chosen initially, but this doesn't
         * satisfy the `isSufficient` predicate (i.e. a Transient replica can't be the source for a Full one), so we
         * pick the first existing Full replica for the range, which might not be the same one TCM picked.
         *
         * In the end, neither implementation can pick the replica giving up the range entirely (because it is Transient
         * and the new replica is Full), despite strict consistency and so both pick an existing Full replica as the
         * source.
         */

        NodeId newNode = ClusterMetadataTestHelper.register(address05);
        ClusterMetadataTestHelper.JoinProcess join = ClusterMetadataTestHelper.lazyJoin(address05, range30_40.right);
        join.prepareJoin();
        ClusterMetadata metadata = ClusterMetadata.current();
        BootstrapAndJoin joiningPlan = (BootstrapAndJoin) metadata.inProgressSequences.get(newNode);
        invokeCalculateRangesToFetchWithPreferredEndpoints(toFetch, metadata, joiningPlan, expectedResult);
    }

    private void invokeCalculateRangesToFetchWithPreferredEndpoints(ReplicaCollection<?> toFetch,
                                                                    ClusterMetadata metadata,
                                                                    BootstrapAndJoin joinPlan,
                                                                    EndpointsByReplica expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        Pair<MovementMap, MovementMap> movements = joinPlan.getMovementMaps(metadata);
        EndpointsByReplica result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> replicas,
                                                                                               simpleStrategy(metadata),
                                                                                               true,
                                                                                               metadata,
                                                                                               KEYSPACE,
                                                                                               sourceFilters,
                                                                                               movements.left,
                                                                                               movements.right);
        result.asMap().forEach((replica, list) -> System.out.printf("Replica %s, sources %s%n", replica, list));
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    private AbstractReplicationStrategy simpleStrategy(ClusterMetadata metadata)
    {
        return AbstractReplicationStrategy.createReplicationStrategy(KEYSPACE, metadata.schema.getKeyspaceMetadata(KEYSPACE).params.replication);
    }

}
