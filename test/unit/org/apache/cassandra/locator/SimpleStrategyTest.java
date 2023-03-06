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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.*;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.ServerTestUtils.resetCMS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.service.LeaveAndBootstrapTest.getWriteEndpoints;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleStrategyTest
{
    public static final String KEYSPACE1 = "SimpleStrategyTest";
    public static final String MULTIDC = "MultiDCSimpleStrategyTest";

    static
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
    }

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public static void withPartitioner(IPartitioner partitioner)
    {
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        SchemaLoader.prepareServer();
        resetCMS();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1));
        SchemaLoader.createKeyspace(MULTIDC, KeyspaceParams.simple(3));
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
    }

    @Before
    public void resetSnitch()
    {
        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
    }

    @Test
    public void tryValidKeyspace()
    {
        assert Keyspace.open(KEYSPACE1).getReplicationStrategy() != null;
    }

    @Test
    public void testBigIntegerEndpoints() throws UnknownHostException
    {
        withPartitioner(RandomPartitioner.instance);
        List<Token> endpointTokens = new ArrayList<>();
        List<Token> keyTokens = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
        }
        verifyGetNaturalEndpoints(endpointTokens.toArray(new Token[0]), keyTokens.toArray(new Token[0]));
    }

    @Test
    public void testStringEndpoints() throws UnknownHostException
    {
        IPartitioner partitioner = OrderPreservingPartitioner.instance;
        withPartitioner(partitioner);
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        List<Token> endpointTokens = new ArrayList<Token>();
        List<Token> keyTokens = new ArrayList<Token>();
        for (int i = 0; i < 5; i++) {
            endpointTokens.add(new StringToken(String.valueOf((char)('a' + i * 2))));
            keyTokens.add(partitioner.getToken(ByteBufferUtil.bytes(String.valueOf((char) ('a' + i * 2 + 1)))));
        }
        verifyGetNaturalEndpoints(endpointTokens.toArray(new Token[0]), keyTokens.toArray(new Token[0]));
    }

    @Test
    public void testMultiDCSimpleStrategyEndpoints() throws UnknownHostException
    {
        withPartitioner(Murmur3Partitioner.instance);
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        // Topology taken directly from the topology_test.test_size_estimates_multidc dtest that regressed
        Multimap<InetAddressAndPort, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new Murmur3Partitioner.LongToken(-6639341390736545756L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new Murmur3Partitioner.LongToken(-2688160409776496397L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new Murmur3Partitioner.LongToken(-2506475074448728501L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new Murmur3Partitioner.LongToken(8473270337963525440L));
        dc1.asMap().forEach((e, t) -> ClusterMetadataTestHelper.addEndpoint(e, t, "dc1", "rack1"));

        Multimap<InetAddressAndPort, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new Murmur3Partitioner.LongToken(-3736333188524231709L));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new Murmur3Partitioner.LongToken(8673615181726552074L));
        dc2.asMap().forEach((e, t) -> ClusterMetadataTestHelper.addEndpoint(e, t, "dc2", "rack1"));

        Map<InetAddressAndPort, Integer> primaryCount = new HashMap<>();
        Map<InetAddressAndPort, Integer> replicaCount = new HashMap<>();
        ClusterMetadata metadata = ClusterMetadata.current();
        for (Token t : metadata.tokenMap.tokens())
        {
            EndpointsForToken replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(MULTIDC, t);
            primaryCount.compute(replicas.get(0).endpoint(), (k, v) -> (v == null) ? 1 : v + 1);
            for (Replica replica : replicas)
                replicaCount.compute(replica.endpoint(), (k, v) -> (v == null) ? 1 : v + 1);
        }

        // All three hosts should have 2 "primary" replica ranges and 6 total ranges with RF=3, 3 nodes and 2 DCs.
        for (InetAddressAndPort addr : primaryCount.keySet())
        {
            assertEquals(2, (int) primaryCount.get(addr));
            assertEquals(6, (int) replicaCount.get(addr));
        }
    }

    // given a list of endpoint tokens, and a set of key tokens falling between the endpoint tokens,
    // make sure that the Strategy picks the right endpoints for the keys.
    private void verifyGetNaturalEndpoints(Token[] endpointTokens, Token[] keyTokens) throws UnknownHostException
    {
        List<InetAddressAndPort> hosts = new ArrayList<>();
        for (int i = 0; i < endpointTokens.length; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + (i + 1));
            ClusterMetadataTestHelper.addEndpoint(ep, endpointTokens[i]);
            hosts.add(ep);
        }

        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces()
                                                  .without(SchemaConstants.METADATA_KEYSPACE_NAME)
                                                  .names())
        {
            strategy = getStrategy(keyspaceName);
            for (int i = 0; i < keyTokens.length; i++)
            {
                EndpointsForToken replicas = ClusterMetadataTestHelper.getNaturalReplicasForToken(keyspaceName, keyTokens[i]);
                assertEquals(strategy.getReplicationFactor().allReplicas, replicas.size());
                List<InetAddressAndPort> correctEndpoints = new ArrayList<>();
                for (int j = 0; j < replicas.size(); j++)
                    correctEndpoints.add(hosts.get((i + j + 1) % hosts.size()));
                assertEquals(new HashSet<>(correctEndpoints), replicas.endpoints());
            }
        }
    }

    @Test
    public void testGetEndpointsDuringBootstrap() throws UnknownHostException, ExecutionException, InterruptedException
    {
        withPartitioner(RandomPartitioner.instance);
        // the token difference will be RING_SIZE * 2.
        final int RING_SIZE = 10;

        Token[] endpointTokens = new Token[RING_SIZE];
        Token[] keyTokens = new Token[RING_SIZE];

        for (int i = 0; i < RING_SIZE; i++)
        {
            endpointTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i));
            keyTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i + RING_SIZE));
        }

        List<InetAddressAndPort> hosts = new ArrayList<>();
        for (int i = 0; i < endpointTokens.length; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + String.valueOf(i + 1));
            ClusterMetadataTestHelper.addEndpoint(ep, endpointTokens[i]);
            hosts.add(ep);
        }

        // bootstrap at the end of the ring
        Token bsToken = new BigIntegerToken(String.valueOf(210));
        InetAddressAndPort bootstrapEndpoint = InetAddressAndPort.getByName("127.0.0.11");
        Location l = new Location("dc1", "rack1");
        ClusterMetadataTestHelper.commit(new Register(ClusterMetadataTestHelper.addr(bootstrapEndpoint), l, NodeVersion.CURRENT));
        ClusterMetadataTestHelper.lazyJoin(bootstrapEndpoint, bsToken)
                                 .prepareJoin()
                                 .startJoin();

        AbstractReplicationStrategy strategy = null;
        ClusterMetadata metadata = ClusterMetadata.current();
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces().names())
        {
            strategy = getStrategy(keyspaceName);

            int replicationFactor = strategy.getReplicationFactor().allReplicas;

            for (int i = 0; i < keyTokens.length; i++)
            {
                EndpointsForToken replicas = getWriteEndpoints(metadata, ReplicationParams.fromMap(strategy.configOptions), keyTokens[i]);
                assertTrue(replicas.size() >= replicationFactor);

                for (int j = 0; j < replicationFactor; j++)
                {
                   InetAddressAndPort host = hosts.get((i + j + 1) % hosts.size());
                    //Check that the old nodes are definitely included
                   assertTrue(String.format("%s should contain %s but it did not. RF=%d \n%s",
                                            replicas, host, replicationFactor, metadata),
                              replicas.endpoints().contains(host));
                }

                // bootstrapEndpoint should be in the endpoints for i in MAX-RF to MAX, but not in any earlier ep.
                if (i < RING_SIZE - replicationFactor)
                    assertFalse(replicas.endpoints().contains(bootstrapEndpoint));
                else
                    assertTrue(replicas.endpoints().contains(bootstrapEndpoint));
            }
        }
    }

    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static Range<Token> range(long l, long r)
    {
        return new Range<>(tk(l), tk(r));
    }

    @Test
    public void transientReplica() throws Exception
    {
        withPartitioner(Murmur3Partitioner.instance);
        IEndpointSnitch snitch = new SimpleSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        List<InetAddressAndPort> endpoints = Lists.newArrayList(InetAddressAndPort.getByName("127.0.0.1"),
                                                                InetAddressAndPort.getByName("127.0.0.2"),
                                                                InetAddressAndPort.getByName("127.0.0.3"),
                                                                InetAddressAndPort.getByName("127.0.0.4"));

        Multimap<InetAddressAndPort, Token> tokens = HashMultimap.create();
        tokens.put(endpoints.get(0), tk(100));
        tokens.put(endpoints.get(1), tk(200));
        tokens.put(endpoints.get(2), tk(300));
        tokens.put(endpoints.get(3), tk(400));
        tokens.forEach(ClusterMetadataTestHelper::addEndpoint);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("replication_factor", "3/1");

        SimpleStrategy strategy = new SimpleStrategy("ks", configOptions);

        Range<Token> range1 = range(400, 100);
        Util.assertRCEquals(EndpointsForToken.of(range1.right,
                                                 Replica.fullReplica(endpoints.get(0), range1),
                                                 Replica.fullReplica(endpoints.get(1), range1),
                                                 Replica.transientReplica(endpoints.get(2), range1)),
                            ClusterMetadataTestHelper.getNaturalReplicasForToken("ks", tk(99)));


        Range<Token> range2 = range(100, 200);
        Util.assertRCEquals(EndpointsForToken.of(range2.right,
                                                 Replica.fullReplica(endpoints.get(1), range2),
                                                 Replica.fullReplica(endpoints.get(2), range2),
                                                 Replica.transientReplica(endpoints.get(3), range2)),
                            ClusterMetadataTestHelper.getNaturalReplicasForToken("ks", tk(101)));
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSimpleStrategyThrowsConfigurationException() throws ConfigurationException, UnknownHostException
    {
        withPartitioner(Murmur3Partitioner.instance);
        expectedEx.expect(ConfigurationException.class);
        expectedEx.expectMessage("SimpleStrategy requires a replication_factor strategy option.");

        IEndpointSnitch snitch = new SimpleSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        List<InetAddressAndPort> endpoints = Lists.newArrayList(InetAddressAndPort.getByName("127.0.0.1"),
                                                                InetAddressAndPort.getByName("127.0.0.2"),
                                                                InetAddressAndPort.getByName("127.0.0.3"));

        Multimap<InetAddressAndPort, Token> tokens = HashMultimap.create();
        tokens.put(endpoints.get(0), tk(100));
        tokens.put(endpoints.get(1), tk(200));
        tokens.put(endpoints.get(2), tk(300));

        Map<String, String> configOptions = new HashMap<>();

        @SuppressWarnings("unused")
        SimpleStrategy strategy = new SimpleStrategy("ks", configOptions);
    }
    
    @Test
    public void shouldReturnNoEndpointsForEmptyRing()
    {
        withPartitioner(Murmur3Partitioner.instance);

        HashMap<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "1");
        
        SimpleStrategy strategy = new SimpleStrategy("ks", configOptions);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(null, new ClusterMetadata(Murmur3Partitioner.instance));
        assertTrue(replicas.endpoints().isEmpty());
    }

    @Test
    public void shouldWarnOnHigherReplicationFactorThanNodes()
    {
        withPartitioner(Murmur3Partitioner.instance);
        HashMap<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        SimpleStrategy strategy = new SimpleStrategy("ks", configOptions);
        ClusterMetadataTestHelper.addEndpoint(1);
        ClientWarn.instance.captureWarnings();
        strategy.maybeWarnOnOptions(null);
        assertTrue(ClientWarn.instance.getWarnings().stream().anyMatch(s -> s.contains("Your replication factor")));
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKeyspaceMetadata(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(keyspaceName,
                                                                     ksmd.params.replication.klass,
                                                                     ksmd.params.replication.options);
    }
}
