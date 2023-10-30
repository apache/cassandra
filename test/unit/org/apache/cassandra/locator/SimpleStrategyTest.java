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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleStrategyTest
{
    public static final String KEYSPACE1 = "SimpleStrategyTest";
    public static final String MULTIDC = "MultiDCSimpleStrategyTest";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
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
        List<Token> endpointTokens = new ArrayList<Token>();
        List<Token> keyTokens = new ArrayList<Token>();
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
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        TokenMetadata metadata = new TokenMetadata();

        AbstractReplicationStrategy strategy = getStrategy(MULTIDC, metadata, snitch);

        // Topology taken directly from the topology_test.test_size_estimates_multidc dtest that regressed
        Multimap<InetAddressAndPort, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new Murmur3Partitioner.LongToken(-6639341390736545756L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.1"), new Murmur3Partitioner.LongToken(-2688160409776496397L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new Murmur3Partitioner.LongToken(-2506475074448728501L));
        dc1.put(InetAddressAndPort.getByName("127.0.0.2"), new Murmur3Partitioner.LongToken(8473270337963525440L));
        metadata.updateNormalTokens(dc1);

        Multimap<InetAddressAndPort, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new Murmur3Partitioner.LongToken(-3736333188524231709L));
        dc2.put(InetAddressAndPort.getByName("127.0.0.4"), new Murmur3Partitioner.LongToken(8673615181726552074L));
        metadata.updateNormalTokens(dc2);

        Map<InetAddressAndPort, Integer> primaryCount = new HashMap<>();
        Map<InetAddressAndPort, Integer> replicaCount = new HashMap<>();
        for (Token t : metadata.sortedTokens())
        {
            EndpointsForToken replicas = strategy.getNaturalReplicasForToken(t);
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
        TokenMetadata tmd;
        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            tmd = new TokenMetadata();
            strategy = getStrategy(keyspaceName, tmd, new SimpleSnitch());
            List<InetAddressAndPort> hosts = new ArrayList<>();
            for (int i = 0; i < endpointTokens.length; i++)
            {
                InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + String.valueOf(i + 1));
                tmd.updateNormalToken(endpointTokens[i], ep);
                hosts.add(ep);
            }

            for (int i = 0; i < keyTokens.length; i++)
            {
                EndpointsForToken replicas = strategy.getNaturalReplicasForToken(keyTokens[i]);
                assertEquals(strategy.getReplicationFactor().allReplicas, replicas.size());
                List<InetAddressAndPort> correctEndpoints = new ArrayList<>();
                for (int j = 0; j < replicas.size(); j++)
                    correctEndpoints.add(hosts.get((i + j + 1) % hosts.size()));
                assertEquals(new HashSet<>(correctEndpoints), replicas.endpoints());
            }
        }
    }

    @Test
    public void testGetEndpointsDuringBootstrap() throws UnknownHostException
    {
        // the token difference will be RING_SIZE * 2.
        final int RING_SIZE = 10;
        TokenMetadata tmd = new TokenMetadata();
        TokenMetadata oldTmd = StorageServiceAccessor.setTokenMetadata(tmd);

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
            tmd.updateNormalToken(endpointTokens[i], ep);
            hosts.add(ep);
        }

        // bootstrap at the end of the ring
        Token bsToken = new BigIntegerToken(String.valueOf(210));
        InetAddressAndPort bootstrapEndpoint = InetAddressAndPort.getByName("127.0.0.11");
        tmd.addBootstrapTokens(Collections.singleton(bsToken), bootstrapEndpoint);

        AbstractReplicationStrategy strategy = null;
        for (String keyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            strategy = getStrategy(keyspaceName, tmd, new SimpleSnitch());

            PendingRangeCalculatorService.calculatePendingRanges(strategy, keyspaceName);

            int replicationFactor = strategy.getReplicationFactor().allReplicas;

            for (int i = 0; i < keyTokens.length; i++)
            {
                EndpointsForToken replicas = tmd.getWriteEndpoints(keyTokens[i], keyspaceName, strategy.getNaturalReplicasForToken(keyTokens[i]));
                assertTrue(replicas.size() >= replicationFactor);

                for (int j = 0; j < replicationFactor; j++)
                {
                    //Check that the old nodes are definitely included
                   assertTrue(replicas.endpoints().contains(hosts.get((i + j + 1) % hosts.size())));
                }

                // bootstrapEndpoint should be in the endpoints for i in MAX-RF to MAX, but not in any earlier ep.
                if (i < RING_SIZE - replicationFactor)
                    assertFalse(replicas.endpoints().contains(bootstrapEndpoint));
                else
                    assertTrue(replicas.endpoints().contains(bootstrapEndpoint));
            }
        }

        StorageServiceAccessor.setTokenMetadata(oldTmd);
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
        TokenMetadata metadata = new TokenMetadata();
        metadata.updateNormalTokens(tokens);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("replication_factor", "3/1");

        SimpleStrategy strategy = new SimpleStrategy("ks", metadata, snitch, configOptions);

        Range<Token> range1 = range(400, 100);
        Util.assertRCEquals(EndpointsForToken.of(range1.right,
                                                 Replica.fullReplica(endpoints.get(0), range1),
                                                 Replica.fullReplica(endpoints.get(1), range1),
                                                 Replica.transientReplica(endpoints.get(2), range1)),
                            strategy.getNaturalReplicasForToken(tk(99)));


        Range<Token> range2 = range(100, 200);
        Util.assertRCEquals(EndpointsForToken.of(range2.right,
                                                 Replica.fullReplica(endpoints.get(1), range2),
                                                 Replica.fullReplica(endpoints.get(2), range2),
                                                 Replica.transientReplica(endpoints.get(3), range2)),
                            strategy.getNaturalReplicasForToken(tk(101)));
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSimpleStrategyThrowsConfigurationException() throws ConfigurationException, UnknownHostException
    {
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

        TokenMetadata metadata = new TokenMetadata();
        metadata.updateNormalTokens(tokens);

        Map<String, String> configOptions = new HashMap<>();

        @SuppressWarnings("unused")
        SimpleStrategy strategy = new SimpleStrategy("ks", metadata, snitch, configOptions);
    }
    
    @Test
    public void shouldReturnNoEndpointsForEmptyRing()
    {
        TokenMetadata metadata = new TokenMetadata();
        
        HashMap<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "1");
        
        SimpleStrategy strategy = new SimpleStrategy("ks", metadata, new SimpleSnitch(), configOptions);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(null, metadata);
        assertTrue(replicas.endpoints().isEmpty());
    }

    @Test
    public void shouldWarnOnHigherReplicationFactorThanNodes()
    {
        HashMap<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        SimpleStrategy strategy = new SimpleStrategy("ks", new TokenMetadata(), new SimpleSnitch(), configOptions);
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), FBUtilities.getBroadcastAddressAndPort());
        
        ClientWarn.instance.captureWarnings();
        strategy.maybeWarnOnOptions(null);
        assertTrue(ClientWarn.instance.getWarnings().stream().anyMatch(s -> s.contains("Your replication factor")));
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd, IEndpointSnitch snitch)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKeyspaceMetadata(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                                                                    keyspaceName,
                                                                    ksmd.params.replication.klass,
                                                                    tmd,
                                                                    snitch,
                                                                    ksmd.params.replication.options);
    }
}
