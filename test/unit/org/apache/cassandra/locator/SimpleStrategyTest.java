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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageServiceAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleStrategyTest
{
    public static final String KEYSPACE1 = "SimpleStrategyTest";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1));
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
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

    // given a list of endpoint tokens, and a set of key tokens falling between the endpoint tokens,
    // make sure that the Strategy picks the right endpoints for the keys.
    private void verifyGetNaturalEndpoints(Token[] endpointTokens, Token[] keyTokens) throws UnknownHostException
    {
        TokenMetadata tmd;
        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            tmd = new TokenMetadata();
            strategy = getStrategy(keyspaceName, tmd);
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
        tmd.addBootstrapToken(bsToken, bootstrapEndpoint);

        AbstractReplicationStrategy strategy = null;
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            strategy = getStrategy(keyspaceName, tmd);

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

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKeyspaceMetadata(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                                                                    keyspaceName,
                                                                    ksmd.params.replication.klass,
                                                                    tmd,
                                                                    new SimpleSnitch(),
                                                                    ksmd.params.replication.options);
    }
}
