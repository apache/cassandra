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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class ConsistencyLevelTest
{
    private static final String KS = "test";
    private static final Map<String, String> RACK = new HashMap<>(), DATACENTER = new HashMap<>();
    private static final IEndpointSnitch SNITCH = new AbstractNetworkTopologySnitch()
    {
        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return RACK.getOrDefault(endpoint.getHostAddress(false), "RC1");
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return DATACENTER.getOrDefault(endpoint.getHostAddress(false), "DC1");
        }
    };

    @BeforeClass
    public static void setSnitch()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setEndpointSnitch(SNITCH);
    }

    @AfterClass
    public static void resetSnitch()
    {
        DatabaseDescriptor.setEndpointSnitch(null);
    }

    @After
    public void resetSnitchState()
    {
        RACK.clear();
        DATACENTER.clear();
    }

    @Test
    public void allButOne_shouldBe_2_forReplicationFactor_3()
    {
        testAllButOne(simpleStrategy(3), 2);
    }

    @Test
    public void allButOne_shouldBe_1_forReplicationFactor_2()
    {
        testAllButOne(simpleStrategy(2), 1);
    }

    @Test
    public void allButOne_shouldBe_1_forReplicationFactor_1()
    {
        testAllButOne(simpleStrategy(1), 1);
    }

    @Test
    public void allButOne_shouldBe_1_forLocalStrategy()
    {
        testAllButOne(localStrategy(), 1);
    }

    @Test
    public void allButOne_shouldBe_8_forReplicationFactor_3_3_3()
    {
        testAllButOne(networkTopologyStrategy(3, 3, 3), 8);
    }

    @Test
    public void allButOne_shouldBe_11_forEverywhereStrategyOnClusterOf_12() throws Exception
    {
        testAllButOne(everywhereStrategy(
                        dc(1, Pair.create("192.168.0.1", "A"), Pair.create("192.168.0.2", "E"), Pair.create("192.168.0.3", "H"),
                                Pair.create("192.168.0.4", "C"), Pair.create("192.168.0.5", "I"), Pair.create("192.168.0.6", "J")),
                        dc(2, Pair.create("192.168.1.1", "B"), Pair.create("192.168.1.2", "G"), Pair.create("192.168.1.3", "L"),
                                Pair.create("192.168.1.4", "D"), Pair.create("192.168.1.5", "F"), Pair.create("192.168.1.6", "K"))),
                11);
    }

    private void testAllButOne(AbstractReplicationStrategy replicationStrategy, int expected)
    {
        // when
        int blockFor = ConsistencyLevel.allButOneFor(replicationStrategy);

        // then
        assertEquals("number of nodes to block for", expected, blockFor);
    }

    private static NetworkTopologyStrategy networkTopologyStrategy(int... dc)
    {
        Map<String, String> config = new HashMap<>();
        for (int i = 0; i < dc.length; i++)
        {
            config.put("DC" + i, Integer.toString(dc[i]));
        }
        return new NetworkTopologyStrategy(KS, new TokenMetadata(), SNITCH, config);
    }

    private static AbstractReplicationStrategy simpleStrategy(int replicationFactory)
    {
        Map<String, String> config = Collections.singletonMap("replication_factor", Integer.toString(replicationFactory));
        return new SimpleStrategy(KS, new TokenMetadata(), SNITCH, config);
    }

    @SafeVarargs
    private static AbstractReplicationStrategy everywhereStrategy(Multimap<InetAddressAndPort, Token>... dcs)
    {
        TokenMetadata metadata = new TokenMetadata();
        for (Multimap<InetAddressAndPort, Token> dc : dcs)
        {
            metadata.updateNormalTokens(dc);
        }
        return new EverywhereStrategy(KS, metadata, SNITCH, Collections.emptyMap());
    }

    private static AbstractReplicationStrategy localStrategy()
    {
        return new LocalStrategy(KS, new TokenMetadata(), SNITCH, Collections.emptyMap());
    }

    private static Multimap<InetAddressAndPort, Token> dc(int id, Pair<String, String>... addressToken) throws UnknownHostException
    {
        Multimap<InetAddressAndPort, Token> dc = HashMultimap.create();
        for (Pair<String, String> pair : addressToken)
        {
            DATACENTER.put(pair.left, "DC" + id);
            dc.put(InetAddressAndPort.getByName(pair.left), new OrderPreservingPartitioner.StringToken(pair.right));
        }
        return dc;
    }
}
