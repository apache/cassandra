/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.Token;

public class OldNetworkTopologyStrategyTest extends SchemaLoader
{
    private List<Token> endpointTokens;
    private List<Token> keyTokens;
    private TokenMetadata tmd;
    private Map<String, ArrayList<InetAddress>> expectedResults;

    @Before
    public void init()
    {
        endpointTokens = new ArrayList<Token>();
        keyTokens = new ArrayList<Token>();
        tmd = new TokenMetadata();
        expectedResults = new HashMap<String, ArrayList<InetAddress>>();
    }

    /**
     * 4 same rack endpoints
     *
     * @throws UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsA() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, KSMetaData.optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.0.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.0.2", "254.0.0.3"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 3 same rack endpoints
     * 1 external datacenter
     *
     * @throws UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsB() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, KSMetaData.optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.1.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.1.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.1.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.1.0.3", "254.0.0.1"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.1.0.3", "254.0.0.2"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 2 same rack endpoints
     * 1 same datacenter, different rack endpoints
     * 1 external datacenter
     *
     * @throws UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsC() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, KSMetaData.optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.1.3");
        addEndpoint("30", "35", "254.1.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.1.3", "254.1.0.4"));
        expectedResults.put("15", buildResult("254.0.1.3", "254.1.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.1.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.1.3", "254.1.0.4"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    private ArrayList<InetAddress> buildResult(String... addresses) throws UnknownHostException
    {
        ArrayList<InetAddress> result = new ArrayList<InetAddress>();
        for (String address : addresses)
        {
            result.add(InetAddress.getByName(address));
        }
        return result;
    }

    private void addEndpoint(String endpointTokenID, String keyTokenID, String endpointAddress) throws UnknownHostException
    {
        BigIntegerToken endpointToken = new BigIntegerToken(endpointTokenID);
        endpointTokens.add(endpointToken);

        BigIntegerToken keyToken = new BigIntegerToken(keyTokenID);
        keyTokens.add(keyToken);

        InetAddress ep = InetAddress.getByName(endpointAddress);
        tmd.updateNormalToken(endpointToken, ep);
    }

    private void testGetEndpoints(AbstractReplicationStrategy strategy, Token[] keyTokens) throws UnknownHostException
    {
        for (Token keyToken : keyTokens)
        {
            List<InetAddress> endpoints = strategy.getNaturalEndpoints(keyToken);
            for (int j = 0; j < endpoints.size(); j++)
            {
                ArrayList<InetAddress> hostsExpected = expectedResults.get(keyToken.toString());
                assertEquals(endpoints.get(j), hostsExpected.get(j));
            }
        }
    }

}
