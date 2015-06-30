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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;

public class ReplicationStrategyEndpointCacheTest
{
    private TokenMetadata tmd;
    private Token searchToken;
    private AbstractReplicationStrategy strategy;
    public static final String KEYSPACE = "ReplicationStrategyEndpointCacheTest";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(5));
    }

    public void setup(Class stratClass, Map<String, String> strategyOptions) throws Exception
    {
        tmd = new TokenMetadata();
        searchToken = new BigIntegerToken(String.valueOf(15));

        strategy = getStrategyWithNewTokenMetadata(Keyspace.open(KEYSPACE).getReplicationStrategy(), tmd);

        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(10)), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(20)), InetAddress.getByName("127.0.0.2"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(30)), InetAddress.getByName("127.0.0.3"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(40)), InetAddress.getByName("127.0.0.4"));
        //tmd.updateNormalToken(new BigIntegerToken(String.valueOf(50)), InetAddress.getByName("127.0.0.5"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(60)), InetAddress.getByName("127.0.0.6"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(70)), InetAddress.getByName("127.0.0.7"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(80)), InetAddress.getByName("127.0.0.8"));
    }

    @Test
    public void testEndpointsWereCached() throws Exception
    {
        runEndpointsWereCachedTest(FakeSimpleStrategy.class, null);
        runEndpointsWereCachedTest(FakeOldNetworkTopologyStrategy.class, null);
        runEndpointsWereCachedTest(FakeNetworkTopologyStrategy.class, new HashMap<String, String>());
    }

    public void runEndpointsWereCachedTest(Class stratClass, Map<String, String> configOptions) throws Exception
    {
        setup(stratClass, configOptions);
        assert strategy.getNaturalEndpoints(searchToken).equals(strategy.getNaturalEndpoints(searchToken));
    }

    @Test
    public void testCacheRespectsTokenChanges() throws Exception
    {
        runCacheRespectsTokenChangesTest(SimpleStrategy.class, null);
        runCacheRespectsTokenChangesTest(OldNetworkTopologyStrategy.class, null);
        runCacheRespectsTokenChangesTest(NetworkTopologyStrategy.class, new HashMap<String, String>());
    }

    public void runCacheRespectsTokenChangesTest(Class stratClass, Map<String, String> configOptions) throws Exception
    {
        setup(stratClass, configOptions);
        ArrayList<InetAddress> initial;
        ArrayList<InetAddress> endpoints;

        endpoints = strategy.getNaturalEndpoints(searchToken);
        assert endpoints.size() == 5 : StringUtils.join(endpoints, ",");

        // test token addition, in DC2 before existing token
        initial = strategy.getNaturalEndpoints(searchToken);
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(35)), InetAddress.getByName("127.0.0.5"));
        endpoints = strategy.getNaturalEndpoints(searchToken);
        assert endpoints.size() == 5 : StringUtils.join(endpoints, ",");
        assert !endpoints.equals(initial);

        // test token removal, newly created token
        initial = strategy.getNaturalEndpoints(searchToken);
        tmd.removeEndpoint(InetAddress.getByName("127.0.0.5"));
        endpoints = strategy.getNaturalEndpoints(searchToken);
        assert endpoints.size() == 5 : StringUtils.join(endpoints, ",");
        assert !endpoints.contains(InetAddress.getByName("127.0.0.5"));
        assert !endpoints.equals(initial);

        // test token change
        initial = strategy.getNaturalEndpoints(searchToken);
        //move .8 after search token but before other DC3
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(25)), InetAddress.getByName("127.0.0.8"));
        endpoints = strategy.getNaturalEndpoints(searchToken);
        assert endpoints.size() == 5 : StringUtils.join(endpoints, ",");
        assert !endpoints.equals(initial);
    }

    protected static class FakeSimpleStrategy extends SimpleStrategy
    {
        private boolean called = false;

        public FakeSimpleStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata);
        }
    }

    protected static class FakeOldNetworkTopologyStrategy extends OldNetworkTopologyStrategy
    {
        private boolean called = false;

        public FakeOldNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata);
        }
    }

    protected static class FakeNetworkTopologyStrategy extends NetworkTopologyStrategy
    {
        private boolean called = false;

        public FakeNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata);
        }
    }

    private AbstractReplicationStrategy getStrategyWithNewTokenMetadata(AbstractReplicationStrategy strategy, TokenMetadata newTmd) throws ConfigurationException
    {
        return AbstractReplicationStrategy.createReplicationStrategy(
                                                                    strategy.keyspaceName,
                                                                    AbstractReplicationStrategy.getClass(strategy.getClass().getName()),
                                                                    newTmd,
                                                                    strategy.snitch,
                                                                    strategy.configOptions);
    }

}
