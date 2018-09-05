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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
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
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(5));
    }

    public void setup(Class stratClass, Map<String, String> strategyOptions) throws Exception
    {
        tmd = new TokenMetadata();
        searchToken = new BigIntegerToken(String.valueOf(15));

        strategy = getStrategyWithNewTokenMetadata(Keyspace.open(KEYSPACE).getReplicationStrategy(), tmd);

        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(10)), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(20)), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(30)), InetAddressAndPort.getByName("127.0.0.3"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(40)), InetAddressAndPort.getByName("127.0.0.4"));
        //tmd.updateNormalToken(new BigIntegerToken(String.valueOf(50)), InetAddressAndPort.getByName("127.0.0.5", null, null));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(60)), InetAddressAndPort.getByName("127.0.0.6"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(70)), InetAddressAndPort.getByName("127.0.0.7"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(80)), InetAddressAndPort.getByName("127.0.0.8"));
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
        Util.assertRCEquals(strategy.getNaturalReplicasForToken(searchToken), strategy.getNaturalReplicasForToken(searchToken));
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
        EndpointsForToken initial;
        EndpointsForToken replicas;

        replicas = strategy.getNaturalReplicasForToken(searchToken);
        assert replicas.size() == 5 : StringUtils.join(replicas, ",");

        // test token addition, in DC2 before existing token
        initial = strategy.getNaturalReplicasForToken(searchToken);
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(35)), InetAddressAndPort.getByName("127.0.0.5"));
        replicas = strategy.getNaturalReplicasForToken(searchToken);
        assert replicas.size() == 5 : StringUtils.join(replicas, ",");
        Util.assertNotRCEquals(replicas, initial);

        // test token removal, newly created token
        initial = strategy.getNaturalReplicasForToken(searchToken);
        tmd.removeEndpoint(InetAddressAndPort.getByName("127.0.0.5"));
        replicas = strategy.getNaturalReplicasForToken(searchToken);
        assert replicas.size() == 5 : StringUtils.join(replicas, ",");
        assert !replicas.endpoints().contains(InetAddressAndPort.getByName("127.0.0.5"));
        Util.assertNotRCEquals(replicas, initial);

        // test token change
        initial = strategy.getNaturalReplicasForToken(searchToken);
        //move .8 after search token but before other DC3
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(25)), InetAddressAndPort.getByName("127.0.0.8"));
        replicas = strategy.getNaturalReplicasForToken(searchToken);
        assert replicas.size() == 5 : StringUtils.join(replicas, ",");
        Util.assertNotRCEquals(replicas, initial);
    }

    protected static class FakeSimpleStrategy extends SimpleStrategy
    {
        private boolean called = false;

        public FakeSimpleStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalReplicas was already called, result should have been cached";
            called = true;
            return super.calculateNaturalReplicas(token, metadata);
        }
    }

    protected static class FakeOldNetworkTopologyStrategy extends OldNetworkTopologyStrategy
    {
        private boolean called = false;

        public FakeOldNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalReplicas was already called, result should have been cached";
            called = true;
            return super.calculateNaturalReplicas(token, metadata);
        }
    }

    protected static class FakeNetworkTopologyStrategy extends NetworkTopologyStrategy
    {
        private boolean called = false;

        public FakeNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }

        public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata metadata)
        {
            assert !called : "calculateNaturalReplicas was already called, result should have been cached";
            called = true;
            return super.calculateNaturalReplicas(token, metadata);
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
