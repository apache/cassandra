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

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.Token;

public class ReplicationStrategyEndpointCacheTest extends SchemaLoader
{
    private TokenMetadata tmd;
    private Token searchToken;
    private AbstractReplicationStrategy strategy;

    public void setup(Class stratClass) throws Exception
    {
        tmd = new TokenMetadata();
        searchToken = new BigIntegerToken(String.valueOf(15));
        Constructor constructor = stratClass.getConstructor(TokenMetadata.class, IEndpointSnitch.class);
        strategy = (AbstractReplicationStrategy) constructor.newInstance(tmd, new PropertyFileSnitch());

        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(10)), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(20)), InetAddress.getByName("127.0.0.2"));
    }

    @Test
    public void testEndpointsWereCached() throws Exception
    {
        runEndpointsWereCachedTest(FakeRackUnawareStrategy.class);
        runEndpointsWereCachedTest(FakeRackAwareStrategy.class);
        runEndpointsWereCachedTest(FakeDatacenterShardStrategy.class);
    }

    public void runEndpointsWereCachedTest(Class stratClass) throws Exception
    {
        setup(stratClass);
        assert strategy.getNaturalEndpoints(searchToken, "Keyspace3").equals(strategy.getNaturalEndpoints(searchToken, "Keyspace3"));
    }

    @Test
    public void testCacheRespectsTokenChanges() throws Exception
    {
        runCacheRespectsTokenChangesTest(RackUnawareStrategy.class);
        runCacheRespectsTokenChangesTest(RackAwareStrategy.class);
        runCacheRespectsTokenChangesTest(DatacenterShardStrategy.class);
    }

    public void runCacheRespectsTokenChangesTest(Class stratClass) throws Exception
    {
        // TODO DSS is asked to provide a total of 6 replicas, but we never give it 6 endpoints.
        // thus we are testing undefined behavior, at best.
        setup(stratClass);
        ArrayList<InetAddress> endpoints;

        endpoints = strategy.getNaturalEndpoints(searchToken, "Keyspace3");
        assert endpoints.size() == 2 : StringUtils.join(endpoints, ",");

        // test token addition
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(30)), InetAddress.getByName("127.0.0.3"));
        endpoints = strategy.getNaturalEndpoints(searchToken, "Keyspace3");
        assert endpoints.size() == 3 : StringUtils.join(endpoints, ",");

        // test token removal
        tmd.removeEndpoint(InetAddress.getByName("127.0.0.2"));
        endpoints = strategy.getNaturalEndpoints(searchToken, "Keyspace3");
        assert endpoints.size() == 2 : StringUtils.join(endpoints, ",");

        // test token change
        tmd.updateNormalToken(new BigIntegerToken(String.valueOf(30)), InetAddress.getByName("127.0.0.5"));
        endpoints = strategy.getNaturalEndpoints(searchToken, "Keyspace3");
        assert endpoints.size() == 2 : StringUtils.join(endpoints, ",");
        assert endpoints.contains(InetAddress.getByName("127.0.0.5"));
        assert !endpoints.contains(InetAddress.getByName("127.0.0.3"));
    }

    protected static class FakeRackUnawareStrategy extends RackUnawareStrategy
    {
        private boolean called = false;

        public FakeRackUnawareStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
        {
            super(tokenMetadata, snitch);
        }

        @Override
        public Set<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata, String table)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata, table);
        }
    }

    protected static class FakeRackAwareStrategy extends RackAwareStrategy
    {
        private boolean called = false;

        public FakeRackAwareStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
        {
            super(tokenMetadata, snitch);
        }

        @Override
        public Set<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata, String table)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata, table);
        }
    }

    protected static class FakeDatacenterShardStrategy extends DatacenterShardStrategy
    {
        private boolean called = false;

        public FakeDatacenterShardStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch) throws ConfigurationException
        {
            super(tokenMetadata, snitch);
        }

        @Override
        public Set<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata, String table)
        {
            assert !called : "calculateNaturalEndpoints was already called, result should have been cached";
            called = true;
            return super.calculateNaturalEndpoints(token, metadata, table);
        }
    }
}
