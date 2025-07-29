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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AbstractReplicationStrategyTest
{
    private static final String KEYSPACE_NAME = "AbstractReplicationStrategyTest";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE_NAME, KeyspaceParams.simple(2));
        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
    }

    @Test
    public void testReplicaCache()
    {
        AbstractReplicationStrategy.ReplicaCache<Integer, Integer> cache = new AbstractReplicationStrategy.ReplicaCache<>();

        cache.put(10, 1, 1);
        assertEquals(1, (int)cache.get(10, 1));
        assertNull(cache.get(9,1)); // get with old ringversion, return null to force a recalculation
        assertNull(cache.get(11,1)); // newer ringVersion - cache gets cleared
        assertNull(cache.get(10,1)); // and make sure the map got cleared

        cache.put(11, 1, 100);
        cache.put(10, 1, 99);
        assertEquals(100, (int)cache.get(11, 1));
        assertNull(cache.get(12, 55));
        assertNull(cache.get(11, 1));
    }

    @Test
    public void testCacheInvalidationOnRingVersionChange() throws UnknownHostException
    {
        IEndpointSnitch snitch = new SimpleSnitch();

        int numEndpoints = 30;
        for (int i = 0; i < numEndpoints - 1; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + i);
            Token token = new Murmur3Partitioner.LongToken(ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, Long.MAX_VALUE));
            StorageService.instance.tokenMetadata.updateNormalTokens(Collections.singleton(token), endpoint);
        }

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "3");
        AbstractReplicationStrategy strategy = new SimpleStrategy(KEYSPACE_NAME, StorageService.instance.getTokenMetadata(), snitch, configOptions);

        TokenMetadata oldTokenMetadata = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap();
        long initialRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        List<Token> initTokens = StorageService.instance.getTokenMetadata().sortedTokens();

        // Verify cache is returning the same result
        for (int i = 0; i < numEndpoints - 1; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + i);
            RangesAtEndpoint cacheResult = strategy.getAddressReplicasWithCache(endpoint);
            RangesAtEndpoint calResult = strategy.getAddressReplicas(endpoint);
            assertTrue("Results should match", Iterables.elementsEqual(cacheResult, calResult));
        }

        // Add a new token to bump ring version
        InetAddressAndPort newEndpoint = InetAddressAndPort.getByName("127.0.0." + numEndpoints);
        Token newToken = new Murmur3Partitioner.LongToken(ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, Long.MAX_VALUE));
        StorageService.instance.tokenMetadata.updateNormalTokens(Collections.singleton(newToken), newEndpoint);

        long newRingVersion = StorageService.instance.tokenMetadata.getRingVersion();
        assertTrue("Ring version should have changed", newRingVersion != initialRingVersion);

        // old ringVersion is still accessible
        for (Token token : initTokens) {
            assertNotNull(strategy.getCachedReplicas(initialRingVersion, token));
        }

        // invalidate old ring cache once it calculates the replicas for the newer ringVersion
        strategy.getNaturalReplicasFromMetadata(List.of(newToken), StorageService.instance.tokenMetadata);

        for (Token token : initTokens) {
            assertNull("Old ring version should not return cached results", strategy.getCachedReplicas(initialRingVersion, token));
        }

        for (int i = 0; i < numEndpoints; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + i);
            RangesAtEndpoint cacheResult = strategy.getAddressReplicasWithCache(StorageService.instance.getTokenMetadata().cachedOnlyTokenMap(), endpoint);
            RangesAtEndpoint calResult = strategy.getAddressReplicas(StorageService.instance.getTokenMetadata().cachedOnlyTokenMap(), endpoint);
            // New calls should work correctly with new tokenMetadata
            assertTrue("Results should match", Iterables.elementsEqual(cacheResult, calResult));
            // ... even we somehow were reading from old token metadata, the result will still be based on the given tokenMetadata
            cacheResult = strategy.getAddressReplicasWithCache(oldTokenMetadata, endpoint);
            calResult = strategy.getAddressReplicas(oldTokenMetadata, endpoint);
            assertTrue("Results should match after cache invalidation", Iterables.elementsEqual(cacheResult, calResult));
        }
    }
}
