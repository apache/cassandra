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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EverywhereStrategyTest
{
    private final Random random = new Random();

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void allRingMembersAreReplicas() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void allRingMembersAreReplicasWithvnodes() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();
        populateTokenMetadata(5, 8, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void bootstrappingNodesAreNotIncludedAsReplicas() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        metadata.addBootstrapTokens(Arrays.asList(getRandomToken()),
                                    InetAddressAndPort.getByName("127.0.0.4"));

        EverywhereStrategy strategy = createStrategy(metadata);

        assertAllNodesCoverFullRing(strategy, metadata);
    }

    @Test
    public void leavingNodesDoNotAddPendingRanges() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);
        InetAddressAndPort leavingEndpoint = metadata.getAllRingMembers().iterator().next();
        metadata.addLeavingEndpoint(leavingEndpoint);

        EverywhereStrategy strategy = createStrategy(metadata);

        metadata.calculatePendingRanges(strategy, strategy.keyspaceName);
        PendingRangeMaps pendingRanges = metadata.getPendingRanges(strategy.keyspaceName);

        assertFalse("pending ranges must be empty",
                    pendingRanges.iterator().hasNext());
    }

    @Test
    public void bootstrapNodesNeedFullRingOnPendingRangesCalculation() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();

        populateTokenMetadata(3, 1, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        InetAddressAndPort bootstrapNode = InetAddressAndPort.getByName("127.0.0.4");
        metadata.addBootstrapTokens(Arrays.asList(getRandomToken()), bootstrapNode);

        metadata.calculatePendingRanges(strategy, strategy.keyspaceName);
        PendingRangeMaps pendingRangeMaps = metadata.getPendingRanges(strategy.keyspaceName);

        List<Range<Token>> pendingRanges = new ArrayList<>();
        for (Map.Entry<Range<Token>, EndpointsForRange.Builder> pendingRangeEntry : pendingRangeMaps)
        {
            EndpointsForRange.Builder pendingNodes = pendingRangeEntry.getValue();
            // only the bootstrap node has pending ranges
            assertEquals(1, pendingNodes.size());
            assertTrue(pendingNodes.endpoints().contains(bootstrapNode));
            pendingRanges.add(pendingRangeEntry.getKey());
        }

        List<Range<Token>> normalizedRanges = Range.normalize(pendingRanges);
        assertEquals(1, normalizedRanges.size());
        Range<Token> tokenRange = normalizedRanges.get(0);
        // it must cover all ranges
        assertEquals(tokenRange.left, tokenRange.right);
    }

    @Test
    public void allRingMembersContributeToReplicationFactor() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();
        populateTokenMetadata(10, 5, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertEquals(10, strategy.getReplicationFactor().fullReplicas);
        assertEquals(10, strategy.getReplicationFactor().allReplicas);
    }

    @Test
    public void noRecognizedOptions() throws Throwable
    {
        TokenMetadata metadata = new TokenMetadata();
        populateTokenMetadata(10, 5, metadata);

        EverywhereStrategy strategy = createStrategy(metadata);

        assertTrue("EverywhereStrategy should have no options", strategy.recognizedOptions().isEmpty());
    }

    private EverywhereStrategy createStrategy(TokenMetadata tokenMetadata)
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);

        return new EverywhereStrategy("keyspace", tokenMetadata, snitch, Collections.emptyMap());
    }

    private void populateTokenMetadata(int nodeCount, int tokens, TokenMetadata metadata) throws UnknownHostException
    {
        List<InetAddressAndPort> nodes = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++)
        {
            InetAddress byName = InetAddress.getByName(String.format("127.0.0.%d", i));
            InetAddressAndPort inetAddressAndPort = InetAddressAndPort.getByAddress(byName);
            nodes.add(inetAddressAndPort);
        }

        for (int i = 0; i < tokens; i++)
        {
            for (InetAddressAndPort node : nodes)
            {
                Token randomToken = getRandomToken();
                metadata.updateNormalToken(randomToken, node);
            }
        }
    }

    private void assertAllNodesCoverFullRing(AbstractReplicationStrategy strategy, TokenMetadata metadata)
    {
        for (Token ringToken : metadata.sortedTokens())
        {
            EndpointsForRange endpointsForRange = strategy.calculateNaturalReplicas(ringToken, metadata);
            assertEquals(metadata.getAllRingMembers().size(), endpointsForRange.size());
            assertEquals(Sets.newHashSet(metadata.getAllRingMembers()), Sets.newHashSet(endpointsForRange.endpoints()));
        }
    }

    private Token getRandomToken()
    {
        return Murmur3Partitioner.instance.getRandomToken(random);
    }
}
