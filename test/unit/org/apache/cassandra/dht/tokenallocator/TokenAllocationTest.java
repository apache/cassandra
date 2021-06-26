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

package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TokenAllocationTest
{
    static IPartitioner oldPartitioner;
    static Random rand = new Random(1);

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("TokenAllocationTest");
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
    }

    private static TokenAllocation createForTest(TokenMetadata tokenMetadata, int replicas, int numTokens)
    {
        return TokenAllocation.create(DatabaseDescriptor.getEndpointSnitch(), tokenMetadata, replicas, numTokens);
    }

    @Test
    public void testAllocateTokensForKeyspace() throws UnknownHostException
    {
        int vn = 16;
        String ks = "TokenAllocationTestKeyspace3";
        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        allocateTokensForKeyspace(vn, ks, tm, addr);
    }

    @Test
    public void testAllocateTokensForLocalRF() throws UnknownHostException
    {
        int vn = 16;
        int allocateTokensForLocalRf = 3;
        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        allocateTokensForLocalReplicationFactor(vn, allocateTokensForLocalRf, tm, addr);
    }

    private Collection<Token> allocateTokensForKeyspace(int vnodes, String keyspace, TokenMetadata tm, InetAddressAndPort addr)
    {
        AbstractReplicationStrategy rs = Keyspace.open(keyspace).getReplicationStrategy();
        TokenAllocation tokenAllocation = TokenAllocation.create(tm, rs, vnodes);
        return allocateAndVerify(vnodes, addr, tokenAllocation);
    }

    private void allocateTokensForLocalReplicationFactor(int vnodes, int rf, TokenMetadata tm, InetAddressAndPort addr)
    {
        TokenAllocation tokenAllocation = createForTest(tm, rf, vnodes);
        allocateAndVerify(vnodes, addr, tokenAllocation);
    }

    private Collection<Token> allocateAndVerify(int vnodes, InetAddressAndPort addr, TokenAllocation tokenAllocation)
    {
        SummaryStatistics os = tokenAllocation.getAllocationRingOwnership(addr);
        Collection<Token> tokens = tokenAllocation.allocate(addr);
        assertEquals(vnodes, tokens.size());
        SummaryStatistics ns = tokenAllocation.getAllocationRingOwnership(addr);
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
        return tokens;
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRack() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 3);
    }

    @Test(expected = ConfigurationException.class)
    public void testAllocateTokensNetworkStrategyTwoRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(2, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyThreeRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(3, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyFiveRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(5, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRackOneReplica() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 1);
    }


    public void testAllocateTokensNetworkStrategy(int rackCount, int replicas) throws UnknownHostException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
            int vn = 16;
            String ks = "TokenAllocationTestNTSKeyspace" + rackCount + replicas;
            String dc = "1";

            // Register peers with expected DC for NetworkTopologyStrategy.
            TokenMetadata metadata = StorageService.instance.getTokenMetadata();
            metadata.clearUnsafe();
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.99"));
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.15.0.99"));

            SchemaLoader.createKeyspace(ks, KeyspaceParams.nts(dc, replicas, "15", 15), SchemaLoader.standardCFMD(ks, "Standard1"));
            TokenMetadata tm = StorageService.instance.getTokenMetadata();
            tm.clearUnsafe();
            for (int i = 0; i < rackCount; ++i)
                generateFakeEndpoints(tm, 10, vn, dc, Integer.toString(i));
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + ".0.99");
            allocateTokensForKeyspace(vn, ks, tm, addr);
            // Note: Not matching replication factor in second datacentre, but this should not affect us.
        } finally {
            DatabaseDescriptor.setEndpointSnitch(oldSnitch);
        }
    }

    @Test
    public void testAllocateTokensRfEqRacks() throws UnknownHostException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
            int vn = 8;
            int replicas = 3;
            int rackCount = replicas;
            String ks = "TokenAllocationTestNTSKeyspaceRfEqRacks";
            String dc = "1";

            TokenMetadata metadata = StorageService.instance.getTokenMetadata();
            metadata.clearUnsafe();
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.99"));
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.15.0.99"));

            SchemaLoader.createKeyspace(ks, KeyspaceParams.nts(dc, replicas, "15", 15), SchemaLoader.standardCFMD(ks, "Standard1"));
            int base = 5;
            for (int i = 0; i < rackCount; ++i)
                generateFakeEndpoints(metadata, base << i, vn, dc, Integer.toString(i));     // unbalanced racks

            int cnt = 5;
            for (int i = 0; i < cnt; ++i)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByName("127." + dc + ".0." + (99 + i));
                Collection<Token> tokens = allocateTokensForKeyspace(vn, ks, metadata, endpoint);
                metadata.updateNormalTokens(tokens, endpoint);
            }

            double target = 1.0 / (base + cnt);
            double permittedOver = 1.0 / (2 * vn + 1) + 0.01;

            Map<InetAddress, Float> ownership = StorageService.instance.effectiveOwnership(ks);
            boolean failed = false;
            for (Map.Entry<InetAddress, Float> o : ownership.entrySet())
            {
                int rack = o.getKey().getAddress()[2];
                if (rack != 0)
                    continue;

                System.out.format("Node %s owns %f ratio to optimal %.2f\n", o.getKey(), o.getValue(), o.getValue() / target);
                if (o.getValue()/target > 1 + permittedOver)
                    failed = true;
            }
            Assert.assertFalse(String.format("One of the nodes in the rack has over %.2f%% overutilization.", permittedOver * 100), failed);
        } finally {
            DatabaseDescriptor.setEndpointSnitch(oldSnitch);
        }
    }


    /**
     * TODO: This scenario isn't supported very well. Investigate a multi-keyspace version of the algorithm.
     */
    @Test
    public void testAllocateTokensMultipleKeyspaces() throws UnknownHostException
    {
        final int TOKENS = 16;

        TokenMetadata tokenMetadata = new TokenMetadata();
        generateFakeEndpoints(tokenMetadata, 10, TOKENS);

        // Do not clone token metadata so tokens allocated by different allocators are reflected on the parent TokenMetadata
        TokenAllocation rf2Allocator = createForTest(tokenMetadata, 2, TOKENS);
        TokenAllocation rf3Allocator = createForTest(tokenMetadata, 3, TOKENS);

        SummaryStatistics rf2StatsBefore = rf2Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());
        SummaryStatistics rf3StatsBefore = rf3Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());

        TokenAllocation current = rf3Allocator;
        TokenAllocation next = rf2Allocator;

        for (int i=11; i<=20; ++i)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + (i + 1));
            Collection<Token> tokens = current.allocate(endpoint);
            // Update tokens on next to verify ownership calculation below
            next.tokenMetadata.updateNormalTokens(tokens, endpoint);
            TokenAllocation tmp = current;
            current = next;
            next = tmp;
        }

        SummaryStatistics rf2StatsAfter = rf2Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());
        SummaryStatistics rf3StatsAfter = rf3Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());

        verifyImprovement(rf2StatsBefore, rf2StatsAfter);
        verifyImprovement(rf3StatsBefore, rf3StatsAfter);
    }

    private void verifyImprovement(SummaryStatistics os, SummaryStatistics ns)
    {
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes) throws UnknownHostException
    {
        tmd.clearUnsafe();
        generateFakeEndpoints(tmd, numOldNodes, numVNodes, "0", "0");
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int nodes, int vnodes, String dc, String rack) throws UnknownHostException
    {
        System.out.printf("Adding %d nodes to dc=%s, rack=%s.%n", nodes, dc, rack);
        IPartitioner p = tmd.partitioner;

        for (int i = 1; i <= nodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + '.' + rack + '.' + (i + 1));
            List<Token> tokens = Lists.newArrayListWithCapacity(vnodes);
            for (int j = 0; j < vnodes; ++j)
                tokens.add(p.getRandomToken(rand));
            tmd.updateNormalTokens(tokens, addr);
        }
    }
}
