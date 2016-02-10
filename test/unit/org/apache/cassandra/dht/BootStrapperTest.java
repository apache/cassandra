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
package org.apache.cassandra.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BootStrapperTest
{
    static IPartitioner oldPartitioner;

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("BootStrapperTest");
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            int replicationFactor = Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor();
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(keyspaceName, clusterSize, replicationFactor);
        }
    }

    private RangeStreamer testSourceTargetComputation(String keyspaceName, int numOldNodes, int replicationFactor) throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();

        generateFakeEndpoints(numOldNodes);
        Token myToken = tmd.partitioner.getRandomToken();
        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");

        assertEquals(numOldNodes, tmd.sortedTokens().size());
        RangeStreamer s = new RangeStreamer(tmd, null, myEndpoint, "Bootstrap", true, DatabaseDescriptor.getEndpointSnitch(), new StreamStateStore(), false);
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddress ep)
            {
                return true;
            }

            public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddress ep) { throw new UnsupportedOperationException(); }
        };
        s.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(mockFailureDetector));
        s.addRanges(keyspaceName, Keyspace.open(keyspaceName).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));

        Collection<Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch = s.toFetch().get(keyspaceName);

        // Check we get get RF new ranges in total
        Set<Range<Token>> ranges = new HashSet<>();
        for (Map.Entry<InetAddress, Collection<Range<Token>>> e : toFetch)
            ranges.addAll(e.getValue());

        assertEquals(replicationFactor, ranges.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.iterator().next().getValue().size() > 0;
        assert !toFetch.iterator().next().getKey().equals(myEndpoint);
        return s;
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        generateFakeEndpoints(StorageService.instance.getTokenMetadata(), numOldNodes, 1);
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes) throws UnknownHostException
    {
        tmd.clearUnsafe();
        generateFakeEndpoints(tmd, numOldNodes, numVNodes, "0", "0");
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes, String dc, String rack) throws UnknownHostException
    {
        IPartitioner p = tmd.partitioner;

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddress addr = InetAddress.getByName("127." + dc + "." + rack + "." + (i + 1));
            List<Token> tokens = Lists.newArrayListWithCapacity(numVNodes);
            for (int j = 0; j < numVNodes; ++j)
                tokens.add(p.getRandomToken());
            
            tmd.updateNormalTokens(tokens, addr);
        }
    }
    
    @Test
    public void testAllocateTokens() throws UnknownHostException
    {
        int vn = 16;
        String ks = "BootStrapperTestKeyspace3";
        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        InetAddress addr = FBUtilities.getBroadcastAddress();
        allocateTokensForNode(vn, ks, tm, addr);
    }

    public void testAllocateTokensNetworkStrategy(int rackCount, int replicas) throws UnknownHostException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
            int vn = 16;
            String ks = "BootStrapperTestNTSKeyspace" + rackCount + replicas;
            String dc = "1";
            SchemaLoader.createKeyspace(ks, KeyspaceParams.nts(dc, replicas, "15", 15), SchemaLoader.standardCFMD(ks, "Standard1"));
            TokenMetadata tm = new TokenMetadata();
            tm.clearUnsafe();
            for (int i = 0; i < rackCount; ++i)
                generateFakeEndpoints(tm, 10, vn, dc, Integer.toString(i));
            InetAddress addr = InetAddress.getByName("127." + dc + ".0.99");
            allocateTokensForNode(vn, ks, tm, addr);
            // Note: Not matching replication factor in second datacentre, but this should not affect us.
        } finally {
            DatabaseDescriptor.setEndpointSnitch(oldSnitch);
        }
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

    private void allocateTokensForNode(int vn, String ks, TokenMetadata tm, InetAddress addr)
    {
        SummaryStatistics os = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks).getReplicationStrategy(), addr);
        Collection<Token> tokens = BootStrapper.allocateTokens(tm, addr, ks, vn);
        assertEquals(vn, tokens.size());
        tm.updateNormalTokens(tokens, addr);
        SummaryStatistics ns = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks).getReplicationStrategy(), addr);
        verifyImprovement(os, ns);
    }

    private void verifyImprovement(SummaryStatistics os, SummaryStatistics ns)
    {
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
    }

    
    @Test
    public void testAllocateTokensMultipleKeyspaces() throws UnknownHostException
    {
        // TODO: This scenario isn't supported very well. Investigate a multi-keyspace version of the algorithm.
        int vn = 16;
        String ks3 = "BootStrapperTestKeyspace4"; // RF = 3
        String ks2 = "BootStrapperTestKeyspace5"; // RF = 2

        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        
        InetAddress dcaddr = FBUtilities.getBroadcastAddress();
        SummaryStatistics os3 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks3).getReplicationStrategy(), dcaddr);
        SummaryStatistics os2 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks2).getReplicationStrategy(), dcaddr);
        String cks = ks3;
        String nks = ks2;
        for (int i=11; i<=20; ++i)
        {
            allocateTokensForNode(vn, cks, tm, InetAddress.getByName("127.0.0." + (i + 1)));
            String t = cks; cks = nks; nks = t;
        }
        
        SummaryStatistics ns3 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks3).getReplicationStrategy(), dcaddr);
        SummaryStatistics ns2 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks2).getReplicationStrategy(), dcaddr);
        verifyImprovement(os3, ns3);
        verifyImprovement(os2, ns2);
    }
}
