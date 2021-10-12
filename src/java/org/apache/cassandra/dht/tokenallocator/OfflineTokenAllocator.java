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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.OutputHandler;

public class OfflineTokenAllocator
{
    public static List<FakeNode> allocate(int rf, int numTokens, int[] nodesPerRack, OutputHandler logger, IPartitioner partitioner)
    {
        Preconditions.checkArgument(rf > 0, "rf must be greater than zero");
        Preconditions.checkArgument(numTokens > 0, "num_tokens must be greater than zero");
        Preconditions.checkNotNull(nodesPerRack);
        Preconditions.checkArgument(nodesPerRack.length > 0, "nodesPerRack must contain a node count for at least one rack");
        Preconditions.checkNotNull(logger);
        Preconditions.checkNotNull(partitioner);

        int nodes = Arrays.stream(nodesPerRack).sum();

        Preconditions.checkArgument(nodes >= rf,
                                    "not enough nodes %s for rf %s in %s", Arrays.stream(nodesPerRack).sum(), rf, Arrays.toString(nodesPerRack));

        List<FakeNode> fakeNodes = new ArrayList<>(nodes);
        MultinodeAllocator allocator = new MultinodeAllocator(rf, numTokens, logger, partitioner);

        // Defensive-copy method argument
        nodesPerRack = Arrays.copyOf(nodesPerRack, nodesPerRack.length);

        int racks = nodesPerRack.length;
        int nodeId = 0;
        int rackId = 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        while (nodesPerRack[rackId] > 0)
        {
            // Allocate tokens for current node
            fakeNodes.add(allocator.allocateTokensForNode(nodeId++, rackId));

            // Find next rack with unallocated node
            int nextRack = (rackId+1) % racks;
            while (nodesPerRack[nextRack] == 0 && nextRack != rackId)
                nextRack = (nextRack+1) % racks;

            // Update nodesPerRack and rackId
            nodesPerRack[rackId]--;
            rackId = nextRack;
        }
        return fakeNodes;
    }

    public static class FakeNode
    {
        private final InetAddressAndPort fakeAddressAndPort;
        private final int rackId;
        private final Collection<Token> tokens;

        public FakeNode(InetAddressAndPort address, Integer rackId, Collection<Token> tokens)
        {
            this.fakeAddressAndPort = address;
            this.rackId = rackId;
            // Sort tokens for better presentation
            this.tokens = new TreeSet<>(tokens);
        }

        public int nodeId()
        {
            return fakeAddressAndPort.getPort();
        }

        public int rackId()
        {
            return rackId;
        }

        public Collection<Token> tokens()
        {
            return tokens;
        }
    }

    private static class MultinodeAllocator
    {
        private final FakeSnitch fakeSnitch;
        private final TokenMetadata fakeMetadata;
        private final TokenAllocation allocation;
        private final Map<Integer, SummaryStatistics> lastCheckPoint = Maps.newHashMap();
        private final OutputHandler logger;

        private MultinodeAllocator(int rf, int numTokens, OutputHandler logger, IPartitioner partitioner)
        {
            this.fakeSnitch = new FakeSnitch();
            this.fakeMetadata = new TokenMetadata(fakeSnitch).cloneWithNewPartitioner(partitioner);
            this.allocation = TokenAllocation.create(fakeSnitch, fakeMetadata, rf, numTokens);
            this.logger = logger;
        }

        private FakeNode allocateTokensForNode(int nodeId, Integer rackId)
        {
            // Update snitch and token metadata info
            InetAddressAndPort fakeNodeAddressAndPort = getLoopbackAddressWithPort(nodeId);
            fakeSnitch.nodeByRack.put(fakeNodeAddressAndPort, rackId);
            fakeMetadata.updateTopology(fakeNodeAddressAndPort);

            // Allocate tokens
            Collection<Token> tokens = allocation.allocate(fakeNodeAddressAndPort);

            // Validate ownership stats
            validateAllocation(nodeId, rackId);

            return new FakeNode(fakeNodeAddressAndPort, rackId, tokens);
        }

        private void validateAllocation(int nodeId, int rackId)
        {
            SummaryStatistics newOwnership = allocation.getAllocationRingOwnership(SimpleSnitch.DATA_CENTER_NAME, Integer.toString(rackId));
            SummaryStatistics oldOwnership = lastCheckPoint.put(rackId, newOwnership);
            if (oldOwnership != null)
                logger.debug(String.format("Replicated node load in rack=%d before allocating node %d: %s.", rackId, nodeId,
                                           TokenAllocation.statToString(oldOwnership)));
            logger.debug(String.format("Replicated node load in rack=%d after allocating node %d: %s.", rackId, nodeId,
                                       TokenAllocation.statToString(newOwnership)));
            if (oldOwnership != null && oldOwnership.getStandardDeviation() != 0.0)
            {
                double stdDevGrowth = newOwnership.getStandardDeviation() - oldOwnership.getStandardDeviation();
                if (stdDevGrowth > TokenAllocation.WARN_STDEV_GROWTH)
                {
                    logger.warn(String.format("Growth of %.2f%% in token ownership standard deviation after allocating node %d on rack %d above warning threshold of %d%%",
                                              stdDevGrowth * 100, nodeId, rackId, (int)(TokenAllocation.WARN_STDEV_GROWTH * 100)));
                }
            }
        }
    }

    private static class FakeSnitch extends SimpleSnitch
    {
        final Map<InetAddressAndPort, Integer> nodeByRack = new HashMap<>();

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return Integer.toString(nodeByRack.get(endpoint));
        }
    }

    private static InetAddressAndPort getLoopbackAddressWithPort(int port)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.0.0.1"), port);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException("Unexpected UnknownHostException", e);
        }
    }
}
