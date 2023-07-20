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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.TokenMetadata.Topology;

public class TokenAllocation
{
    public static final double WARN_STDEV_GROWTH = 0.05;

    private static final Logger logger = LoggerFactory.getLogger(TokenAllocation.class);
    final TokenMetadata tokenMetadata;
    final AbstractReplicationStrategy replicationStrategy;
    final int numTokens;
    final Map<String, Map<String, StrategyAdapter>> strategyByRackDc = new HashMap<>();

    private TokenAllocation(TokenMetadata tokenMetadata, AbstractReplicationStrategy replicationStrategy, int numTokens)
    {
        this.tokenMetadata = tokenMetadata.cloneOnlyTokenMap();
        this.replicationStrategy = replicationStrategy;
        this.numTokens = numTokens;
    }

    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
                                                   final AbstractReplicationStrategy rs,
                                                   final InetAddressAndPort endpoint,
                                                   int numTokens)
    {
        return create(tokenMetadata, rs, numTokens).allocate(endpoint);
    }

    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
                                                   final int replicas,
                                                   final InetAddressAndPort endpoint,
                                                   int numTokens)
    {
        return create(DatabaseDescriptor.getEndpointSnitch(), tokenMetadata, replicas, numTokens).allocate(endpoint);
    }

    static TokenAllocation create(IEndpointSnitch snitch, TokenMetadata tokenMetadata, int replicas, int numTokens)
    {
        // We create a fake NTS replication strategy with the specified RF in the local DC
        HashMap<String, String> options = new HashMap<>();
        options.put(snitch.getLocalDatacenter(), Integer.toString(replicas));
        NetworkTopologyStrategy fakeReplicationStrategy = new NetworkTopologyStrategy(null, tokenMetadata, snitch, options);

        TokenAllocation allocator = new TokenAllocation(tokenMetadata, fakeReplicationStrategy, numTokens);
        return allocator;
    }

    static TokenAllocation create(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs, int numTokens)
    {
        return new TokenAllocation(tokenMetadata, rs, numTokens);
    }

    Collection<Token> allocate(InetAddressAndPort endpoint)
    {
        StrategyAdapter strategy = getOrCreateStrategy(endpoint);
        Collection<Token> tokens = strategy.createAllocator().addUnit(endpoint, numTokens);
        tokens = strategy.adjustForCrossDatacenterClashes(tokens);

        SummaryStatistics os = strategy.replicatedOwnershipStats();
        tokenMetadata.updateNormalTokens(tokens, endpoint);

        SummaryStatistics ns = strategy.replicatedOwnershipStats();
        logger.info("Selected tokens {}", tokens);
        logger.debug("Replicated node load in datacenter before allocation {}", statToString(os));
        logger.debug("Replicated node load in datacenter after allocation {}", statToString(ns));

        double stdDevGrowth = ns.getStandardDeviation() - os.getStandardDeviation();
        if (stdDevGrowth > TokenAllocation.WARN_STDEV_GROWTH)
        {
            logger.warn(String.format("Growth of %.2f%% in token ownership standard deviation after allocation above warning threshold of %d%%",
                                      stdDevGrowth * 100, (int)(TokenAllocation.WARN_STDEV_GROWTH * 100)));
        }

        return tokens;
    }

    static String statToString(SummaryStatistics stat)
    {
        return String.format("max %.2f min %.2f stddev %.4f", stat.getMax() / stat.getMean(), stat.getMin() / stat.getMean(), stat.getStandardDeviation());
    }

    SummaryStatistics getAllocationRingOwnership(String datacenter, String rack)
    {
        return getOrCreateStrategy(datacenter, rack).replicatedOwnershipStats();
    }

    SummaryStatistics getAllocationRingOwnership(InetAddressAndPort endpoint)
    {
        return getOrCreateStrategy(endpoint).replicatedOwnershipStats();
    }

    abstract class StrategyAdapter implements ReplicationStrategy<InetAddressAndPort>
    {
        // return true iff the provided endpoint occurs in the same virtual token-ring we are allocating for
        // i.e. the set of the nodes that share ownership with the node we are allocating
        // alternatively: return false if the endpoint's ownership is independent of the node we are allocating tokens for
        abstract boolean inAllocationRing(InetAddressAndPort other);

        final TokenAllocator<InetAddressAndPort> createAllocator()
        {
            NavigableMap<Token, InetAddressAndPort> sortedTokens = new TreeMap<>();
            for (Map.Entry<Token, InetAddressAndPort> en : tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().entrySet())
            {
                if (inAllocationRing(en.getValue()))
                    sortedTokens.put(en.getKey(), en.getValue());
            }
            return TokenAllocatorFactory.createTokenAllocator(sortedTokens, this, tokenMetadata.partitioner);
        }

        final Collection<Token> adjustForCrossDatacenterClashes(Collection<Token> tokens)
        {
            List<Token> filtered = Lists.newArrayListWithCapacity(tokens.size());

            for (Token t : tokens)
            {
                while (tokenMetadata.getEndpoint(t) != null)
                {
                    InetAddressAndPort other = tokenMetadata.getEndpoint(t);
                    if (inAllocationRing(other))
                        throw new ConfigurationException(String.format("Allocated token %s already assigned to node %s. Is another node also allocating tokens?", t, other));
                    t = t.nextValidToken();
                }
                filtered.add(t);
            }
            return filtered;
        }

        final SummaryStatistics replicatedOwnershipStats()
        {
            SummaryStatistics stat = new SummaryStatistics();
            for (Map.Entry<InetAddressAndPort, Double> en : evaluateReplicatedOwnership().entrySet())
            {
                // Filter only in the same allocation ring
                if (inAllocationRing(en.getKey()))
                    stat.addValue(en.getValue() / tokenMetadata.getTokens(en.getKey()).size());
            }
            return stat;
        }

        // return the ratio of ownership for each endpoint
        private Map<InetAddressAndPort, Double> evaluateReplicatedOwnership()
        {
            Map<InetAddressAndPort, Double> ownership = Maps.newHashMap();
            List<Token> sortedTokens = tokenMetadata.sortedTokens();
            if (sortedTokens.isEmpty())
                return ownership;

            Iterator<Token> it = sortedTokens.iterator();
            Token current = it.next();
            while (it.hasNext())
            {
                Token next = it.next();
                addOwnership(current, next, ownership);
                current = next;
            }
            addOwnership(current, sortedTokens.get(0), ownership);

            return ownership;
        }

        private void addOwnership(Token current, Token next, Map<InetAddressAndPort, Double> ownership)
        {
            double size = current.size(next);
            Token representative = current.getPartitioner().midpoint(current, next);
            for (InetAddressAndPort n : replicationStrategy.calculateNaturalReplicas(representative, tokenMetadata).endpoints())
            {
                Double v = ownership.get(n);
                ownership.put(n, v != null ? v + size : size);
            }
        }
    }

    private StrategyAdapter getOrCreateStrategy(InetAddressAndPort endpoint)
    {
        String dc = replicationStrategy.snitch.getDatacenter(endpoint);
        String rack = replicationStrategy.snitch.getRack(endpoint);
        return getOrCreateStrategy(dc, rack);
    }

    private StrategyAdapter getOrCreateStrategy(String dc, String rack)
    {
        return strategyByRackDc.computeIfAbsent(dc, k -> new HashMap<>()).computeIfAbsent(rack, k -> createStrategy(dc, rack));
    }

    private StrategyAdapter createStrategy(String dc, String rack)
    {
        if (replicationStrategy instanceof NetworkTopologyStrategy)
            return createStrategy(tokenMetadata, (NetworkTopologyStrategy) replicationStrategy, dc, rack);
        if (replicationStrategy instanceof SimpleStrategy)
            return createStrategy((SimpleStrategy) replicationStrategy);
        throw new ConfigurationException("Token allocation does not support replication strategy " + replicationStrategy.getClass().getSimpleName());
    }

    private StrategyAdapter createStrategy(final SimpleStrategy rs)
    {
        return createStrategy(rs.snitch, null, null, rs.getReplicationFactor().allReplicas, false);
    }

    private StrategyAdapter createStrategy(TokenMetadata tokenMetadata, NetworkTopologyStrategy strategy, String dc, String rack)
    {
        int replicas = strategy.getReplicationFactor(dc).allReplicas;

        Topology topology = tokenMetadata.getTopology();
        // if topology hasn't been setup yet for this dc+rack then treat it as a separate unit
        int racks = topology.getDatacenterRacks().get(dc) != null && topology.getDatacenterRacks().get(dc).containsKey(rack)
                ? topology.getDatacenterRacks().get(dc).asMap().size()
                : 1;

        if (replicas <= 1)
        {
            // each node is treated as separate and replicates once
            return createStrategy(strategy.snitch, dc, null, 1, false);
        }
        else if (racks == replicas)
        {
            // each node is treated as separate and replicates once, with separate allocation rings for each rack
            return createStrategy(strategy.snitch, dc, rack, 1, false);
        }
        else if (racks > replicas)
        {
            // group by rack
            return createStrategy(strategy.snitch, dc, null, replicas, true);
        }
        else if (racks == 1)
        {
            return createStrategy(strategy.snitch, dc, null, replicas, false);
        }

        throw new ConfigurationException(String.format("Token allocation failed: the number of racks %d in datacenter %s is lower than its replication factor %d.",
                                                       racks, dc, replicas));
    }

    // a null dc will always return true for inAllocationRing(..)
    // a null rack will return true for inAllocationRing(..) for all nodes in the same dc
    private StrategyAdapter createStrategy(IEndpointSnitch snitch, String dc, String rack, int replicas, boolean groupByRack)
    {
        return new StrategyAdapter()
        {
            @Override
            public int replicas()
            {
                return replicas;
            }

            @Override
            public Object getGroup(InetAddressAndPort unit)
            {
                return groupByRack ? snitch.getRack(unit) : unit;
            }

            @Override
            public boolean inAllocationRing(InetAddressAndPort other)
            {
                return (dc == null || dc.equals(snitch.getDatacenter(other))) && (rack == null || rack.equals(snitch.getRack(other)));
            }
        };
    }
}

