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
import java.util.Collection;
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

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.TokenMetadata.Topology;

public class TokenAllocation
{
    private static final Logger logger = LoggerFactory.getLogger(TokenAllocation.class);

    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
                                                   final AbstractReplicationStrategy rs,
                                                   final InetAddress endpoint,
                                                   int numTokens)
    {
        StrategyAdapter strategy = getStrategy(tokenMetadata, rs, endpoint);
        Collection<Token> tokens = create(tokenMetadata, strategy).addUnit(endpoint, numTokens);
        tokens = adjustForCrossDatacenterClashes(tokenMetadata, strategy, tokens);

        if (logger.isWarnEnabled())
        {
            logger.warn("Selected tokens {}", tokens);
            SummaryStatistics os = replicatedOwnershipStats(tokenMetadata, rs, endpoint);
            TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
            tokenMetadataCopy.updateNormalTokens(tokens, endpoint);
            SummaryStatistics ns = replicatedOwnershipStats(tokenMetadataCopy, rs, endpoint);
            logger.warn("Replicated node load in datacentre before allocation " + statToString(os));
            logger.warn("Replicated node load in datacentre after allocation " + statToString(ns));

            // TODO: Is it worth doing the replicated ownership calculation always to be able to raise this alarm?
            if (ns.getStandardDeviation() > os.getStandardDeviation())
                logger.warn("Unexpected growth in standard deviation after allocation.");
        }
        return tokens;
    }

    private static Collection<Token> adjustForCrossDatacenterClashes(final TokenMetadata tokenMetadata,
                                                                     StrategyAdapter strategy, Collection<Token> tokens)
    {
        List<Token> filtered = Lists.newArrayListWithCapacity(tokens.size());

        for (Token t : tokens)
        {
            while (tokenMetadata.getEndpoint(t) != null)
            {
                InetAddress other = tokenMetadata.getEndpoint(t);
                if (strategy.inAllocationRing(other))
                    throw new ConfigurationException(String.format("Allocated token %s already assigned to node %s. Is another node also allocating tokens?", t, other));
                t = t.increaseSlightly();
            }
            filtered.add(t);
        }
        return filtered;
    }

    // return the ratio of ownership for each endpoint
    public static Map<InetAddress, Double> evaluateReplicatedOwnership(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs)
    {
        Map<InetAddress, Double> ownership = Maps.newHashMap();
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        Iterator<Token> it = sortedTokens.iterator();
        Token current = it.next();
        while (it.hasNext())
        {
            Token next = it.next();
            addOwnership(tokenMetadata, rs, current, next, ownership);
            current = next;
        }
        addOwnership(tokenMetadata, rs, current, sortedTokens.get(0), ownership);

        return ownership;
    }

    static void addOwnership(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, Token current, Token next, Map<InetAddress, Double> ownership)
    {
        double size = current.size(next);
        Token representative = current.getPartitioner().midpoint(current, next);
        for (InetAddress n : rs.calculateNaturalEndpoints(representative, tokenMetadata))
        {
            Double v = ownership.get(n);
            ownership.put(n, v != null ? v + size : size);
        }
    }

    public static String statToString(SummaryStatistics stat)
    {
        return String.format("max %.2f min %.2f stddev %.4f", stat.getMax() / stat.getMean(), stat.getMin() / stat.getMean(), stat.getStandardDeviation());
    }

    public static SummaryStatistics replicatedOwnershipStats(TokenMetadata tokenMetadata,
                                                             AbstractReplicationStrategy rs, InetAddress endpoint)
    {
        SummaryStatistics stat = new SummaryStatistics();
        StrategyAdapter strategy = getStrategy(tokenMetadata, rs, endpoint);
        for (Map.Entry<InetAddress, Double> en : evaluateReplicatedOwnership(tokenMetadata, rs).entrySet())
        {
            // Filter only in the same datacentre.
            if (strategy.inAllocationRing(en.getKey()))
                stat.addValue(en.getValue() / tokenMetadata.getTokens(en.getKey()).size());
        }
        return stat;
    }

    static TokenAllocator<InetAddress> create(TokenMetadata tokenMetadata, StrategyAdapter strategy)
    {
        NavigableMap<Token, InetAddress> sortedTokens = new TreeMap<>();
        for (Map.Entry<Token, InetAddress> en : tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().entrySet())
        {
            if (strategy.inAllocationRing(en.getValue()))
                sortedTokens.put(en.getKey(), en.getValue());
        }
        return new ReplicationAwareTokenAllocator<>(sortedTokens, strategy, tokenMetadata.partitioner);
    }

    interface StrategyAdapter extends ReplicationStrategy<InetAddress>
    {
        // return true iff the provided endpoint occurs in the same virtual token-ring we are allocating for
        // i.e. the set of the nodes that share ownership with the node we are allocating
        // alternatively: return false if the endpoint's ownership is independent of the node we are allocating tokens for
        boolean inAllocationRing(InetAddress other);
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, final InetAddress endpoint)
    {
        if (rs instanceof NetworkTopologyStrategy)
            return getStrategy(tokenMetadata, (NetworkTopologyStrategy) rs, rs.snitch, endpoint);
        if (rs instanceof SimpleStrategy)
            return getStrategy(tokenMetadata, (SimpleStrategy) rs, endpoint);
        throw new ConfigurationException("Token allocation does not support replication strategy " + rs.getClass().getSimpleName());
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final SimpleStrategy rs, final InetAddress endpoint)
    {
        final int replicas = rs.getReplicationFactor();

        return new StrategyAdapter()
        {
            @Override
            public int replicas()
            {
                return replicas;
            }

            @Override
            public Object getGroup(InetAddress unit)
            {
                return unit;
            }

            @Override
            public boolean inAllocationRing(InetAddress other)
            {
                return true;
            }
        };
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final NetworkTopologyStrategy rs, final IEndpointSnitch snitch, final InetAddress endpoint)
    {
        final String dc = snitch.getDatacenter(endpoint);
        final int replicas = rs.getReplicationFactor(dc);

        Topology topology = tokenMetadata.getTopology();
        int racks = topology.getDatacenterRacks().get(dc).asMap().size();

        if (racks >= replicas)
        {
            return new StrategyAdapter()
            {
                @Override
                public int replicas()
                {
                    return replicas;
                }

                @Override
                public Object getGroup(InetAddress unit)
                {
                    return snitch.getRack(unit);
                }

                @Override
                public boolean inAllocationRing(InetAddress other)
                {
                    return dc.equals(snitch.getDatacenter(other));
                }
            };
        }
        else if (racks == 1)
        {
            // One rack, each node treated as separate.
            return new StrategyAdapter()
            {
                @Override
                public int replicas()
                {
                    return replicas;
                }

                @Override
                public Object getGroup(InetAddress unit)
                {
                    return unit;
                }

                @Override
                public boolean inAllocationRing(InetAddress other)
                {
                    return dc.equals(snitch.getDatacenter(other));
                }
            };
        }
        else
            throw new ConfigurationException(
                    String.format("Token allocation failed: the number of racks %d in datacenter %s is lower than its replication factor %d.",
                                  racks, dc, replicas));
    }
}

