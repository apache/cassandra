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
    private static final Logger logger = LoggerFactory.getLogger(TokenAllocation.class);

    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
                                                   final AbstractReplicationStrategy rs,
                                                   final InetAddressAndPort endpoint,
                                                   int numTokens)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12715
        TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
        StrategyAdapter strategy = getStrategy(tokenMetadataCopy, rs, endpoint);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        Collection<Token> tokens = create(tokenMetadata, strategy).addUnit(endpoint, numTokens);
        tokens = adjustForCrossDatacenterClashes(tokenMetadata, strategy, tokens);

        if (logger.isWarnEnabled())
        {
            logger.warn("Selected tokens {}", tokens);
            SummaryStatistics os = replicatedOwnershipStats(tokenMetadataCopy, rs, endpoint);
            tokenMetadataCopy.updateNormalTokens(tokens, endpoint);
            SummaryStatistics ns = replicatedOwnershipStats(tokenMetadataCopy, rs, endpoint);
            logger.warn("Replicated node load in datacenter before allocation {}", statToString(os));
            logger.warn("Replicated node load in datacenter after allocation {}", statToString(ns));

            // TODO: Is it worth doing the replicated ownership calculation always to be able to raise this alarm?
            if (ns.getStandardDeviation() > os.getStandardDeviation())
                logger.warn("Unexpected growth in standard deviation after allocation.");
        }
        return tokens;
    }

    public static Collection<Token> allocateTokens(final TokenMetadata tokenMetadata,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15260
                                                   final int replicas,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
                                                   final InetAddressAndPort endpoint,
                                                   int numTokens)
    {
        TokenMetadata tokenMetadataCopy = tokenMetadata.cloneOnlyTokenMap();
        StrategyAdapter strategy = getStrategy(tokenMetadataCopy, replicas, endpoint);
        Collection<Token> tokens = create(tokenMetadata, strategy).addUnit(endpoint, numTokens);
        tokens = adjustForCrossDatacenterClashes(tokenMetadata, strategy, tokens);
        logger.warn("Selected tokens {}", tokens);
        // SummaryStatistics is not implemented for `allocate_tokens_for_local_replication_factor`
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
                InetAddressAndPort other = tokenMetadata.getEndpoint(t);
                if (strategy.inAllocationRing(other))
                    throw new ConfigurationException(String.format("Allocated token %s already assigned to node %s. Is another node also allocating tokens?", t, other));
                t = t.increaseSlightly();
            }
            filtered.add(t);
        }
        return filtered;
    }

    // return the ratio of ownership for each endpoint
    public static Map<InetAddressAndPort, Double> evaluateReplicatedOwnership(TokenMetadata tokenMetadata, AbstractReplicationStrategy rs)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
        Map<InetAddressAndPort, Double> ownership = Maps.newHashMap();
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

    static void addOwnership(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, Token current, Token next, Map<InetAddressAndPort, Double> ownership)
    {
        double size = current.size(next);
        Token representative = current.getPartitioner().midpoint(current, next);
        for (InetAddressAndPort n : rs.calculateNaturalReplicas(representative, tokenMetadata).endpoints())
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
                                                             AbstractReplicationStrategy rs, InetAddressAndPort endpoint)
    {
        SummaryStatistics stat = new SummaryStatistics();
        StrategyAdapter strategy = getStrategy(tokenMetadata, rs, endpoint);
        for (Map.Entry<InetAddressAndPort, Double> en : evaluateReplicatedOwnership(tokenMetadata, rs).entrySet())
        {
            // Filter only in the same datacentre.
            if (strategy.inAllocationRing(en.getKey()))
                stat.addValue(en.getValue() / tokenMetadata.getTokens(en.getKey()).size());
        }
        return stat;
    }

    static TokenAllocator<InetAddressAndPort> create(TokenMetadata tokenMetadata, StrategyAdapter strategy)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
        NavigableMap<Token, InetAddressAndPort> sortedTokens = new TreeMap<>();
        for (Map.Entry<Token, InetAddressAndPort> en : tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap().entrySet())
        {
            if (strategy.inAllocationRing(en.getValue()))
                sortedTokens.put(en.getKey(), en.getValue());
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12777
        return TokenAllocatorFactory.createTokenAllocator(sortedTokens, strategy, tokenMetadata.partitioner);
    }

    interface StrategyAdapter extends ReplicationStrategy<InetAddressAndPort>
    {
        // return true iff the provided endpoint occurs in the same virtual token-ring we are allocating for
        // i.e. the set of the nodes that share ownership with the node we are allocating
        // alternatively: return false if the endpoint's ownership is independent of the node we are allocating tokens for
        boolean inAllocationRing(InetAddressAndPort other);
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final AbstractReplicationStrategy rs, final InetAddressAndPort endpoint)
    {
        if (rs instanceof NetworkTopologyStrategy)
            return getStrategy(tokenMetadata, (NetworkTopologyStrategy) rs, rs.snitch, endpoint);
        if (rs instanceof SimpleStrategy)
            return getStrategy(tokenMetadata, (SimpleStrategy) rs, endpoint);
        throw new ConfigurationException("Token allocation does not support replication strategy " + rs.getClass().getSimpleName());
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final SimpleStrategy rs, final InetAddressAndPort endpoint)
    {
        final int replicas = rs.getReplicationFactor().allReplicas;

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
                return unit;
            }

            @Override
            public boolean inAllocationRing(InetAddressAndPort other)
            {
                return true;
            }
        };
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final NetworkTopologyStrategy rs, final IEndpointSnitch snitch, final InetAddressAndPort endpoint)
    {
        final String dc = snitch.getDatacenter(endpoint);
        final int replicas = rs.getReplicationFactor(dc).allReplicas;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15260
        return getStrategy(tokenMetadata, replicas, snitch, endpoint);
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final int replicas, final InetAddressAndPort endpoint)
    {
        return getStrategy(tokenMetadata, replicas, DatabaseDescriptor.getEndpointSnitch(), endpoint);
    }

    static StrategyAdapter getStrategy(final TokenMetadata tokenMetadata, final int replicas, final IEndpointSnitch snitch, final InetAddressAndPort endpoint)
    {
        final String dc = snitch.getDatacenter(endpoint);
        if (replicas == 0 || replicas == 1)
        {
            // No replication, each node is treated as separate.
            return new StrategyAdapter()
            {
                @Override
                public int replicas()
                {
                    return 1;
                }

                @Override
                public Object getGroup(InetAddressAndPort unit)
                {
                    return unit;
                }

                @Override
                public boolean inAllocationRing(InetAddressAndPort other)
                {
                    return dc.equals(snitch.getDatacenter(other));
                }
            };
        }

        Topology topology = tokenMetadata.getTopology();

        // if topology hasn't been setup yet for this endpoint+rack then treat it as a separate unit
        int racks = topology.getDatacenterRacks().get(dc) != null && topology.getDatacenterRacks().get(dc).containsKey(snitch.getRack(endpoint))
                ? topology.getDatacenterRacks().get(dc).asMap().size()
                : 1;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15600
        if (racks > replicas)
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
                    return snitch.getRack(unit);
                }

                @Override
                public boolean inAllocationRing(InetAddressAndPort other)
                {
                    return dc.equals(snitch.getDatacenter(other));
                }
            };
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15600
        else if (racks == replicas)
        {
            // When the number of racks is the same as the replication factor, everything must replicate exactly once
            // in each rack. This is the same as having independent rings from each rack.
            final String rack = snitch.getRack(endpoint);
            return new StrategyAdapter()
            {
                @Override
                public int replicas()
                {
                    return 1;
                }

                @Override
                public Object getGroup(InetAddressAndPort unit)
                {
                    return unit;
                }

                @Override
                public boolean inAllocationRing(InetAddressAndPort other)
                {
                    return dc.equals(snitch.getDatacenter(other)) && rack.equals(snitch.getRack(other));
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
                public Object getGroup(InetAddressAndPort unit)
                {
                    return unit;
                }

                @Override
                public boolean inAllocationRing(InetAddressAndPort other)
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

