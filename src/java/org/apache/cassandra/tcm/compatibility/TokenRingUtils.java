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

package org.apache.cassandra.tcm.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Functions for interacting with a sorted list of tokens as modelled as a ring
 */
public class TokenRingUtils
{
    private static final Logger logger = LoggerFactory.getLogger(TokenRingUtils.class);

    public static int firstTokenIndex(final List<Token> ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0 : ring.toString();
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token getPredecessor(List<Token> ring, Token start)
    {
        int idx = firstTokenIndex(ring, start, false);
        return ring.get(idx == 0 ? ring.size() - 1 : idx - 1);
    }

    public static Collection<Range<Token>> getPrimaryRangesFor(List<Token> ring, Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<>(getPredecessor(ring, right), right));
        return ranges;
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final List<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(start.getPartitioner().getMinimumToken())
                              : Collections.emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return start.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalReplicas}.
     *
     * @param keyspace Keyspace name to check primary ranges
     * @param ep endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public static Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddressAndPort ep)
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Token> tokens = metadata.tokenMap.tokens();
        for (Token token : tokens)
        {
            EndpointsForRange replicas = strategy.calculateNaturalReplicas(token, metadata);
            if (replicas.size() > 0 && replicas.get(0).endpoint().equals(ep))
            {
                Preconditions.checkState(replicas.get(0).isFull());
                primaryRanges.add(new Range<>(getPredecessor(tokens, token), token));
            }
        }
        return primaryRanges;
    }

    /**
     * Get the "primary ranges" within local DC for the specified keyspace and endpoint.
     *
     * @see #getPrimaryRangesForEndpoint(String, InetAddressAndPort)
     * @param keyspace Keyspace name to check primary ranges
     * @param referenceEndpoint endpoint we are interested in.
     * @return primary ranges within local DC for the specified endpoint.
     */
    public static Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddressAndPort referenceEndpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
        Collection<InetAddressAndPort> localDcNodes = metadata.directory.datacenterEndpoints(localDC);
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

        Collection<Range<Token>> localDCPrimaryRanges = new HashSet<>();
        for (Token token : metadata.tokenMap.tokens())
        {
            EndpointsForRange replicas = strategy.calculateNaturalReplicas(token, metadata);
            for (Replica replica : replicas)
            {
                if (localDcNodes.contains(replica.endpoint()))
                {
                    if (replica.endpoint().equals(referenceEndpoint))
                    {
                        localDCPrimaryRanges.add(new Range<>(getPredecessor(metadata.tokenMap.tokens(), token), token));
                    }
                    break;
                }
            }
        }

        return localDCPrimaryRanges;
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
     */
    public static List<Range<Token>> getAllRanges(List<Token> sortedTokens)
    {
        if (logger.isTraceEnabled())
            logger.trace("computing ranges for {}", StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<>(size + 1);
        for (int i = 1; i < size; ++i)
        {
            Range<Token> range = new Range<>(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range<Token> range = new Range<>(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    public static Range<Token> getRange(List<Token> ring, Token token)
    {
        int idx = firstTokenIndex(ring, token, false);
        Token replicaEnd = ring.get(idx);
        Token replicaStart = ring.get(idx == 0 ? ring.size() - 1 : idx - 1);
        return new Range<>(replicaStart, replicaEnd);
    }
}
