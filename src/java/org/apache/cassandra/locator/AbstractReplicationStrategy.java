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

import java.net.InetAddress;
import java.util.*;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.IResponseResolver;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.QuorumResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * This class contains a helper method that will be used by
 * all abstraction that implement the IReplicaPlacementStrategy
 * interface.
*/
public abstract class AbstractReplicationStrategy
{
    protected static final Logger logger_ = Logger.getLogger(AbstractReplicationStrategy.class);

    protected TokenMetadata tokenMetadata_;
    protected IPartitioner partitioner_;
    protected int replicas_;
    protected int storagePort_;

    AbstractReplicationStrategy(TokenMetadata tokenMetadata, IPartitioner partitioner, int replicas, int storagePort)
    {
        tokenMetadata_ = tokenMetadata;
        partitioner_ = partitioner;
        replicas_ = replicas;
        storagePort_ = storagePort;
    }

    public abstract ArrayList<InetAddress> getNaturalEndpoints(Token token, Map<Token, InetAddress> tokenToEndPointMap);
    
    public <T> QuorumResponseHandler<T> getResponseHandler(IResponseResolver<T> responseResolver, int blockFor, int consistency_level) throws InvalidRequestException
    {
        return new QuorumResponseHandler<T>(blockFor, responseResolver);
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token token)
    {
        return getNaturalEndpoints(token, tokenMetadata_.cloneTokenEndPointMap());
    }
    
    /*
     * This method returns the hint map. The key is the endpoint
     * on which the data is being placed and the value is the
     * endpoint to which it should be forwarded.
     */
    public Map<InetAddress, InetAddress> getHintedEndpoints(Token token, Collection<InetAddress> naturalEndpoints)
    {
        return getHintedMapForEndpoints(getWriteEndpoints(token, naturalEndpoints));
    }

    /**
     * write endpoints may be different from read endpoints, because read endpoints only need care about the
     * "natural" nodes for a token, but write endpoints also need to account for nodes that are bootstrapping
     * into the ring, and write data there too so that they stay up to date during the bootstrap process.
     * Thus, this method may return more nodes than the Replication Factor.
     *
     * Only ReplicationStrategy should care about this method (higher level users should only ask for Hinted).
     */
    public ArrayList<InetAddress> getWriteEndpoints(Token token, Collection<InetAddress> naturalEndpoints)
    {
        Map<Token, InetAddress> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Map<Token, InetAddress> bootstrapTokensToEndpointMap = tokenMetadata_.cloneBootstrapNodes();
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(naturalEndpoints);

        for (Token t : bootstrapTokensToEndpointMap.keySet())
        {
            InetAddress ep = bootstrapTokensToEndpointMap.get(t);
            tokenToEndPointMap.put(t, ep);
            try
            {
                for (Range r : getAddressRanges(tokenToEndPointMap).get(ep))
                {
                    if (r.contains(token))
                    {
                        endpoints.add(ep);
                        break;
                    }
                }
            }
            finally
            {
                tokenToEndPointMap.remove(t);
            }
        }

        return endpoints;
    }

    private Map<InetAddress, InetAddress> getHintedMapForEndpoints(Iterable<InetAddress> topN)
    {
        Set<InetAddress> usedEndpoints = new HashSet<InetAddress>();
        Map<InetAddress, InetAddress> map = new HashMap<InetAddress, InetAddress>();

        for (InetAddress ep : topN)
        {
            if (FailureDetector.instance().isAlive(ep))
            {
                map.put(ep, ep);
                usedEndpoints.add(ep);
            }
            else
            {
                // find another endpoint to store a hint on.  prefer endpoints that aren't already in use
                InetAddress hintLocation = null;
                Map<Token, InetAddress> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
                List tokens = new ArrayList(tokenToEndPointMap.keySet());
                Collections.sort(tokens);
                Token token = tokenMetadata_.getToken(ep);
                int index = Collections.binarySearch(tokens, token);
                if (index < 0)
                {
                    index = (index + 1) * (-1);
                    if (index >= tokens.size()) // handle wrap
                        index = 0;
                }
                int totalNodes = tokens.size();
                int startIndex = (index + 1) % totalNodes;
                for (int i = startIndex, count = 1; count < totalNodes; ++count, i = (i + 1) % totalNodes)
                {
                    InetAddress tmpEndPoint = tokenToEndPointMap.get(tokens.get(i));
                    if (FailureDetector.instance().isAlive(tmpEndPoint) && !Arrays.asList(topN).contains(tmpEndPoint) && !usedEndpoints.contains(tmpEndPoint))
                    {
                        hintLocation = tmpEndPoint;
                        break;
                    }
                }
                // if all endpoints are already in use, might as well store it locally to save the network trip
                if (hintLocation == null)
                    hintLocation = FBUtilities.getLocalAddress();

                map.put(hintLocation, ep);
                usedEndpoints.add(hintLocation);
            }
        }
        return map;
    }

    // TODO this is pretty inefficient. also the inverse (getRangeAddresses) below.
    // fixing this probably requires merging tokenmetadata into replicationstrategy, so we can cache/invalidate cleanly
    public Map<InetAddress, Set<Range>> getAddressRanges(Map<Token, InetAddress> tokenMap)
    {
        Map<InetAddress, Set<Range>> map = new HashMap<InetAddress, Set<Range>>();

        for (InetAddress ep : tokenMap.values())
        {
            map.put(ep, new HashSet<Range>());
        }

        for (Token token : tokenMap.keySet())
        {
            Range range = getPrimaryRangeFor(token, tokenMap);
            for (InetAddress ep : getNaturalEndpoints(token, tokenMap))
            {
                map.get(ep).add(range);
            }
        }

        return map;
    }

    public Map<Range, Set<InetAddress>> getRangeAddresses(Map<Token, InetAddress> tokenMap)
    {
        Map<Range, Set<InetAddress>> map = new HashMap<Range, Set<InetAddress>>();

        for (Token token : tokenMap.keySet())
        {
            Range range = getPrimaryRangeFor(token, tokenMap);
            HashSet<InetAddress> addresses = new HashSet<InetAddress>();
            for (InetAddress ep : getNaturalEndpoints(token, tokenMap))
            {
                addresses.add(ep);
            }
            map.put(range, addresses);
        }

        return map;
    }

    public Map<InetAddress, Set<Range>> getAddressRanges()
    {
        return getAddressRanges(tokenMetadata_.cloneTokenEndPointMap());
    }

    public Range getPrimaryRangeFor(Token right, Map<Token, InetAddress> tokenToEndPointMap)
    {
        return new Range(getPredecessor(right, tokenToEndPointMap), right);
    }

    public Token getPredecessor(Token token, Map<Token, InetAddress> tokenToEndPointMap)
    {
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        return (Token) (index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(--index));
    }

    public Token getSuccessor(Token token, Map<Token, InetAddress> tokenToEndPointMap)
    {
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        return (Token) ((index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(++index));
    }
}
