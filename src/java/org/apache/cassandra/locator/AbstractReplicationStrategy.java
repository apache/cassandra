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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.IResponseResolver;
import org.apache.cassandra.service.QuorumResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    private TokenMetadata tokenMetadata;
    protected final IEndpointSnitch snitch;
    private volatile Map<EndpointCacheKey, ArrayList<InetAddress>> cachedEndpoints;

    AbstractReplicationStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
    {
        assert snitch != null;
        assert tokenMetadata != null;
        this.tokenMetadata = tokenMetadata;
        this.snitch = snitch;
        cachedEndpoints = new NonBlockingHashMap<EndpointCacheKey, ArrayList<InetAddress>>();
        this.tokenMetadata.register(this);
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token, for the given table.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchToken the token the natural endpoints are requested for
     * @param table the table the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token and table
     */
    public ArrayList<InetAddress> getNaturalEndpoints(Token searchToken, String table)
    {
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        EndpointCacheKey cacheKey = new EndpointCacheKey(table, keyToken);
        ArrayList<InetAddress> endpoints = cachedEndpoints.get(cacheKey);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = tokenMetadata.cloneOnlyTokenMap();
            keyToken = TokenMetadata.firstToken(tokenMetadataClone.sortedTokens(), searchToken);
            cacheKey = new EndpointCacheKey(table, keyToken);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(searchToken, tokenMetadataClone, table));
            cachedEndpoints.put(cacheKey, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    public abstract Set<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata, String table);

    public AbstractWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> writeEndpoints,
                                                                Multimap<InetAddress, InetAddress> hintedEndpoints,
                                                                ConsistencyLevel consistencyLevel,
                                                                String table)
    {
        return new WriteResponseHandler(writeEndpoints, hintedEndpoints, consistencyLevel, table);
    }
    
    /**
     * returns <tt>Multimap</tt> of {live destination: ultimate targets}, where if target is not the same
     * as the destination, it is a "hinted" write, and will need to be sent to
     * the ultimate target when it becomes alive again.
     */
    public Multimap<InetAddress, InetAddress> getHintedEndpoints(Collection<InetAddress> targets)
    {
        Multimap<InetAddress, InetAddress> map = HashMultimap.create(targets.size(), 1);

        IEndpointSnitch endpointSnitch = DatabaseDescriptor.getEndpointSnitch();

        // first, add the live endpoints
        for (InetAddress ep : targets)
        {
            if (FailureDetector.instance.isAlive(ep))
                map.put(ep, ep);
        }

        // if everything was alive or we're not doing HH on this keyspace, stop with just the live nodes
        if (map.size() == targets.size() || !DatabaseDescriptor.hintedHandoffEnabled())
            return map;

        // assign dead endpoints to be hinted to the closest live one, or to the local node
        // (since it is trivially the closest) if none are alive.  This way, the cost of doing
        // a hint is only adding the hint header, rather than doing a full extra write, if any
        // destination nodes are alive.
        //
        // we do a 2nd pass on targets instead of using temporary storage,
        // to optimize for the common case (everything was alive).
        InetAddress localAddress = FBUtilities.getLocalAddress();
        for (InetAddress ep : targets)
        {
            if (map.containsKey(ep))
                continue;

            InetAddress destination = map.isEmpty()
                                    ? localAddress
                                    : endpointSnitch.getSortedListByProximity(localAddress, map.keySet()).get(0);
            map.put(destination, ep);
        }

        return map;
    }

    /*
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public Multimap<InetAddress, Range> getAddressRanges(TokenMetadata metadata, String table)
    {
        Multimap<InetAddress, Range> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata, table))
            {
                map.put(ep, range);
            }
        }

        return map;
    }

    public Multimap<Range, InetAddress> getRangeAddresses(TokenMetadata metadata, String table)
    {
        Multimap<Range, InetAddress> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata, table))
            {
                map.put(range, ep);
            }
        }

        return map;
    }

    public Multimap<InetAddress, Range> getAddressRanges(String table)
    {
        return getAddressRanges(tokenMetadata, table);
    }

    public Collection<Range> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress, String table)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalToken(pendingToken, pendingAddress);
        return getAddressRanges(temp, table).get(pendingAddress);
    }

    public QuorumResponseHandler getQuorumResponseHandler(IResponseResolver responseResolver, ConsistencyLevel consistencyLevel, String table)
    {
        return new QuorumResponseHandler(responseResolver, consistencyLevel, table);
    }

    protected static class EndpointCacheKey extends Pair<String, Token>
    {
        public EndpointCacheKey(String table, Token keyToken) {super(table, keyToken);}
    }

    protected void clearCachedEndpoints()
    {
        logger.debug("clearing cached endpoints");
        cachedEndpoints = new NonBlockingHashMap<EndpointCacheKey, ArrayList<InetAddress>>();
    }

    public void invalidateCachedTokenEndpointValues()
    {
        clearCachedEndpoints();
    }

    public void invalidateCachedSnitchValues()
    {
        clearCachedEndpoints();
    }
}
