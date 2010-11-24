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

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    public final String table;
    private final TokenMetadata tokenMetadata;
    public final IEndpointSnitch snitch;
    public final Map<String, String> configOptions;

    AbstractReplicationStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        assert table != null;
        assert snitch != null;
        assert tokenMetadata != null;
        this.tokenMetadata = tokenMetadata;
        this.snitch = snitch;
        this.tokenMetadata.register(this);
        this.configOptions = configOptions;
        this.table = table;
    }

    private final Map<Token, ArrayList<InetAddress>> cachedEndpoints = new NonBlockingHashMap<Token, ArrayList<InetAddress>>();

    public ArrayList<InetAddress> getCachedEndpoints(Token t)
    {
        return cachedEndpoints.get(t);
    }

    public void cacheEndpoint(Token t, ArrayList<InetAddress> addr)
    {
        cachedEndpoints.put(t, addr);
    }

    public void clearEndpointCache()
    {
        logger.debug("clearing cached endpoints");
        cachedEndpoints.clear();
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     * @throws IllegalStateException if the number of requested replicas is greater than the number of known endpoints
     */
    public ArrayList<InetAddress> getNaturalEndpoints(Token searchToken) throws IllegalStateException
    {
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        ArrayList<InetAddress> endpoints = getCachedEndpoints(keyToken);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = tokenMetadata.cloneOnlyTokenMap();
            keyToken = TokenMetadata.firstToken(tokenMetadataClone.sortedTokens(), searchToken);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(searchToken, tokenMetadataClone));
            cacheEndpoint(keyToken, endpoints);
            // calculateNaturalEndpoints should have checked this already, this is a safety
            assert getReplicationFactor() <= endpoints.size() : String.format("endpoints %s generated for RF of %s",
                                                                              Arrays.toString(endpoints.toArray()),
                                                                              getReplicationFactor());
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalEndpoints(org.apache.cassandra.dht.Token)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     * @throws IllegalStateException if the number of requested replicas is greater than the number of known endpoints
     */
    public abstract List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata) throws IllegalStateException;

    public IWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> writeEndpoints,
                                                         Multimap<InetAddress, InetAddress> hintedEndpoints,
                                                         ConsistencyLevel consistencyLevel)
    {
        return WriteResponseHandler.create(writeEndpoints, hintedEndpoints, consistencyLevel, table);
    }

    // instance method so test subclasses can override it
    int getReplicationFactor()
    {
       return DatabaseDescriptor.getReplicationFactor(table);
    }

    /**
     * returns <tt>Multimap</tt> of {live destination: ultimate targets}, where if target is not the same
     * as the destination, it is a "hinted" write, and will need to be sent to
     * the ultimate target when it becomes alive again.
     */
    public Multimap<InetAddress, InetAddress> getHintedEndpoints(Collection<InetAddress> targets)
    {
        Multimap<InetAddress, InetAddress> map = HashMultimap.create(targets.size(), 1);

        // first, add the live endpoints
        for (InetAddress ep : targets)
        {
            if (FailureDetector.instance.isAlive(ep))
                map.put(ep, ep);
        }

        // if everything was alive or we're not doing HH on this keyspace, stop with just the live nodes
        if (map.size() == targets.size() || !StorageProxy.isHintedHandoffEnabled())
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
                                    : snitch.getSortedListByProximity(localAddress, map.keySet()).get(0);
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
    public Multimap<InetAddress, Range> getAddressRanges(TokenMetadata metadata)
    {
        Multimap<InetAddress, Range> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(ep, range);
            }
        }

        return map;
    }

    public Multimap<Range, InetAddress> getRangeAddresses(TokenMetadata metadata)
    {
        Multimap<Range, InetAddress> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(range, ep);
            }
        }

        return map;
    }

    public Multimap<InetAddress, Range> getAddressRanges()
    {
        return getAddressRanges(tokenMetadata);
    }

    public Collection<Range> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalToken(pendingToken, pendingAddress);
        return getAddressRanges(temp).get(pendingAddress);
    }

    public QuorumResponseHandler getQuorumResponseHandler(IResponseResolver responseResolver, ConsistencyLevel consistencyLevel)
    {
        return new QuorumResponseHandler(responseResolver, consistencyLevel, table);
    }

    public void invalidateCachedTokenEndpointValues()
    {
        clearEndpointCache();
    }

    public static AbstractReplicationStrategy createReplicationStrategy(String table,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
            throws ConfigurationException
    {
        AbstractReplicationStrategy strategy;
        Class [] parameterTypes = new Class[] {String.class, TokenMetadata.class, IEndpointSnitch.class, Map.class};
        try
        {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
            strategy = constructor.newInstance(table, tokenMetadata, snitch, strategyOptions);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return strategy;
    }

    public static AbstractReplicationStrategy createReplicationStrategy(String table,
                                                                        String strategyClassName,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
            throws ConfigurationException
    {
        Class<AbstractReplicationStrategy> c = FBUtilities.<AbstractReplicationStrategy>classForName(strategyClassName, "replication-strategy");
        return createReplicationStrategy(table, c, tokenMetadata, snitch, strategyOptions);
    }
}
