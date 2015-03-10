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
package org.apache.cassandra.locator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.DatacenterSyncWriteResponseHandler;
import org.apache.cassandra.service.DatacenterWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    @VisibleForTesting
    final String keyspaceName;
    private Keyspace keyspace;
    public final Map<String, String> configOptions;
    private final TokenMetadata tokenMetadata;

    // track when the token range changes, signaling we need to invalidate our endpoint cache
    private volatile long lastInvalidatedVersion = 0;

    public IEndpointSnitch snitch;

    AbstractReplicationStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        assert keyspaceName != null;
        assert snitch != null;
        assert tokenMetadata != null;
        this.tokenMetadata = tokenMetadata;
        this.snitch = snitch;
        this.configOptions = configOptions == null ? Collections.<String, String>emptyMap() : configOptions;
        this.keyspaceName = keyspaceName;
        // lazy-initialize keyspace itself since we don't create them until after the replication strategies
    }

    private final Map<Token, ArrayList<InetAddress>> cachedEndpoints = new NonBlockingHashMap<Token, ArrayList<InetAddress>>();

    public ArrayList<InetAddress> getCachedEndpoints(Token t)
    {
        long lastVersion = tokenMetadata.getRingVersion();

        if (lastVersion > lastInvalidatedVersion)
        {
            synchronized (this)
            {
                if (lastVersion > lastInvalidatedVersion)
                {
                    logger.debug("clearing cached endpoints");
                    cachedEndpoints.clear();
                    lastInvalidatedVersion = lastVersion;
                }
            }
        }

        return cachedEndpoints.get(t);
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ArrayList<InetAddress> getNaturalEndpoints(RingPosition searchPosition)
    {
        Token searchToken = searchPosition.getToken();
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        ArrayList<InetAddress> endpoints = getCachedEndpoints(keyToken);
        if (endpoints == null)
        {
            TokenMetadata tm = tokenMetadata.cachedOnlyTokenMap();
            // if our cache got invalidated, it's possible there is a new token to account for too
            keyToken = TokenMetadata.firstToken(tm.sortedTokens(), searchToken);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(searchToken, tm));
            cachedEndpoints.put(keyToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalEndpoints(org.apache.cassandra.dht.RingPosition)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata);

    public AbstractWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints, ConsistencyLevel consistency_level, Runnable callback, WriteType writeType)
    {
        if (consistency_level.isDatacenterLocal())
        {
            // block for in this context will be localnodes block.
            return new DatacenterWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType);
        }
        else if (consistency_level == ConsistencyLevel.EACH_QUORUM && (this instanceof NetworkTopologyStrategy))
        {
            return new DatacenterSyncWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType);
        }
        return new WriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType);
    }

    private Keyspace getKeyspace()
    {
        if (keyspace == null)
            keyspace = Keyspace.open(keyspaceName);
        return keyspace;
    }

    /**
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

    /*
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public Multimap<InetAddress, Range<Token>> getAddressRanges(TokenMetadata metadata)
    {
        Multimap<InetAddress, Range<Token>> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(ep, range);
            }
        }

        return map;
    }

    public Multimap<Range<Token>, InetAddress> getRangeAddresses(TokenMetadata metadata)
    {
        Multimap<Range<Token>, InetAddress> map = HashMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata))
            {
                map.put(range, ep);
            }
        }

        return map;
    }

    public Multimap<InetAddress, Range<Token>> getAddressRanges()
    {
        return getAddressRanges(tokenMetadata.cloneOnlyTokenMap());
    }

    public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress)
    {
        return getPendingAddressRanges(metadata, Arrays.asList(pendingToken), pendingAddress);
    }

    public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Collection<Token> pendingTokens, InetAddress pendingAddress)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalTokens(pendingTokens, pendingAddress);
        return getAddressRanges(temp).get(pendingAddress);
    }

    public abstract void validateOptions() throws ConfigurationException;

    /*
     * The options recognized by the strategy.
     * The empty collection means that no options are accepted, but null means
     * that any option is accepted.
     */
    public Collection<String> recognizedOptions()
    {
        // We default to null for backward compatibility sake
        return null;
    }

    private static AbstractReplicationStrategy createInternal(String keyspaceName,
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
            strategy = constructor.newInstance(keyspaceName, tokenMetadata, snitch, strategyOptions);
        }
        catch (InvocationTargetException e)
        {
            Throwable targetException = e.getTargetException();
            throw new ConfigurationException(targetException.getMessage(), targetException);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Error constructing replication strategy class", e);
        }
        return strategy;
    }

    public static AbstractReplicationStrategy createReplicationStrategy(String keyspaceName,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        TokenMetadata tokenMetadata,
                                                                        IEndpointSnitch snitch,
                                                                        Map<String, String> strategyOptions)
    {
        try
        {
            AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, tokenMetadata, snitch, strategyOptions);

            // Because we used to not properly validate unrecognized options, we only log a warning if we find one.
            try
            {
                strategy.validateExpectedOptions();
            }
            catch (ConfigurationException e)
            {
                logger.warn("Ignoring {}", e.getMessage());
            }

            strategy.validateOptions();
            return strategy;
        }
        catch (ConfigurationException e)
        {
            // If that happens at this point, there is nothing we can do about it.
            throw new RuntimeException(e);
        }
    }

    public static void validateReplicationStrategy(String keyspaceName,
                                                   Class<? extends AbstractReplicationStrategy> strategyClass,
                                                   TokenMetadata tokenMetadata,
                                                   IEndpointSnitch snitch,
                                                   Map<String, String> strategyOptions) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, tokenMetadata, snitch, strategyOptions);
        strategy.validateExpectedOptions();
        strategy.validateOptions();
    }

    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException
    {
        String className = cls.contains(".") ? cls : "org.apache.cassandra.locator." + cls;
        Class<AbstractReplicationStrategy> strategyClass = FBUtilities.classForName(className, "replication strategy");
        if (!AbstractReplicationStrategy.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(String.format("Specified replication strategy class (%s) is not derived from AbstractReplicationStrategy", className));
        }
        return strategyClass;
    }

    protected void validateReplicationFactor(String rf) throws ConfigurationException
    {
        try
        {
            if (Integer.parseInt(rf) < 0)
            {
                throw new ConfigurationException("Replication factor must be non-negative; found " + rf);
            }
        }
        catch (NumberFormatException e2)
        {
            throw new ConfigurationException("Replication factor must be numeric; found " + rf);
        }
    }

    private void validateExpectedOptions() throws ConfigurationException
    {
        Collection expectedOptions = recognizedOptions();
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet())
        {
            if (!expectedOptions.contains(key))
                throw new ConfigurationException(String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s", key, getClass().getSimpleName(), keyspaceName));
        }
    }
}
