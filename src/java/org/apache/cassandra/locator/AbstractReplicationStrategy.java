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
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
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

    protected AbstractReplicationStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
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

    private final Map<Token, ReplicaList> cachedReplicas = new NonBlockingHashMap<>();

    public ReplicaList getCachedReplicas(Token t)
    {
        long lastVersion = tokenMetadata.getRingVersion();

        if (lastVersion > lastInvalidatedVersion)
        {
            synchronized (this)
            {
                if (lastVersion > lastInvalidatedVersion)
                {
                    logger.trace("clearing cached endpoints");
                    cachedReplicas.clear();
                    lastInvalidatedVersion = lastVersion;
                }
            }
        }

        return cachedReplicas.get(t);
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ReplicaList getNaturalReplicas(RingPosition searchPosition)
    {
        Token searchToken = searchPosition.getToken();
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        ReplicaList endpoints = getCachedReplicas(keyToken);
        if (endpoints == null)
        {
            TokenMetadata tm = tokenMetadata.cachedOnlyTokenMap();
            // if our cache got invalidated, it's possible there is a new token to account for too
            keyToken = TokenMetadata.firstToken(tm.sortedTokens(), searchToken);
            endpoints = calculateNaturalReplicas(searchToken, tm);
            cachedReplicas.put(keyToken, endpoints);
        }

        return new ReplicaList(endpoints);
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalReplicas(org.apache.cassandra.dht.RingPosition)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract ReplicaList calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata);

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaCollection naturalEndpoints,
                                                                       ReplicaCollection pendingEndpoints,
                                                                       ConsistencyLevel consistency_level,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime)
    {
        return getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, callback, writeType, queryStartNanoTime, DatabaseDescriptor.getIdealConsistencyLevel());
    }

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaCollection naturalEndpoints,
                                                                       ReplicaCollection pendingEndpoints,
                                                                       ConsistencyLevel consistency_level,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime,
                                                                       ConsistencyLevel idealConsistencyLevel)
    {
        AbstractWriteResponseHandler resultResponseHandler;
        if (consistency_level.isDatacenterLocal())
        {
            // block for in this context will be localnodes block.
            resultResponseHandler = new DatacenterWriteResponseHandler<T>(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType, queryStartNanoTime);
        }
        else if (consistency_level == ConsistencyLevel.EACH_QUORUM && (this instanceof NetworkTopologyStrategy))
        {
            resultResponseHandler = new DatacenterSyncWriteResponseHandler<T>(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType, queryStartNanoTime);
        }
        else
        {
            resultResponseHandler = new WriteResponseHandler<T>(naturalEndpoints, pendingEndpoints, consistency_level, getKeyspace(), callback, writeType, queryStartNanoTime);
        }

        //Check if tracking the ideal consistency level is configured
        if (idealConsistencyLevel != null)
        {
            //If ideal and requested are the same just use this handler to track the ideal consistency level
            //This is also used so that the ideal consistency level handler when constructed knows it is the ideal
            //one for tracking purposes
            if (idealConsistencyLevel == consistency_level)
            {
                resultResponseHandler.setIdealCLResponseHandler(resultResponseHandler);
            }
            else
            {
                //Construct a delegate response handler to use to track the ideal consistency level
                AbstractWriteResponseHandler idealHandler = getWriteResponseHandler(naturalEndpoints,
                                                                                    pendingEndpoints,
                                                                                    idealConsistencyLevel,
                                                                                    callback,
                                                                                    writeType,
                                                                                    queryStartNanoTime,
                                                                                    idealConsistencyLevel);
                resultResponseHandler.setIdealCLResponseHandler(idealHandler);
            }
        }

        return resultResponseHandler;
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
    public abstract ReplicationFactor getReplicationFactor();

    /*
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public ReplicaMultimap<InetAddressAndPort, ReplicaSet> getAddressReplicas(TokenMetadata metadata)
    {
        ReplicaMultimap<InetAddressAndPort, ReplicaSet> map = ReplicaMultimap.set();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (Replica replica : calculateNaturalReplicas(token, metadata))
            {
                // LocalStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                Preconditions.checkState(range.equals(replica.getRange()) || this instanceof LocalStrategy);
                map.put(replica.getEndpoint(), replica);
            }
        }

        return map;
    }

    public ReplicaMultimap<Range<Token>, ReplicaSet> getRangeAddresses(TokenMetadata metadata)
    {
        ReplicaMultimap<Range<Token>, ReplicaSet> map = ReplicaMultimap.set();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (Replica replica : calculateNaturalReplicas(token, metadata))
            {
                // LocalStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                Preconditions.checkState(range.equals(replica.getRange()) || this instanceof LocalStrategy);
                map.put(range, replica);
            }
        }

        return map;
    }

    public ReplicaMultimap<InetAddressAndPort, ReplicaSet> getAddressReplicas()
    {
        return getAddressReplicas(tokenMetadata.cloneOnlyTokenMap());
    }

    public ReplicaSet getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddressAndPort pendingAddress)
    {
        return getPendingAddressRanges(metadata, Collections.singleton(pendingToken), pendingAddress);
    }

    public ReplicaSet getPendingAddressRanges(TokenMetadata metadata, Collection<Token> pendingTokens, InetAddressAndPort pendingAddress)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalTokens(pendingTokens, pendingAddress);
        return getAddressReplicas(temp).get(pendingAddress);
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

    public static void validateReplicationStrategy(String keyspaceName,
                                                   Class<? extends AbstractReplicationStrategy> strategyClass,
                                                   TokenMetadata tokenMetadata,
                                                   IEndpointSnitch snitch,
                                                   Map<String, String> strategyOptions) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, tokenMetadata, snitch, strategyOptions);
        strategy.validateExpectedOptions();
        strategy.validateOptions();
        if (strategy.getReplicationFactor().trans > 0 && !DatabaseDescriptor.isTransientReplicationEnabled())
        {
            throw new ConfigurationException("Transient replication is disabled. Enable in cassandra.yaml to use.");
        }
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

    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return getClass().equals(other.getClass()) && getReplicationFactor().equals(other.getReplicationFactor());
    }

    protected void validateReplicationFactor(String s) throws ConfigurationException
    {
        try
        {
            ReplicationFactor.fromString(s);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    protected void validateExpectedOptions() throws ConfigurationException
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
