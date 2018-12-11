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
import java.lang.reflect.Method;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
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

    private final Map<Token, EndpointsForRange> cachedReplicas = new NonBlockingHashMap<>();

    public EndpointsForRange getCachedReplicas(Token t)
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
    public EndpointsForToken getNaturalReplicasForToken(RingPosition searchPosition)
    {
        return getNaturalReplicas(searchPosition).forToken(searchPosition.getToken());
    }

    public EndpointsForRange getNaturalReplicas(RingPosition searchPosition)
    {
        Token searchToken = searchPosition.getToken();
        Token keyToken = TokenMetadata.firstToken(tokenMetadata.sortedTokens(), searchToken);
        EndpointsForRange endpoints = getCachedReplicas(keyToken);
        if (endpoints == null)
        {
            TokenMetadata tm = tokenMetadata.cachedOnlyTokenMap();
            // if our cache got invalidated, it's possible there is a new token to account for too
            keyToken = TokenMetadata.firstToken(tm.sortedTokens(), searchToken);
            endpoints = calculateNaturalReplicas(searchToken, tm);
            cachedReplicas.put(keyToken, endpoints);
        }

        return endpoints;
    }

    public Replica getLocalReplicaFor(RingPosition searchPosition)
    {
        return getNaturalReplicas(searchPosition)
               .byEndpoint()
               .get(FBUtilities.getBroadcastAddressAndPort());
    }

    /**
     * Calculate the natural endpoints for the given token. Endpoints are returned in the order
     * they occur in the ring following the searchToken, as defined by the replication strategy.
     *
     * Note that the order of the replicas is _implicitly relied upon_ by the definition of
     * "primary" range in
     * {@link org.apache.cassandra.service.StorageService#getPrimaryRangesForEndpoint(String, InetAddressAndPort)}
     * which is in turn relied on by various components like repair and size estimate calculations.
     *
     * @see #getNaturalReplicasForToken(org.apache.cassandra.dht.RingPosition)
     *
     * @param tokenMetadata the token metadata used to find the searchToken, e.g. contains token to endpoint
     *                      mapping information
     * @param searchToken the token to find the natural endpoints for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract EndpointsForRange calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata);

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime)
    {
        return getWriteResponseHandler(replicaPlan, callback, writeType, queryStartNanoTime, DatabaseDescriptor.getIdealConsistencyLevel());
    }

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime,
                                                                       ConsistencyLevel idealConsistencyLevel)
    {
        AbstractWriteResponseHandler resultResponseHandler;
        if (replicaPlan.consistencyLevel().isDatacenterLocal())
        {
            // block for in this context will be localnodes block.
            resultResponseHandler = new DatacenterWriteResponseHandler<T>(replicaPlan, callback, writeType, queryStartNanoTime);
        }
        else if (replicaPlan.consistencyLevel() == ConsistencyLevel.EACH_QUORUM && (this instanceof NetworkTopologyStrategy))
        {
            resultResponseHandler = new DatacenterSyncWriteResponseHandler<T>(replicaPlan, callback, writeType, queryStartNanoTime);
        }
        else
        {
            resultResponseHandler = new WriteResponseHandler<T>(replicaPlan, callback, writeType, queryStartNanoTime);
        }

        //Check if tracking the ideal consistency level is configured
        if (idealConsistencyLevel != null)
        {
            //If ideal and requested are the same just use this handler to track the ideal consistency level
            //This is also used so that the ideal consistency level handler when constructed knows it is the ideal
            //one for tracking purposes
            if (idealConsistencyLevel == replicaPlan.consistencyLevel())
            {
                resultResponseHandler.setIdealCLResponseHandler(resultResponseHandler);
            }
            else
            {
                //Construct a delegate response handler to use to track the ideal consistency level
                AbstractWriteResponseHandler idealHandler = getWriteResponseHandler(replicaPlan.withConsistencyLevel(idealConsistencyLevel),
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

    public boolean hasTransientReplicas()
    {
        return getReplicationFactor().hasTransientReplicas();
    }

    /*
     * NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     * this is fine as long as we don't use this on any critical path.
     * (fixing this would probably require merging tokenmetadata into replicationstrategy,
     * so we could cache/invalidate cleanly.)
     */
    public RangesByEndpoint getAddressReplicas(TokenMetadata metadata)
    {
        RangesByEndpoint.Builder map = new RangesByEndpoint.Builder();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (Replica replica : calculateNaturalReplicas(token, metadata))
            {
                // LocalStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                Preconditions.checkState(range.equals(replica.range()) || this instanceof LocalStrategy);
                map.put(replica.endpoint(), replica);
            }
        }

        return map.build();
    }

    public RangesAtEndpoint getAddressReplicas(TokenMetadata metadata, InetAddressAndPort endpoint)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);
        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            Replica replica = calculateNaturalReplicas(token, metadata)
                    .byEndpoint().get(endpoint);
            if (replica != null)
            {
                // LocalStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                Preconditions.checkState(range.equals(replica.range()) || this instanceof LocalStrategy);
                builder.add(replica, Conflict.DUPLICATE);
            }
        }
        return builder.build();
    }


    public EndpointsByRange getRangeAddresses(TokenMetadata metadata)
    {
        EndpointsByRange.Builder map = new EndpointsByRange.Builder();

        for (Token token : metadata.sortedTokens())
        {
            Range<Token> range = metadata.getPrimaryRangeFor(token);
            for (Replica replica : calculateNaturalReplicas(token, metadata))
            {
                // LocalStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                Preconditions.checkState(range.equals(replica.range()) || this instanceof LocalStrategy);
                map.put(range, replica);
            }
        }

        return map.build();
    }

    public RangesByEndpoint getAddressReplicas()
    {
        return getAddressReplicas(tokenMetadata.cloneOnlyTokenMap());
    }

    public RangesAtEndpoint getAddressReplicas(InetAddressAndPort endpoint)
    {
        return getAddressReplicas(tokenMetadata.cloneOnlyTokenMap(), endpoint);
    }

    public RangesAtEndpoint getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddressAndPort pendingAddress)
    {
        return getPendingAddressRanges(metadata, Collections.singleton(pendingToken), pendingAddress);
    }

    public RangesAtEndpoint getPendingAddressRanges(TokenMetadata metadata, Collection<Token> pendingTokens, InetAddressAndPort pendingAddress)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalTokens(pendingTokens, pendingAddress);
        return getAddressReplicas(temp, pendingAddress);
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

    /**
     * Before constructing the ARS we first give it a chance to prepare the options map in any way it
     * would like to. For example datacenter auto-expansion or other templating to make the user interface
     * more usable. Note that this may mutate the passed strategyOptions Map.
     *
     * We do this prior to the construction of the strategyClass itself because at that point the option
     * map is already immutable and comes from {@link org.apache.cassandra.schema.ReplicationParams}
     * (and should probably stay that way so we don't start having bugs related to ReplicationParams being mutable).
     * Instead ARS classes get a static hook here via the prepareOptions(Map, Map) method to mutate the user input
     * before it becomes an immutable part of the ReplicationParams.
     *
     * @param strategyClass The class to call prepareOptions on
     * @param strategyOptions The proposed strategy options that will be potentially mutated by the prepareOptions
     *                        method.
     * @param previousStrategyOptions In the case of an ALTER statement, the previous strategy options of this class.
     *                                This map cannot be mutated.
     */
    public static void prepareReplicationStrategyOptions(Class<? extends AbstractReplicationStrategy> strategyClass,
                                                         Map<String, String> strategyOptions,
                                                         Map<String, String> previousStrategyOptions)
    {
        try
        {
            Method method = strategyClass.getDeclaredMethod("prepareOptions", Map.class, Map.class);
            method.invoke(null, strategyOptions, previousStrategyOptions);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ign)
        {
            // If the subclass doesn't specify a prepareOptions method, then that means that it
            // doesn't want to do anything to the options. So do nothing on reflection related exceptions.
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
        if (strategy.hasTransientReplicas() && !DatabaseDescriptor.isTransientReplicationEnabled())
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
            ReplicationFactor rf = ReplicationFactor.fromString(s);
            if (rf.hasTransientReplicas())
            {
                if (DatabaseDescriptor.getNumTokens() > 1)
                    throw new ConfigurationException(String.format("Transient replication is not supported with vnodes yet"));
            }
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
