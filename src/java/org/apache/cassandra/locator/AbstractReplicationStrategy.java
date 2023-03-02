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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import java.util.*;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DatacenterSyncWriteResponseHandler;
import org.apache.cassandra.service.DatacenterWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    public final Map<String, String> configOptions;
    protected final String keyspaceName;

    protected AbstractReplicationStrategy(String keyspaceName, Map<String, String> configOptions)
    {
        this.configOptions = configOptions == null ? Collections.<String, String>emptyMap() : configOptions;
        this.keyspaceName = keyspaceName;
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
     * @param metadata the token metadata used to find the searchToken, e.g. contains token to endpoint
     *                      mapping information
     * @param searchToken the token to find the natural endpoints for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract EndpointsForRange calculateNaturalReplicas(Token searchToken, ClusterMetadata metadata);

    public abstract DataPlacement calculateDataPlacement(List<Range<Token>> ranges, ClusterMetadata metadata);

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       Supplier<Mutation> hintOnFailure,
                                                                       long queryStartNanoTime)
    {
        return getWriteResponseHandler(replicaPlan, callback, writeType, hintOnFailure,
                                       queryStartNanoTime, DatabaseDescriptor.getIdealConsistencyLevel());
    }

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       Supplier<Mutation> hintOnFailure,
                                                                       long queryStartNanoTime,
                                                                       ConsistencyLevel idealConsistencyLevel)
    {
        AbstractWriteResponseHandler<T> resultResponseHandler;
        if (replicaPlan.consistencyLevel().isDatacenterLocal())
        {
            // block for in this context will be localnodes block.
            resultResponseHandler = new DatacenterWriteResponseHandler<T>(replicaPlan, callback, writeType, hintOnFailure, queryStartNanoTime);
        }
        else if (replicaPlan.consistencyLevel() == ConsistencyLevel.EACH_QUORUM && (this instanceof NetworkTopologyStrategy))
        {
            resultResponseHandler = new DatacenterSyncWriteResponseHandler<T>(replicaPlan, callback, writeType, hintOnFailure, queryStartNanoTime);
        }
        else
        {
            resultResponseHandler = new WriteResponseHandler<T>(replicaPlan, callback, writeType, hintOnFailure, queryStartNanoTime);
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
                AbstractWriteResponseHandler<T> idealHandler = getWriteResponseHandler(replicaPlan.withConsistencyLevel(idealConsistencyLevel),
                                                                                       callback,
                                                                                       writeType,
                                                                                       hintOnFailure,
                                                                                       queryStartNanoTime,
                                                                                       idealConsistencyLevel);
                resultResponseHandler.setIdealCLResponseHandler(idealHandler);
            }
        }

        return resultResponseHandler;
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
    public RangesByEndpoint getAddressReplicas(ClusterMetadata metadata)
    {
        RangesByEndpoint.Builder map = new RangesByEndpoint.Builder();
        List<Token> tokens = metadata.tokenMap.tokens();
        for (Token token : tokens)
        {
            for (Range<Token> range : TokenRingUtils.getPrimaryRangesFor(tokens, Collections.singleton(token)))
            {
                for (Replica replica : calculateNaturalReplicas(token, metadata))
                {
                    // SystemStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                    Preconditions.checkState(range.equals(replica.range()) || this instanceof SystemStrategy);
                    map.put(replica.endpoint(), replica);
                }
            }
        }

        return map.build();
    }

    public RangesAtEndpoint getAddressReplicas(ClusterMetadata metadata, InetAddressAndPort endpoint)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);
        List<Token> tokens = metadata.tokenMap.tokens();
        for (Token token : tokens)
        {
            for (Range<Token> range : TokenRingUtils.getPrimaryRangesFor(tokens, Collections.singleton(token)))
            {
                Replica replica = calculateNaturalReplicas(token, metadata)
                                  .byEndpoint().get(endpoint);
                if (replica != null)
                {
                    // SystemStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                    Preconditions.checkState(range.equals(replica.range()) || this instanceof SystemStrategy);
                    builder.add(replica, Conflict.DUPLICATE);
                }
            }
        }
        return builder.build();
    }


    public EndpointsByRange getRangeAddresses(ClusterMetadata metadata)
    {
        EndpointsByRange.Builder map = new EndpointsByRange.Builder();
        List<Token> tokens = metadata.tokenMap.tokens();
        for (Token token : tokens)
        {
            for (Range<Token> range : TokenRingUtils.getPrimaryRangesFor(tokens, Collections.singleton(token)))
            {
                for (Replica replica : calculateNaturalReplicas(token, metadata))
                {
                    // SystemStrategy always returns (min, min] ranges for it's replicas, so we skip the check here
                    Preconditions.checkState(range.equals(replica.range()) || this instanceof SystemStrategy);
                    map.put(range, replica);
                }
            }
        }

        return map.build();
    }

    public abstract void validateOptions() throws ConfigurationException;

    @Deprecated // use #maybeWarnOnOptions(ClientState) instead
    public void maybeWarnOnOptions()
    {
        // nothing to do here
    }

    public void maybeWarnOnOptions(ClientState state)
    {
        maybeWarnOnOptions();
    }


    /*
     * The options recognized by the strategy.
     * The empty collection means that no options are accepted, but null means
     * that any option is accepted.
     */
    public Collection<String> recognizedOptions(ClusterMetadata metadata)
    {
        // We default to null for backward compatibility sake
        return null;
    }

    private static AbstractReplicationStrategy createInternal(String keyspaceName,
                                                              Class<? extends AbstractReplicationStrategy> strategyClass,
                                                              Map<String, String> strategyOptions)
        throws ConfigurationException
    {
        AbstractReplicationStrategy strategy;
        Class<?>[] parameterTypes = new Class[] {String.class, Map.class};
        try
        {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
            strategy = constructor.newInstance(keyspaceName, strategyOptions);
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
                                                                        ReplicationParams replicationParams)
    {
        return createReplicationStrategy(keyspaceName, replicationParams.klass, replicationParams.options);
    }
    public static AbstractReplicationStrategy createReplicationStrategy(String keyspaceName,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        Map<String, String> strategyOptions)
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, strategyOptions);
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
                                                   ClusterMetadata metadata,
                                                   Map<String, String> strategyOptions,
                                                   ClientState state) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, strategyOptions);
        strategy.validateExpectedOptions(metadata);
        strategy.validateOptions();
        strategy.maybeWarnOnOptions(state);
        if (strategy.hasTransientReplicas() && !DatabaseDescriptor.isTransientReplicationEnabled())
        {
            throw new ConfigurationException("Transient replication is disabled. Enable in cassandra.yaml to use.");
        }
    }

    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException
    {
        String className = cls.contains(".") ? cls : "org.apache.cassandra.locator." + cls;

        if ("org.apache.cassandra.locator.OldNetworkTopologyStrategy".equals(className)) // see CASSANDRA-16301 
            throw new ConfigurationException("The support for the OldNetworkTopologyStrategy has been removed in C* version 4.0. The keyspace strategy should be switch to NetworkTopologyStrategy");

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
                    throw new ConfigurationException("Transient replication is not supported with vnodes yet");
            }
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    public void validate(ClusterMetadata snapshot) throws ConfigurationException
    {
        validateExpectedOptions(snapshot);
        validateOptions();
        maybeWarnOnOptions();
        if (hasTransientReplicas() && !DatabaseDescriptor.isTransientReplicationEnabled())
        {
            throw new ConfigurationException("Transient replication is disabled. Enable in cassandra.yaml to use.");
        }
    }

    public void validateExpectedOptions(ClusterMetadata snapshot) throws ConfigurationException
    {
        Collection<String> expectedOptions = recognizedOptions(snapshot);
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet())
        {
            if (!expectedOptions.contains(key))
                throw new ConfigurationException(String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s. Expected options: %s", key, getClass().getSimpleName(), keyspaceName, expectedOptions));
        }
    }
}
