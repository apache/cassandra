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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DatacenterSyncWriteResponseHandler;
import org.apache.cassandra.service.DatacenterWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.locator.ReplicaLayout.forTokenWriteLiveAndDown;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy
{
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<AbstractReplicationStrategy, Pair> LOCAL_RANGES_UPDATER = AtomicReferenceFieldUpdater.newUpdater(AbstractReplicationStrategy.class, Pair.class, "localRanges");

    public final Map<String, String> configOptions;
    public final ReplicationType replicationType;
    // TODO: remove keyspace name; add a cache that allows going between replication params and replication strategy
    protected final String keyspaceName;

    private volatile Pair<Epoch, RangesAtEndpoint> localRanges;

    protected AbstractReplicationStrategy(String keyspaceName, Map<String, String> configOptions, ReplicationType replicationType)
    {
        this.configOptions = configOptions == null ? Collections.<String, String>emptyMap() : configOptions;
        this.keyspaceName = keyspaceName;
        this.replicationType = replicationType;
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

    public abstract DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata);

    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(CoordinationPlan.ForWrite coordinationPlan,
                                                                       CoordinationPlan.ForWrite idealPlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       Supplier<Mutation> hintOnFailure,
                                                                       Dispatcher.RequestTime requestTime)
    {
        AbstractWriteResponseHandler<T> resultResponseHandler;
        if (coordinationPlan.consistencyLevel().isDatacenterLocal())
        {
            // block for in this context will be localnodes block.
            resultResponseHandler = new DatacenterWriteResponseHandler<T>(coordinationPlan, callback, writeType, hintOnFailure, requestTime);
        }
        else if (coordinationPlan.consistencyLevel() == ConsistencyLevel.EACH_QUORUM && (this instanceof NetworkTopologyStrategy))
        {
            resultResponseHandler = new DatacenterSyncWriteResponseHandler<T>(coordinationPlan, callback, writeType, hintOnFailure, requestTime);
        }
        else
        {
            resultResponseHandler = new WriteResponseHandler<T>(coordinationPlan, callback, writeType, hintOnFailure, requestTime);
        }

        //Check if tracking the ideal consistency level is configured
        if (idealPlan != null)
        {
            //If ideal and requested are the same just use this handler to track the ideal consistency level
            //This is also used so that the ideal consistency level handler when constructed knows it is the ideal
            //one for tracking purposes
            if (coordinationPlan.consistencyLevel() == idealPlan.consistencyLevel())
            {
                resultResponseHandler.setIdealCLResponseHandler(resultResponseHandler);
            }
            else
            {
                // Construct a delegate response handler to track the ideal consistency level.
                // We pass idealPlan twice so that the recursive call sees coordinationPlan == idealPlan,
                // causing the ideal handler to set itself as its own idealCLDelegate. This is required
                // for the idealCLWriteLatency metric to be recorded (only fires when idealCLDelegate == this).
                AbstractWriteResponseHandler<T> idealHandler = getWriteResponseHandler(idealPlan, idealPlan,
                                                                                       callback,
                                                                                       writeType,
                                                                                       hintOnFailure,
                                                                                       requestTime);
                resultResponseHandler.setIdealCLResponseHandler(idealHandler);
            }
        }

        return resultResponseHandler;
    }


    public <T> AbstractWriteResponseHandler<T> getWriteResponseHandler(CoordinationPlan.ForWriteWithIdeal forWritePlan,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       Supplier<Mutation> hintOnFailure,
                                                                       Dispatcher.RequestTime requestTime)
    {
        return getWriteResponseHandler(forWritePlan, forWritePlan.ideal, callback, writeType, hintOnFailure, requestTime);
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

    /** @deprecated See CASSANDRA-17212 */
    @Deprecated(since = "4.1") // use #maybeWarnOnOptions(ClientState) instead
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
                                                              Map<String, String> strategyOptions,
                                                              ReplicationType replicationType)
        throws ConfigurationException
    {
        AbstractReplicationStrategy strategy;
        Class<?>[] parameterTypes = new Class[] {String.class, Map.class, ReplicationType.class};
        try
        {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
            strategy = constructor.newInstance(keyspaceName, strategyOptions, replicationType);
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
                                                                        ReplicationParams replicationParams,
                                                                        ReplicationType replicationType)
    {
        return createReplicationStrategy(keyspaceName, replicationParams.klass, replicationParams.options, replicationType);
    }
    public static AbstractReplicationStrategy createReplicationStrategy(String keyspaceName,
                                                                        Class<? extends AbstractReplicationStrategy> strategyClass,
                                                                        Map<String, String> strategyOptions,
                                                                        ReplicationType replicationType)
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, strategyOptions, replicationType);
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
                                                   ReplicationType replicationType,
                                                   ClientState state) throws ConfigurationException
    {
        AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, strategyOptions, replicationType);
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
                if (rf.fullReplicas == 0)
                    throw new ConfigurationException("Replication factor must have at least one full replica, got " + s);
                if (DatabaseDescriptor.getNumTokens() > 1)
                    throw new ConfigurationException("Transient replication is not supported with vnodes yet");
                if (!replicationType.isTracked())
                    throw new ConfigurationException("Transient replication requires mutation tracking");
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

    public boolean usesMutationTracking()
    {
        return replicationType.isTracked();
    }

    /**
     * Returns local ranges for the epoch specified in the supplied cluster metadata or some later epoch. This caches
     * the resulting RangesAtEndpoint so it should be a little more efficient.
     */
    public RangesAtEndpoint getLocalRanges(ClusterMetadata cm)
    {
        while (true)
        {
            Pair<Epoch, RangesAtEndpoint> localRanges = this.localRanges;
            if (localRanges != null && localRanges.left.isEqualOrAfter(cm.epoch))
                return localRanges.right;

            ClusterMetadata latestMetadata = ClusterMetadata.current();
            RangesAtEndpoint newRanges = getAddressReplicas(latestMetadata, FBUtilities.getBroadcastAddressAndPort());
            Pair<Epoch, RangesAtEndpoint> replacementLocalRanges = Pair.create(latestMetadata.epoch, newRanges);
            if (LOCAL_RANGES_UPDATER.compareAndSet(this, localRanges, replacementLocalRanges))
                return newRanges;
        }
    }

    protected CoordinationPlan.ForWrite planForWriteInternal(ClusterMetadata metadata,
                                                             Keyspace keyspace,
                                                             ConsistencyLevel consistencyLevel,
                                                             Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDown,
                                                             ReplicaPlans.Selector selector)
    {
        ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(metadata, keyspace, consistencyLevel, liveAndDown, selector);
        ResponseTracker tracker = createTrackerForWrite(consistencyLevel, plan, plan.pending, metadata);
        return new CoordinationPlan.ForWrite(plan, tracker);
    }

    public CoordinationPlan.ForWriteWithIdeal planForWrite(ClusterMetadata metadata,
                                                           Keyspace keyspace,
                                                           ConsistencyLevel consistencyLevel,
                                                           Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDown,
                                                           ReplicaPlans.Selector selector)
    {
        CoordinationPlan.ForWrite actual = planForWriteInternal(metadata, keyspace, consistencyLevel, liveAndDown, selector);

        CoordinationPlan.ForWrite ideal = null;
        ConsistencyLevel idealCL = DatabaseDescriptor.getIdealConsistencyLevel();
        if (idealCL != null)
        {
            if (idealCL == consistencyLevel)
            {
                ideal = actual;
            }
            else
            {
                ideal = planForWriteInternal(metadata, keyspace, idealCL, liveAndDown, selector);
            }
        }

        return new CoordinationPlan.ForWriteWithIdeal(actual.replicas(), actual.responses(), ideal);
    }

    public CoordinationPlan.ForWriteWithIdeal planForWrite(ClusterMetadata metadata,
                                                           Keyspace keyspace,
                                                           ConsistencyLevel consistencyLevel,
                                                           Token token,
                                                           ReplicaPlans.Selector selector)
    {
        return planForWrite(metadata, keyspace, consistencyLevel,
                            (newClusterMetadata) -> ReplicaLayout.forTokenWriteLiveAndDown(newClusterMetadata, keyspace, token), selector);
    }

    /**
     * Create coordination plan for forwarding a counter write to the leader replica.
     *
     * In cases where the original coordinator is not a replica of the counter key, the counter
     * mutation is forwarded to a leader replica that will coordinate the actual counter update.
     */
    public CoordinationPlan.ForWrite planForForwardingCounterWrite(ClusterMetadata metadata,
                                                                   Keyspace keyspace,
                                                                   Token token,
                                                                   Function<ClusterMetadata, Replica> replicaSupplier)
    {
        ReplicaPlan.ForWrite plan = ReplicaPlans.forSingleReplicaWrite(metadata, keyspace, token, replicaSupplier);
        ResponseTracker tracker = createTrackerForWrite(plan.consistencyLevel(), plan, plan.pending, metadata);

        return new CoordinationPlan.ForWriteWithIdeal(plan, tracker, null);
    }

    /**
     * Create coordination plan for replaying a mutation from the batchlog.
     *
     * When recovering failed batches, mutations are replayed to remote replicas only
     * (local replica is handled separately). This method creates a replica plan
     * targeting live remote replicas with CL.ONE, and a response tracker that waits on
     * all contacts
     */
    public CoordinationPlan.ForWriteWithIdeal planForReplayMutation(ClusterMetadata metadata,
                                                                    Keyspace keyspace,
                                                                    Token token)
    {
        Preconditions.checkState(!replicationType.isTracked(), "Batch replay not supported with tracked keyspaces");

        ReplicaPlan.ForWrite plan = ReplicaPlans.forReplayMutation(metadata, keyspace, token);

        // wait until all contacts respond
        int blockFor = plan.contacts().size();
        ResponseTracker tracker = new SimpleResponseTracker(blockFor, blockFor);

        return new CoordinationPlan.ForWriteWithIdeal(plan, tracker, null);
    }

    /**
     * Create coordination plan for a single-partition token read.
     */
    public CoordinationPlan.ForTokenRead planForTokenRead(ClusterMetadata metadata,
                                                          Keyspace keyspace,
                                                          TableId tableId,
                                                          Token token,
                                                          @Nullable Index.QueryPlan indexQueryPlan,
                                                          ConsistencyLevel consistencyLevel,
                                                          SpeculativeRetryPolicy retry,
                                                          ReadCoordinator coordinator)
    {
        ReplicaPlan.ForTokenRead plan = ReplicaPlans.forRead(metadata, keyspace, tableId, token, indexQueryPlan, consistencyLevel, retry, coordinator);
        ReplicaPlan.SharedForTokenRead shared = ReplicaPlan.shared(plan);
        ResponseTracker tracker = createTrackerForRead(plan);
        return new CoordinationPlan.ForTokenRead(shared, tracker);
    }

    /**
     * Create coordination plan for a range read.
     */
    public CoordinationPlan.ForRangeRead planForRangeRead(ClusterMetadata metadata,
                                                          Keyspace keyspace,
                                                          TableId tableId,
                                                          @Nullable Index.QueryPlan indexQueryPlan,
                                                          ConsistencyLevel consistencyLevel,
                                                          AbstractBounds<PartitionPosition> range,
                                                          int vnodeCount)
    {
        ReplicaPlan.ForRangeRead plan = ReplicaPlans.forRangeRead(metadata, keyspace, tableId, indexQueryPlan, consistencyLevel, range, vnodeCount, true);
        ReplicaPlan.SharedForRangeRead shared = ReplicaPlan.shared(plan);
        ResponseTracker tracker = createTrackerForRead(plan);
        return new CoordinationPlan.ForRangeRead(shared, tracker);
    }

    /**
     * Attempt to merge two adjacent range read coordination plans into one.
     *
     * If the two plans share enough live endpoints to satisfy the consistency level
     * and the merge is worthwhile returns a merged plan otherwise returns null.
     */
    public CoordinationPlan.ForRangeRead maybeMergeRangeReads(ClusterMetadata metadata,
                                                               Keyspace keyspace,
                                                               TableId tableId,
                                                               ConsistencyLevel consistencyLevel,
                                                               ReplicaPlan.ForRangeRead left,
                                                               ReplicaPlan.ForRangeRead right)
    {
        ReplicaPlan.ForRangeRead merged = ReplicaPlans.maybeMerge(metadata, keyspace, tableId, consistencyLevel, left, right);
        if (merged == null)
            return null;

        ReplicaPlan.SharedForRangeRead shared = ReplicaPlan.shared(merged);
        ResponseTracker tracker = createTrackerForRead(merged);
        return new CoordinationPlan.ForRangeRead(shared, tracker);
    }

    /**
     * Create coordination plan for a full range read
     */
    public CoordinationPlan.ForRangeRead planForFullRangeRead(Keyspace keyspace,
                                                              ConsistencyLevel consistencyLevel,
                                                              AbstractBounds<PartitionPosition> range,
                                                              Set<InetAddressAndPort> endpointsToContact,
                                                              int vnodeCount)
    {
        ReplicaPlan.ForRangeRead plan = ReplicaPlans.forFullRangeRead(keyspace, consistencyLevel, range, endpointsToContact, vnodeCount);
        ReplicaPlan.SharedForRangeRead shared = ReplicaPlan.shared(plan);
        ResponseTracker tracker = createTrackerForRead(plan);
        return new CoordinationPlan.ForRangeRead(shared, tracker);
    }

    /**
     * Create coordination plan for a single-replica token read.
     */
    public CoordinationPlan.ForTokenRead planForSingleReplicaTokenRead(Keyspace keyspace, Token token, Replica replica)
    {
        ReplicaPlan.ForTokenRead plan = ReplicaPlans.forSingleReplicaRead(keyspace, token, replica);
        ReplicaPlan.SharedForTokenRead shared = ReplicaPlan.shared(plan);
        ResponseTracker tracker = createTrackerForRead(plan);
        return new CoordinationPlan.ForTokenRead(shared, tracker);
    }

    /**
     * Create coordination plan for a single-replica range read.
     *
     * Used by short read protection to fetch additional partitions from a
     * specific replica. blockFor=1, totalReplicas=1.
     */
    public CoordinationPlan.ForRangeRead planForSingleReplicaRangeRead(Keyspace keyspace,
                                                                       AbstractBounds<PartitionPosition> range,
                                                                       Replica replica,
                                                                       int vnodeCount)
    {
        ReplicaPlan.ForRangeRead plan = ReplicaPlans.forSingleReplicaRead(keyspace, range, replica, vnodeCount);
        ReplicaPlan.SharedForRangeRead shared = ReplicaPlan.shared(plan);
        ResponseTracker tracker = createTrackerForRead(plan);
        return new CoordinationPlan.ForRangeRead(shared, tracker);
    }

    /**
     * Create ResponseTracker for read operation.
     */
    private  <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> ResponseTracker createTrackerForRead(P plan)
    {
        int blockFor = plan.readQuorum();

        // Use candidates.size() for totalReplicas to allow for speculation
        // (speculation can contact additional candidates beyond initial contacts)
        int totalReplicas = plan.readCandidates().size();

        return new SimpleResponseTracker(blockFor, totalReplicas);
    }

    public Paxos.Participants paxosParticipants(ClusterMetadata metadata,
                                                TableMetadata table,
                                                Token token,
                                                ConsistencyLevel consistencyForConsensus,
                                                Predicate<Replica> isReplicaAlive)
    {

        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaceMetadata(table.keyspace);
        // MetaStrategy distributes the entire keyspace to all replicas. In addition, its tables (currently only
        // the dist log table) don't use the globally configured partitioner. For these reasons we don't lookup the
        // replicas using the supplied token as this can actually be of the incorrect type (for example when
        // performing Paxos repair).
        final Token actualToken = table.partitioner == MetaStrategy.partitioner ? MetaStrategy.entireRange.right : token;
        ReplicaLayout.ForTokenWrite all = forTokenWriteLiveAndDown(metadata, keyspaceMetadata, actualToken);
        ReplicaLayout.ForTokenWrite electorate = consistencyForConsensus.isDatacenterLocal()
                                                 ? all.filter(InOurDc.replicas()) : all;

        EndpointsForToken live = all.all().filter(isReplicaAlive);
        return new Paxos.Participants(metadata.epoch, Keyspace.open(table.keyspace), consistencyForConsensus, all, electorate, live,
                                      (cm) -> Paxos.Participants.get(cm, table, actualToken, consistencyForConsensus));
    }

    /**
     * Hook for replication strategies to send additional mutations alongside a paxos commit.
     * Called from PaxosCommit.start() after local synchronous execution for tracked keyspaces.
     *
     * If the method doesn't return null, the returned future is composed with the paxos consensus:
     * onDone fires only after both the paxos quorum decision AND this future complete, or after
     * one of them fails.
     */
    public Future<Void> sendPaxosCommitMutations(Agreed commit, boolean isUrgent)
    {
        return null;
    }

    /**
     * Check whether paxos operations should be rejected for the given token.
     */
    public boolean shouldRejectPaxos(Token token)
    {
        return false;
    }

    /**
     * Create ResponseTracker for write operation based on consistency level.
     */
    @VisibleForTesting
    public ResponseTracker createTrackerForWrite(ConsistencyLevel cl, ReplicaPlan.ForWrite plan, Endpoints<?> pending, ClusterMetadata metadata)
    {
        switch (cl)
        {
            case ANY:
            case ONE:
            case TWO:
            case THREE:
            case QUORUM:
            case ALL:
            {
                int totalContacts = plan.contacts().size();
                int baseBlockFor = cl.blockFor(this);
                int totalBlockFor = cl.blockForWrite(this, pending);

                // Check if double count model applies (some CLs like ANY don't add pending)
                // If totalBlockFor == baseBlockFor, no double-count needed (e.g., ANY)
                if (totalBlockFor == baseBlockFor)
                    return new SimpleResponseTracker(baseBlockFor, totalContacts);

                // Double count model: natural must satisfy base CL, total must include pending
                int pendingReplicas = pending.size();
                // contacts() includes both natural and pending replicas
                int naturalReplicas = totalContacts - pendingReplicas;
                return new WriteResponseTracker(baseBlockFor, totalBlockFor,
                                                naturalReplicas, pendingReplicas,
                                                endpoint -> pending.endpoints().contains(endpoint));
            }

            case LOCAL_ONE:
            case LOCAL_QUORUM:
            {
                int localContacts = plan.contacts().filter(InOurDc.replicas()).size();
                // Check if double count model applies (depends on local pending)
                int baseBlockFor = cl.blockFor(this);
                int totalBlockFor = cl.blockForWrite(this, pending);

                // If totalBlockFor == baseBlockFor, no local pending so no double-count needed
                if (totalBlockFor == baseBlockFor)
                    return new SimpleResponseTracker(baseBlockFor, localContacts, InOurDc.endpoints());

                // Double count model for local DC
                int localPending = pending.count(InOurDc.replicas());
                // localContacts includes both natural and pending in local DC
                int localNatural = localContacts - localPending;
                return new WriteResponseTracker(baseBlockFor, totalBlockFor,
                                                localNatural, localPending,
                                                endpoint -> pending.endpoints().contains(endpoint),
                                                InOurDc.endpoints());
            }

            case EACH_QUORUM:
                return createPerDcTracker(plan, pending, metadata);

            default:
                throw new UnsupportedOperationException("Unsupported consistency level for writes: " + cl);
        }
    }

    /**
     * Create per-datacenter tracker for EACH_QUORUM.
     */
    private ResponseTracker createPerDcTracker(ReplicaPlan.ForWrite plan, Endpoints<?> pending, ClusterMetadata metadata)
    {
        Map<String, ResponseTracker> trackerPerDc = new HashMap<>();
        Locator locator = metadata.locator;

        // Group replicas by datacenter
        Map<String, List<Replica>> replicasByDc = new HashMap<>();
        for (Replica replica : plan.contacts())
        {
            String dc = locator.location(replica.endpoint()).datacenter;
            replicasByDc.computeIfAbsent(dc, k -> new ArrayList<>()).add(replica);
        }

        // Group pending replicas by datacenter
        Map<String, List<Replica>> pendingByDc = new HashMap<>();
        for (Replica replica : pending)
        {
            String dc = locator.location(replica.endpoint()).datacenter;
            pendingByDc.computeIfAbsent(dc, k -> new ArrayList<>()).add(replica);
        }

        // Create tracker for each DC
        for (Map.Entry<String, List<Replica>> entry : replicasByDc.entrySet())
        {
            String dc = entry.getKey();
            int dcContacts = entry.getValue().size();
            List<Replica> dcPending = pendingByDc.getOrDefault(dc, Collections.emptyList());
            int dcPendingCount = dcPending.size();
            int dcNatural = dcContacts - dcPendingCount;
            int dcBlockFor = dcNatural / 2 + 1;

            // Each sub-tracker must filter by DC since CompositeTracker broadcasts to all children
            Predicate<InetAddressAndPort> dcFilter = endpoint -> dc.equals(locator.location(endpoint).datacenter);

            if (dcPending.isEmpty())
            {
                trackerPerDc.put(dc, new SimpleResponseTracker(dcBlockFor, dcContacts, dcFilter));
            }
            else
            {
                int totalBlockFor = dcBlockFor + dcPendingCount;
                trackerPerDc.put(dc, new WriteResponseTracker(dcBlockFor, totalBlockFor,
                                                              dcNatural, dcPendingCount,
                                                              endpoint -> pending.endpoints().contains(endpoint),
                                                              dcFilter));
            }
        }

        return new CompositeTracker(trackerPerDc.size(), trackerPerDc.values());
    }
}
