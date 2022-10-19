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
package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.locator.Replica.fullReplica;

/**
 * Assists in streaming ranges to this node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    public static Predicate<Replica> ALIVE_PREDICATE = replica ->
                                                             (!Gossiper.instance.isEnabled() ||
                                                              (Gossiper.instance.getEndpointStateForEndpoint(replica.endpoint()) == null ||
                                                               Gossiper.instance.getEndpointStateForEndpoint(replica.endpoint()).isAlive())) &&
                                                             FailureDetector.instance.isAlive(replica.endpoint());

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddressAndPort address;
    /* streaming description */
    private final String description;
    private final Map<String, Multimap<InetAddressAndPort, FetchReplica>> toFetch = new HashMap<>();
    private final List<SourceFilter> sourceFilters = new ArrayList<>();
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;

    public static class FetchReplica
    {
        public final Replica local;
        // Source replica
        public final Replica remote;

        public FetchReplica(Replica local, Replica remote)
        {
            Preconditions.checkNotNull(local);
            Preconditions.checkNotNull(remote);
            assert local.isSelf() && !remote.isSelf();
            this.local = local;
            this.remote = remote;
        }

        public String toString()
        {
            return "FetchReplica{" +
                   "local=" + local +
                   ", remote=" + remote +
                   '}';
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FetchReplica that = (FetchReplica) o;

            if (!local.equals(that.local)) return false;
            return remote.equals(that.remote);
        }

        public int hashCode()
        {
            int result = local.hashCode();
            result = 31 * result + remote.hashCode();
            return result;
        }
    }

    public interface SourceFilter extends Predicate<Replica>
    {
        public boolean apply(Replica replica);
        public String message(Replica replica);
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements SourceFilter
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        @Override
        public boolean apply(Replica replica)
        {
            return fd.isAlive(replica.endpoint());
        }

        @Override
        public String message(Replica replica)
        {
            return "Filtered " + replica + " out because it was down";
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements SourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        @Override
        public boolean apply(Replica replica)
        {
            return snitch.getDatacenter(replica).equals(sourceDc);
        }

        @Override
        public String message(Replica replica)
        {
            return "Filtered " + replica + " out because it does not belong to " + sourceDc + " datacenter";
        }
    }

    /**
    * Source filter which excludes nodes from local DC.
    */
    public static class ExcludeLocalDatacenterFilter implements SourceFilter
    {
        private final IEndpointSnitch snitch;
        private final String localDc;

        public ExcludeLocalDatacenterFilter(IEndpointSnitch snitch)
        {
            this.snitch = snitch;
            this.localDc = snitch.getLocalDatacenter();
        }

        @Override
        public boolean apply(Replica replica)
        {
            return !snitch.getDatacenter(replica).equals(localDc);
        }

        @Override
        public String message(Replica replica)
        {
            return "Filtered " + replica + " out because it belongs to the local datacenter";
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements SourceFilter
    {
        @Override
        public boolean apply(Replica replica)
        {
            return !replica.isSelf();
        }

        @Override
        public String message(Replica replica)
        {
            return "Filtered " + replica + " out because it is local";
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class AllowedSourcesFilter implements SourceFilter
    {
        private final Set<InetAddressAndPort> allowedSources;

        public AllowedSourcesFilter(Set<InetAddressAndPort> allowedSources)
        {
            this.allowedSources = allowedSources;
        }

        public boolean apply(Replica replica)
        {
            return allowedSources.contains(replica.endpoint());
        }

        @Override
        public String message(Replica replica)
        {
            return "Filtered " + replica + " out because it was not in the allowed set: " + allowedSources;
        }
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         InetAddressAndPort address,
                         StreamOperation streamOperation,
                         boolean useStrictConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         int connectionsPerHost)
    {
        this(metadata, tokens, address, streamOperation, useStrictConsistency, snitch, stateStore,
             FailureDetector.instance, connectSequentially, connectionsPerHost);
    }

    RangeStreamer(TokenMetadata metadata,
                  Collection<Token> tokens,
                  InetAddressAndPort address,
                  StreamOperation streamOperation,
                  boolean useStrictConsistency,
                  IEndpointSnitch snitch,
                  StreamStateStore stateStore,
                  IFailureDetector failureDetector,
                  boolean connectSequentially,
                  int connectionsPerHost)
    {
        Preconditions.checkArgument(streamOperation == StreamOperation.BOOTSTRAP || streamOperation == StreamOperation.REBUILD, streamOperation);
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = streamOperation.getDescription();
        this.streamPlan = new StreamPlan(streamOperation, connectionsPerHost, connectSequentially, null, PreviewKind.NONE);
        this.useStrictConsistency = useStrictConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;
        streamPlan.listeners(this.stateStore);

        // We're _always_ filtering out a local node and down sources
        addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(failureDetector));
        addSourceFilter(new RangeStreamer.ExcludeLocalNodeFilter());
    }

    public void addSourceFilter(SourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    // Creates error message from source filters
    private static String buildErrorMessage(Collection<SourceFilter> sourceFilters, ReplicaCollection<?> replicas)
    {
        StringBuilder failureMessage = new StringBuilder();
        for (Replica r : replicas)
        {
            for (SourceFilter filter : sourceFilters)
            {
                if (!filter.apply(r))
                {
                    failureMessage.append(filter.message(r));
                    break;
                }
            }
        }
        return failureMessage.toString();
    }
    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param replicas ranges to be streamed
     */
    public void addRanges(String keyspaceName, ReplicaCollection<?> replicas)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        AbstractReplicationStrategy strat = keyspace.getReplicationStrategy();
        if(strat instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        boolean useStrictSource = useStrictSourcesForRanges(strat);
        EndpointsByReplica fetchMap = calculateRangesToFetchWithPreferredEndpoints(replicas, keyspace, useStrictSource);

        for (Map.Entry<Replica, Replica> entry : fetchMap.flattenEntries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        Multimap<InetAddressAndPort, FetchReplica> workMap;
        //Only use the optimized strategy if we don't care about strict sources, have a replication factor > 1, and no
        //transient replicas.
        if (useStrictSource || strat == null || strat.getReplicationFactor().allReplicas == 1 || strat.getReplicationFactor().hasTransientReplicas())
        {
            workMap = convertPreferredEndpointsToWorkMap(fetchMap);
        }
        else
        {
            workMap = getOptimizedWorkMap(fetchMap, sourceFilters, keyspaceName);
        }

        if (toFetch.put(keyspaceName, workMap) != null)
            throw new IllegalArgumentException("Keyspace is already added to fetch map");

        if (logger.isTraceEnabled())
        {
            for (Map.Entry<InetAddressAndPort, Collection<FetchReplica>> entry : workMap.asMap().entrySet())
            {
                for (FetchReplica r : entry.getValue())
                    logger.trace("{}: range source {} local range {} for keyspace {}", description, r.remote, r.local, keyspaceName);
            }
        }
    }

    /**
     * @param strat AbstractReplicationStrategy of keyspace to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(AbstractReplicationStrategy strat)
    {
        boolean res = useStrictConsistency && tokens != null;
        
        if (res)
        {
            int nodes = 0;

            if (strat instanceof NetworkTopologyStrategy)
            {
                ImmutableMultimap<String, InetAddressAndPort> dc2Nodes = metadata.getDC2AllEndpoints(snitch);

                NetworkTopologyStrategy ntps = (NetworkTopologyStrategy) strat;
                for (String dc : dc2Nodes.keySet())
                    nodes += ntps.getReplicationFactor(dc).allReplicas > 0 ? dc2Nodes.get(dc).size() : 0;
            }
            else
                nodes = metadata.getSizeOfAllEndpoints();
    
            res = nodes > strat.getReplicationFactor().allReplicas;
        }
        
        return res;
    }

    /**
     * Wrapper method to assemble the arguments for invoking the implementation with RangeStreamer's parameters
     */
    private EndpointsByReplica calculateRangesToFetchWithPreferredEndpoints(ReplicaCollection<?> fetchRanges, Keyspace keyspace, boolean useStrictConsistency)
    {
        AbstractReplicationStrategy strat = keyspace.getReplicationStrategy();

        TokenMetadata tmd = metadata.cloneOnlyTokenMap();

        TokenMetadata tmdAfter = null;

        if (tokens != null)
        {
            // Pending ranges
            tmdAfter = tmd.cloneOnlyTokenMap();
            tmdAfter.updateNormalTokens(tokens, address);
        }
        else if (useStrictConsistency)
        {
            throw new IllegalArgumentException("Can't ask for strict consistency and not supply tokens");
        }

        return calculateRangesToFetchWithPreferredEndpoints(snitch::sortedByProximity,
                                                            strat,
                                                            fetchRanges,
                                                            useStrictConsistency,
                                                            tmd,
                                                            tmdAfter,
                                                            keyspace.getName(),
                                                            sourceFilters);

    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     **/
     public static EndpointsByReplica
     calculateRangesToFetchWithPreferredEndpoints(BiFunction<InetAddressAndPort, EndpointsForRange, EndpointsForRange> snitchGetSortedListByProximity,
                                                  AbstractReplicationStrategy strat,
                                                  ReplicaCollection<?> fetchRanges,
                                                  boolean useStrictConsistency,
                                                  TokenMetadata tmdBefore,
                                                  TokenMetadata tmdAfter,
                                                  String keyspace,
                                                  Collection<SourceFilter> sourceFilters)
     {
         EndpointsByRange rangeAddresses = strat.getRangeAddresses(tmdBefore);

         InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
         logger.debug ("Keyspace: {}", keyspace);
         logger.debug("To fetch RN: {}", fetchRanges);
         logger.debug("Fetch ranges: {}", rangeAddresses);

         Predicate<Replica> testSourceFilters = and(sourceFilters);
         Function<EndpointsForRange, EndpointsForRange> sorted =
         endpoints -> snitchGetSortedListByProximity.apply(localAddress, endpoints);

         //This list of replicas is just candidates. With strict consistency it's going to be a narrow list.
         EndpointsByReplica.Builder rangesToFetchWithPreferredEndpoints = new EndpointsByReplica.Builder();
         for (Replica toFetch : fetchRanges)
         {
             //Replica that is sufficient to provide the data we need
             //With strict consistency and transient replication we may end up with multiple types
             //so this isn't used with strict consistency
             Predicate<Replica> isSufficient = r -> toFetch.isTransient() || r.isFull();

             logger.debug("To fetch {}", toFetch);
             for (Range<Token> range : rangeAddresses.keySet())
             {
                 if (!range.contains(toFetch.range()))
                     continue;

                 final EndpointsForRange oldEndpoints = sorted.apply(rangeAddresses.get(range));

                 //Ultimately we populate this with whatever is going to be fetched from to satisfy toFetch
                 //It could be multiple endpoints and we must fetch from all of them if they are there
                 //With transient replication and strict consistency this is to get the full data from a full replica and
                 //transient data from the transient replica losing data
                 EndpointsForRange sources;
                 //Due to CASSANDRA-5953 we can have a higher RF than we have endpoints.
                 //So we need to be careful to only be strict when endpoints == RF
                 boolean isStrictConsistencyApplicable = useStrictConsistency && (oldEndpoints.size() == strat.getReplicationFactor().allReplicas);
                 if (isStrictConsistencyApplicable)
                 {
                     EndpointsForRange strictEndpoints;

                     //Start with two sets of who replicates the range before and who replicates it after
                     EndpointsForRange newEndpoints = strat.calculateNaturalReplicas(toFetch.range().right, tmdAfter);
                     logger.debug("Old endpoints {}", oldEndpoints);
                     logger.debug("New endpoints {}", newEndpoints);

                     // Remove new endpoints from old endpoints based on address
                     strictEndpoints = oldEndpoints.without(newEndpoints.endpoints());

                     if (strictEndpoints.size() > 1)
                         throw new AssertionError("Expected <= 1 endpoint but found " + strictEndpoints);

                     //We have to check the source filters here to see if they will remove any replicas
                     //required for strict consistency
                     if (!all(strictEndpoints, testSourceFilters))
                         throw new IllegalStateException("Necessary replicas for strict consistency were removed by source filters: " + buildErrorMessage(sourceFilters, strictEndpoints));

                     //If we are transitioning from transient to full and and the set of replicas for the range is not changing
                     //we might end up with no endpoints to fetch from by address. In that case we can pick any full replica safely
                     //since we are already a transient replica and the existing replica remains.
                     //The old behavior where we might be asked to fetch ranges we don't need shouldn't occur anymore.
                     //So it's an error if we don't find what we need.
                     if (strictEndpoints.isEmpty() && toFetch.isTransient())
                         throw new AssertionError("If there are no endpoints to fetch from then we must be transitioning from transient to full for range " + toFetch);

                     if (!any(strictEndpoints, isSufficient))
                     {
                         // need an additional replica; include all our filters, to ensure we include a matching node
                         Optional<Replica> fullReplica = Iterables.<Replica>tryFind(oldEndpoints, and(isSufficient, testSourceFilters)).toJavaUtil();
                         if (fullReplica.isPresent())
                             strictEndpoints = Endpoints.concat(strictEndpoints, EndpointsForRange.of(fullReplica.get()));
                         else
                             throw new IllegalStateException("Couldn't find any matching sufficient replica out of " + buildErrorMessage(sourceFilters, oldEndpoints));
                     }

                     sources = strictEndpoints;
                 }
                 else
                 {
                     //Without strict consistency we have given up on correctness so no point in fetching from
                     //a random full + transient replica since it's also likely to lose data
                     //Also apply testSourceFilters that were given to us so we can safely select a single source
                     sources = sorted.apply(oldEndpoints.filter(and(isSufficient, testSourceFilters)));
                     //Limit it to just the first possible source, we don't need more than one and downstream
                     //will fetch from every source we supply
                     sources = sources.size() > 0 ? sources.subList(0, 1) : sources;
                 }

                 // storing range and preferred endpoint set
                 rangesToFetchWithPreferredEndpoints.putAll(toFetch, sources, Conflict.NONE);
                 logger.debug("Endpoints to fetch for {} are {}", toFetch, sources);
             }

             EndpointsForRange addressList = rangesToFetchWithPreferredEndpoints.getIfPresent(toFetch);
             if (addressList == null)
                 throw new IllegalStateException("Failed to find endpoints to fetch " + toFetch);

             /*
              * When we move forwards (shrink our bucket) we are the one losing a range and no one else loses
              * from that action (we also don't gain). When we move backwards there are two people losing a range. One is a full replica
              * and the other is a transient replica. So we must need fetch from two places in that case for the full range we gain.
              * For a transient range we only need to fetch from one.
              */
             if (useStrictConsistency && addressList.size() > 1 && (addressList.filter(Replica::isFull).size() > 1 || addressList.filter(Replica::isTransient).size() > 1))
                 throw new IllegalStateException(String.format("Multiple strict sources found for %s, sources: %s", toFetch, addressList));

             //We must have enough stuff to fetch from
             if (!any(addressList, isSufficient))
             {
                 if (strat.getReplicationFactor().allReplicas == 1)
                 {
                     if (useStrictConsistency)
                     {
                         logger.warn("A node required to move the data consistently is down");
                         throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace + " with RF=1. " +
                                                         "Ensure this keyspace contains replicas in the source datacenter.");
                     }
                     else
                         logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                     "Keyspace might be missing data.", toFetch, keyspace);
                 }
                 else
                 {
                     if (useStrictConsistency)
                         logger.warn("A node required to move the data consistently is down");
                     throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace);
                 }
             }
         }
         return rangesToFetchWithPreferredEndpoints.build();
     }

    /**
     * The preferred endpoint list is the wrong format because it is keyed by Replica (this node) rather than the source
     * endpoint we will fetch from which streaming wants.
     */
    public static Multimap<InetAddressAndPort, FetchReplica> convertPreferredEndpointsToWorkMap(EndpointsByReplica preferredEndpoints)
    {
        Multimap<InetAddressAndPort, FetchReplica> workMap = HashMultimap.create();
        for (Map.Entry<Replica, EndpointsForRange> e : preferredEndpoints.entrySet())
        {
            for (Replica source : e.getValue())
            {
                assert (e.getKey()).isSelf();
                assert !source.isSelf();
                workMap.put(source.endpoint(), new FetchReplica(e.getKey(), source));
            }
        }
        logger.debug("Work map {}", workMap);
        return workMap;
    }

    /**
     * Optimized version that also outputs the final work map
     */
    private static Multimap<InetAddressAndPort, FetchReplica> getOptimizedWorkMap(EndpointsByReplica rangesWithSources,
                                                                                  Collection<SourceFilter> sourceFilters,
                                                                                  String keyspace)
    {
        //For now we just aren't going to use the optimized range fetch map with transient replication to shrink
        //the surface area to test and introduce bugs.
        //In the future it's possible we could run it twice once for full ranges with only full replicas
        //and once with transient ranges and all replicas. Then merge the result.
        EndpointsByRange.Builder unwrapped = new EndpointsByRange.Builder();
        for (Map.Entry<Replica, Replica> entry : rangesWithSources.flattenEntries())
        {
            Replicas.temporaryAssertFull(entry.getValue());
            unwrapped.put(entry.getKey().range(), entry.getValue());
        }

        EndpointsByRange unwrappedView = unwrapped.build();
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(unwrappedView, sourceFilters, keyspace);
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(unwrappedView, rangeFetchMapMap, keyspace);

        //Need to rewrap as Replicas
        Multimap<InetAddressAndPort, FetchReplica> wrapped = HashMultimap.create();
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            Replica toFetch = null;
            for (Replica r : rangesWithSources.keySet())
            {
                if (r.range().equals(entry.getValue()))
                {
                    if (toFetch != null)
                        throw new AssertionError(String.format("There shouldn't be multiple replicas for range %s, replica %s and %s here", r.range(), r, toFetch));
                    toFetch = r;
                }
            }
            if (toFetch == null)
                throw new AssertionError("Shouldn't be possible for the Replica we fetch to be null here");
            //Committing the cardinal sin of synthesizing a Replica, but it's ok because we assert earlier all of them
            //are full and optimized range fetch map doesn't support transient replication yet.
            wrapped.put(entry.getKey(), new FetchReplica(toFetch, fullReplica(entry.getKey(), entry.getValue())));
        }

        return wrapped;
    }

    /**
     * Verify that source returned for each range is correct
     */
    @VisibleForTesting
    static void validateRangeFetchMap(EndpointsByRange rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).endpoints().contains(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    // For testing purposes
    @VisibleForTesting
    Map<String, Multimap<InetAddressAndPort, FetchReplica>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        toFetch.forEach((keyspace, sources) -> {
            logger.debug("Keyspace {} Sources {}", keyspace, sources);
            sources.asMap().forEach((source, fetchReplicas) -> {

                List<FetchReplica> remaining;

                // If the operator's specified they want to reset bootstrap progress, we don't check previous attempted
                // bootstraps and just restart with all.
                if (RESET_BOOTSTRAP_PROGRESS.getBoolean())
                {
                    // TODO: Also remove the files on disk. See discussion in CASSANDRA-17679
                    SystemKeyspace.resetAvailableStreamedRangesForKeyspace(keyspace);
                    remaining = new ArrayList<>(fetchReplicas);
                }
                else
                {
                    // Filter out already streamed ranges
                    SystemKeyspace.AvailableRanges available = stateStore.getAvailableRanges(keyspace, metadata.partitioner);

                    Predicate<FetchReplica> isAvailable = fetch -> {
                        boolean isInFull = available.full.contains(fetch.local.range());
                        boolean isInTrans = available.trans.contains(fetch.local.range());

                        if (!isInFull && !isInTrans)
                            // Range is unavailable
                            return false;

                        if (fetch.local.isFull())
                            // For full, pick only replicas with matching transientness
                            return isInFull == fetch.remote.isFull();

                        // Any transient or full will do
                        return true;
                    };

                    remaining = fetchReplicas.stream().filter(not(isAvailable)).collect(Collectors.toList());

                    if (remaining.size() < available.full.size() + available.trans.size())
                    {
                        // If the operator hasn't specified what to do when we discover a previous partially successful bootstrap,
                        // we error out and tell them to manually reconcile it. See CASSANDRA-17679.
                        if (!RESET_BOOTSTRAP_PROGRESS.isPresent())
                        {
                            List<FetchReplica> skipped = fetchReplicas.stream().filter(isAvailable).collect(Collectors.toList());
                            String msg = String.format("Discovered existing bootstrap data and %s " +
                                                       "is not configured; aborting bootstrap. Please clean up local files manually " +
                                                       "and try again or set cassandra.reset_bootstrap_progress=true to ignore. " +
                                                       "Found: %s. Fully available: %s. Transiently available: %s",
                                                       RESET_BOOTSTRAP_PROGRESS.getKey(), skipped, available.full, available.trans);
                            logger.error(msg);
                            throw new IllegalStateException(msg);
                        }

                        if (!RESET_BOOTSTRAP_PROGRESS.getBoolean())
                        {
                            List<FetchReplica> skipped = fetchReplicas.stream().filter(isAvailable).collect(Collectors.toList());
                            logger.info("Some ranges of {} are already available. Skipping streaming those ranges. Skipping {}. Fully available {} Transiently available {}",
                                        fetchReplicas, skipped, available.full, available.trans);
                        }
                    }
                }

                if (logger.isTraceEnabled())
                    logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(remaining, ", "));

                InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
                RangesAtEndpoint full = remaining.stream()
                        .filter(pair -> pair.remote.isFull())
                        .map(pair -> pair.local)
                        .collect(RangesAtEndpoint.collector(self));
                RangesAtEndpoint transientReplicas = remaining.stream()
                        .filter(pair -> pair.remote.isTransient())
                        .map(pair -> pair.local)
                        .collect(RangesAtEndpoint.collector(self));

                logger.debug("Source and our replicas {}", fetchReplicas);
                logger.debug("Source {} Keyspace {}  streaming full {} transient {}", source, keyspace, full, transientReplicas);

                /* Send messages to respective folks to stream data over to me */
                streamPlan.requestRanges(source, keyspace, full, transientReplicas);
            });
        });

        return streamPlan.execute();
    }
}
