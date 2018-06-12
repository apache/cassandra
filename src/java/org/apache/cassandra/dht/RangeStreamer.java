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

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.ReplicaMultimap;
import org.apache.cassandra.locator.ReplicaSet;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Assists in streaming ranges to this node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddressAndPort address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> toFetch = HashMultimap.create();
    private final Set<ISourceFilter> sourceFilters = new HashSet<>();
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;

    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    public static interface ISourceFilter
    {
        public boolean shouldInclude(Replica replica);
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements ISourceFilter
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean shouldInclude(Replica replica)
        {
            return fd.isAlive(replica.getEndpoint());
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements ISourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean shouldInclude(Replica replica)
        {
            return snitch.getDatacenter(replica).equals(sourceDc);
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements ISourceFilter
    {
        public boolean shouldInclude(Replica replica)
        {
            return !FBUtilities.getBroadcastAddressAndPort().equals(replica.getEndpoint());
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class WhitelistedSourcesFilter implements ISourceFilter
    {
        private final Set<InetAddressAndPort> whitelistedSources;

        public WhitelistedSourcesFilter(Set<InetAddressAndPort> whitelistedSources)
        {
            this.whitelistedSources = whitelistedSources;
        }

        public boolean shouldInclude(Replica replica)
        {
            return whitelistedSources.contains(replica.getEndpoint());
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
    }

    public void addSourceFilter(ISourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param replicas ranges to be streamed
     */
    public void addRanges(String keyspaceName, ReplicaCollection replicas)
    {
        if(Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        Replicas.checkFull(replicas);

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        ReplicaMultimap<Range<Token>, ReplicaList> rangesForKeyspace = useStrictSource
                                                                       ? getAllRangesWithStrictSourcesFor(keyspaceName, replicas.fullRanges())
                                                                       : getAllRangesWithSourcesFor(keyspaceName, replicas.fullRanges());

        for (Map.Entry<Range<Token>, Replica> entry : rangesForKeyspace.entries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = useStrictSource || strat == null || strat.getReplicationFactor().replicas == 1
                                                            ? getRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName, useStrictConsistency)
                                                            : getOptimizedRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName);

        for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : rangeFetchMap.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (Range<Token> r : entry.getValue())
                    logger.trace("{}: range {} from source {} for keyspace {}", description, r, entry.getKey(), keyspaceName);
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor().replicas;
    }

    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed
     */
    private ReplicaMultimap<Range<Token>, ReplicaList> getAllRangesWithSourcesFor(String keyspaceName, Iterable<Range<Token>> desiredRanges)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = strat.getRangeAddresses(metadata.cloneOnlyTokenMap());
        Replicas.checkFull(rangeAddresses.values());

        ReplicaMultimap<Range<Token>, ReplicaList> rangeSources = ReplicaMultimap.list();
        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(desiredRange))
                {
                    ReplicaList preferred = snitch.getSortedListByProximity(address, rangeAddresses.get(range));
                    rangeSources.putAll(desiredRange, preferred);
                    break;
                }
            }

            if (!rangeSources.keySet().contains(desiredRange))
                throw new IllegalStateException("No sources found for " + desiredRange);
        }

        return rangeSources;
    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed, or more than 1 source found.
     */
    private ReplicaMultimap<Range<Token>, ReplicaList> getAllRangesWithStrictSourcesFor(String keyspace, Iterable<Range<Token>> desiredRanges)
    {
        assert tokens != null;
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        // Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        ReplicaMultimap<Range<Token>, ReplicaSet> addressRanges = strat.getRangeAddresses(metadataClone);

        // Pending ranges
        metadataClone.updateNormalTokens(tokens, address);
        ReplicaMultimap<Range<Token>, ReplicaSet> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);

        // Collects the source that will have its range moved to the new node
        ReplicaMultimap<Range<Token>, ReplicaList> rangeSources = ReplicaMultimap.list();

        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Map.Entry<Range<Token>, ReplicaSet> preEntry : addressRanges.asMap().entrySet())
            {
                if (preEntry.getKey().contains(desiredRange))
                {
                    ReplicaSet oldEndpoints = new ReplicaSet(preEntry.getValue());
                    ReplicaSet newEndpoints = new ReplicaSet(pendingRangeAddresses.get(desiredRange));

                    // Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                    // So we need to be careful to only be strict when endpoints == RF
                    if (oldEndpoints.size() == strat.getReplicationFactor().replicas)
                    {
                        oldEndpoints.removeEndpoints(newEndpoints);
                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                    }

                    rangeSources.put(desiredRange, oldEndpoints.iterator().next());
                }
            }

            // Validate
            ReplicaList replicaList = rangeSources.get(desiredRange);
            if (replicaList == null || replicaList.isEmpty())
                throw new IllegalStateException("No sources found for " + desiredRange);

            if (replicaList.size() > 1)
                throw new IllegalStateException("Multiple endpoints found for " + desiredRange);

            Replica sourceReplica = replicaList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceReplica.getEndpoint());
            if (Gossiper.instance.isEnabled() && (sourceState == null || !sourceState.isAlive()))
                throw new RuntimeException("A node required to move the data consistently is down (" + sourceReplica + "). " +
                                           "If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
        }

        return rangeSources;
    }

    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @param keyspace keyspace name
     * @return Map of source endpoint to collection of ranges
     */
    private static Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMap(ReplicaMultimap<Range<Token>, ReplicaList> rangesWithSources,
                                                                               Collection<ISourceFilter> sourceFilters, String keyspace,
                                                                               boolean useStrictConsistency)
    {
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        for (Range<Token> range : rangesWithSources.keySet())
        {
            boolean foundSource = false;

            outer:
            for (Replica replica : rangesWithSources.get(range))
            {
                Replicas.checkFull(replica);
                for (ISourceFilter filter : sourceFilters)
                {
                    if (!filter.shouldInclude(replica))
                        continue outer;
                }

                if (replica.isLocal())
                {
                    // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                    foundSource = true;
                    continue;
                }

                rangeFetchMapMap.put(replica.getEndpoint(), range);
                foundSource = true;
                break; // ensure we only stream from one other node for each range
            }

            if (!foundSource)
            {
                AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();
                if (strat != null && strat.getReplicationFactor().replicas == 1)
                {
                    if (useStrictConsistency)
                        throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace + " with RF=1. " +
                                                        "Ensure this keyspace contains replicas in the source datacenter.");
                    else
                        logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                    "Keyspace might be missing data.", range, keyspace);
                }
                else
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace);
            }
        }

        return rangeFetchMapMap;
    }


    private static Multimap<InetAddressAndPort, Range<Token>> getOptimizedRangeFetchMap(ReplicaMultimap<Range<Token>, ReplicaList> rangesWithSources,
                                                                                        Collection<ISourceFilter> sourceFilters, String keyspace)
    {
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, sourceFilters, keyspace);
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(rangesWithSources, rangeFetchMapMap, keyspace);
        return rangeFetchMapMap;
    }

    /**
     * Verify that source returned for each range is correct
     * @param rangesWithSources
     * @param rangeFetchMapMap
     * @param keyspace
     */
    private static void validateRangeFetchMap(ReplicaMultimap<Range<Token>, ReplicaList> rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        Replicas.checkFull(rangesWithSources.values());
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).containsEndpoint(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    public static Multimap<InetAddressAndPort, Range<Token>> getWorkMap(ReplicaMultimap<Range<Token>, ReplicaList> rangesWithSourceTarget, String keyspace,
                                                                        IFailureDetector fd, boolean useStrictConsistency)
    {
        return getRangeFetchMap(rangesWithSourceTarget, Collections.<ISourceFilter>singleton(new FailureDetectorSourceFilter(fd)), keyspace, useStrictConsistency);
    }

    // For testing purposes
    @VisibleForTesting
    Multimap<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        for (Map.Entry<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            InetAddressAndPort source = entry.getValue().getKey();
            Collection<Range<Token>> ranges = entry.getValue().getValue();

            // filter out already streamed ranges
            Set<Range<Token>> availableRanges = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);
            if (ranges.removeAll(availableRanges))
            {
                logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
            }

            if (logger.isTraceEnabled())
                logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(ranges, ", "));
            /* Send messages to respective folks to stream data over to me */
            streamPlan.requestRanges(source, keyspace, ranges);
        }

        return streamPlan.execute();
    }
}
