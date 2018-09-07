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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

@VisibleForTesting
public class RangeRelocator
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    private final StreamPlan streamPlan = new StreamPlan(StreamOperation.RELOCATION);
    private final InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
    private final TokenMetadata tokenMetaCloneAllSettled;
    // clone to avoid concurrent modification in calculateNaturalReplicas
    private final TokenMetadata tokenMetaClone;
    private final Collection<Token> tokens;
    private final List<String> keyspaceNames;


    RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames, TokenMetadata tmd)
    {
        this.tokens = tokens;
        this.keyspaceNames = keyspaceNames;
        this.tokenMetaCloneAllSettled = tmd.cloneAfterAllSettled();
        // clone to avoid concurrent modification in calculateNaturalReplicas
        this.tokenMetaClone = tmd.cloneOnlyTokenMap();
    }

    @VisibleForTesting
    public RangeRelocator()
    {
        this.tokens = null;
        this.keyspaceNames = null;
        this.tokenMetaCloneAllSettled = null;
        this.tokenMetaClone = null;
    }

    /**
     * Wrapper that supplies accessors to the real implementations of the various dependencies for this method
     */
    private static Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> calculateRangesToFetchWithPreferredEndpoints(RangesAtEndpoint fetchRanges,
                                                                                                                         AbstractReplicationStrategy strategy,
                                                                                                                         String keyspace,
                                                                                                                         TokenMetadata tmdBefore,
                                                                                                                         TokenMetadata tmdAfter)
    {
        EndpointsByReplica preferredEndpoints =
        RangeStreamer.calculateRangesToFetchWithPreferredEndpoints(DatabaseDescriptor.getEndpointSnitch()::sortedByProximity,
                                                                   strategy,
                                                                   fetchRanges,
                                                                   StorageService.useStrictConsistency,
                                                                   tmdBefore,
                                                                   tmdAfter,
                                                                   keyspace,
                                                                   Arrays.asList(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance),
                                                                                 new RangeStreamer.ExcludeLocalNodeFilter()));
        return RangeStreamer.convertPreferredEndpointsToWorkMap(preferredEndpoints);
    }

    /**
     * calculating endpoints to stream current ranges to if needed
     * in some situations node will handle current ranges as part of the new ranges
     **/
    public static RangesByEndpoint calculateRangesToStreamWithEndpoints(RangesAtEndpoint streamRanges,
                                                                        AbstractReplicationStrategy strat,
                                                                        TokenMetadata tmdBefore,
                                                                        TokenMetadata tmdAfter)
    {
        RangesByEndpoint.Builder endpointRanges = new RangesByEndpoint.Builder();
        for (Replica toStream : streamRanges)
        {
            //If the range we are sending is full only send it to the new full replica
            //There will also be a new transient replica we need to send the data to, but not
            //the repaired data
            EndpointsForRange oldEndpoints = strat.calculateNaturalReplicas(toStream.range().right, tmdBefore);
            EndpointsForRange newEndpoints = strat.calculateNaturalReplicas(toStream.range().right, tmdAfter);
            logger.debug("Need to stream {}, current endpoints {}, new endpoints {}", toStream, oldEndpoints, newEndpoints);

            for (Replica newEndpoint : newEndpoints)
            {
                Replica oldEndpoint = oldEndpoints.byEndpoint().get(newEndpoint.endpoint());

                // Nothing to do
                if (newEndpoint.equals(oldEndpoint))
                    continue;

                // Completely new range for this endpoint
                if (oldEndpoint == null)
                {
                    if (toStream.isTransient() && newEndpoint.isFull())
                        throw new AssertionError(String.format("Need to stream %s, but only have %s which is transient and not full", newEndpoint, toStream));

                    for (Range<Token> intersection : newEndpoint.range().intersectionWith(toStream.range()))
                    {
                        endpointRanges.put(newEndpoint.endpoint(), newEndpoint.decorateSubrange(intersection));
                    }
                }
                else
                {
                    Set<Range<Token>> subsToStream = Collections.singleton(toStream.range());

                    //First subtract what we already have
                    if (oldEndpoint.isFull() == newEndpoint.isFull() || oldEndpoint.isFull())
                        subsToStream = toStream.range().subtract(oldEndpoint.range());

                    //Now we only stream what is still replicated
                    subsToStream.stream()
                                .flatMap(range -> range.intersectionWith(newEndpoint.range()).stream())
                                .forEach(tokenRange -> endpointRanges.put(newEndpoint.endpoint(), newEndpoint.decorateSubrange(tokenRange)));
                }
            }
        }
        return endpointRanges.build();
    }

    public void calculateToFromStreams()
    {
        logger.debug("Current tmd: {}, Updated tmd: {}", tokenMetaClone, tokenMetaCloneAllSettled);

        for (String keyspace : keyspaceNames)
        {
            // replication strategy of the current keyspace
            AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

            logger.info("Calculating ranges to stream and request for keyspace {}", keyspace);
            //From what I have seen we only ever call this with a single token from StorageService.move(Token)
            for (Token newToken : tokens)
            {
                Collection<Token> currentTokens = tokenMetaClone.getTokens(localAddress);
                if (currentTokens.size() > 1 || currentTokens.isEmpty())
                {
                    throw new AssertionError("Unexpected current tokens: " + currentTokens);
                }

                // calculated parts of the ranges to request/stream from/to nodes in the ring
                Pair<RangesAtEndpoint, RangesAtEndpoint> streamAndFetchOwnRanges;

                //In the single node token move there is nothing to do and Range subtraction is broken
                //so it's easier to just identify this case up front.
                if (tokenMetaClone.getTopology().getDatacenterEndpoints().get(DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter()).size() > 1)
                {
                    // getting collection of the currently used ranges by this keyspace
                    RangesAtEndpoint currentReplicas = strategy.getAddressReplicas(localAddress);

                    // collection of ranges which this node will serve after move to the new token
                    RangesAtEndpoint updatedReplicas = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);

                    streamAndFetchOwnRanges = calculateStreamAndFetchRanges(currentReplicas, updatedReplicas);
                }
                else
                {
                     streamAndFetchOwnRanges = Pair.create(RangesAtEndpoint.empty(localAddress), RangesAtEndpoint.empty(localAddress));
                }

                RangesByEndpoint rangesToStream = calculateRangesToStreamWithEndpoints(streamAndFetchOwnRanges.left, strategy, tokenMetaClone, tokenMetaCloneAllSettled);
                logger.info("Endpoint ranges to stream to " + rangesToStream);

                // stream ranges
                for (InetAddressAndPort address : rangesToStream.keySet())
                {
                    logger.debug("Will stream range {} of keyspace {} to endpoint {}", rangesToStream.get(address), keyspace, address);
                    RangesAtEndpoint ranges = rangesToStream.get(address);
                    streamPlan.transferRanges(address, keyspace, ranges);
                }

                Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> rangesToFetch = calculateRangesToFetchWithPreferredEndpoints(streamAndFetchOwnRanges.right, strategy, keyspace, tokenMetaClone, tokenMetaCloneAllSettled);

                // stream requests
                rangesToFetch.asMap().forEach((address, sourceAndOurReplicas) -> {
                    RangesAtEndpoint full = sourceAndOurReplicas.stream()
                            .filter(pair -> pair.remote.isFull())
                            .map(pair -> pair.local)
                            .collect(RangesAtEndpoint.collector(localAddress));
                    RangesAtEndpoint trans = sourceAndOurReplicas.stream()
                            .filter(pair -> pair.remote.isTransient())
                            .map(pair -> pair.local)
                            .collect(RangesAtEndpoint.collector(localAddress));
                    logger.debug("Will request range {} of keyspace {} from endpoint {}", rangesToFetch.get(address), keyspace, address);
                    streamPlan.requestRanges(address, keyspace, full, trans);
                });

                logger.debug("Keyspace {}: work map {}.", keyspace, rangesToFetch);
            }
        }
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for keyspace and ranges after move to new token)
     *
     * With transient replication the added wrinkle is that if a range transitions from full to transient then
     * we need to stream the range despite the fact that we are retaining it as transient. Some replica
     * somewhere needs to transition from transient to full and we will be the source.
     *
     * If the range is transient and is transitioning to full then always fetch even if the range was already transient
     * since a transiently replicated obviously needs to fetch data to become full.
     *
     * This why there is a continue after checking for instersection because intersection is not sufficient reason
     * to do the subtraction since we might need to stream/fetch data anyways.
     *
     * @param currentRanges collection of the ranges by current token
     * @param updatedRanges collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    public static Pair<RangesAtEndpoint, RangesAtEndpoint> calculateStreamAndFetchRanges(RangesAtEndpoint currentRanges, RangesAtEndpoint updatedRanges)
    {
        RangesAtEndpoint.Builder toStream = RangesAtEndpoint.builder(currentRanges.endpoint());
        RangesAtEndpoint.Builder toFetch  = RangesAtEndpoint.builder(currentRanges.endpoint());
        logger.debug("Calculating toStream");
        computeRanges(currentRanges, updatedRanges, toStream);

        logger.debug("Calculating toFetch");
        computeRanges(updatedRanges, currentRanges, toFetch);

        logger.debug("To stream {}", toStream);
        logger.debug("To fetch {}", toFetch);
        return Pair.create(toStream.build(), toFetch.build());
    }

    private static void computeRanges(RangesAtEndpoint srcRanges, RangesAtEndpoint dstRanges, RangesAtEndpoint.Builder ranges)
    {
        for (Replica src : srcRanges)
        {
            boolean intersect = false;
            RangesAtEndpoint remainder = null;
            for (Replica dst : dstRanges)
            {
                logger.debug("Comparing {} and {}", src, dst);
                // Stream the full range if there's no intersection
                if (!src.intersectsOnRange(dst))
                    continue;

                // If we're transitioning from full to transient
                if (src.isFull() && dst.isTransient())
                    continue;

                if (remainder == null)
                {
                    remainder = src.subtractIgnoreTransientStatus(dst.range());
                }
                else
                {
                    // Re-subtract ranges to avoid overstreaming in cases when the single range is split or merged
                    RangesAtEndpoint.Builder newRemainder = new RangesAtEndpoint.Builder(remainder.endpoint());
                    for (Replica replica : remainder)
                        newRemainder.addAll(replica.subtractIgnoreTransientStatus(dst.range()));
                    remainder = newRemainder.build();
                }
                intersect = true;
            }

            if (!intersect)
            {
                assert remainder == null;
                logger.debug("    Doesn't intersect adding {}", src);
                ranges.add(src); // should stream whole old range
            }
            else
            {
                ranges.addAll(remainder);
                logger.debug("    Intersects adding {}", remainder);
            }
        }
    }

    public Future<StreamState> stream()
    {
        return streamPlan.execute();
    }

    public boolean streamsNeeded()
    {
        return !streamPlan.isEmpty();
    }
}
