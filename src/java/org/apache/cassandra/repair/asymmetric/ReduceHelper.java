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

package org.apache.cassandra.repair.asymmetric;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Basic idea is that we track incoming ranges instead of blindly just exchanging the ranges that mismatch between two nodes
 *
 * Say node X has tracked that it will stream range r1 from node Y. Now we see find a diffing range
 * r1 between node X and Z. When adding r1 from Z as an incoming to X we check if Y and Z are equal on range r (ie, there is
 * no difference between them). If they are equal X can stream from Y or Z and the end result will be the same.
 *
 * The ranges wont match perfectly since we don't iterate over leaves so we always split based on the
 * smallest range (either the new difference or the existing one)
 */
public class ReduceHelper
{
    /**
     * Reduces the differences provided by the merkle trees to a minimum set of differences
     */
    public static ImmutableMap<InetAddress, HostDifferences> reduce(DifferenceHolder differences, PreferedNodeFilter filter)
    {
        Map<InetAddress, IncomingRepairStreamTracker> trackers = createIncomingRepairStreamTrackers(differences);
        Map<InetAddress, Integer> outgoingStreamCounts = new HashMap<>();
        ImmutableMap.Builder<InetAddress, HostDifferences> mapBuilder = ImmutableMap.builder();
        for (Map.Entry<InetAddress, IncomingRepairStreamTracker> trackerEntry : trackers.entrySet())
        {
            IncomingRepairStreamTracker tracker = trackerEntry.getValue();
            HostDifferences rangesToFetch = new HostDifferences();
            for (Map.Entry<Range<Token>, StreamFromOptions> entry : tracker.getIncoming().entrySet())
            {
                Range<Token> rangeToFetch = entry.getKey();
                for (InetAddress remoteNode : pickLeastStreaming(trackerEntry.getKey(), entry.getValue(), outgoingStreamCounts, filter))
                    rangesToFetch.addSingleRange(remoteNode, rangeToFetch);
            }
            mapBuilder.put(trackerEntry.getKey(), rangesToFetch);
        }

        return mapBuilder.build();
    }

    @VisibleForTesting
    static Map<InetAddress, IncomingRepairStreamTracker> createIncomingRepairStreamTrackers(DifferenceHolder differences)
    {
        Map<InetAddress, IncomingRepairStreamTracker> trackers = new HashMap<>();

        for (InetAddress hostWithDifference : differences.keyHosts())
        {
            HostDifferences hostDifferences = differences.get(hostWithDifference);
            for (InetAddress differingHost : hostDifferences.hosts())
            {
                List<Range<Token>> differingRanges = hostDifferences.get(differingHost);
                // hostWithDifference has mismatching ranges differingRanges with differingHost:
                for (Range<Token> range : differingRanges)
                {
                    // a difference means that we need to sync that range between two nodes - add the diffing range to both
                    // hosts:
                    getTracker(differences, trackers, hostWithDifference).addIncomingRangeFrom(range, differingHost);
                    getTracker(differences, trackers, differingHost).addIncomingRangeFrom(range, hostWithDifference);
                }
            }
        }
        return trackers;
    }

    private static IncomingRepairStreamTracker getTracker(DifferenceHolder differences,
                                                          Map<InetAddress, IncomingRepairStreamTracker> trackers,
                                                          InetAddress host)
    {
        return trackers.computeIfAbsent(host, (h) -> new IncomingRepairStreamTracker(differences));
    }

    // greedily pick the nodes doing the least amount of streaming
    private static Collection<InetAddress> pickLeastStreaming(InetAddress streamingNode,
                                                              StreamFromOptions toStreamFrom,
                                                              Map<InetAddress, Integer> outgoingStreamCounts,
                                                              PreferedNodeFilter filter)
    {
        Set<InetAddress> retSet = new HashSet<>();
        for (Set<InetAddress> toStream : toStreamFrom.allStreams())
        {
            InetAddress candidate = null;
            Set<InetAddress> prefered = filter.apply(streamingNode, toStream);
            for (InetAddress node : prefered)
            {
                if (candidate == null || outgoingStreamCounts.getOrDefault(candidate, 0) > outgoingStreamCounts.getOrDefault(node, 0))
                {
                    candidate = node;
                }
            }
            // ok, found no prefered hosts, try all of them
            if (candidate == null)
            {
                for (InetAddress node : toStream)
                {
                    if (candidate == null || outgoingStreamCounts.getOrDefault(candidate, 0) > outgoingStreamCounts.getOrDefault(node, 0))
                    {
                        candidate = node;
                    }
                }
            }
            assert candidate != null;
            outgoingStreamCounts.put(candidate, outgoingStreamCounts.getOrDefault(candidate, 0) + 1);
            retSet.add(candidate);
        }
        return retSet;
    }
}
