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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

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
    public static ImmutableMap<InetAddressAndPort, HostDifferences> reduce(DifferenceHolder differences, PreferedNodeFilter filter)
    {
        Map<InetAddressAndPort, IncomingRepairStreamTracker> trackers = createIncomingRepairStreamTrackers(differences);

        ImmutableMap.Builder<InetAddressAndPort, HostDifferences> mapBuilder = ImmutableMap.builder();
        for (Map.Entry<InetAddressAndPort, IncomingRepairStreamTracker> trackerEntry : trackers.entrySet())
        {
            IncomingRepairStreamTracker tracker = trackerEntry.getValue();
            HostDifferences rangesToFetch = new HostDifferences();
            for (Map.Entry<Range<Token>, StreamFromOptions> entry : tracker.getIncoming().entrySet())
            {
                Range<Token> rangeToFetch = entry.getKey();
                // StreamFromOptions contains a Set<Set<InetAddress>> with endpoints we need to stream
                // rangeToFetch from - if the inner set size > 1 means those endpoints are identical
                // for the range. pickConsistent picks a single endpoint from each of these sets.
                for (InetAddressAndPort remoteNode : pickConsistent(trackerEntry.getKey(), entry.getValue(), filter))
                    rangesToFetch.addSingleRange(remoteNode, rangeToFetch);
            }
            mapBuilder.put(trackerEntry.getKey(), rangesToFetch);
        }

        return mapBuilder.build();
    }

    @VisibleForTesting
    static Map<InetAddressAndPort, IncomingRepairStreamTracker> createIncomingRepairStreamTrackers(DifferenceHolder differences)
    {
        Map<InetAddressAndPort, IncomingRepairStreamTracker> trackers = new HashMap<>();

        for (InetAddressAndPort hostWithDifference : differences.keyHosts())
        {
            HostDifferences hostDifferences = differences.get(hostWithDifference);
            for (InetAddressAndPort differingHost : hostDifferences.hosts())
            {
                Iterable<Range<Token>> differingRanges = hostDifferences.get(differingHost);
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
                                                          Map<InetAddressAndPort, IncomingRepairStreamTracker> trackers,
                                                          InetAddressAndPort host)
    {
        return trackers.computeIfAbsent(host, (h) -> new IncomingRepairStreamTracker(differences));
    }

    private static final Comparator<InetAddressAndPort> comparator = Comparator.comparing(InetAddressAndPort::getHostAddressAndPort);
    /**
     * Consistently picks the node after the streaming node to stream from
     *
     * this is done to reduce the amount of sstables created on the receiving node
     *
     * todo: note that this can be improved - if we have a case like:
     *         addr3 will stream range1 from addr1 or addr2
     *                           range2 from addr1
     *       in a perfect world we would stream both range1 and range2 from addr1 - but in this case we might stream
     *       range1 from addr2 depending on how the addresses sort
     */
    private static Collection<InetAddressAndPort> pickConsistent(InetAddressAndPort streamingNode,
                                                                 StreamFromOptions toStreamFrom,
                                                                 PreferedNodeFilter filter)
    {
        Set<InetAddressAndPort> retSet = new HashSet<>();
        for (Set<InetAddressAndPort> toStream : toStreamFrom.allStreams())
        {
            List<InetAddressAndPort> toSearch = new ArrayList<>(filter.apply(streamingNode, toStream));
            if (toSearch.isEmpty())
                toSearch = new ArrayList<>(toStream);

            toSearch.sort(comparator);
            int pos = Collections.binarySearch(toSearch, streamingNode, comparator);
            assert pos < 0;
            pos = -pos - 1;
            if (pos == toSearch.size())
                retSet.add(toSearch.get(0));
            else
                retSet.add(toSearch.get(pos));
        }
        return retSet;
    }
}
