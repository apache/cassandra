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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Tracks the differences for a single host
 */
public class HostDifferences
{
    private final Map<InetAddressAndPort, NavigableSet<Range<Token>>> perHostDifferences = new HashMap<>();
    private static final Comparator<Range<Token>> comparator = Comparator.comparing((Range<Token> o) -> o.left);

    /**
     * Adds a set of differences between the node this instance is tracking and endpoint
     */
    public void add(InetAddressAndPort endpoint, Collection<Range<Token>> difference)
    {
        TreeSet<Range<Token>> sortedDiffs = new TreeSet<>(comparator);
        sortedDiffs.addAll(difference);
        perHostDifferences.put(endpoint, sortedDiffs);
    }

    public void addSingleRange(InetAddressAndPort remoteNode, Range<Token> rangeToFetch)
    {
        perHostDifferences.computeIfAbsent(remoteNode, (x) -> new TreeSet<>(comparator)).add(rangeToFetch);
    }

    /**
     * Does this instance have differences for range with node2?
     */
    public boolean hasDifferencesFor(InetAddressAndPort node2, Range<Token> range)
    {
        NavigableSet<Range<Token>> differences = get(node2);

        if (differences.size() > 0 && differences.last().isWrapAround() && differences.last().intersects(range))
            return true;

        for (Range<Token> unwrappedRange : range.unwrap())
        {
            Range<Token> startKey = differences.floor(unwrappedRange);
            Iterator<Range<Token>> iter = startKey == null ? differences.iterator() : differences.tailSet(startKey, true).iterator();

            while (iter.hasNext())
            {
                Range<Token> diff = iter.next();
                // if the other node has a diff for this range, we know they are not equal.
                if (unwrappedRange.equals(diff) || unwrappedRange.intersects(diff))
                    return true;
                if (unwrappedRange.right.compareTo(diff.left) < 0 && !unwrappedRange.isWrapAround())
                    break;
            }
        }
        return false;
    }

    public Set<InetAddressAndPort> hosts()
    {
        return perHostDifferences.keySet();
    }

    public NavigableSet<Range<Token>> get(InetAddressAndPort differingHost)
    {
        return perHostDifferences.getOrDefault(differingHost, Collections.emptyNavigableSet());
    }

    public String toString()
    {
        return "HostDifferences{" +
               "perHostDifferences=" + perHostDifferences +
               '}';
    }
}
