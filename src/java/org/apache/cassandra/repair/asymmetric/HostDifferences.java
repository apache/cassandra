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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Tracks the differences for a single host
 */
public class HostDifferences
{
    private final Map<InetAddress, List<Range<Token>>> perHostDifferences = new HashMap<>();

    /**
     * Adds a set of differences between the node this instance is tracking and endpoint
     */
    public void add(InetAddress endpoint, List<Range<Token>> difference)
    {
        perHostDifferences.put(endpoint, difference);
    }

    public void addSingleRange(InetAddress remoteNode, Range<Token> rangeToFetch)
    {
        perHostDifferences.computeIfAbsent(remoteNode, (x) -> new ArrayList<>()).add(rangeToFetch);
    }

    /**
     * Does this instance have differences for range with node2?
     */
    public boolean hasDifferencesFor(InetAddress node2, Range<Token> range)
    {
        List<Range<Token>> differences = get(node2);
        for (Range<Token> diff : differences)
        {
            // if the other node has a diff for this range, we know they are not equal.
            if (range.equals(diff) || range.intersects(diff))
                return true;
        }
        return false;
    }

    public Set<InetAddress> hosts()
    {
        return perHostDifferences.keySet();
    }

    public List<Range<Token>> get(InetAddress differingHost)
    {
        return perHostDifferences.getOrDefault(differingHost, Collections.emptyList());
    }

    public String toString()
    {
        return "HostDifferences{" +
               "perHostDifferences=" + perHostDifferences +
               '}';
    }
}