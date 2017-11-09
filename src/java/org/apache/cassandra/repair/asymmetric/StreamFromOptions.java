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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Keeps track of where a node needs to stream a given range from.
 *
 * If the remote range is identical on several remote nodes, this class keeps track of them
 *
 * These stream from options get 'split' during denormalization - for example if we track range
 * (100, 200] and we find a new differing range (180, 200] - then the denormalization will create two
 * new StreamFromOptions (see copy below) with the same streamOptions, one with range (100, 180] and one with (180, 200] - then it
 * adds the new incoming difference to the StreamFromOptions for the new range (180, 200].
 */
public class StreamFromOptions
{
    /**
     * all differences - used to figure out if two nodes are equals on the range
     */
    private final DifferenceHolder differences;
    /**
     * The range to stream
     */
    @VisibleForTesting
    final Range<Token> range;
    /**
     * Contains the hosts to stream from - if two nodes are in the same inner set, they are identical for the range we are handling
     */
    private final Set<Set<InetAddressAndPort>> streamOptions = new HashSet<>();

    public StreamFromOptions(DifferenceHolder differences, Range<Token> range)
    {
        this(differences, range, Collections.emptySet());
    }

    private StreamFromOptions(DifferenceHolder differences, Range<Token> range, Set<Set<InetAddressAndPort>> existing)
    {
        this.differences = differences;
        this.range = range;
        for (Set<InetAddressAndPort> addresses : existing)
            this.streamOptions.add(Sets.newHashSet(addresses));
    }

    /**
     * Add new node to the stream options
     *
     * If we have no difference between the new node and a currently tracked on, we know they are matching over the
     * range we are tracking, then just add it to the set with the identical remote nodes. Otherwise create a new group
     * of nodes containing this new node.
     */
    public void add(InetAddressAndPort streamFromNode)
    {
        for (Set<InetAddressAndPort> options : streamOptions)
        {
            InetAddressAndPort first = options.iterator().next();
            if (!differences.hasDifferenceBetween(first, streamFromNode, range))
            {
                options.add(streamFromNode);
                return;
            }
        }
        streamOptions.add(Sets.newHashSet(streamFromNode));
    }

    public StreamFromOptions copy(Range<Token> withRange)
    {
        return new StreamFromOptions(differences, withRange, streamOptions);
    }

    public Iterable<Set<InetAddressAndPort>> allStreams()
    {
        return streamOptions;
    }

    public String toString()
    {
        return "StreamFromOptions{" +
               ", range=" + range +
               ", streamOptions=" + streamOptions +
               '}';
    }
}
