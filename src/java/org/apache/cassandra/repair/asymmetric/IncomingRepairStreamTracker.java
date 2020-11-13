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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Tracks incoming streams for a single host
 */
public class IncomingRepairStreamTracker
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingRepairStreamTracker.class);
    private final DifferenceHolder differences;
    private final RangeMap<StreamFromOptions> incoming = new RangeMap<>();

    public IncomingRepairStreamTracker(DifferenceHolder differences)
    {
        this.differences = differences;
    }

    public String toString()
    {
        return "IncomingStreamTracker{" +
               "incoming=" + incoming +
               '}';
    }

    /**
     * Adds a range to be streamed from streamFromNode
     *
     * First the currently tracked ranges are denormalized to make sure that no ranges overlap, then
     * the streamFromNode is added to the StreamFromOptions for the range
     *
     * @param range the range we need to stream from streamFromNode
     * @param streamFromNode the node we should stream from
     */
    public void addIncomingRangeFrom(Range<Token> range, InetAddressAndPort streamFromNode)
    {
        logger.trace("adding incoming range {} from {}", range, streamFromNode);
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range, incoming);
        for (Range<Token> input : newInput)
            incoming.computeIfAbsent(input, (newRange) -> new StreamFromOptions(differences, newRange)).add(streamFromNode);
    }

    public ImmutableMap<Range<Token>, StreamFromOptions> getIncoming()
    {
        return ImmutableMap.copyOf(incoming);
    }
}




