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
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class RangeDenormalizer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeDenormalizer.class);

    /**
     * "Denormalizes" (kind of the opposite of what Range.normalize does) the ranges in the keys of {{incoming}}
     *
     * It makes sure that if there is an intersection between {{range}} and some of the ranges in {{incoming.keySet()}}
     * we know that all intersections are keys in the updated {{incoming}}
     */
    public static Set<Range<Token>> denormalize(Range<Token> range, Map<Range<Token>, StreamFromOptions> incoming)
    {
        logger.trace("Denormalizing range={} incoming={}", range, incoming);
        Set<Range<Token>> existingRanges = new HashSet<>(incoming.keySet());
        Map<Range<Token>, StreamFromOptions> existingOverlappingRanges = new HashMap<>();
        // remove all overlapping ranges from the incoming map
        for (Range<Token> existingRange : existingRanges)
        {
            if (range.intersects(existingRange))
                existingOverlappingRanges.put(existingRange, incoming.remove(existingRange));
        }

        Set<Range<Token>> intersections = intersection(existingRanges, range);
        Set<Range<Token>> newExisting = Sets.union(subtractFromAllRanges(existingOverlappingRanges.keySet(), range), intersections);
        Set<Range<Token>> newInput = Sets.union(range.subtractAll(existingOverlappingRanges.keySet()), intersections);
        assertNonOverLapping(newExisting);
        assertNonOverLapping(newInput);
        for (Range<Token> r : newExisting)
        {
            for (Map.Entry<Range<Token>, StreamFromOptions> entry : existingOverlappingRanges.entrySet())
            {
                if (r.intersects(entry.getKey()))
                    incoming.put(r, entry.getValue().copy(r));
            }
        }
        logger.trace("denormalized {} to {}", range, newInput);
        logger.trace("denormalized incoming to {}", incoming);
        assertNonOverLapping(incoming.keySet());
        return newInput;
    }

    /**
     * Subtract the given range from all the input ranges.
     *
     * for example:
     * ranges = [(0, 10], (20, 30]]
     * and range = (8, 22]
     *
     * the result should be [(0, 8], (22, 30]]
     *
     */
    @VisibleForTesting
    static Set<Range<Token>> subtractFromAllRanges(Collection<Range<Token>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Range<Token> r : ranges)
            result.addAll(r.subtract(range)); // subtract can return two ranges if we remove the middle part
        return result;
    }

    /**
     * Makes sure non of the input ranges are overlapping
     */
    private static void assertNonOverLapping(Set<Range<Token>> ranges)
    {
        List<Range<Token>> sortedRanges = Range.sort(ranges);
        Token lastToken = null;
        for (Range<Token> range : sortedRanges)
        {
            if (lastToken != null && lastToken.compareTo(range.left) > 0)
            {
                throw new AssertionError("Ranges are overlapping: "+ranges);
            }
            lastToken = range.right;
        }
    }

    /**
     * Returns all intersections between the ranges in ranges and the given range
     */
    private static Set<Range<Token>> intersection(Collection<Range<Token>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Range<Token> r : ranges)
            result.addAll(range.intersectionWith(r));
        return result;
    }
}
