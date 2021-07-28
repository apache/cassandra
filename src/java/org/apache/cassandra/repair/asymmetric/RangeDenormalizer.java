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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
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
    public static Set<Range<Token>> denormalize(Range<Token> range, RangeMap<StreamFromOptions> incoming)
    {
        logger.trace("Denormalizing range={} incoming={}", range, incoming);
        Set<Map.Entry<Range<Token>, StreamFromOptions>> existingOverlappingRanges = incoming.removeIntersecting(range);

        Set<Range<Token>> intersections = intersection(existingOverlappingRanges, range);
        Set<Range<Token>> newExisting = Sets.union(subtractFromAllRanges(existingOverlappingRanges, range), intersections);
        Set<Range<Token>> newInput = Sets.union(subtractAll(existingOverlappingRanges, range), intersections);
        for (Range<Token> r : newExisting)
        {
            for (Map.Entry<Range<Token>, StreamFromOptions> entry : existingOverlappingRanges)
            {
                if (r.intersects(entry.getKey()))
                    incoming.put(r, entry.getValue().copy(r));
            }
        }
        logger.trace("denormalized {} to {}", range, newInput);
        logger.trace("denormalized incoming to {}", incoming);
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
    static Set<Range<Token>> subtractFromAllRanges(Collection<Map.Entry<Range<Token>, StreamFromOptions>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Map.Entry<Range<Token>, ?> r : ranges)
            result.addAll(r.getKey().subtract(range)); // subtract can return two ranges if we remove the middle part
        return result;
    }

    /**
     * Returns all intersections between the ranges in ranges and the given range
     */
    private static Set<Range<Token>> intersection(Set<Map.Entry<Range<Token>, StreamFromOptions>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Map.Entry<Range<Token>, StreamFromOptions> r : ranges)
            result.addAll(range.intersectionWith(r.getKey()));
        return result;
    }

    /**
     * copied from Range - need to iterate over the map entries
     */
    public static Set<Range<Token>> subtractAll(Collection<Map.Entry<Range<Token>, StreamFromOptions>> ranges, Range<Token> toSubtract)
    {
        Set<Range<Token>> result = new HashSet<>();
        result.add(toSubtract);
        for(Map.Entry<Range<Token>, StreamFromOptions> range : ranges)
        {
            result = substractAllFromToken(result, range.getKey());
        }

        return result;
    }

    private static <T extends RingPosition<T>> Set<Range<T>> substractAllFromToken(Set<Range<T>> ranges, Range<T> subtract)
    {
        Set<Range<T>> result = new HashSet<>();
        for(Range<T> range : ranges)
        {
            result.addAll(range.subtract(subtract));
        }

        return result;
    }
}
