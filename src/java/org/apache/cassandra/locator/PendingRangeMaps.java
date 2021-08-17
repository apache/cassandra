/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.locator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;

import java.util.*;

import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;

public class PendingRangeMaps implements Iterable<Map.Entry<Range<Token>, EndpointsForRange.Builder>>
{
    /**
     * We have for NavigableMap to be able to search for ranges containing a token efficiently.
     *
     * First two are for non-wrap-around ranges, and the last two are for wrap-around ranges.
     */
    // ascendingMap will sort the ranges by the ascending order of right token
    private final NavigableMap<Range<Token>, EndpointsForRange.Builder> ascendingMap;

    /**
     * sorting end ascending, if ends are same, sorting begin descending, so that token (end, end) will
     * come before (begin, end] with the same end, and (begin, end) will be selected in the tailMap.
     */
    private static final Comparator<Range<Token>> ascendingComparator = (o1, o2) -> {
        int res = o1.right.compareTo(o2.right);
        if (res != 0)
            return res;

        return o2.left.compareTo(o1.left);
    };

    // ascendingMap will sort the ranges by the descending order of left token
    private final NavigableMap<Range<Token>, EndpointsForRange.Builder> descendingMap;

    /**
     * sorting begin descending, if begins are same, sorting end descending, so that token (begin, begin) will
     * come after (begin, end] with the same begin, and (begin, end) won't be selected in the tailMap.
     */
    private static final Comparator<Range<Token>> descendingComparator = (o1, o2) -> {
        int res = o2.left.compareTo(o1.left);
        if (res != 0)
            return res;

        // if left tokens are same, sort by the descending of the right tokens.
        return o2.right.compareTo(o1.right);
    };

    // these two maps are for warp around ranges.
    private final NavigableMap<Range<Token>, EndpointsForRange.Builder> ascendingMapForWrapAround;

    /**
     * for wrap around range (begin, end], which begin > end.
     * Sorting end ascending, if ends are same, sorting begin ascending,
     * so that token (end, end) will come before (begin, end] with the same end, and (begin, end] will be selected in
     * the tailMap.
     */
    private static final Comparator<Range<Token>> ascendingComparatorForWrapAround = (o1, o2) -> {
        int res = o1.right.compareTo(o2.right);
        if (res != 0)
            return res;

        return o1.left.compareTo(o2.left);
    };

    private final NavigableMap<Range<Token>, EndpointsForRange.Builder> descendingMapForWrapAround;

    /**
     * for wrap around ranges, which begin > end.
     * Sorting end ascending, so that token (begin, begin) will come after (begin, end] with the same begin,
     * and (begin, end) won't be selected in the tailMap.
     */
    private static final Comparator<Range<Token>> descendingComparatorForWrapAround = (o1, o2) -> {
        int res = o2.left.compareTo(o1.left);
        if (res != 0)
            return res;
        return o1.right.compareTo(o2.right);
    };

    public PendingRangeMaps()
    {
        this.ascendingMap = new TreeMap<>(ascendingComparator);
        this.descendingMap = new TreeMap<>(descendingComparator);
        this.ascendingMapForWrapAround = new TreeMap<>(ascendingComparatorForWrapAround);
        this.descendingMapForWrapAround = new TreeMap<>(descendingComparatorForWrapAround);
    }

    static final void addToMap(Range<Token> range,
                               Replica replica,
                               NavigableMap<Range<Token>, EndpointsForRange.Builder> ascendingMap,
                               NavigableMap<Range<Token>, EndpointsForRange.Builder> descendingMap)
    {
        EndpointsForRange.Builder replicas = ascendingMap.get(range);
        if (replicas == null)
        {
            replicas = new EndpointsForRange.Builder(range,1);
            ascendingMap.put(range, replicas);
            descendingMap.put(range, replicas);
        }
        replicas.add(replica, Conflict.DUPLICATE);
    }

    public void addPendingRange(Range<Token> range, Replica replica)
    {
        if (Range.isWrapAround(range.left, range.right))
        {
            addToMap(range, replica, ascendingMapForWrapAround, descendingMapForWrapAround);
        }
        else
        {
            addToMap(range, replica, ascendingMap, descendingMap);
        }
    }

    static final void addIntersections(EndpointsForToken.Builder replicasToAdd,
                                       NavigableMap<Range<Token>, EndpointsForRange.Builder> smallerMap,
                                       NavigableMap<Range<Token>, EndpointsForRange.Builder> biggerMap)
    {
        // find the intersection of two sets
        for (Range<Token> range : smallerMap.keySet())
        {
            EndpointsForRange.Builder replicas = biggerMap.get(range);
            if (replicas != null)
            {
                replicasToAdd.addAll(replicas);
            }
        }
    }

    public EndpointsForToken pendingEndpointsFor(Token token)
    {
        EndpointsForToken.Builder replicas = EndpointsForToken.builder(token);

        Range<Token> searchRange = new Range<>(token, token);

        // search for non-wrap-around maps
        NavigableMap<Range<Token>, EndpointsForRange.Builder> ascendingTailMap = ascendingMap.tailMap(searchRange, true);
        NavigableMap<Range<Token>, EndpointsForRange.Builder> descendingTailMap = descendingMap.tailMap(searchRange, false);

        // add intersections of two maps
        if (ascendingTailMap.size() < descendingTailMap.size())
        {
            addIntersections(replicas, ascendingTailMap, descendingTailMap);
        }
        else
        {
            addIntersections(replicas, descendingTailMap, ascendingTailMap);
        }

        // search for wrap-around sets
        ascendingTailMap = ascendingMapForWrapAround.tailMap(searchRange, true);
        descendingTailMap = descendingMapForWrapAround.tailMap(searchRange, false);

        // add them since they are all necessary.
        for (Map.Entry<Range<Token>, EndpointsForRange.Builder> entry : ascendingTailMap.entrySet())
        {
            replicas.addAll(entry.getValue());
        }
        for (Map.Entry<Range<Token>, EndpointsForRange.Builder> entry : descendingTailMap.entrySet())
        {
            replicas.addAll(entry.getValue());
        }

        return replicas.build();
    }

    public String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Range<Token>, EndpointsForRange.Builder> entry : this)
        {
            Range<Token> range = entry.getKey();

            for (Replica replica : entry.getValue())
            {
                sb.append(replica).append(':').append(range);
                sb.append(LINE_SEPARATOR.getString());
            }
        }

        return sb.toString();
    }

    @Override
    public Iterator<Map.Entry<Range<Token>, EndpointsForRange.Builder>> iterator()
    {
        return Iterators.concat(ascendingMap.entrySet().iterator(), ascendingMapForWrapAround.entrySet().iterator());
    }
}
