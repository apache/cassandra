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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiPredicate;

public class Overlaps
{
    /**
     * Construct a minimal list of overlap sets, i.e. the sections of the range span when we have overlapping items,
     * where we ensure:
     * - non-overlapping items are never put in the same set
     * - no item is present in non-consecutive sets
     * - for any point where items overlap, the result includes a set listing all overlapping items
     * <p>
     * For example, for inputs A[0, 4), B[2, 8), C[6, 10), D[1, 9) the result would be the sets ABD and BCD. We are not
     * interested in the spans where A, B, or C are present on their own or in combination with D, only that there
     * exists a set in the list that is a superset of any such combination, and that the non-overlapping A and C are
     * never together in a set.
     * <p>
     * Note that the full list of overlap sets A, AD, ABD, BD, BCD, CD, C is also an answer that satisfies the three
     * conditions above, but it contains redundant sets (e.g. AD is already contained in ABD).
     *
     * @param items            A list of items to distribute in overlap sets. This is assumed to be a transient list and the method
     *                         may modify or consume it. It is assumed that the start and end positions of an item are ordered,
     *                         and the items are non-empty.
     * @param startsAfter      Predicate determining if its left argument's start if fully after the right argument's end.
     *                         This will only be used with arguments where left's start is known to be after right's start.
     *                         It is up to the caller if this is a strict comparison -- strict (>) for end-inclusive spans
     *                         and non-strict (>=) for end-exclusive.
     * @param startsComparator Comparator of items' starting positions.
     * @param endsComparator   Comparator of items' ending positions.
     * @return List of overlap sets.
     */
    public static <E> List<Set<E>> constructOverlapSets(List<E> items,
                                                        BiPredicate<E, E> startsAfter,
                                                        Comparator<E> startsComparator,
                                                        Comparator<E> endsComparator)
    {
        List<Set<E>> overlaps = new ArrayList<>();
        if (items.isEmpty())
            return overlaps;

        PriorityQueue<E> active = new PriorityQueue<>(endsComparator);
        items.sort(startsComparator);
        for (E item : items)
        {
            if (!active.isEmpty() && startsAfter.test(item, active.peek()))
            {
                // New item starts after some active ends. It does not overlap with it, so:
                // -- output the previous active set
                overlaps.add(new HashSet<>(active));
                // -- remove all items that also end before the current start
                do
                {
                    active.poll();
                }
                while (!active.isEmpty() && startsAfter.test(item, active.peek()));
            }

            // Add the new item to the active state. We don't care if it starts later than others in the active set,
            // the important point is that it overlaps with all of them.
            active.add(item);
        }

        assert !active.isEmpty();
        overlaps.add(new HashSet<>(active));

        return overlaps;
    }
    public enum InclusionMethod
    {
        NONE, SINGLE, TRANSITIVE;
    }

    public interface BucketMaker<E, B>
    {
        B makeBucket(List<Set<E>> sets, int startIndexInclusive, int endIndexExclusive);
    }

    /**
     * Assign overlap sections into buckets. Identify sections that have at least threshold-many overlapping
     * items and apply the overlap inclusion method to combine with any neighbouring sections that contain
     * selected sstables to make sure we make full use of any sstables selected for compaction (i.e. avoid
     * recompacting, see {@link org.apache.cassandra.db.compaction.unified.Controller#overlapInclusionMethod()}).
     *
     * @param threshold       Threshold for selecting a bucket. Sets below this size will be ignored, unless they need
     *                        to be grouped with a neighboring set due to overlap.
     * @param inclusionMethod NONE to only form buckets of the overlapping sets, SINGLE to include all
     *                        sets that share an sstable with a selected bucket, or TRANSITIVE to include
     *                        all sets that have an overlap chain to a selected bucket.
     * @param overlaps        An ordered list of overlap sets as returned by {@link #constructOverlapSets}.
     * @param bucketer        Method used to create a bucket out of the supplied set indexes.
     */
    public static <E, B> List<B> assignOverlapsIntoBuckets(int threshold,
                                                           InclusionMethod inclusionMethod,
                                                           List<Set<E>> overlaps,
                                                           BucketMaker<E, B> bucketer)
    {
        List<B> buckets = new ArrayList<>();
        int regionCount = overlaps.size();
        int lastEnd = -1;
        for (int i = 0; i < regionCount; ++i)
        {
            Set<E> bucket = overlaps.get(i);
            int maxOverlap = bucket.size();
            if (maxOverlap < threshold)
                continue;
            int startIndex = i;
            int endIndex = i + 1;

            if (inclusionMethod != InclusionMethod.NONE)
            {
                Set<E> allOverlapping = new HashSet<>(bucket);
                Set<E> overlapTarget = inclusionMethod == InclusionMethod.TRANSITIVE
                                       ? allOverlapping
                                       : bucket;
                int j;
                for (j = i - 1; j > lastEnd; --j)
                {
                    Set<E> next = overlaps.get(j);
                    if (!setsIntersect(next, overlapTarget))
                        break;
                    allOverlapping.addAll(next);
                }
                startIndex = j + 1;
                for (j = i + 1; j < regionCount; ++j)
                {
                    Set<E> next = overlaps.get(j);
                    if (!setsIntersect(next, overlapTarget))
                        break;
                    allOverlapping.addAll(next);
                }
                i = j - 1;
                endIndex = j;
            }
            buckets.add(bucketer.makeBucket(overlaps, startIndex, endIndex));
            lastEnd = i;
        }
        return buckets;
    }

    private static <E> boolean setsIntersect(Set<E> s1, Set<E> s2)
    {
        // Note: optimized for small sets and O(1) lookup.
        for (E s : s1)
            if (s2.contains(s))
                return true;

        return false;
    }

    /**
     * Pull the last elements from the given list, up to the given limit.
     */
    public static <T> List<T> pullLast(List<T> source, int limit)
    {
        List<T> result = new ArrayList<>(limit);
        while (--limit >= 0)
            result.add(source.remove(source.size() - 1));
        return result;
    }

    /**
     * Select up to `limit` sstables from each overlapping set (more than `limit` in total) by taking the last entries
     * from `allObjectsSorted`. To achieve this, keep selecting the last sstable until the next one we would add would
     * bring the number selected in some overlap section over `limit`.
     */
    public static <T> Collection<T> pullLastWithOverlapLimit(List<T> allObjectsSorted, List<Set<T>> overlapSets, int limit)
    {
        int setsCount = overlapSets.size();
        int[] selectedInBucket = new int[setsCount];
        int allCount = allObjectsSorted.size();
        for (int selectedCount = 0; selectedCount < allCount; ++selectedCount)
        {
            T candidate = allObjectsSorted.get(allCount - 1 - selectedCount);
            for (int i = 0; i < setsCount; ++i)
            {
                if (overlapSets.get(i).contains(candidate))
                {
                    ++selectedInBucket[i];
                    if (selectedInBucket[i] > limit)
                        return pullLast(allObjectsSorted, selectedCount);
                }
            }
        }
        return allObjectsSorted;
    }
}
