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

package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.utils.Pair;

/**
 * Splits a range at the boundaries of a set of {@link NormalizedRanges}.
 *
 * <p>After splitting, each sub-range is either entirely within or entirely outside the boundary set, and
 * each sub-range carries that classification (see {@link Split#isWithinBoundary}). Used by both MigrationRouter
 * (tracked/untracked routing) and CoordinationPlanIterator (failover state boundary splitting).
 */
public class RangeSplitter
{
    /**
     * A sub-range produced by splitting, tagged with whether it falls within the boundary set.
     * <p>
     * The classification is derived directly from the split itself (intersections are within, gaps and the
     * trailing remainder are outside), so callers never need to re-derive membership and cannot disagree with
     * how the range was actually split.
     */
    public static class Split
    {
        public final AbstractBounds<PartitionPosition> range;
        /** true if this sub-range lies entirely within the boundary set, false if entirely outside */
        public final boolean isWithinBoundary;

        Split(AbstractBounds<PartitionPosition> range, boolean isWithinBoundary)
        {
            this.range = range;
            this.isWithinBoundary = isWithinBoundary;
        }
    }

    /**
     * Split a range at the boundaries of the given {@link NormalizedRanges} set.
     *
     * Returns a list of contiguous sub-ranges covering the original range. Each sub-range is either
     * entirely within a boundary range (intersection) or entirely outside all boundary ranges (gap).
     *
     * If the range does not cross any boundary, returns a single-element list containing the original range.
     *
     * @param range the range to split
     * @param boundaries the set of ranges to split at
     * @return ordered list of sub-ranges covering the original range
     */
    public static List<AbstractBounds<PartitionPosition>> splitAtBoundaries(AbstractBounds<PartitionPosition> range,
                                                                            List<Range<Token>> boundaries)
    {
        List<Split> tagged = splitAtBoundariesTagged(range, boundaries);
        List<AbstractBounds<PartitionPosition>> result = new ArrayList<>(tagged.size());
        for (Split split : tagged)
            result.add(split.range);
        return result;
    }

    /**
     * Split a range at the boundaries of the given {@link NormalizedRanges} set, tagging each sub-range with
     * whether it falls within the boundary set.
     *
     * Behaves exactly like {@link #splitAtBoundaries(AbstractBounds, List)} but each returned {@link Split}
     * also reports whether its range is within (intersection) or outside (gap/trailing remainder) the boundaries.
     *
     * @param range the range to split
     * @param boundaries the set of ranges to split at
     * @return ordered list of tagged sub-ranges covering the original range
     */
    public static List<Split> splitAtBoundariesTagged(AbstractBounds<PartitionPosition> range,
                                                       List<Range<Token>> boundaries)
    {
        List<Split> result = new ArrayList<>();
        AbstractBounds<PartitionPosition> remainder = range;

        for (Range<Token> boundary : boundaries)
        {
            if (addGapBefore(result, remainder, boundary))
            {
                remainder = null;
                break;
            }

            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> split =
                Range.intersectionAndRemainder(remainder, boundary);

            // The intersection lies entirely within this boundary range
            if (split.left != null)
                result.add(new Split(split.left, true));

            remainder = split.right;
            if (remainder == null)
                break;
        }

        // Anything left after the last boundary is outside all boundaries
        if (remainder != null)
            result.add(new Split(remainder, false));

        return result;
    }

    /**
     * If the remainder starts before the boundary range, add the gap (the portion before the boundary)
     * to the result. Gaps are always outside the boundary set.
     *
     * @return true if the remainder ends before the boundary (no intersection possible, remainder fully consumed)
     */
    private static boolean addGapBefore(List<Split> result,
                                        AbstractBounds<PartitionPosition> remainder,
                                        Range<Token> boundary)
    {
        Token boundaryStart = boundary.left;
        Token remainderStart = remainder.left.getToken();
        Token remainderEnd = remainder.right.getToken();

        if (remainderStart.compareTo(boundaryStart) >= 0)
            return false; // No gap -- remainder starts at or after boundary

        // Check if remainder ends before boundary starts
        if (!remainderEnd.isMinimum() && remainderEnd.compareTo(boundaryStart) <= 0)
        {
            result.add(new Split(remainder, false));
            return true;
        }

        // Add the gap before boundary
        AbstractBounds<PartitionPosition> gap = remainder.withNewRight(boundaryStart.maxKeyBound());
        if (!gap.left.equals(gap.right))
            result.add(new Split(gap, false));

        return false;
    }
}
