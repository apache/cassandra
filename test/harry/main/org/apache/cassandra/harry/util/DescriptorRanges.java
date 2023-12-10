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

package org.apache.cassandra.harry.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// TODO: this is not really an interval tree, just two sorted arrays. However, given that ExhaustiveChecker has
//       to inflate a partition every time we execute the query, we always know all boundaries at any given point in time.
public class DescriptorRanges
{
    private List<DescriptorRange> sortedByMin;

    public DescriptorRanges(List<DescriptorRange> ranges)
    {
        this.sortedByMin = new ArrayList<>(ranges);
        Collections.sort(sortedByMin, Comparator.comparingLong(a -> a.minBound));
    }

    public boolean isShadowed(long cd, long lts)
    {
        return !shadowedBy(cd, lts).isEmpty();
    }

    public List<DescriptorRange> shadowedBy(long cd, long lts)
    {
        List<DescriptorRange> shadowedBy = new ArrayList<>();
        for (DescriptorRange range : sortedByMin)
        {
            if (range.minBound > cd)
                break;

            if (range.contains(cd, lts))
                shadowedBy.add(range);
        }

        return shadowedBy;
    }

    public List<DescriptorRange> newerThan(long ts)
    {
        return sortedByMin.stream().filter((rt) -> {
            return rt.timestamp >= ts;
        }).collect(Collectors.toList());
    }


    private static int toIdx(int idxOrIP)
    {
        if (idxOrIP >= 0)
            return idxOrIP;

        return -1 * (idxOrIP + 1);
    }

    public static class DescriptorRange
    {
        public final long minBound;
        public final long maxBound;
        public final boolean minInclusive;
        public final boolean maxInclusive;

        public final long timestamp;

        public DescriptorRange(long minBound, long maxBound, boolean minInclusive, boolean maxInclusive, long timestamp)
        {
            this.minBound = minBound;
            this.maxBound = maxBound;
            this.minInclusive = minInclusive;
            this.maxInclusive = maxInclusive;

            this.timestamp = timestamp;
        }

        public boolean contains(long descriptor)
        {
            if (minInclusive && maxInclusive)
                return descriptor >= minBound && descriptor <= maxBound;

            if (!minInclusive && !maxInclusive)
                return descriptor > minBound && descriptor < maxBound;

            if (!minInclusive && maxInclusive)
                return descriptor > minBound && descriptor <= maxBound;

            assert (minInclusive && !maxInclusive);
            return descriptor >= minBound && descriptor < maxBound;
        }

        public boolean contains(long descriptor, long ts)
        {
            return contains(descriptor) && timestamp >= ts;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DescriptorRange range = (DescriptorRange) o;
            return minBound == range.minBound &&
                   maxBound == range.maxBound &&
                   minInclusive == range.minInclusive &&
                   maxInclusive == range.maxInclusive &&
                   timestamp == range.timestamp;
        }

        public int hashCode()
        {
            return Objects.hash(minBound, maxBound, minInclusive, maxInclusive, timestamp);
        }

        public String toString()
        {
            return "Range{" +
                   "minBound=" + minBound +
                   ", maxBound=" + maxBound +
                   ", minInclusive=" + minInclusive +
                   ", maxInclusive=" + maxInclusive +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }

    public String toString()
    {
        return "Ranges{" +
               "sortedByMin=" + sortedByMin +
               '}';
    }
}
