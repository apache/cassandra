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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import static com.google.common.base.Preconditions.checkState;

/*
 * Immutable list of ranges that are statically known to be normalized
 */
public class NormalizedRanges<T extends RingPosition<T>> extends AbstractList<Range<T>> implements List<Range<T>>, RandomAccess
{
    private static final NormalizedRanges EMPTY_NORMALIZED_RANGES = new NormalizedRanges(Collections.emptyList());

    private static final Comparator NORMALIZED_TOKEN_RANGE_COMPARATOR = (o1, o2) -> {
        Range range = (Range) o1;
        RingPosition key = (RingPosition) o2;
        boolean rangeRightIsMin = range.right.isMinimum();
        boolean keyIsMinimum = key.isMinimum();

        if (keyIsMinimum & rangeRightIsMin)
            return 0;

        int lc = key.compareTo(range.left);
        int rc = key.compareTo(range.right);
        if ((lc < 0 & !keyIsMinimum) | lc == 0) return 1;
        if (rc > 0 & !rangeRightIsMin) return -1;
        return 0;
    };

    public static <T extends RingPosition<T>> NormalizedRanges<T> empty()
    {
        return (NormalizedRanges<T>) EMPTY_NORMALIZED_RANGES;
    }

    /*
     * Compares ranges by right token. Used for intersecting normalized ranges.
     *
     * Assumes no wrap around ranges except for RHS = minValue which is essentialy synonymous with the maximal value.
     * This shows up coming out of unwrap because Range is not left inclusive so the only way to include minValue
     * in the range is by wrapping from maxValue.
     */
    private static <T extends RingPosition<T>> int compareNormalized(Range<T> lhs, Range<T> rhs)
    {
        // otherwise compare by right.
        int cmp = lhs.right.compareTo(rhs.right);
        // minValue on the RHS is maxValue, but doesn't work with compare so check for it explicitly
        boolean rhsRMin = rhs.right.isMinimum();
        boolean lhsRMin = lhs.right.isMinimum();

        if (rhsRMin && lhsRMin)
            return 0;

        if (cmp < 0)
        {
            if (lhsRMin)
            {
                return 1;
            }
            return -1;
        }
        else if (cmp > 0)
        {
            if (rhsRMin)
            {
                return -1;
            }
            return 1;
        }
        return 0;
    }

    private final Object[] ranges;

    private NormalizedRanges(Collection<Range<T>> ranges)
    {
        this.ranges = new Object[ranges.size()];
        int index = 0;
        for (Range<T> range : ranges)
            this.ranges[index++] = range;
    }

    public static <T extends RingPosition<T>> NormalizedRanges<T> normalizedRanges(Collection<Range<T>> ranges)
    {
        if (ranges instanceof NormalizedRanges)
            return (NormalizedRanges<T>) ranges;
        return new NormalizedRanges<>(Range.normalize(ranges));
    }

    @Override
    public Range<T> get(int index)
    {
        Objects.checkIndex(index, ranges.length);
        return (Range<T>) ranges[index];
    }

    public boolean intersects(T token)
    {
        if (this.size() == 1 && this.get(0).isFull())
            return true;
        boolean isIn = Collections.binarySearch((List) this, token, NORMALIZED_TOKEN_RANGE_COMPARATOR) >= 0;
        if (Range.EXPENSIVE_CHECKS)
            checkState(Range.isInRanges(token, this) == isIn);
        return isIn;
    }

    public NormalizedRanges<T> subtract(NormalizedRanges<T> b)
    {
        if (b.size() == 1 && b.get(0).isFull())
            return NormalizedRanges.empty();

        if (this.size() == 1 && this.get(0).isFull())
            return b.invert();

        List<Range<T>> remaining = new ArrayList<>();
        Iterator<Range<T>> aIter = this.iterator();
        Iterator<Range<T>> bIter = b.iterator();
        Range<T> aRange = aIter.hasNext() ? aIter.next() : null;
        Range<T> bRange = bIter.hasNext() ? bIter.next() : null;
        while (aRange != null && bRange != null)
        {
            boolean aRMin = aRange.right.isMinimum();
            boolean bRMin = bRange.right.isMinimum();

            if (aRMin && bRMin)
            {
                if (aRange.left.compareTo(bRange.left) < 0)
                    remaining.add(new Range<>(aRange.left, bRange.left));
                checkState(!aIter.hasNext() && !bIter.hasNext());
                aRange = null;
                break;
            }

            if (!aRMin && aRange.right.compareTo(bRange.left) <= 0)
            {
                remaining.add(aRange);
                aRange = aIter.hasNext() ? aIter.next() : null;
            }
            else if (!bRMin && aRange.left.compareTo(bRange.right) >= 0)
            {
                bRange = bIter.hasNext() ? bIter.next() : null;
            }
            else
            {
                // Handle what remains to the left of the intersection
                if (aRange.left.compareTo(bRange.left) < 0)
                {
                    remaining.add(new Range(aRange.left, bRange.left));
                }

                // Handle what remains to the right of the intersection
                if (!aRMin && (aRange.right.compareTo(bRange.right) <= 0 | bRMin))
                    aRange = aIter.hasNext() ? aIter.next() : null;
                else
                    aRange = new Range(bRange.right, aRange.right);
            }
        }

        while (aRange != null)
        {
            remaining.add(aRange);
            aRange = aIter.hasNext() ? aIter.next() : null;
        }

        NormalizedRanges<T> result = normalizedRanges(remaining);
        if (Range.EXPENSIVE_CHECKS)
            checkState(result.equals(Range.normalize(Range.subtract(this, b))));
        return result;
    }

    @VisibleForTesting
    public NormalizedRanges<T> invert()
    {
        if (isEmpty())
            return this;

        List<Range<T>> result = new ArrayList<>(size() + 2);
        T minValue = get(0).left.minValue();
        T left = minValue;
        for (Range<T> r : this)
        {
            if (!r.left.equals(left))
            {
                result.add(new Range<>(left, r.left));
            }
            left = r.right;
        }

        // Loop doesn't add the range to the right of the last one
        Range<T> last = get(size() - 1);
        if (!last.right.isMinimum())
            result.add(new Range<>(last.right, minValue));

        result = Range.normalize(result);
        if (Range.EXPENSIVE_CHECKS)
            checkState(result.equals(Range.normalize(Range.subtract(ImmutableList.of(new Range<>(minValue, minValue)), this))));
        return new NormalizedRanges<>(result);
    }

    public NormalizedRanges<T> intersection(NormalizedRanges<T> b)
    {
        if (this.size() == 1 && this.get(0).isFull())
            return b;
        if (b.size() == 1 && b.get(0).isFull())
            return this;

        List<Range<T>> merged = new ArrayList<>();
        PeekingIterator<Range<T>> aIter = Iterators.peekingIterator(this.iterator());
        PeekingIterator<Range<T>> bIter = Iterators.peekingIterator(b.iterator());
        while (aIter.hasNext() && bIter.hasNext())
        {
            Range<T> aRange = aIter.peek();
            Range<T> bRange = bIter.peek();

            int cmp = compareNormalized(aRange, bRange);
            if (aRange.intersects(bRange))
            {
                merged.addAll(aRange.intersectionWith(bRange));
                if (cmp == 0)
                {
                    aIter.next();
                    bIter.next();
                }
                else if (cmp < 0)
                {
                    aIter.next();
                }
                else
                {
                    bIter.next();
                }
            }
            else
            {
                if (cmp <= 0)
                    aIter.next();
                if (cmp >= 0)
                    bIter.next();
            }
        }

        NormalizedRanges<T> result = normalizedRanges(merged);

        if (Range.EXPENSIVE_CHECKS)
        {
            List<Range<T>> expensiveResult = new ArrayList<>();
            for (Range<T> r1 : this)
            {
                for (Range<T> r2 : b)
                {
                    expensiveResult.addAll(r1.intersectionWith(r2));
                }
            }
            checkState(result.equals(Range.normalize(expensiveResult)));
        }

        return result;
    }

    @Override
    public int size()
    {
        return ranges.length;
    }
}
