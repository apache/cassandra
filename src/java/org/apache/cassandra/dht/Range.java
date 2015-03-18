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

import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.utils.Pair;

/**
 * A representation of the range that a node is responsible for on the DHT ring.
 *
 * A Range is responsible for the tokens between (left, right].
 *
 * Used by the partitioner and by map/reduce by-token range scans.
 */
public class Range<T extends RingPosition<T>> extends AbstractBounds<T> implements Comparable<Range<T>>, Serializable
{
    public static final long serialVersionUID = 1L;

    public Range(T left, T right)
    {
        super(left, right);
    }

    public static <T extends RingPosition<T>> boolean contains(T left, T right, T point)
    {
        if (isWrapAround(left, right))
        {
            /*
             * We are wrapping around, so the interval is (a,b] where a >= b,
             * then we have 3 cases which hold for any given token k:
             * (1) a < k -- return true
             * (2) k <= b -- return true
             * (3) b < k <= a -- return false
             */
            if (point.compareTo(left) > 0)
                return true;
            else
                return right.compareTo(point) >= 0;
        }
        else
        {
            /*
             * This is the range (a, b] where a < b.
             */
            return point.compareTo(left) > 0 && right.compareTo(point) >= 0;
        }
    }

    public boolean contains(Range<T> that)
    {
        if (this.left.equals(this.right))
        {
            // full ring always contains all other ranges
            return true;
        }

        boolean thiswraps = isWrapAround(left, right);
        boolean thatwraps = isWrapAround(that.left, that.right);
        if (thiswraps == thatwraps)
        {
            return left.compareTo(that.left) <= 0 && that.right.compareTo(right) <= 0;
        }
        else if (thiswraps)
        {
            // wrapping might contain non-wrapping
            // that is contained if both its tokens are in one of our wrap segments
            return left.compareTo(that.left) <= 0 || that.right.compareTo(right) <= 0;
        }
        else
        {
            // (thatwraps)
            // non-wrapping cannot contain wrapping
            return false;
        }
    }

    /**
     * Helps determine if a given point on the DHT ring is contained
     * in the range in question.
     * @param point point in question
     * @return true if the point contains within the range else false.
     */
    public boolean contains(T point)
    {
        return contains(left, right, point);
    }

    /**
     * @param that range to check for intersection
     * @return true if the given range intersects with this range.
     */
    public boolean intersects(Range<T> that)
    {
        return intersectionWith(that).size() > 0;
    }

    public boolean intersects(AbstractBounds<T> that)
    {
        // implemented for cleanup compaction membership test, so only Range + Bounds are supported for now
        if (that instanceof Range)
            return intersects((Range<T>) that);
        if (that instanceof Bounds)
            return intersects((Bounds<T>) that);
        throw new UnsupportedOperationException("Intersection is only supported for Bounds and Range objects; found " + that.getClass());
    }

    /**
     * @param that range to check for intersection
     * @return true if the given range intersects with this range.
     */
    public boolean intersects(Bounds<T> that)
    {
        // Same punishment than in Bounds.contains(), we must be carefull if that.left == that.right as
        // as new Range<T>(that.left, that.right) will then cover the full ring which is not what we
        // want.
        return contains(that.left) || (!that.left.equals(that.right) && intersects(new Range<T>(that.left, that.right)));
    }

    @SafeVarargs
    public static <T extends RingPosition<T>> Set<Range<T>> rangeSet(Range<T> ... ranges)
    {
        return Collections.unmodifiableSet(new HashSet<Range<T>>(Arrays.asList(ranges)));
    }

    public static <T extends RingPosition<T>> Set<Range<T>> rangeSet(Range<T> range)
    {
        return Collections.singleton(range);
    }

    /**
     * @param that
     * @return the intersection of the two Ranges.  this can be two disjoint Ranges if one is wrapping and one is not.
     * say you have nodes G and M, with query range (D,T]; the intersection is (M-T] and (D-G].
     * If there is no intersection, an empty list is returned.
     */
    public Set<Range<T>> intersectionWith(Range<T> that)
    {
        if (that.contains(this))
            return rangeSet(this);
        if (this.contains(that))
            return rangeSet(that);

        boolean thiswraps = isWrapAround(left, right);
        boolean thatwraps = isWrapAround(that.left, that.right);
        if (!thiswraps && !thatwraps)
        {
            // neither wraps.  the straightforward case.
            if (!(left.compareTo(that.right) < 0 && that.left.compareTo(right) < 0))
                return Collections.emptySet();
            return rangeSet(new Range<T>(ObjectUtils.max(this.left, that.left),
                                         ObjectUtils.min(this.right, that.right)));
        }
        if (thiswraps && thatwraps)
        {
            // if the starts are the same, one contains the other, which we have already ruled out.
            assert !this.left.equals(that.left);
            // two wrapping ranges always intersect.
            // since we have already determined that neither this nor that contains the other, we have 2 cases,
            // and mirror images of those case.
            // (1) both of that's (1, 2] endpoints lie in this's (A, B] right segment:
            //  ---------B--------A--1----2------>
            // (2) only that's start endpoint lies in this's right segment:
            //  ---------B----1---A-------2------>
            // or, we have the same cases on the left segement, which we can handle by swapping this and that.
            return this.left.compareTo(that.left) < 0
                   ? intersectionBothWrapping(this, that)
                   : intersectionBothWrapping(that, this);
        }
        if (thiswraps && !thatwraps)
            return intersectionOneWrapping(this, that);
        assert (!thiswraps && thatwraps);
        return intersectionOneWrapping(that, this);
    }

    private static <T extends RingPosition<T>> Set<Range<T>> intersectionBothWrapping(Range<T> first, Range<T> that)
    {
        Set<Range<T>> intersection = new HashSet<Range<T>>(2);
        if (that.right.compareTo(first.left) > 0)
            intersection.add(new Range<T>(first.left, that.right));
        intersection.add(new Range<T>(that.left, first.right));
        return Collections.unmodifiableSet(intersection);
    }

    private static <T extends RingPosition<T>> Set<Range<T>> intersectionOneWrapping(Range<T> wrapping, Range<T> other)
    {
        Set<Range<T>> intersection = new HashSet<Range<T>>(2);
        if (other.contains(wrapping.right))
            intersection.add(new Range<T>(other.left, wrapping.right));
        // need the extra compareto here because ranges are asymmetrical; wrapping.left _is not_ contained by the wrapping range
        if (other.contains(wrapping.left) && wrapping.left.compareTo(other.right) < 0)
            intersection.add(new Range<T>(wrapping.left, other.right));
        return Collections.unmodifiableSet(intersection);
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position) || left.equals(position);
        // Check if the split would have no effect on the range
        if (position.equals(left) || position.equals(right))
            return null;

        AbstractBounds<T> lb = new Range<T>(left, position);
        AbstractBounds<T> rb = new Range<T>(position, right);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return false;
    }

    public boolean inclusiveRight()
    {
        return true;
    }

    public List<Range<T>> unwrap()
    {
        T minValue = right.minValue();
        if (!isWrapAround() || right.equals(minValue))
            return Arrays.asList(this);
        List<Range<T>> unwrapped = new ArrayList<Range<T>>(2);
        unwrapped.add(new Range<T>(left, minValue));
        unwrapped.add(new Range<T>(minValue, right));
        return unwrapped;
    }

    /**
     * Tells if the given range is a wrap around.
     */
    public static <T extends RingPosition<T>> boolean isWrapAround(T left, T right)
    {
       return left.compareTo(right) >= 0;
    }

    public int compareTo(Range<T> rhs)
    {
        /*
         * If the range represented by the "this" pointer
         * is a wrap around then it is the smaller one.
         */
        if ( isWrapAround(left, right) )
            return -1;

        if ( isWrapAround(rhs.left, rhs.right) )
            return 1;

        return right.compareTo(rhs.right);
    }

    /**
     * Subtracts a portion of this range.
     * @param contained The range to subtract from this. It must be totally
     * contained by this range.
     * @return An ArrayList of the Ranges left after subtracting contained
     * from this.
     */
    private ArrayList<Range<T>> subtractContained(Range<T> contained)
    {
        ArrayList<Range<T>> difference = new ArrayList<Range<T>>(2);

        if (!left.equals(contained.left))
            difference.add(new Range<T>(left, contained.left));
        if (!right.equals(contained.right))
            difference.add(new Range<T>(contained.right, right));
        return difference;
    }

    public Set<Range<T>> subtract(Range<T> rhs)
    {
        return rhs.differenceToFetch(this);
    }


    /**
     * Calculate set of the difference ranges of given two ranges
     * (as current (A, B] and rhs is (C, D])
     * which node will need to fetch when moving to a given new token
     *
     * @param rhs range to calculate difference
     * @return set of difference ranges
     */
    public Set<Range<T>> differenceToFetch(Range<T> rhs)
    {
        Set<Range<T>> result;
        Set<Range<T>> intersectionSet = this.intersectionWith(rhs);
        if (intersectionSet.isEmpty())
        {
            result = new HashSet<Range<T>>();
            result.add(rhs);
        }
        else
        {
            @SuppressWarnings("unchecked")
            Range<T>[] intersections = (Range<T>[]) new Range[intersectionSet.size()];
            intersectionSet.toArray(intersections);
            if (intersections.length == 1)
            {
                result = new HashSet<Range<T>>(rhs.subtractContained(intersections[0]));
            }
            else
            {
                // intersections.length must be 2
                Range<T> first = intersections[0];
                Range<T> second = intersections[1];
                ArrayList<Range<T>> temp = rhs.subtractContained(first);

                // Because there are two intersections, subtracting only one of them
                // will yield a single Range.
                Range<T> single = temp.get(0);
                result = new HashSet<Range<T>>(single.subtractContained(second));
            }
        }
        return result;
    }

    public static <T extends RingPosition<T>> boolean isInRanges(T token, Iterable<Range<T>> ranges)
    {
        assert ranges != null;

        for (Range<T> range : ranges)
        {
            if (range.contains(token))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Range))
            return false;
        Range<?> rhs = (Range<?>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "(" + left + "," + right + "]";
    }

    protected String getOpeningString()
    {
        return "(";
    }

    protected String getClosingString()
    {
        return "]";
    }

    public List<String> asList()
    {
        ArrayList<String> ret = new ArrayList<String>(2);
        ret.add(left.toString());
        ret.add(right.toString());
        return ret;
    }

    public boolean isWrapAround()
    {
        return isWrapAround(left, right);
    }

    /**
     * @return A copy of the given list of with all ranges unwrapped, sorted by left bound and with overlapping bounds merged.
     */
    public static <T extends RingPosition<T>> List<Range<T>> normalize(Collection<Range<T>> ranges)
    {
        // unwrap all
        List<Range<T>> output = new ArrayList<Range<T>>(ranges.size());
        for (Range<T> range : ranges)
            output.addAll(range.unwrap());

        // sort by left
        Collections.sort(output, new Comparator<Range<T>>()
        {
            public int compare(Range<T> b1, Range<T> b2)
            {
                return b1.left.compareTo(b2.left);
            }
        });

        // deoverlap
        return deoverlap(output);
    }

    /**
     * Given a list of unwrapped ranges sorted by left position, return an
     * equivalent list of ranges but with no overlapping ranges.
     */
    private static <T extends RingPosition<T>> List<Range<T>> deoverlap(List<Range<T>> ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        List<Range<T>> output = new ArrayList<Range<T>>();

        Iterator<Range<T>> iter = ranges.iterator();
        Range<T> current = iter.next();

        T min = (T) current.left.minValue();
        while (iter.hasNext())
        {
            // If current goes to the end of the ring, we're done
            if (current.right.equals(min))
            {
                // If one range is the full range, we return only that
                if (current.left.equals(min))
                    return Collections.<Range<T>>singletonList(current);

                output.add(new Range<T>(current.left, min));
                return output;
            }

            Range<T> next = iter.next();

            // if next left is equal to current right, we do not intersect per se, but replacing (A, B] and (B, C] by (A, C] is
            // legit, and since this avoid special casing and will result in more "optimal" ranges, we do the transformation
            if (next.left.compareTo(current.right) <= 0)
            {
                // We do overlap
                // (we've handled current.right.equals(min) already)
                if (next.right.equals(min) || current.right.compareTo(next.right) < 0)
                    current = new Range<T>(current.left, next.right);
            }
            else
            {
                output.add(current);
                current = next;
            }
        }
        output.add(current);
        return output;
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new Range<T>(left, newRight);
    }

    /**
     * Compute a range of keys corresponding to a given range of token.
     */
    public static Range<RowPosition> makeRowRange(Token left, Token right)
    {
        return new Range<RowPosition>(left.maxKeyBound(), right.maxKeyBound());
    }

    public static Range<RowPosition> makeRowRange(Range<Token> tokenBounds)
    {
        return makeRowRange(tokenBounds.left, tokenBounds.right);
    }
}
