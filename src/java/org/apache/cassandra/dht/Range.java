/**
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
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.ObjectUtils;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A representation of the range that a node is responsible for on the DHT ring.
 *
 * A Range is responsible for the tokens between (left, right].
 */
public class Range<T extends RingPosition> extends AbstractBounds<T> implements Comparable<Range<T>>, Serializable
{
    public static final long serialVersionUID = 1L;

    public Range(T left, T right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    public Range(T left, T right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
    }

    public static <T extends RingPosition> boolean contains(T left, T right, T bi)
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
            if (bi.compareTo(left) > 0)
                return true;
            else
                return right.compareTo(bi) >= 0;
        }
        else
        {
            /*
             * This is the range (a, b] where a < b.
             */
            return bi.compareTo(left) > 0 && right.compareTo(bi) >= 0;
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
     * @param bi point in question
     * @return true if the point contains within the range else false.
     */
    public boolean contains(T bi)
    {
        return contains(left, right, bi);
    }

    /**
     * @param that range to check for intersection
     * @return true if the given range intersects with this range.
     */
    public boolean intersects(Range<T> that)
    {
        return intersectionWith(that).size() > 0;
    }

    public static <T extends RingPosition> Set<Range<T>> rangeSet(Range<T> ... ranges)
    {
        return Collections.unmodifiableSet(new HashSet<Range<T>>(Arrays.asList(ranges)));
    }

    public static <T extends RingPosition> Set<Range<T>> rangeSet(Range<T> range)
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
            return rangeSet(new Range<T>((T)ObjectUtils.max(this.left, that.left),
                                         (T)ObjectUtils.min(this.right, that.right),
                                         partitioner));
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

    private static <T extends RingPosition> Set<Range<T>> intersectionBothWrapping(Range<T> first, Range<T> that)
    {
        Set<Range<T>> intersection = new HashSet<Range<T>>(2);
        if (that.right.compareTo(first.left) > 0)
            intersection.add(new Range<T>(first.left, that.right, first.partitioner));
        intersection.add(new Range<T>(that.left, first.right, first.partitioner));
        return Collections.unmodifiableSet(intersection);
    }

    private static <T extends RingPosition> Set<Range<T>> intersectionOneWrapping(Range<T> wrapping, Range<T> other)
    {
        Set<Range<T>> intersection = new HashSet<Range<T>>(2);
        if (other.contains(wrapping.right))
            intersection.add(new Range<T>(other.left, wrapping.right, wrapping.partitioner));
        // need the extra compareto here because ranges are asymmetrical; wrapping.left _is not_ contained by the wrapping range
        if (other.contains(wrapping.left) && wrapping.left.compareTo(other.right) < 0)
            intersection.add(new Range<T>(wrapping.left, other.right, wrapping.partitioner));
        return Collections.unmodifiableSet(intersection);
    }

    public AbstractBounds<T> createFrom(T pos)
    {
        if (pos.equals(left))
            return null;
        return new Range<T>(left, pos, partitioner);
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        T minValue = (T) partitioner.minValue(right.getClass());
        if (!isWrapAround() || right.equals(minValue))
            return Arrays.asList(this);
        List<AbstractBounds<T>> unwrapped = new ArrayList<AbstractBounds<T>>(2);
        unwrapped.add(new Range<T>(left, minValue, partitioner));
        unwrapped.add(new Range<T>(minValue, right, partitioner));
        return unwrapped;
    }

    /**
     * Tells if the given range is a wrap around.
     */
    public static <T extends RingPosition> boolean isWrapAround(T left, T right)
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
        ArrayList<Range<T>> difference = new ArrayList<Range<T>>();

        if (!left.equals(contained.left))
            difference.add(new Range<T>(left, contained.left, partitioner));
        if (!right.equals(contained.right))
            difference.add(new Range<T>(contained.right, right, partitioner));
        return difference;
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
            Range[] intersections = new Range[intersectionSet.size()];
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

    public static <T extends RingPosition> boolean isInRanges(T token, Iterable<Range<T>> ranges)
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
        Range<T> rhs = (Range<T>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "(" + left + "," + right + "]";
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
     * Compute a range of keys corresponding to a given range of token.
     */
    public static Range<RowPosition> makeRowRange(Token left, Token right, IPartitioner partitioner)
    {
        return new Range<RowPosition>(left.maxKeyBound(partitioner), right.maxKeyBound(partitioner), partitioner);
    }

    public AbstractBounds<RowPosition> toRowBounds()
    {
        return (left instanceof Token) ? makeRowRange((Token)left, (Token)right, partitioner) : (Range<RowPosition>)this;
    }

    public AbstractBounds<Token> toTokenBounds()
    {
        return (left instanceof RowPosition) ? new Range<Token>(((RowPosition)left).getToken(), ((RowPosition)right).getToken(), partitioner) : (Range<Token>)this;
    }
}
