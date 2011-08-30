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

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A representation of the range that a node is responsible for on the DHT ring.
 *
 * A Range is responsible for the tokens between (left, right].
 */
public class Range extends AbstractBounds implements Comparable<Range>, Serializable
{
    public static final long serialVersionUID = 1L;
    
    public Range(Token left, Token right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    public Range(Token left, Token right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
    }

    public static boolean contains(Token left, Token right, Token bi)
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
            return ( compare(bi,left) > 0 && compare(right,bi) >= 0);
        }
    }

    public boolean contains(Range that)
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
            return compare(left,that.left) <= 0 && compare(that.right,right) <= 0;
        }
        else if (thiswraps)
        {
            // wrapping might contain non-wrapping
            // that is contained if both its tokens are in one of our wrap segments
            return compare(left,that.left) <= 0 || compare(that.right,right) <= 0;
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
    public boolean contains(Token bi)
    {
        return contains(left, right, bi);
    }

    /**
     * @param that range to check for intersection
     * @return true if the given range intersects with this range.
     */
    public boolean intersects(Range that)
    {
        return intersectionWith(that).size() > 0;
    }

    public static Set<Range> rangeSet(Range ... ranges)
    {
        return Collections.unmodifiableSet(new HashSet<Range>(Arrays.asList(ranges)));
    }

    /**
     * @param that
     * @return the intersection of the two Ranges.  this can be two disjoint Ranges if one is wrapping and one is not.
     * say you have nodes G and M, with query range (D,T]; the intersection is (M-T] and (D-G].
     * If there is no intersection, an empty list is returned.
     */
    public Set<Range> intersectionWith(Range that)
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
            return rangeSet(new Range((Token)ObjectUtils.max(this.left, that.left),
                                      (Token)ObjectUtils.min(this.right, that.right),
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

    private static Set<Range> intersectionBothWrapping(Range first, Range that)
    {
        Set<Range> intersection = new HashSet<Range>(2);
        if (that.right.compareTo(first.left) > 0)
            intersection.add(new Range(first.left, that.right, first.partitioner));
        intersection.add(new Range(that.left, first.right, first.partitioner));
        return Collections.unmodifiableSet(intersection);
    }

    private static Set<Range> intersectionOneWrapping(Range wrapping, Range other)
    {
        Set<Range> intersection = new HashSet<Range>(2);
        if (other.contains(wrapping.right))
            intersection.add(new Range(other.left, wrapping.right, wrapping.partitioner));
        // need the extra compareto here because ranges are asymmetrical; wrapping.left _is not_ contained by the wrapping range
        if (other.contains(wrapping.left) && wrapping.left.compareTo(other.right) < 0)
            intersection.add(new Range(wrapping.left, other.right, wrapping.partitioner));
        return Collections.unmodifiableSet(intersection);
    }

    public AbstractBounds createFrom(Token token)
    {
        if (token.equals(left))
            return null;
        return new Range(left, token, partitioner);
    }

    public List<AbstractBounds> unwrap()
    {
        if (!isWrapAround() || right.equals(partitioner.getMinimumToken()))
            return (List)Arrays.asList(this);
        List<AbstractBounds> unwrapped = new ArrayList<AbstractBounds>(2);
        unwrapped.add(new Range(left, partitioner.getMinimumToken(), partitioner));
        unwrapped.add(new Range(partitioner.getMinimumToken(), right, partitioner));
        return unwrapped;
    }

    /**
     * Tells if the given range is a wrap around.
     */
    public static boolean isWrapAround(Token left, Token right)
    {
       return compare(left,right) >= 0;           
    }
    
    public static int compare(Token left, Token right)
    {
        ByteBuffer l,r;

        if (left.token instanceof byte[])
        {
            l  = ByteBuffer.wrap((byte[]) left.token);
        }
        else if (left.token instanceof ByteBuffer)
        {
            l  = (ByteBuffer) left.token;
        }
        else
        {
            //Handles other token types
            return left.compareTo(right);
        }

        if (right.token instanceof byte[])
        {
            r  = ByteBuffer.wrap((byte[]) right.token);
        }
        else
        {
            r  = (ByteBuffer) right.token;
        }

        return ByteBufferUtil.compareUnsigned(l, r);
     }
    
    public int compareTo(Range rhs)
    {
        /* 
         * If the range represented by the "this" pointer
         * is a wrap around then it is the smaller one.
         */
        if ( isWrapAround(left, right) )
            return -1;

        if ( isWrapAround(rhs.left, rhs.right) )
            return 1;
        
        return compare(right,rhs.right);
    }

    /**
     * Subtracts a portion of this range.
     * @param contained The range to subtract from this. It must be totally
     * contained by this range.
     * @return An ArrayList of the Ranges left after subtracting contained
     * from this.
     */
    private ArrayList<Range> subtractContained(Range contained)
    {
        ArrayList<Range> difference = new ArrayList<Range>();

        if (!left.equals(contained.left))
            difference.add(new Range(left, contained.left, partitioner));
        if (!right.equals(contained.right))
            difference.add(new Range(contained.right, right, partitioner));
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
    public Set<Range> differenceToFetch(Range rhs)
    {
        Set<Range> result;
        Set<Range> intersectionSet = this.intersectionWith(rhs);
        if (intersectionSet.isEmpty())
        {
            result = new HashSet<Range>();
            result.add(rhs);
        }
        else
        {
            Range[] intersections = new Range[intersectionSet.size()];
            intersectionSet.toArray(intersections);
            if (intersections.length == 1)
            {
                result = new HashSet<Range>(rhs.subtractContained(intersections[0]));
            }
            else
            {
                // intersections.length must be 2
                Range first = intersections[0];
                Range second = intersections[1];
                ArrayList<Range> temp = rhs.subtractContained(first);

                // Because there are two intersections, subtracting only one of them
                // will yield a single Range.
                Range single = temp.get(0);
                result = new HashSet<Range>(single.subtractContained(second));
            }
        }
        return result;
    }

    public static boolean isTokenInRanges(Token token, Iterable<Range> ranges)
    {
        assert ranges != null;

        for (Range range : ranges)
        {
            if (range.contains(token))
            {
                return true;
            }
        }
        return false;
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof Range))
            return false;
        Range rhs = (Range)o;
        return compare(left,rhs.left) == 0 && compare(right,rhs.right) == 0;
    }
    
    public String toString()
    {
        return "(" + left + "," + right + "]";
    }

    public boolean isWrapAround()
    {
        return isWrapAround(left, right);
    }
}
