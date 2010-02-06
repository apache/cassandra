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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;


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
        super(left, right);
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
            return (bi.compareTo(left) > 0 && right.compareTo(bi) >= 0);
        }
    }

    public boolean contains(Range that)
    {
        boolean thiswraps = isWrapAround(left, right);
        boolean thatwraps = isWrapAround(that.left, that.right);
        if (thiswraps == thatwraps)
            return left.compareTo(that.left) <= 0 &&
                that.right.compareTo(right) <= 0;
        else if (thiswraps)
            // wrapping might contain non-wrapping
            return left.compareTo(that.left) <= 0 ||
                that.right.compareTo(right) <= 0;
        else // (thatwraps)
            // non-wrapping cannot contain wrapping
            return false;
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

    public List<Range> intersectionWith(Range that)
    {
        boolean thiswraps = isWrapAround(left, right);
        boolean thatwraps = isWrapAround(that.left, that.right);
        if (thiswraps && thatwraps)
        {
            // there is always an intersection when both wrap
            return Arrays.asList(new Range((Token)ObjectUtils.max(this.left, that.left),
                                           (Token)ObjectUtils.min(this.right, that.right)));
        }
        if (!thiswraps && !thatwraps)
        {
            if (!(left.compareTo(that.right) < 0 && that.left.compareTo(right) < 0))
                return Collections.emptyList();
            return Arrays.asList(new Range((Token)ObjectUtils.max(this.left, that.left),
                                           (Token)ObjectUtils.min(this.right, that.right)));
        }
        if (thiswraps && !thatwraps)
            return intersectionOneWrapping(this, that);
        assert (!thiswraps && thatwraps);
        return intersectionOneWrapping(that, this);
    }

    public List<AbstractBounds> restrictTo(Range range)
    {
        return (List) intersectionWith(range);
    }

    private static List<Range> intersectionOneWrapping(Range wrapping, Range other)
    {
        List<Range> intersection = new ArrayList<Range>(2);
        if (wrapping.contains(other))
        {
            return Arrays.asList(other);
        }
        if (other.contains(wrapping.right) || other.left.equals(wrapping.left))
            intersection.add(new Range(other.left, wrapping.right));
        if (other.contains(wrapping.left) && wrapping.left.compareTo(other.right) < 0)
            intersection.add(new Range(wrapping.left, other.right));
        return Collections.unmodifiableList(intersection);
    }

    /**
     * Tells if the given range is a wrap around.
         */
    public static boolean isWrapAround(Token left, Token right)
    {
        return left.compareTo(right) >= 0;
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
        
        return right.compareTo(rhs.right);
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
        return left.equals(rhs.left) && right.equals(rhs.right);
    }
    
    @Override
    public int hashCode()
    {
        return toString().hashCode();
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
