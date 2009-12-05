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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer;


/**
 * A representation of the range that a node is responsible for on the DHT ring.
 *
 * A Range is responsible for the tokens between (left, right].
 */
public class Range implements Comparable<Range>, Serializable
{
    private static ICompactSerializer<Range> serializer_;
    static
    {
        serializer_ = new RangeSerializer();
    }
    
    public static ICompactSerializer<Range> serializer()
    {
        return serializer_;
    }

    private final Token left_;
    private final Token right_;

    public Range(Token left, Token right)
    {
        left_ = left;
        right_ = right;
    }

    /**
     * Returns the left endpoint of a range.
     * @return left endpoint
     */
    public Token left()
    {
        return left_;
    }
    
    /**
     * Returns the right endpoint of a range.
     * @return right endpoint
     */
    public Token right()
    {
        return right_;
    }

    public static boolean contains(Token left, Token right, Token bi)
    {
        if ( isWrapAround(left, right) )
        {
            /* 
             * We are wrapping around, so the interval is (a,b] where a >= b,
             * then we have 3 cases which hold for any given token k:
             * (1) a < k -- return true
             * (2) k <= b -- return true
             * (3) b < k <= a -- return false
             */
            if ( bi.compareTo(left) > 0 )
                return true;
            else
                return right.compareTo(bi) >= 0;
        }
        else
        {
            /*
             * This is the range (a, b] where a < b. 
             */
            return ( bi.compareTo(left) > 0 && right.compareTo(bi) >= 0 );
        }        
    }

    public boolean contains(Range that)
    {
        boolean thiswraps = isWrapAround(this.left(), this.right());
        boolean thatwraps = isWrapAround(that.left(), that.right());
        if (thiswraps == thatwraps)
            return this.left().compareTo(that.left()) <= 0 &&
                that.right().compareTo(this.right()) <= 0;
        else if (thiswraps)
            // wrapping might contain non-wrapping
            return this.left().compareTo(that.left()) <= 0 ||
                that.right().compareTo(this.right()) <= 0;
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
        return contains(left_, right_, bi);
    }

    /**
     * @param range range to check for intersection
     * @return true if the given range intersects with this range.
     */
    public boolean intersects(Range that)
    {
        boolean thiswraps = isWrapAround(this.left(), this.right());
        boolean thatwraps = isWrapAround(that.left(), that.right());
        if (thiswraps && thatwraps)
            // both (must contain the minimum token)
            return true;
        else if (!thiswraps && !thatwraps)
            // neither
            return this.left().compareTo(that.right()) < 0 &&
                that.left().compareTo(this.right()) < 0;
        else
            // either
            return this.left().compareTo(that.right()) < 0 ||
                that.left().compareTo(this.right()) < 0;
    }

    /**
     * Tells if the given range is a wrap around.
     * @param range
     * @return
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
        if ( isWrapAround(left(), right()) )
            return -1;
        
        if ( isWrapAround(rhs.left(), rhs.right()) )
            return 1;
        
        return right_.compareTo(rhs.right_);
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
        if ( !(o instanceof Range) )
            return false;
        Range rhs = (Range)o;
        return left_.equals(rhs.left_) && right_.equals(rhs.right_);
    }
    
    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }
    
    public String toString()
    {
        return "(" + left_ + "," + right_ + "]";
    }
}

class RangeSerializer implements ICompactSerializer<Range>
{
    public void serialize(Range range, DataOutputStream dos) throws IOException
    {
        Token.serializer().serialize(range.left(), dos);
        Token.serializer().serialize(range.right(), dos);
    }

    public Range deserialize(DataInputStream dis) throws IOException
    {
        return new Range(Token.serializer().deserialize(dis), Token.serializer().deserialize(dis));
    }
}
