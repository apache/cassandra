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
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;


/**
 * A representation of the range that a node is responsible for on the DHT ring.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Range implements Comparable<Range>
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

    private Token left_;
    private Token right_;
    
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

    /**
     * Helps determine if a given point on the DHT ring is contained
     * in the range in question.
     * @param bi point in question
     * @return true if the point contains within the range else false.
     */
    public boolean contains(Token bi)
    {
        if ( left_.compareTo(right_) > 0 )
        {
            /* 
             * left is greater than right we are wrapping around.
             * So if the interval is [a,b) where a > b then we have
             * 3 cases one of which holds for any given token k.
             * (1) k > a -- return true
             * (2) k < b -- return true
             * (3) b < k < a -- return false
            */
            if ( bi.compareTo(left_) >= 0 )
                return true;
            else return right_.compareTo(bi) > 0;
        }
        else if ( left_.compareTo(right_) < 0 )
        {
            /*
             * This is the range [a, b) where a < b. 
            */
            return ( bi.compareTo(left_) >= 0 && right_.compareTo(bi) >=0 );
        }        
        else
    	{
    		return true;
    	}    	
    }

    /**
     * Tells if the given range is a wrap around.
     * @param range
     * @return
     */
    private static boolean isWrapAround(Range range)
    {
        return range.left_.compareTo(range.right_) > 0;
    }
    
    public int compareTo(Range rhs)
    {
        /* 
         * If the range represented by the "this" pointer
         * is a wrap around then it is the smaller one.
        */
        if ( isWrapAround(this) )
            return -1;
        
        if ( isWrapAround(rhs) )
            return 1;
        
        return right_.compareTo(rhs.right_);
    }
    

    public static boolean isKeyInRanges(String key, List<Range> ranges)
    {
        assert ranges != null;

        Token token = StorageService.token(key);
        for (Range range : ranges)
        {
            if(range.contains(token))
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
