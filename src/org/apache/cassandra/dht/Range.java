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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import org.apache.cassandra.net.EndPoint;
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
    
    public static boolean isKeyInRanges(List<Range> ranges, String key)
    {
        if(ranges == null ) 
            return false;
        
        for ( Range range : ranges)
        {
            if(range.contains(StorageService.hash(key)))
            {
                return true ;
            }
        }
        return false;
    }
        
    
    private final BigInteger left_;
    private final BigInteger right_;
    
    public Range(BigInteger left, BigInteger right)
    {
        left_ = left;
        right_ = right;
    }
    
    /**
     * Returns the left endpoint of a range.
     * @return left endpoint
     */
    public BigInteger left()
    {
        return left_;
    }
    
    /**
     * Returns the right endpoint of a range.
     * @return right endpoint
     */
    public BigInteger right()
    {
        return right_;
    }

    /**
     * Helps determine if a given point on the DHT ring is contained
     * in the range in question.
     * @param bi point in question
     * @return true if the point contains within the range else false.
     */
    public boolean contains(BigInteger bi)
    {
        if ( left_.subtract(right_).signum() > 0 )
        {
            /* 
             * left is greater than right we are wrapping around.
             * So if the interval is [a,b) where a > b then we have
             * 3 cases one of which holds for any given token k.
             * (1) k > a -- return true
             * (2) k < b -- return true
             * (3) b < k < a -- return false
            */
            if ( bi.subtract(left_).signum() >= 0 )
                return true;
            else return right_.subtract(bi).signum() > 0;
        }
        else if ( left_.subtract(right_).signum() < 0 )
        {
            /*
             * This is the range [a, b) where a < b. 
            */
            return ( bi.subtract(left_).signum() >= 0 && right_.subtract(bi).signum() >=0 );
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
    private boolean isWrapAround(Range range)
    {
        return range.left_.subtract(range.right_).signum() > 0;
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
        dos.writeUTF(range.left().toString());
        dos.writeUTF(range.right().toString());
    }

    public Range deserialize(DataInputStream dis) throws IOException
    {
        BigInteger left = new BigInteger(dis.readUTF());
        BigInteger right = new BigInteger(dis.readUTF());        
        return new Range(left, right);
    }
}
