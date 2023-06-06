/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.utils;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedInts;

/**
 * This class packages several UInt utility methods so we can easily change it's implementation when needed.
 */
public class CassandraUInt
{
    /**
     * Returns the long representation of the maximum value we can hold
     */
    public final static long MAX_VALUE_LONG = UnsignedInteger.MAX_VALUE.longValue();
    /**
     * Returns the UInt representation of the maximum value we can hold
     */
    public final static int MAX_VALUE_UINT = UnsignedInteger.MAX_VALUE.intValue();
    
    /**
     * Converts a long to it's unsigned integer representation
     * 
     * @param value A long between 0 and 2<sup>32</sup>-1 inclusive
     * @return an unsigned integer representation of the long
     * @throws IllegalArgumentException if value '< 0' or '>= 2<sup>32</sup>'
     */
    public static int fromLong(long value)
    {
        return UnsignedInts.checkedCast(value);
    }

    /**
     * The same but works on an array on longs
     * 
     */
    public static int[] fromLong(long[] values)
    {
        int[] delTimesUints = new int[values.length];
        for (int i=0; i < values.length; i++)
            delTimesUints[i] = CassandraUInt.fromLong(values[i]);
        return delTimesUints;
    }

    /**
     * Returns the long resulting from parsing the int as an unsigned integer
     * 
     * @param value Unsigned integer representation of the long
     * @return The long resulting from parsing the int as an unsigned integer
     */
    public static long toLong(int value)
    {
        return UnsignedInts.toLong(value);
    }

    /**
     * Compare 2 Uints
     *
     * @return < 0 if x is less than y. > 0 if x is greater than y. Zero if they are equal
     */
    public static int compare(int x, int y)
    {
        return UnsignedInts.compare(x, y);
    }
}
