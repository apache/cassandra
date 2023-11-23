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

package org.apache.cassandra.distributed.test.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: this is borrowed from Harry, temporarily
public class RngUtils
{
    private static final Logger logger = LoggerFactory.getLogger(RngUtils.class);

    private static final long CONSTANT = 0x2545F4914F6CDD1DL;
    public static long next(long input)
    {
        if (input == 0)
            return next(CONSTANT);

        return xorshift64star(input);
    }

    public static long xorshift64star(long input)
    {
        input ^= input >> 12;
        input ^= input << 25; // b
        input ^= input >> 27; // c
        return input * CONSTANT;
    }

    public static long[] next(long current, int n)
    {
        long[] next = new long[n];
        for (int i = 0; i < n; i++)
        {
            current = next(current);
            next[i] = current;
        }
        return next;
    }

    public static byte[] asBytes(long current)
    {
        byte[] bytes = new byte[Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++)
        {
            bytes[i] = (byte) (current & 0xFF);
            current >>= current;
        }
        return bytes;
    }

    public static byte asByte(long current)
    {
        return (byte) current;
    }

    public static int asInt(long current)
    {
        return (int) current;
    }

    // TODO: this needs some improvement
    public static int asInt(long current, int max)
    {
        return Math.abs((int) current % max);
    }

    // Generate a value in [min, max] range: from min _inclusive_ to max _inclusive_.
    public static int asInt(long current, int min, int max)
    {
        if (min == max)
            return min;
        return min + asInt(current, max - min);
    }

    public static boolean asBoolean(long current)
    {
        return (current & 1) == 1;
    }

    public static float asFloat(long current)
    {
        return Float.intBitsToFloat((int) current);
    }

    public static double asDouble(long current)
    {
        return Double.longBitsToDouble(current);
    }
}
