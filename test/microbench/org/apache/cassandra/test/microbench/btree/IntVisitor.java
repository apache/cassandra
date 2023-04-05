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

package org.apache.cassandra.test.microbench.btree;

import java.util.Random;

/**
 * A utility class to visit 2^n integers, around a median value, in an order that rapidly covers a wide range of values
 */
public class IntVisitor
{
    int invocations;
    int shift;
    int mask;
    int base;

    public IntVisitor()
    {
    }

    public IntVisitor(int median)
    {
        setup(median);
    }

    public void setup(int median)
    {
        int variance = Integer.highestOneBit(median);
        mask = variance - 1;
        base = median - variance/2;
        shift = 32 - Integer.bitCount(mask);
    }

    public int i()
    {
        return invocations;
    }

    public int cur()
    {
        return size(invocations);
    }

    public int next()
    {
        return size(invocations = invocations == Integer.MAX_VALUE ? 0 : invocations + 1);
    }

    public int min()
    {
        return base;
    };

    public int size()
    {
        return mask + 1;
    };

    public int size(int invocations)
    {
        // we reverse the integer to more evenly distribute the visitation of sizes
        return base + (Integer.reverse(invocations & mask) >>> shift);
    }

    public void randomise(Random random)
    {
        invocations = random.nextInt(Integer.MAX_VALUE);
    }
}