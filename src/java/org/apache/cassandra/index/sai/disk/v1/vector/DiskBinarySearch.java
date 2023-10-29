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

package org.apache.cassandra.index.sai.disk.v1.vector;


import java.util.function.Function;

public class DiskBinarySearch
{
    /**
     * Search for the target int between positions low and high, using the provided function
     * to retrieve the int value at the given ordinal.
     *
     * Returns the position at which target is found.  Raises an exception if it is not found.
     *
     * This will not call f() after the target is found, so if f is performing disk seeks,
     * it will leave the underlying reader at the position right after reading the target.
     *
     * @return index if target is found; otherwise return -1 if targer is not found
     */
    public static long searchInt(long low, long high, int target, Function<Long, Integer> f)
    {
        assert high < Long.MAX_VALUE >> 2 : "high is too large to avoid potential overflow: " + high;
        assert low < high : "low must be less than high: " + low + " >= " + high;

        while (low < high)
        {
            long i = low + (high - low) / 2;
            int value = f.apply(i);
            if (target == value)
                return i;
            else if (target > value)
                low = i + 1;
            else
                high = i;
        }
        return -1;
    }
}
