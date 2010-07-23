/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.Arrays;

public class EstimatedHistogram
{

    /**
     * The series of values to which the counts in `buckets` correspond:
     * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 18, 22, etc.
     * Thus, a `buckets` of [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of 4.
     *
     * The series starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1
     * to around 36M by default (creating 90+1 buckets), which will give us timing resolution from microseconds to
     * 36 seconds, with less precision as the numbers get larger.
     */
    private long[] bucketOffsets;
    private int numBuckets;

    final AtomicLongArray buckets;

    public EstimatedHistogram()
    {
        makeOffsets(90);
        buckets = new AtomicLongArray(numBuckets);
    }

    public EstimatedHistogram(int bucketCount)
    {
        makeOffsets(bucketCount);
        buckets = new AtomicLongArray(numBuckets);
    }

    public EstimatedHistogram(long[] bucketData)
    {
        makeOffsets(bucketData.length - 1);
        buckets = new AtomicLongArray(bucketData);
    }

    private void makeOffsets(int size)
    {
        bucketOffsets = new long[size];
        long last = 1;
        bucketOffsets[0] = last;
        for(int i = 1; i < size; i++)
        {
            long next = Math.round(last * 1.2);
            if (next == last)
                next++;
            bucketOffsets[i] = next;
            last = next;
        }
        numBuckets = bucketOffsets.length + 1;
    }

    public long[] getBucketOffsets()
    {
        return bucketOffsets;
    }
    
    public void add(long n)
    {
        int index = Arrays.binarySearch(bucketOffsets, n);
        if (index < 0)
        {
            //inexact match, find closest bucket
            index = -index - 1;
        }
        else
        {
            //exact match, so we want the next highest one
            index += 1;
        }
        buckets.incrementAndGet(index);
    }

    public long[] get(boolean reset)
    {
        long[] rv = new long[numBuckets];
        for (int i = 0; i < numBuckets; i++)
            rv[i] = buckets.get(i);

        if (reset)
            for (int i = 0; i < numBuckets; i++)
                buckets.set(i, 0L);

        return rv;
    }

    public long min()
    {
        for (int i = 0; i < numBuckets; i++)
        {
            if (buckets.get(i) > 0)
                return bucketOffsets[i == 0 ? 0 : i - 1];
        }
        return 0;
    }

    public long max()
    {
        for (int i = numBuckets - 1; i >= 0; i--)
        {
            if (buckets.get(i) > 0)
                return bucketOffsets[i == 0 ? 0 : i - 1];
        }
        return 0;
    }

    public long median()
    {
        long max = 0;
        long median = 0;
        for (int i = 0; i < numBuckets; i++)
        {
            if (max < 1 || buckets.get(i) > max)
            {
                max = buckets.get(i);
                if (max > 0)
                    median = bucketOffsets[i == 0 ? 0 : i - 1];
            }
        }
        return median;
    }
}
