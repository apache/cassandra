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
     * This series starts at 1 and grows by 1.2 each time (rounding down and removing duplicates). It goes from 1
     * to around 30M, which will give us timing resolution from microseconds to 30 seconds, with less precision
     * as the numbers get larger.
     */
    private static final long[] bucketOffsets = {
            1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 18, 22, 26, 31, 38, 46, 55, 66, 79, 95, 114, 137, 164, 197, 237, 284, 341, 410, 492, 590,
            708, 850, 1020, 1224, 1469, 1763, 2116, 2539, 3047, 3657, 4388, 5266, 6319, 7583, 9100, 10920, 13104, 15725, 18870, 22644,
            27173, 32608, 39130, 46956, 56347, 67617, 81140, 97368, 116842, 140210, 168252, 201903, 242283, 290740, 348888, 418666,
            502400, 602880, 723456, 868147, 1041776, 1250132, 1500158, 1800190, 2160228, 2592274, 3110728, 3732874, 4479449, 5375339,
            6450407, 7740489, 9288586, 11146304, 13375565, 16050678, 19260813, 23112976, 27735572, 33282686
    };

    private static final int numBuckets = bucketOffsets.length + 1;

    final AtomicLongArray buckets;

    public EstimatedHistogram()
    {
        buckets = new AtomicLongArray(numBuckets);
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

    public long[] get(Boolean reset)
    {
        long[] rv = new long[numBuckets];
        for (int i = 0; i < numBuckets; i++)
            rv[i] = buckets.get(i);

        if (reset)
            for (int i = 0; i < numBuckets; i++)
                buckets.set(i, 0L);

        return rv;
    }
}
