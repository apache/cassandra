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
package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicLongArray;

import org.junit.Test;

import static org.junit.Assert.*;

public class HistogramBuilderTest
{

    @Test
    public void testStdevEmpty()
    {
        EstimatedHistogram hist = new HistogramBuilder().buildWithStdevRangesAroundMean();
        assertArrayEquals(new long[] { }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0 }, toArray(hist.buckets));
    }

    @Test
    public void testStdevSingletonRanges()
    {
        EstimatedHistogram hist;
        hist = new HistogramBuilder(new long[] { 5, 5, 5, 5, 5 }).buildWithStdevRangesAroundMean();
        assertArrayEquals(new long[] { 4, 5 }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0, 5, 0 }, toArray(hist.buckets));
        // should behave exactly the same for negative numbers
        hist = new HistogramBuilder(new long[] { -1 }).buildWithStdevRangesAroundMean();
        assertArrayEquals(new long[] { -2, -1 }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0, 1, 0 }, toArray(hist.buckets));
    }

    @Test
    public void testStdevNearZeroStdev()
    {
        EstimatedHistogram hist;
        long[] vals = new long[100000];
        vals[0] = 99;
        vals[1] = 101;
        for (int i = 2 ; i < vals.length ; i++)
            vals[i] = 100;
        hist = new HistogramBuilder(vals).buildWithStdevRangesAroundMean();
        assertArrayEquals(new long[] { 98, 99, 100, 101 }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0, 1, vals.length - 2, 1, 0 }, toArray(hist.buckets));
    }

    @Test
    public void testStdev()
    {
        long[] vals;
        EstimatedHistogram hist;
        vals = new long[] { -10, -3, -2, -2, -1, -1, -1, -1, -1, -1, -1, 0, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 10 };
        hist = new HistogramBuilder(vals).buildWithStdevRangesAroundMean(2);
        assertArrayEquals(new long[] { -11, -6, -3, 0, 3, 6, 10 }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0, 1, 1, 10, 10, 0, 1, 0 }, toArray(hist.buckets));
    }

    private static long[] toArray(AtomicLongArray a)
    {
        final long[] r = new long[a.length()];
        for (int i = 0 ; i < r.length ; i++)
            r[i] = a.get(i);
        return r;
    }

    @Test
    public void testStdevLargeNumbers()
    {
        long[] vals;
        EstimatedHistogram hist;
        vals = new long[100000];
        for (int i = 0 ; i < vals.length ; i++)
        {
            if (i < vals.length * 0.6f)
                vals[i] = 60;
            else if (i < vals.length * 0.8f)
                vals[i] = 120;
            else if (i < vals.length * 0.9f)
                vals[i] = 180;
            else if (i < vals.length * 0.95f)
                vals[i] = 240;
            else if (i < vals.length * 0.98f)
                vals[i] = 320;
            else
                vals[i] = 1000;
        }
        hist = new HistogramBuilder(vals).buildWithStdevRangesAroundMean(2);
        assertArrayEquals(new long[] { 59, 120, 260, 400, 1000 }, hist.getBucketOffsets());
        assertArrayEquals(new long[] { 0, 80000, 15000, 3000, 2000, 0 }, toArray(hist.buckets));
    }

}
