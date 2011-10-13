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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.cassandra.io.ISerializer;

public class EstimatedHistogram
{
    public static EstimatedHistogramSerializer serializer = new EstimatedHistogramSerializer();

    /**
     * The series of values to which the counts in `buckets` correspond:
     * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, etc.
     * Thus, a `buckets` of [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of 4.
     *
     * The series starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1
     * to around 36M by default (creating 90+1 buckets), which will give us timing resolution from microseconds to
     * 36 seconds, with less precision as the numbers get larger.
     *
     * Each bucket represents values from (previous bucket offset, current offset].
     */
    private long[] bucketOffsets;

    // buckets is one element longer than bucketOffsets -- the last element is values greater than the last offset
    final AtomicLongArray buckets;

    public EstimatedHistogram()
    {
        this(90);
    }

    public EstimatedHistogram(int bucketCount)
    {
        makeOffsets(bucketCount);
        buckets = new AtomicLongArray(bucketOffsets.length + 1);
    }

    public EstimatedHistogram(long[] offsets, long[] bucketData)
    {
        assert bucketData.length == offsets.length +1;
        bucketOffsets = offsets;
        buckets = new AtomicLongArray(bucketData);
    }

    private void makeOffsets(int size)
    {
        bucketOffsets = new long[size];
        long last = 1;
        bucketOffsets[0] = last;
        for (int i = 1; i < size; i++)
        {
            long next = Math.round(last * 1.2);
            if (next == last)
                next++;
            bucketOffsets[i] = next;
            last = next;
        }
    }

    /**
     * @return the histogram values corresponding to each bucket index
     */
    public long[] getBucketOffsets()
    {
        return bucketOffsets;
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     * @param n
     */
    public void add(long n)
    {
        int index = Arrays.binarySearch(bucketOffsets, n);
        if (index < 0)
        {
            // inexact match, take the first bucket higher than n
            index = -index - 1;
        }
        // else exact match; we're good
        buckets.incrementAndGet(index);
    }

    /**
     * @return the count in the given bucket
     */
    long get(int bucket)
    {
        return buckets.get(bucket);
    }

    /**
     * @param reset zero out buckets afterwards if true
     * @return a long[] containing the current histogram buckets
     */
    public long[] getBuckets(boolean reset)
    {
        long[] rv = new long[buckets.length()];
        for (int i = 0; i < buckets.length(); i++)
            rv[i] = buckets.get(i);

        if (reset)
            for (int i = 0; i < buckets.length(); i++)
                buckets.set(i, 0L);

        return rv;
    }

    /**
     * @return the smallest value that could have been added to this histogram
     */
    public long min()
    {
        for (int i = 0; i < buckets.length(); i++)
        {
            if (buckets.get(i) > 0)
                return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
        }
        return 0;
    }

    /**
     * @return the largest value that could have been added to this histogram.  If the histogram
     * overflowed, returns Long.MAX_VALUE.
     */
    public long max()
    {
        int lastBucket = buckets.length() - 1;
        if (buckets.get(lastBucket) > 0)
            return Long.MAX_VALUE;

        for (int i = lastBucket - 1; i >= 0; i--)
        {
            if (buckets.get(i) > 0)
                return bucketOffsets[i];
        }
        return 0;
    }

    /**
     * @return the mean histogram value (average of bucket offsets, weighted by count)
     * @throws IllegalStateException if any values were greater than the largest bucket threshold
     */
    public long mean()
    {
        int lastBucket = buckets.length() - 1;
        if (buckets.get(lastBucket) > 0)
            throw new IllegalStateException("Unable to compute ceiling for max when histogram overflowed");

        long elements = 0;
        long sum = 0;
        for (int i = 0; i < lastBucket; i++)
        {
            elements += buckets.get(i);
            sum += buckets.get(i) * bucketOffsets[i];
        }

        return (long) Math.ceil((double) sum / elements);
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    public boolean isOverflowed()
    {
        return buckets.get(buckets.length() - 1) > 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        
        if (!(o instanceof EstimatedHistogram))
            return false;
        
        EstimatedHistogram that = (EstimatedHistogram) o;
        return Arrays.equals(getBucketOffsets(), that.getBucketOffsets()) &&
               Arrays.equals(getBuckets(false), that.getBuckets(false));
    }

    public static class EstimatedHistogramSerializer implements ISerializer<EstimatedHistogram>
    {
        public void serialize(EstimatedHistogram eh, DataOutput dos) throws IOException
        {
            long[] offsets = eh.getBucketOffsets();
            long[] buckets = eh.getBuckets(false);
            dos.writeInt(buckets.length);
            for (int i = 0; i < buckets.length; i++)
            {
                dos.writeLong(offsets[i == 0 ? 0 : i - 1]);
                dos.writeLong(buckets[i]);
            }
        }

        public EstimatedHistogram deserialize(DataInput dis) throws IOException
        {
            int size = dis.readInt();
            long[] offsets = new long[size - 1];
            long[] buckets = new long[size];

            for (int i = 0; i < size; i++) {
                offsets[i == 0 ? 0 : i - 1] = dis.readLong();
                buckets[i] = dis.readLong();
            }
            return new EstimatedHistogram(offsets, buckets);
        }

        public long serializedSize(EstimatedHistogram object)
        {
            throw new UnsupportedOperationException();
        }
    }
}
