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

package org.apache.cassandra.metrics;

import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

import accord.utils.Invariants;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.utils.EstimatedHistogram;

public interface CassandraReservoir extends Reservoir
{
    enum BucketStrategy
    {
        none(false, length -> { throw new UnsupportedOperationException(); }, scale -> { throw new UnsupportedOperationException(); }),
        exp_12(true, length -> EstimatedHistogram.newOffsets(length, true), scale -> EstimatedHistogram.newOffsetsWithScale(scale, true)),
        exp_12_nozero(true, length -> EstimatedHistogram.newOffsets(length, false), scale -> EstimatedHistogram.newOffsetsWithScale(scale, false)),
        log_linear(false, LogLinearHistogram::bucketsWithLength, LogLinearHistogram::bucketsWithScale);

        final boolean overflows;
        final IntFunction<long[]> bucketsWithLength;
        final LongFunction<long[]> bucketsWithScale;
        volatile long[] cachedBuckets;

        BucketStrategy(boolean overflows, IntFunction<long[]> bucketsWithLength, LongFunction<long[]> bucketsWithScale)
        {
            this.overflows = overflows;
            this.bucketsWithLength = bucketsWithLength;
            this.bucketsWithScale = bucketsWithScale;
        }

        private long[] sharedBucketsWithLength(int length)
        {
            long[] buckets = cachedBuckets;
            if (buckets == null || buckets.length < length)
                cachedBuckets = buckets = this.bucketsWithLength.apply(length);
            return buckets;
        }

        private long[] sharedBucketsWithScale(long scale)
        {
            long[] buckets = cachedBuckets;
            if (buckets == null || buckets.length == 0 || buckets[buckets.length - 1] < scale)
                cachedBuckets = buckets = this.bucketsWithScale.apply(scale);
            return buckets;
        }

        public long[] translateTo(BucketStrategy toStrategy, long[] srcValues)
        {
            if (srcValues.length == 0)
                return srcValues;

            long[] srcBuckets = sharedBucketsWithLength(srcValues.length + 1);
            long[] trgBuckets = toStrategy.sharedBucketsWithScale(srcBuckets[srcValues.length]);
            return translate(srcBuckets, srcValues, overflows ? srcValues.length - 1 : srcValues.length, trgBuckets);
        }

        public static long[] translate(long[] srcBuckets, long[] srcValues, int srcLength, long[] trgBuckets)
        {
            Invariants.requireArgument(srcLength < srcBuckets.length);
            int trgLength = Arrays.binarySearch(trgBuckets, srcBuckets[srcLength]);
            if (trgLength < 0) trgLength = -1 - trgLength;
            else trgLength++;
            long[] trgValues = new long[trgLength];
            long srcStart = srcBuckets[0], srcRemainder = srcValues[0], srcEnd = srcBuckets[1];
            int si = 0, ti = 0;
            while (true)
            {
                long trgEnd = trgBuckets[ti + 1];
                if (trgEnd <= srcStart)
                {
                    if (++ti == trgValues.length) break;
                    continue;
                }

                long end = Math.min(trgEnd, srcEnd);
                if (end == srcEnd)
                {
                    trgValues[ti] += srcRemainder;
                    if (++si == srcLength) break;
                    srcStart = srcEnd;
                    srcEnd = srcBuckets[si + 1];
                    srcRemainder = srcValues[si];
                }
                else
                {
                    double ratio = (end - srcStart) / (double)(srcEnd - srcStart);
                    long incr = Math.round(ratio * srcRemainder);
                    trgValues[ti] += incr;
                    srcRemainder -= incr;
                    srcStart = end;
                }
            }
            return trgValues;
        }
    }

    Snapshot getPercentileSnapshot();
    long[] buckets(int length);
    BucketStrategy bucketStrategy();

}
