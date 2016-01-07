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

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Allows our Histogram implementation to be used by the metrics library.
 *
 * Default buckets allows nanosecond timings.
 */
public class EstimatedHistogramReservoir implements Reservoir
{
    EstimatedHistogram histogram;

    // Default to >4 hours of in nanoseconds of buckets
    public EstimatedHistogramReservoir(boolean considerZeroes)
    {
        this(164, considerZeroes);
    }

    public EstimatedHistogramReservoir(int numBuckets, boolean considerZeroes)
    {
        histogram = new EstimatedHistogram(numBuckets, considerZeroes);
    }

    @Override
    public int size()
    {
        return histogram.getBucketOffsets().length + 1;
    }

    @Override
    public void update(long value)
    {
        histogram.add(value);
    }

    @Override
    public Snapshot getSnapshot()
    {
        return new HistogramSnapshot(histogram);
    }

    @VisibleForTesting
    public void clear()
    {
        histogram.getBuckets(true);
    }

    static class HistogramSnapshot extends UniformSnapshot
    {
        EstimatedHistogram histogram;

        public HistogramSnapshot(EstimatedHistogram histogram)
        {
            super(histogram.getBuckets(false));

            this.histogram = histogram;
        }

        @Override
        public double getValue(double quantile)
        {
            return histogram.percentile(quantile);
        }

        @Override
        public long getMax()
        {
            return histogram.max();
        }

        @Override
        public long getMin()
        {
            return histogram.min();
        }

        @Override
        public double getMean()
        {
            return histogram.rawMean();
        }

        @Override
        public long[] getValues() {
            return histogram.getBuckets(false);
        }
    }
}
