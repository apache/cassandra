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

import java.util.Arrays;

/**
 * Simple class for constructing an EsimtatedHistogram from a set of predetermined values
 */
public class HistogramBuilder
{

    public HistogramBuilder() {}
    public HistogramBuilder(long[] values)
    {
        for (long value : values)
        {
            add(value);
        }
    }

    private long[] values = new long[10];
    int count = 0;

    public void add(long value)
    {
        if (count == values.length)
            values = Arrays.copyOf(values, values.length << 1);
        values[count++] = value;
    }

    /**
     * See {@link #buildWithStdevRangesAroundMean(int)}
     * @return buildWithStdevRangesAroundMean(3)
     */
    public EstimatedHistogram buildWithStdevRangesAroundMean()
    {
        return buildWithStdevRangesAroundMean(3);
    }

    /**
     * Calculate the min, mean, max and standard deviation of the items in the builder, and
     * generate an EstimatedHistogram with upto <code>maxdev</code> stdev size ranges  either
     * side of the mean, until min/max are hit; if either min/max are not reached a further range is
     * inserted at the relevant ends. e.g., with a <code>maxdevs</code> of 3, there may be <i>up to</i> 8 ranges
     * (between 9 boundaries, the middle being the mean); the middle 6 will have the same size (stdev)
     * with the outermost two stretching out to min and max.
     *
     * @param maxdevs
     * @return
     */
    public EstimatedHistogram buildWithStdevRangesAroundMean(int maxdevs)
    {
        if (maxdevs < 0)
            throw new IllegalArgumentException("maxdevs must be greater than or equal to zero");

        final int count = this.count;
        final long[] values = this.values;

        if (count == 0)
            return new EstimatedHistogram(new long[] { }, new long[] { 0 });

        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        double sum = 0, sumsq = 0;
        for (int i = 0 ; i < count ; i++)
        {
            final long value = values[i];
            sum += value;
            sumsq += value * value;
            if (value < min)
                min = value;
            if (value > max)
                max = value;
        }

        final long mean = (long)Math.round(sum / count);
        final double stdev =
                Math.sqrt((sumsq / count) - (mean * (double) mean));

        // build the ranges either side of the mean
        final long[] lowhalf = buildRange(mean, min, true, stdev, maxdevs);
        final long[] highhalf = buildRange(mean, max, false, stdev, maxdevs);

        // combine the ranges
        final long[] ranges = new long[lowhalf.length + highhalf.length + 1];
        System.arraycopy(lowhalf, 0, ranges, 0, lowhalf.length);
        ranges[lowhalf.length] = mean;
        System.arraycopy(highhalf, 0, ranges, lowhalf.length + 1, highhalf.length);

        final EstimatedHistogram hist = new EstimatedHistogram(ranges, new long[ranges.length + 1]);
        for (int i = 0 ; i < count ; i++)
            hist.add(values[i]);
        return hist;
    }

    private static long[] buildRange(long mean, long minormax, boolean ismin, double stdev, int maxdevs)
    {
        if (minormax == mean)
            // minormax == mean we have no range to produce, but given the exclusive starts
            // that begin at zero by default (or -Inf) in EstimatedHistogram we have to generate a min range
            // to indicate where we start from
            return ismin ? new long[] { mean - 1 } : new long[0];

        if (stdev < 1)
        {
            // deal with stdevs too small to generate sensible ranges
            return ismin ? new long[] { minormax - 1, mean - 1 } :
                           new long[] { minormax };
        }

        long larger, smaller;
        if (ismin) { larger = mean;     smaller = minormax; }
        else       { larger = minormax; smaller = mean;     }

        double stdevsTo = (larger - smaller) / stdev;
        if (stdevsTo > 0 && stdevsTo < 1)
            // always round up if there's just one non-empty range
            stdevsTo = 1;
        else
            // otherwise round to the nearest half stdev, to avoid tiny ranges at the start/end
            stdevsTo = Math.round(stdevsTo);

        // limit to 4 stdev ranges - last range will contain everything to boundary
        final int len = Math.min(maxdevs + 1, (int) stdevsTo);
        final long[] range = new long[len];
        long next = ismin ? minormax - 1 : minormax;
        for (int i = 0 ; i < range.length ; i++)
        {
            long delta = (range.length - (i + 1)) * (long) stdev;
            if (ismin)
            {
                range[i] = next;
                next = mean - delta;
            }
            else
            {
                range[len - 1 - i] = next;
                next = mean + delta;
            }
        }
        return range;
    }
}
