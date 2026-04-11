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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.LogLinearDecayingHistograms.LogLinearDecayingHistogram;

import static org.junit.Assert.assertEquals;

public class LogLinearDecayingHistogramTest
{
    public static final Logger logger = LoggerFactory.getLogger(LogLinearDecayingHistogramTest.class);

    private static final double DOUBLE_ASSERT_DELTA = 0;
    private static final long ONE_SECOND = TimeUnit.NANOSECONDS.toSeconds(1);
    private static final long ONE_HOUR = TimeUnit.HOURS.toSeconds(1);
    private static final long[] TIMES = new long[] { 0, ONE_SECOND - 1, ONE_SECOND, ONE_HOUR - 1, ONE_HOUR };

    @Test
    public void testMinMax()
    {
        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            histogram.increment(16, time);
            Snapshot snapshot = histogram.copyToSnapshot(time + 1);
            assertEquals(16, snapshot.getMin());
            assertEquals(20, snapshot.getMax());
        }
    }

    @Test
    public void testMean()
    {
        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            for (int i = 0; i < 40; i++)
                histogram.increment(0, time);
            for (int i = 0; i < 20; i++)
                histogram.increment(1, time);
            for (int i = 0; i < 10; i++)
                histogram.increment(2, time);
            assertEquals(40/70d, histogram.copyToSnapshot(time + 1).getMean(), 0.1D);
        }
    }

    @Test
    public void testStdDev()
    {
        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            for (int i = 0; i < 20; i++)
                histogram.increment(10, time);
            for (int i = 0; i < 40; i++)
                histogram.increment(20, time);
            for (int i = 0; i < 20; i++)
                histogram.increment(30, time);

            Snapshot snapshot = histogram.copyToSnapshot(time + 1);
            assertEquals(20.0D, snapshot.getMean(), 2.0D);
            assertEquals(7.07D, snapshot.getStdDev(), 2.0D);
        }
    }

    @Test
    public void testPercentile()
    {
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            // percentile of empty histogram is 0
            assertEquals(0D, histogram.copyToSnapshot(0).getValue(0.99), DOUBLE_ASSERT_DELTA);
            assertEquals(0D, histogram.copyToSnapshot(ONE_SECOND).getValue(0.99), DOUBLE_ASSERT_DELTA);
            assertEquals(0D, histogram.copyToSnapshot(ONE_HOUR).getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            histogram.increment(1, time);
            // percentile of a histogram with one element should be that element
            assertEquals(1D, histogram.copyToSnapshot(time + 1).getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            histogram.increment(1, time);
            histogram.increment(10, time);
            assertEquals(10D, histogram.copyToSnapshot(time + 1).getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);

            histogram.increment(1, time);
            histogram.increment(2, time);
            histogram.increment(3, time);
            histogram.increment(4, time);
            histogram.increment(5, time);

            Snapshot snapshot = histogram.copyToSnapshot(time + 1);
            assertEquals(1, snapshot.getValue(0.00), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
            assertEquals(5, snapshot.getValue(1.00), DOUBLE_ASSERT_DELTA);
        }

        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);

            for (int i = 11; i <= 20; i++)
                histogram.increment(i, time);

            // Right now the histogram looks like:
            //     10   12   14   16   20
            //      1    2    2    4    1
            // %:  10   30   50   90  100
            Snapshot snapshot = histogram.copyToSnapshot(time + 1);
            assertEquals(10, snapshot.getValue(0.01), DOUBLE_ASSERT_DELTA);
            assertEquals(10, snapshot.getValue(0.10), DOUBLE_ASSERT_DELTA);

            assertEquals(11, snapshot.getValue(0.11), DOUBLE_ASSERT_DELTA);
            assertEquals(11, snapshot.getValue(0.20), DOUBLE_ASSERT_DELTA);
            assertEquals(12, snapshot.getValue(0.21), DOUBLE_ASSERT_DELTA);
            assertEquals(12, snapshot.getValue(0.30), DOUBLE_ASSERT_DELTA);

            assertEquals(13, snapshot.getValue(0.31), DOUBLE_ASSERT_DELTA);
            assertEquals(13, snapshot.getValue(0.40), DOUBLE_ASSERT_DELTA);
            assertEquals(14, snapshot.getValue(0.41), DOUBLE_ASSERT_DELTA);
            assertEquals(14, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
            assertEquals(15, snapshot.getValue(0.51), DOUBLE_ASSERT_DELTA);
            assertEquals(15, snapshot.getValue(0.70), DOUBLE_ASSERT_DELTA);
            assertEquals(16, snapshot.getValue(0.71), DOUBLE_ASSERT_DELTA);
            assertEquals(16, snapshot.getValue(0.90), DOUBLE_ASSERT_DELTA);
            assertEquals(20, snapshot.getValue(0.91), DOUBLE_ASSERT_DELTA);
        }

        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            histogram.increment(0, time);
            histogram.increment(0, time);
            histogram.increment(1, time);

            Snapshot snapshot = histogram.copyToSnapshot(time + 1);
            assertEquals(0, snapshot.getValue(0.5), DOUBLE_ASSERT_DELTA);
            assertEquals(1, snapshot.getValue(0.99), DOUBLE_ASSERT_DELTA);
        }
    }

    @Test
    public void testSize()
    {
        for (long time : TIMES)
        {
            LogLinearDecayingHistogram histogram = new LogLinearDecayingHistograms().allocate(1);
            histogram.increment(42, time);
            histogram.increment(42, time);
            assertEquals(2, histogram.copyToSnapshot(time + 1).size());
        }
    }
}
