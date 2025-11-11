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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.RandomTestRunner;
import com.codahale.metrics.Snapshot;
import static org.junit.Assert.assertEquals;

public class LogLinearHistogramTest
{
    public static final Logger logger = LoggerFactory.getLogger(LogLinearHistogramTest.class);

    private static final double DOUBLE_ASSERT_DELTA = 0;

    @Test
    public void testMinMax()
    {
        LogLinearHistogram histogram = new LogLinearHistogram(1);
        histogram.increment(16);
        Snapshot snapshot = histogram.copyToSnapshot();
        assertEquals(16, snapshot.getMin());
        assertEquals(20, snapshot.getMax());
    }

    @Test
    public void testMean()
    {
        LogLinearHistogram histogram = new LogLinearHistogram(1);
        for (int i = 0; i < 40; i++)
            histogram.increment(0);
        for (int i = 0; i < 20; i++)
            histogram.increment(1);
        for (int i = 0; i < 10; i++)
            histogram.increment(2);
        assertEquals(40/70d, histogram.copyToSnapshot().getMean(), 0.1D);
    }

    @Test
    public void testStdDev()
    {
        LogLinearHistogram histogram = new LogLinearHistogram(1);
        for (int i = 0; i < 20; i++)
            histogram.increment(10);
        for (int i = 0; i < 40; i++)
            histogram.increment(20);
        for (int i = 0; i < 20; i++)
            histogram.increment(30);

        Snapshot snapshot = histogram.copyToSnapshot();
        assertEquals(20.0D, snapshot.getMean(), 2.0D);
        assertEquals(7.07D, snapshot.getStdDev(), 2.0D);
    }

    @Test
    public void testPercentile()
    {
        {
            LogLinearHistogram histogram = new LogLinearHistogram(1);
            // percentile of empty histogram is 0
            assertEquals(0D, histogram.copyToSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);

            histogram.increment(1);
            // percentile of a histogram with one element should be that element
            assertEquals(1D, histogram.copyToSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);

            histogram.increment(10);
            assertEquals(10D, histogram.copyToSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        {
            LogLinearHistogram histogram = new LogLinearHistogram(1);

            histogram.increment(1);
            histogram.increment(2);
            histogram.increment(3);
            histogram.increment(4);
            histogram.increment(5);

            Snapshot snapshot = histogram.copyToSnapshot();
            assertEquals(1, snapshot.getValue(0.00), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
            assertEquals(5, snapshot.getValue(1.00), DOUBLE_ASSERT_DELTA);
        }

        {
            LogLinearHistogram histogram = new LogLinearHistogram(1);

            for (int i = 11; i <= 20; i++)
                histogram.increment(i);

            // Right now the histogram looks like:
            //     10   12   14   16   20
            //      1    2    2    4    1
            // %:  10   30   50   90  100
            Snapshot snapshot = histogram.copyToSnapshot();
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
        {
            LogLinearHistogram histogram = new LogLinearHistogram(1);
            histogram.increment(0);
            histogram.increment(0);
            histogram.increment(1);

            Snapshot snapshot = histogram.copyToSnapshot();
            assertEquals(0, snapshot.getValue(0.5), DOUBLE_ASSERT_DELTA);
            assertEquals(1, snapshot.getValue(0.99), DOUBLE_ASSERT_DELTA);
        }
    }

    @Test
    public void testSize()
    {
        LogLinearHistogram histogram = new LogLinearHistogram(1);
        histogram.increment(42);
        histogram.increment(42);
        assertEquals(2, histogram.copyToSnapshot().size());
    }

    @Test
    public void testRandomizedMinMaxProperties()
    {
        RandomTestRunner.test().check(random -> {
            for (int trial = 0; trial < 100; trial++)
            {
                int maxValue = 500;
                LogLinearHistogram histogram = new LogLinearHistogram(maxValue);
                List<Long> values = new ArrayList<>();

                int numValues = 10 + random.nextInt(100);
                for (int i = 0; i < numValues; i++)
                {
                    long value = random.nextInt(maxValue);
                    histogram.increment(value);
                    values.add(value);
                }

                Snapshot snapshot = histogram.copyToSnapshot();
                long expectedMin = Collections.min(values);
                long expectedMax = Collections.max(values);

                assertEquals("" + trial, LogLinearHistogram.invertIndex(LogLinearHistogram.index(expectedMin)),
                             snapshot.getMin());
                assertEquals(LogLinearHistogram.invertIndex(1 + LogLinearHistogram.index(expectedMax)),
                             snapshot.getMax());
            }
        });
    }

    @Test
    public void testRandomizedIncrementDecrementConsistency()
    {
        RandomTestRunner.test().check(random -> {
            for (int trial = 0; trial < 50; trial++)
            {
                int maxValue = 500;
                LogLinearHistogram histogram = new LogLinearHistogram(maxValue);
                List<Long> values = new ArrayList<>();

                // Add random values
                int numValues = 20 + random.nextInt(80);
                for (int i = 0; i < numValues; i++)
                {
                    long value = random.nextInt(5000);
                    histogram.increment(value);
                    values.add(value);
                }

                Snapshot beforeSnapshot = histogram.copyToSnapshot();

                // Remove and re-add some values
                int numToModify = 1 + random.nextInt(Math.min(10, values.size()));
                for (int i = 0; i < numToModify; i++)
                {
                    int idx = random.nextInt(values.size());
                    long oldValue = values.get(idx);
                    long newValue = random.nextInt(5000);

                    histogram.decrement(oldValue);
                    histogram.increment(newValue);
                    values.set(idx, newValue);
                }

                Snapshot afterSnapshot = histogram.copyToSnapshot();

                assertEquals(beforeSnapshot.size(), afterSnapshot.size());

                long expectedMin = Collections.min(values);
                long expectedMax = Collections.max(values);
                assertEquals(LogLinearHistogram.invertIndex(LogLinearHistogram.index(expectedMin)),
                             afterSnapshot.getMin());
                assertEquals(LogLinearHistogram.invertIndex(1 + LogLinearHistogram.index(expectedMax)),
                             afterSnapshot.getMax());
            }
        });
    }

    @Test
    public void testRandomizedReplaceConsistency()
    {
        RandomTestRunner.test().check(random -> {
            for (int trial = 0; trial < 100; trial++)
            {
                int maxValue = 500;
                LogLinearHistogram histogram = new LogLinearHistogram(maxValue);
                List<Long> values = new ArrayList<>();

                int numValues = 20 + random.nextInt(80);
                for (int i = 0; i < numValues; i++)
                {
                    long value = random.nextInt(maxValue);
                    histogram.increment(value);
                    values.add(value);
                }

                Snapshot beforeSnapshot = histogram.copyToSnapshot();
                int numToReplace = 1 + random.nextInt(Math.min(10, values.size()));
                for (int i = 0; i < numToReplace; i++)
                {
                    int idx = random.nextInt(values.size());
                    long oldValue = values.get(idx);
                    long newValue = random.nextInt(maxValue);

                    histogram.replace(oldValue, newValue);
                    values.set(idx, newValue);
                }

                Snapshot afterSnapshot = histogram.copyToSnapshot();
                assertEquals(beforeSnapshot.size(), afterSnapshot.size());

                long expectedMin = Collections.min(values);
                long expectedMax = Collections.max(values);
                assertEquals(LogLinearHistogram.invertIndex(LogLinearHistogram.index(expectedMin)),
                             afterSnapshot.getMin());
                assertEquals(LogLinearHistogram.invertIndex(1 + LogLinearHistogram.index(expectedMax)),
                             afterSnapshot.getMax());
            }
        });
    }

    @Test
    public void testRandomizedAutoGrowth()
    {
        RandomTestRunner.test().check(random -> {
            for (int trial = 0; trial < 20; trial++)
            {
                int maxValue = 500;
                LogLinearHistogram histogram = new LogLinearHistogram(maxValue);
                List<Long> values = new ArrayList<>();

                int numValues = 50;
                for (int i = 0; i < numValues; i++)
                {
                    long maxRange = (long) Math.pow(2, i / 5.0);
                    long value = random.nextInt((int) Math.min(maxRange, Integer.MAX_VALUE));
                    histogram.increment(value);
                    values.add(value);
                }

                Snapshot snapshot = histogram.copyToSnapshot();

                assertEquals(numValues, snapshot.size());

                long expectedMin = Collections.min(values);
                long expectedMax = Collections.max(values);
                assertEquals(LogLinearHistogram.invertIndex(LogLinearHistogram.index(expectedMin)),
                             snapshot.getMin());
                assertEquals(LogLinearHistogram.invertIndex(1 + LogLinearHistogram.index(expectedMax)),
                             snapshot.getMax());
            }
        });
    }
}
