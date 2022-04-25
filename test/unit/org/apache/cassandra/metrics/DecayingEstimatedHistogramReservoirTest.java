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
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

@RunWith(Enclosed.class)
public class DecayingEstimatedHistogramReservoirTest
{
    public static class NonParameterizedTests
    {
        public static final int numExamples = 1000000;

        public static final Gen<long[]> offsets = integers().from(DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT)
                                                            .upToAndIncluding(DecayingEstimatedHistogramReservoir.MAX_BUCKET_COUNT - 10)
                                                            .zip(booleans().all(), EstimatedHistogram::newOffsets);
        
        @Test
        public void testFindIndex()
        {
            qt().withExamples(numExamples)
                .forAll(booleans().all()
                                  .flatMap(b -> offsets.flatMap(offs -> this.offsetsAndValue(offs, b, 0))))
                .check(this::checkFindIndex);
        }

        @Test
        public void showEstimationWorks()
        {
            qt().withExamples(numExamples)
                .forAll(offsets.flatMap(offs -> this.offsetsAndValue(offs, false, 9)))
                .check(this::checkEstimation);
        }

        //shows that the max before overflow is 238 buckets regardless of consider zeros
        @Test
        @Ignore
        public void showHistogramOffsetOverflow()
        {
            qt().forAll(integers().from(DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT).upToAndIncluding(1000))
                .check(count -> {
                    long[] offsets = EstimatedHistogram.newOffsets(count, false);
                    for (long offset : offsets)
                        if (offset < 0)
                            return false;

                    return true;
                });
        }

        private boolean checkFindIndex(Pair<long[], Long> offsetsAndValue)
        {
            long[] offsets = offsetsAndValue.left;
            long value = offsetsAndValue.right;

            int model = findIndexModel(offsets, value);
            int actual = DecayingEstimatedHistogramReservoir.findIndex(offsets, value);

            return model == actual;
        }

        private int findIndexModel(long[] offsets, long value)
        {
            int modelIndex = Arrays.binarySearch(offsets, value);
            if (modelIndex < 0)
                modelIndex = -modelIndex - 1;

            return modelIndex;
        }

        private Gen<Pair<long[], Long>> offsetsAndValue(long[] offsets, boolean useMaxLong, long minValue)
        {
            return longs().between(minValue, useMaxLong ? Long.MAX_VALUE : offsets[offsets.length - 1] + 100)
                          .mix(longs().between(minValue, minValue + 10),50)
                          .map(value -> Pair.create(offsets, value));
        }

        public boolean checkEstimation(Pair<long[], Long> offsetsAndValue)
        {
            long[] offsets = offsetsAndValue.left;
            long value = offsetsAndValue.right;
            boolean considerZeros = offsets[0] == 0;

            int modelIndex = Arrays.binarySearch(offsets, value);
            if (modelIndex < 0)
                modelIndex = -modelIndex - 1;

            int estimate = (int) DecayingEstimatedHistogramReservoir.fastLog12(value);

            if (considerZeros)
                return estimate - 3 == modelIndex || estimate - 2 == modelIndex;
            else
                return estimate - 4 == modelIndex || estimate - 3 == modelIndex;
        }
    }

    @RunWith(Parameterized.class)
    public static class ParameterizedTests
    {
        private static final double DOUBLE_ASSERT_DELTA = 0;

        @Parameterized.Parameter
        public String description;
        
        @Parameterized.Parameter(1)
        public Function<DecayingEstimatedHistogramReservoir, Snapshot> toSnapshot;

        @Parameterized.Parameters(name="{0}")
        public static Collection<Object[]> suppliers()
        {
            Function<DecayingEstimatedHistogramReservoir, Snapshot> snapshot = DecayingEstimatedHistogramReservoir::getSnapshot;
            Function<DecayingEstimatedHistogramReservoir, Snapshot> decayingOnly = DecayingEstimatedHistogramReservoir::getPercentileSnapshot;
            return ImmutableList.of(new Object[] { "normal", snapshot }, new Object[] { "decaying buckets", decayingOnly });
        }

        @Test
        public void testStriping() throws InterruptedException
        {
            TestClock clock = new TestClock();
            int nStripes = 4;
            DecayingEstimatedHistogramReservoir model = new DecayingEstimatedHistogramReservoir(clock);
            DecayingEstimatedHistogramReservoir test = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION,
                                                                                               DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT,
                                                                                               nStripes,
                                                                                               clock);

            long seed = nanoTime();
            System.out.println("DecayingEstimatedHistogramReservoirTest#testStriping.seed = " + seed);
            Random valGen = new Random(seed);
            ExecutorService executors = Executors.newFixedThreadPool(nStripes * 2);
            for (int i = 0; i < 1_000_000; i++)
            {
                long value = Math.abs(valGen.nextInt());
                executors.submit(() -> {
                    model.update(value);
                    LockSupport.parkNanos(2);
                    test.update(value);
                });
            }

            executors.shutdown();
            Assert.assertTrue(executors.awaitTermination(1, TimeUnit.MINUTES));

            Snapshot modelSnapshot = toSnapshot.apply(model);
            Snapshot testSnapshot = toSnapshot.apply(test);

            assertEquals(modelSnapshot.getMean(), testSnapshot.getMean(), DOUBLE_ASSERT_DELTA);
            assertEquals(modelSnapshot.getMin(), testSnapshot.getMin(), DOUBLE_ASSERT_DELTA);
            assertEquals(modelSnapshot.getMax(), testSnapshot.getMax(), DOUBLE_ASSERT_DELTA);
            assertEquals(modelSnapshot.getMedian(), testSnapshot.getMedian(), DOUBLE_ASSERT_DELTA);
            for (double i = 0.0; i < 1.0; i += 0.1)
                assertEquals(modelSnapshot.getValue(i), testSnapshot.getValue(i), DOUBLE_ASSERT_DELTA);


            int stripedValues = 0;
            for (int i = model.size(); i < model.size() * model.stripeCount(); i++)
            {
                stripedValues += model.stripedBucketValue(i, true);
            }
            assertTrue("no striping found", stripedValues > 0);
        }

        @Test
        public void testSimple()
        {
            {
                // 0 and 1 map to the same, first bucket
                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir();
                histogram.update(0);
                assertEquals(1, histogram.getSnapshot().getValues()[0]);
                histogram.update(1);
                assertEquals(2, histogram.getSnapshot().getValues()[0]);
            }
            {
                // 0 and 1 map to different buckets
                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(true);
                histogram.update(0);
                assertEquals(1, histogram.getSnapshot().getValues()[0]);
                histogram.update(1);
                Snapshot snapshot = histogram.getSnapshot();
                assertEquals(1, snapshot.getValues()[0]);
                assertEquals(1, snapshot.getValues()[1]);
            }
        }

        @Test
        public void testOverflow()
        {
            DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION, 1, 1);
            histogram.update(100);
            assert histogram.isOverflowed();
            assertEquals(Long.MAX_VALUE, toSnapshot.apply(histogram).getMax());
        }

        @Test
        public void testMinMax()
        {
            DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir();
            histogram.update(16);
            Snapshot snapshot = toSnapshot.apply(histogram);
            assertEquals(15, snapshot.getMin());
            assertEquals(17, snapshot.getMax());
        }

        @Test
        public void testMean()
        {
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
                for (int i = 0; i < 40; i++)
                    histogram.update(0);
                for (int i = 0; i < 20; i++)
                    histogram.update(1);
                for (int i = 0; i < 10; i++)
                    histogram.update(2);
                assertEquals(1.14D, toSnapshot.apply(histogram).getMean(), 0.1D);
            }
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(true,
                                                                                                        DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT,
                                                                                                        DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT,
                                                                                                        clock);
                for (int i = 0; i < 40; i++)
                    histogram.update(0);
                for (int i = 0; i < 20; i++)
                    histogram.update(1);
                for (int i = 0; i < 10; i++)
                    histogram.update(2);
                assertEquals(0.57D, toSnapshot.apply(histogram).getMean(), 0.1D);
            }
        }

        @Test
        public void testStdDev()
        {
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
                for (int i = 0; i < 20; i++)
                    histogram.update(10);
                for (int i = 0; i < 40; i++)
                    histogram.update(20);
                for (int i = 0; i < 20; i++)
                    histogram.update(30);

                Snapshot snapshot = toSnapshot.apply(histogram);
                assertEquals(20.0D, snapshot.getMean(), 2.0D);
                assertEquals(7.07D, snapshot.getStdDev(), 2.0D);
            }
        }

        @Test
        public void testFindingCorrectBuckets()
        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION, 90, 1, clock);
            histogram.update(23282687);
            assertFalse(histogram.isOverflowed());
            assertEquals(1, histogram.getSnapshot().getValues()[89]);

            histogram.update(9);
            assertEquals(1, histogram.getSnapshot().getValues()[8]);

            histogram.update(21);
            histogram.update(22);
            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(2, snapshot.getValues()[13]);
            assertEquals(6277304.5D, snapshot.getMean(), DOUBLE_ASSERT_DELTA);
        }

        @Test
        public void testPercentile()
        {
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
                // percentile of empty histogram is 0
                assertEquals(0D, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);

                histogram.update(1);
                // percentile of a histogram with one element should be that element
                assertEquals(1D, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);

                histogram.update(10);
                assertEquals(10D, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);
            }

            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);

                histogram.update(1);
                histogram.update(2);
                histogram.update(3);
                histogram.update(4);
                histogram.update(5);

                Snapshot snapshot = toSnapshot.apply(histogram);
                assertEquals(0, snapshot.getValue(0.00), DOUBLE_ASSERT_DELTA);
                assertEquals(3, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
                assertEquals(3, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
                assertEquals(5, snapshot.getValue(1.00), DOUBLE_ASSERT_DELTA);
            }

            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);

                for (int i = 11; i <= 20; i++)
                    histogram.update(i);

                // Right now the histogram looks like:
                //    10   12   14   17   20
                //     0    2    2    3    3
                // %:  0   20   40   70  100
                Snapshot snapshot = toSnapshot.apply(histogram);
                assertEquals(12, snapshot.getValue(0.01), DOUBLE_ASSERT_DELTA);
                assertEquals(14, snapshot.getValue(0.30), DOUBLE_ASSERT_DELTA);
                assertEquals(17, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
                assertEquals(17, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
                assertEquals(20, snapshot.getValue(0.80), DOUBLE_ASSERT_DELTA);
            }
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(true,
                                                                                                        DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT,
                                                                                                        DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT,
                                                                                                        clock);
                histogram.update(0);
                histogram.update(0);
                histogram.update(1);

                Snapshot snapshot = toSnapshot.apply(histogram);
                assertEquals(0, snapshot.getValue(0.5), DOUBLE_ASSERT_DELTA);
                assertEquals(1, snapshot.getValue(0.99), DOUBLE_ASSERT_DELTA);
            }
        }

        @Test
        public void testDecayingPercentile()
        {
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
                // percentile of empty histogram is 0
                assertEquals(0, toSnapshot.apply(histogram).getValue(1.0), DOUBLE_ASSERT_DELTA);

                for (int v = 1; v <= 100; v++)
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        histogram.update(v);
                    }
                }

                Snapshot snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(5, snapshot.getValue(0.05));
                assertEstimatedQuantile(20, snapshot.getValue(0.20));
                assertEstimatedQuantile(40, snapshot.getValue(0.40));
                assertEstimatedQuantile(99, snapshot.getValue(0.99));

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);
                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(5, snapshot.getValue(0.05));
                assertEstimatedQuantile(20, snapshot.getValue(0.20));
                assertEstimatedQuantile(40, snapshot.getValue(0.40));
                assertEstimatedQuantile(99, snapshot.getValue(0.99));

                for (int v = 1; v <= 50; v++)
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        histogram.update(v);
                    }
                }

                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(4, snapshot.getValue(0.05));
                assertEstimatedQuantile(14, snapshot.getValue(0.20));
                assertEstimatedQuantile(27, snapshot.getValue(0.40));
                assertEstimatedQuantile(98, snapshot.getValue(0.99));

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);
                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(4, snapshot.getValue(0.05));
                assertEstimatedQuantile(14, snapshot.getValue(0.20));
                assertEstimatedQuantile(27, snapshot.getValue(0.40));
                assertEstimatedQuantile(98, snapshot.getValue(0.99));

                for (int v = 1; v <= 50; v++)
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        histogram.update(v);
                    }
                }

                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(3, snapshot.getValue(0.05));
                assertEstimatedQuantile(12, snapshot.getValue(0.20));
                assertEstimatedQuantile(23, snapshot.getValue(0.40));
                assertEstimatedQuantile(96, snapshot.getValue(0.99));

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);
                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(3, snapshot.getValue(0.05));
                assertEstimatedQuantile(12, snapshot.getValue(0.20));
                assertEstimatedQuantile(23, snapshot.getValue(0.40));
                assertEstimatedQuantile(96, snapshot.getValue(0.99));

                for (int v = 11; v <= 20; v++)
                {
                    for (int i = 0; i < 5_000; i++)
                    {
                        histogram.update(v);
                    }
                }

                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(4, snapshot.getValue(0.05));
                assertEstimatedQuantile(12, snapshot.getValue(0.20));
                assertEstimatedQuantile(20, snapshot.getValue(0.40));
                assertEstimatedQuantile(95, snapshot.getValue(0.99));

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);
                snapshot = toSnapshot.apply(histogram);
                assertEstimatedQuantile(4, snapshot.getValue(0.05));
                assertEstimatedQuantile(12, snapshot.getValue(0.20));
                assertEstimatedQuantile(20, snapshot.getValue(0.40));
                assertEstimatedQuantile(95, snapshot.getValue(0.99));

            }

            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
                // percentile of empty histogram is 0
                assertEquals(0, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);

                for (int m = 0; m < 40; m++)
                {
                    for (int i = 0; i < 1_000_000; i++)
                    {
                        histogram.update(2);
                    }
                    // percentile of a histogram with one element should be that element
                    clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);
                    assertEquals(2, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);
                }

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S * 100);
                assertEquals(0, toSnapshot.apply(histogram).getValue(0.99), DOUBLE_ASSERT_DELTA);
            }

            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);

                histogram.update(20);
                histogram.update(21);
                histogram.update(22);
                Snapshot snapshot = histogram.getSnapshot();
                assertEquals(1, snapshot.getValues()[12]);
                assertEquals(2, snapshot.getValues()[13]);

                clock.addSeconds(DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S);

                histogram.update(20);
                histogram.update(21);
                histogram.update(22);
                snapshot = histogram.getSnapshot();
                assertEquals(2, snapshot.getValues()[12]);
                assertEquals(4, snapshot.getValues()[13]);
            }
        }

        @Test
        public void testDecayingMean()
        {
            {
                TestClock clock = new TestClock();

                DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);

                clock.addNanos(DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS - TimeUnit.SECONDS.toNanos(1L));

                while (clock.now() < DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS + TimeUnit.SECONDS.toNanos(1L))
                {
                    clock.addNanos(TimeUnit.MILLISECONDS.toNanos(900));
                    for (int i = 0; i < 1_000_000; i++)
                    {
                        histogram.update(1000);
                        histogram.update(2000);
                        histogram.update(3000);
                        histogram.update(4000);
                        histogram.update(5000);
                    }
                    assertEquals(3000D, toSnapshot.apply(histogram).getMean(), 500D);
                }
            }
        }

        @Test
        public void testAggregation()
        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
            DecayingEstimatedHistogramReservoir another = new DecayingEstimatedHistogramReservoir(clock);

            clock.addNanos(DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS - TimeUnit.SECONDS.toNanos(1L));

            histogram.update(1000);
            clock.addMillis(100);
            another.update(2000);
            clock.addMillis(100);
            histogram.update(2000);
            clock.addMillis(100);
            another.update(3000);
            clock.addMillis(100);
            histogram.update(3000);
            clock.addMillis(100);
            another.update(4000);

            DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot snapshot = (DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot) histogram.getSnapshot();
            DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot anotherSnapshot = (DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot) another.getSnapshot();

            assertEquals(2000, snapshot.getMean(), 500D);
            assertEquals(3000, anotherSnapshot.getMean(), 500D);

            snapshot.add(anotherSnapshot);

            // Another had newer decayLandmark, the aggregated snapshot should use it
            assertEquals(anotherSnapshot.getSnapshotLandmark(), snapshot.getSnapshotLandmark());
            assertEquals(2500, snapshot.getMean(), 500D);
        }

        @Test
        public void testSize()
        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir(clock);
            histogram.update(42);
            histogram.update(42);
            assertEquals(2, toSnapshot.apply(histogram).size());
        }

        private void assertEstimatedQuantile(long expectedValue, double actualValue)
        {
            assertTrue("Expected at least [" + expectedValue + "] but actual is [" + actualValue + ']', actualValue >= expectedValue);
            assertTrue("Expected less than [" + Math.round(expectedValue * 1.2) + "] but actual is [" + actualValue + ']', actualValue < Math.round(expectedValue * 1.2));
        }

        public static class TestClock implements MonotonicClock
        {
            private long tick = 0;

            public void addNanos(long nanos)
            {
                tick += nanos;
            }

            public void addMillis(long millis)
            {
                tick += TimeUnit.MILLISECONDS.toNanos(millis);
            }

            public void addSeconds(long seconds)
            {
                tick += TimeUnit.SECONDS.toNanos(seconds);
            }

            public long now()
            {
                return tick;
            }

            @Override
            public long error()
            {
                return 0;
            }

            @Override
            public MonotonicClockTranslation translate()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isAfter(long instant)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isAfter(long now, long instant)
            {
                throw new UnsupportedOperationException();
            }
        }
    }
}
