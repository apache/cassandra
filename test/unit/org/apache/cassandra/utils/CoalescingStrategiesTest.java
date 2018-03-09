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

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.CoalescingStrategies.Coalescable;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.CoalescingStrategies.FixedCoalescingStrategy;
import org.apache.cassandra.utils.CoalescingStrategies.MovingAverageCoalescingStrategy;
import org.apache.cassandra.utils.CoalescingStrategies.TimeHorizonMovingAverageCoalescingStrategy;

public class CoalescingStrategiesTest
{
    private static final Logger logger = LoggerFactory.getLogger(CoalescingStrategiesTest.class);
    private static final int WINDOW_IN_MICROS = 200;
    private static final long WINDOW_IN_NANOS = TimeUnit.MICROSECONDS.toNanos(WINDOW_IN_MICROS);
    private static final String DISPLAY_NAME = "Stupendopotamus";

    static class SimpleCoalescable implements Coalescable
    {
        final long timestampNanos;

        SimpleCoalescable(long timestampNanos)
        {
            this.timestampNanos = timestampNanos;
        }

        public long timestampNanos()
        {
            return timestampNanos;
        }
    }

    static long toNanos(long micros)
    {
        return TimeUnit.MICROSECONDS.toNanos(micros);
    }

    @Test
    public void testFixedCoalescingStrategy()
    {
        CoalescingStrategy cs = new FixedCoalescingStrategy(WINDOW_IN_MICROS, logger, DISPLAY_NAME);
        Assert.assertEquals(WINDOW_IN_NANOS, cs.currentCoalescingTimeNanos());
    }

    @Test
    public void testMovingAverageCoalescingStrategy_DoCoalesce()
    {
        CoalescingStrategy cs = new MovingAverageCoalescingStrategy(WINDOW_IN_MICROS, logger, DISPLAY_NAME);

        for (int i = 0; i < MovingAverageCoalescingStrategy.SAMPLE_SIZE; i++)
            cs.newArrival(new SimpleCoalescable(toNanos(i)));
        Assert.assertTrue(0 < cs.currentCoalescingTimeNanos());
    }

    @Test
    public void testMovingAverageCoalescingStrategy_DoNotCoalesce()
    {
        CoalescingStrategy cs = new MovingAverageCoalescingStrategy(WINDOW_IN_MICROS, logger, DISPLAY_NAME);

        for (int i = 0; i < MovingAverageCoalescingStrategy.SAMPLE_SIZE; i++)
            cs.newArrival(new SimpleCoalescable(toNanos(WINDOW_IN_MICROS + i) * i));
        Assert.assertTrue(0 >= cs.currentCoalescingTimeNanos());
    }

    @Test
    public void testTimeHorizonStrategy_DoCoalesce()
    {
        long initialEpoch = 0;
        CoalescingStrategy cs = new TimeHorizonMovingAverageCoalescingStrategy(WINDOW_IN_MICROS, logger, DISPLAY_NAME, initialEpoch);

        for (int i = 0; i < 10_000; i++)
            cs.newArrival(new SimpleCoalescable(toNanos(i)));
        Assert.assertTrue(0 < cs.currentCoalescingTimeNanos());
    }

    @Test
    public void testTimeHorizonStrategy_DoNotCoalesce()
    {
        long initialEpoch = 0;
        CoalescingStrategy cs = new TimeHorizonMovingAverageCoalescingStrategy(WINDOW_IN_MICROS, logger, DISPLAY_NAME, initialEpoch);

        for (int i = 0; i < 1_000_000; i++)
            cs.newArrival(new SimpleCoalescable(toNanos(WINDOW_IN_MICROS + i) * i));
        Assert.assertTrue(0 >= cs.currentCoalescingTimeNanos());
    }

    @Test
    public void determineCoalescingTime_LargeAverageGap()
    {
        Assert.assertTrue(0 >= CoalescingStrategies.determineCoalescingTime(WINDOW_IN_NANOS * 2, WINDOW_IN_NANOS));
        Assert.assertTrue(0 >= CoalescingStrategies.determineCoalescingTime(Integer.MAX_VALUE, WINDOW_IN_NANOS));
    }

    @Test
    public void determineCoalescingTime_SmallAvgGap()
    {
        Assert.assertTrue(WINDOW_IN_NANOS >= CoalescingStrategies.determineCoalescingTime(WINDOW_IN_NANOS / 2, WINDOW_IN_NANOS));
        Assert.assertTrue(WINDOW_IN_NANOS >= CoalescingStrategies.determineCoalescingTime(WINDOW_IN_NANOS - 1, WINDOW_IN_NANOS));
        Assert.assertTrue(WINDOW_IN_NANOS >= CoalescingStrategies.determineCoalescingTime(1, WINDOW_IN_NANOS));
        Assert.assertEquals(WINDOW_IN_NANOS, CoalescingStrategies.determineCoalescingTime(0, WINDOW_IN_NANOS));
    }
}
