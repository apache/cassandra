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

import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

public class MonotonicClockTest
{
    @Test
    public void testTimestampOrdering() throws Exception
    {
        long nowNanos = System.nanoTime();
        long now = System.currentTimeMillis();
        long lastConverted = 0;
        for (long ii = 0; ii < 10000000; ii++)
        {
            now = Math.max(now, System.currentTimeMillis());
            if (ii % 10000 == 0)
            {
                ((MonotonicClock.SampledClock) approxTime).refreshNow();
                Thread.sleep(1);
            }

            nowNanos = Math.max(nowNanos, System.nanoTime());
            long convertedNow = approxTime.translate().toMillisSinceEpoch(nowNanos);

            int maxDiff = FBUtilities.isWindows ? 15 : 1;
            assertTrue("convertedNow = " + convertedNow + " lastConverted = " + lastConverted + " in iteration " + ii,
                       convertedNow >= (lastConverted - maxDiff));

            maxDiff = FBUtilities.isWindows ? 25 : 2;
            assertTrue("now = " + now + " convertedNow = " + convertedNow + " in iteration " + ii,
                       (maxDiff - 2) <= convertedNow);

            lastConverted = convertedNow;
        }
    }

    @Test
    public void testTimestampOverflowComparison()
    {
        MonotonicClock clock = MonotonicClock.preciseTime;

        Assert.assertTrue("Overflown long (now) should be after long close to max",
                          clock.isAfter(Long.MIN_VALUE + 1, Long.MAX_VALUE));
    }
}
