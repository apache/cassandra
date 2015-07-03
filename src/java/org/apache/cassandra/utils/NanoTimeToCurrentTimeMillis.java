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

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;

import com.google.common.annotations.VisibleForTesting;

/*
 * Convert from nanotime to non-monotonic current time millis. Beware of weaker ordering guarantees.
 */
public class NanoTimeToCurrentTimeMillis
{
    /*
     * How often to pull a new timestamp from the system.
     */
    private static final String TIMESTAMP_UPDATE_INTERVAL_PROPERTY = Config.PROPERTY_PREFIX + "NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL";
    private static final long TIMESTAMP_UPDATE_INTERVAL = Long.getLong(TIMESTAMP_UPDATE_INTERVAL_PROPERTY, 10000);

    private static volatile long TIMESTAMP_BASE[] = new long[] { System.currentTimeMillis(), System.nanoTime() };

    /*
     * System.currentTimeMillis() is 25 nanoseconds. This is 2 nanoseconds (maybe) according to JMH.
     * Faster than calling both currentTimeMillis() and nanoTime().
     *
     * There is also the issue of how scalable nanoTime() and currentTimeMillis() are which is a moving target.
     *
     * These timestamps don't order with System.currentTimeMillis() because currentTimeMillis() can tick over
     * before this one does. I have seen it behind by as much as 2ms on Linux and 25ms on Windows.
     */
    public static long convert(long nanoTime)
    {
        final long timestampBase[] = TIMESTAMP_BASE;
        return timestampBase[0] + TimeUnit.NANOSECONDS.toMillis(nanoTime - timestampBase[1]);
    }

    public static void updateNow()
    {
        ScheduledExecutors.scheduledFastTasks.submit(NanoTimeToCurrentTimeMillis::updateTimestampBase);
    }

    static
    {
        ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(NanoTimeToCurrentTimeMillis::updateTimestampBase,
                                                                     TIMESTAMP_UPDATE_INTERVAL,
                                                                     TIMESTAMP_UPDATE_INTERVAL,
                                                                     TimeUnit.MILLISECONDS);
    }

    private static void updateTimestampBase()
    {
        TIMESTAMP_BASE = new long[] {
                                    Math.max(TIMESTAMP_BASE[0], System.currentTimeMillis()),
                                    Math.max(TIMESTAMP_BASE[1], System.nanoTime()) };
    }
}
