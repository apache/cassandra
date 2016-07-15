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

package org.apache.cassandra.db.monitoring;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;

/**
 * This is an approximation of System.currentTimeInMillis(). It updates its
 * time value at periodic intervals of CHECK_INTERVAL_MS milliseconds
 * (currently 10 milliseconds by default). It can be used as a faster alternative
 * to System.currentTimeInMillis() every time an imprecision of a few milliseconds
 * can be accepted.
 */
public class ApproximateTime
{
    private static final Logger logger = LoggerFactory.getLogger(ApproximateTime.class);
    private static final int CHECK_INTERVAL_MS = Math.max(5, Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "approximate_time_precision_ms", "10")));

    private static volatile long time = System.currentTimeMillis();
    static
    {
        logger.info("Scheduling approximate time-check task with a precision of {} milliseconds", CHECK_INTERVAL_MS);
        ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(() -> time = System.currentTimeMillis(),
                                                                     CHECK_INTERVAL_MS,
                                                                     CHECK_INTERVAL_MS,
                                                                     TimeUnit.MILLISECONDS);
    }

    public static long currentTimeMillis()
    {
        return time;
    }

    public static long precision()
    {
        return 2 * CHECK_INTERVAL_MS;
    }

}
