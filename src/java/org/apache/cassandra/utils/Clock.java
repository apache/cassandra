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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_GLOBAL;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Wrapper around time related functions that are either implemented by using the default JVM calls
 * or by using a custom implementation for testing purposes.
 *
 * See {@link Global#instance} for how to use a custom implementation.
 *
 * Please note that {@link java.time.Clock} wasn't used, as it would not be possible to provide an
 * implementation for {@link #nanoTime()} with the exact same properties of {@link System#nanoTime()}.
 */
@Shared(scope = SIMULATION)
public interface Clock
{
    static final Logger logger = LoggerFactory.getLogger(Clock.class);

    public static class Global
    {
        /**
         * Static singleton object that will be instantiated by default with a system clock
         * implementation. Set <code>cassandra.clock</code> system property to a FQCN to use a
         * different implementation instead.
         */
        private static final Clock instance;

        static
        {
            String classname = CLOCK_GLOBAL.getString();
            Clock clock = new Default();
            if (classname != null)
            {
                try
                {
                    logger.debug("Using custom clock implementation: {}", classname);
                    clock = (Clock) Class.forName(classname).newInstance();
                }
                catch (Exception e)
                {
                    logger.error("Failed to load clock implementation {}", classname, e);
                }
            }
            instance = clock;
        }

        /**
         * Semantically equivalent to {@link System#nanoTime()}
         */
        public static long nanoTime()
        {
            return instance.nanoTime();
        }

        /**
         * Semantically equivalent to {@link System#currentTimeMillis()}
         */
        public static long currentTimeMillis()
        {
            return instance.currentTimeMillis();
        }
    }

    public static class Default implements Clock
    {
        /**
         * {@link System#nanoTime()}
         */
        public long nanoTime()
        {
            return System.nanoTime();
        }

        /**
         * {@link System#currentTimeMillis()}
         */
        public long currentTimeMillis()
        {
            return System.currentTimeMillis();
        }
    }

    /**
     * Semantically equivalent to {@link System#nanoTime()}
     */
    public long nanoTime();

    /**
     * Semantically equivalent to {@link System#currentTimeMillis()}
     */
    public long currentTimeMillis();

    @Intercept
    public static void waitUntil(long deadlineNanos) throws InterruptedException
    {
        long waitNanos = Clock.Global.nanoTime() - deadlineNanos;
        if (waitNanos > 0)
            TimeUnit.NANOSECONDS.sleep(waitNanos);
    }
}
