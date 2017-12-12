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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around time related functions that are either implemented by using the default JVM calls
 * or by using a custom implementation for testing purposes.
 *
 * See {@link #instance} for how to use a custom implementation.
 *
 * Please note that {@link java.time.Clock} wasn't used, as it would not be possible to provide an
 * implementation for {@link #nanoTime()} with the exact same properties of {@link System#nanoTime()}.
 */
public class Clock
{
    private static final Logger logger = LoggerFactory.getLogger(Clock.class);

    /**
     * Static singleton object that will be instanciated by default with a system clock
     * implementation. Set <code>cassandra.clock</code> system property to a FQCN to use a
     * different implementation instead.
     */
    public static Clock instance;

    static
    {
        String sclock = System.getProperty("cassandra.clock");
        if (sclock == null)
        {
            instance = new Clock();
        }
        else
        {
            try
            {
                logger.debug("Using custom clock implementation: {}", sclock);
                instance = (Clock) Class.forName(sclock).newInstance();
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * @see System#nanoTime()
     */
    public long nanoTime()
    {
        return System.nanoTime();
    }

    /**
     * @see System#currentTimeMillis()
     */
    public long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }

}
