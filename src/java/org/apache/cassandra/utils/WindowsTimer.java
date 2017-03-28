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

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;

public final class WindowsTimer
{
    private static final Logger logger = LoggerFactory.getLogger(WindowsTimer.class);

    static
    {
        try
        {
            Native.register("winmm");
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. winmm.dll cannot be registered. Performance will be negatively impacted on this node.");
        }
        catch (Exception e)
        {
            logger.error("Failed to register winmm.dll. Performance will be negatively impacted on this node.");
        }
    }

    private static native int timeBeginPeriod(int period) throws LastErrorException;
    private static native int timeEndPeriod(int period) throws LastErrorException;

    private WindowsTimer() {}

    public static void startTimerPeriod(int period)
    {
        if (period == 0)
            return;
        assert(period > 0);
        if (timeBeginPeriod(period) != 0)
            logger.warn("Failed to set timer to : {}. Performance will be degraded.", period);
    }

    public static void endTimerPeriod(int period)
    {
        if (period == 0)
            return;
        assert(period > 0);
        if (timeEndPeriod(period) != 0)
            logger.warn("Failed to end accelerated timer period. System timer will remain set to: {} ms.", period);
    }
}
