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

/**
 * A freely adjustable clock that can be used for unit testing. See {@link MonotonicClock#instance} how to
 * enable this class.
 */
public class FreeRunningClock implements MonotonicClock
{
    private long nanoTime;
    private long millisSinceEpoch;
    private long error;

    public FreeRunningClock()
    {
        this.nanoTime = 0;
    }

    public FreeRunningClock(long nanoTime)
    {
        this.nanoTime = nanoTime;
    }

    public FreeRunningClock(long nanoTime, long millisSinceEpoch, long error)
    {
        this.nanoTime = nanoTime;
        this.millisSinceEpoch = millisSinceEpoch;
        this.error = error;
    }

    @Override
    public long now()
    {
        return nanoTime;
    }

    @Override
    public long error()
    {
        return error;
    }

    @Override
    public MonotonicClockTranslation translate()
    {
        return new AbstractEpochSamplingClock.AlmostSameTime(millisSinceEpoch, nanoTime, error);
    }

    @Override
    public boolean isAfter(long instant)
    {
        return instant > nanoTime;
    }

    @Override
    public boolean isAfter(long now, long instant)
    {
        return now > instant;
    }

    public void advance(long time, TimeUnit unit)
    {
        nanoTime += unit.toNanos(time);
    }
}
