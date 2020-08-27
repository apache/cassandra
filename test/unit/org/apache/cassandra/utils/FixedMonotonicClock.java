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

public final class FixedMonotonicClock implements MonotonicClock
{
    public long now()
    {
        return 42;
    }

    public long error()
    {
        return 0;
    }

    public MonotonicClockTranslation translate()
    {
        return FakeMonotonicClockTranslation.instance;
    }

    public boolean isAfter(long instant)
    {
        return false;
    }

    public boolean isAfter(long now, long instant)
    {
        return false;
    }

    private static final class FakeMonotonicClockTranslation implements MonotonicClockTranslation
    {
        private static final FakeMonotonicClockTranslation instance = new FakeMonotonicClockTranslation();

        public long fromMillisSinceEpoch(long currentTimeMillis)
        {
            return TimeUnit.MILLISECONDS.toNanos(currentTimeMillis);
        }

        public long toMillisSinceEpoch(long nanoTime)
        {
            return TimeUnit.NANOSECONDS.toMillis(nanoTime);
        }

        public long error()
        {
            return 0;
        }
    }
}
