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
import java.util.concurrent.atomic.AtomicLong;

public class TestTimeSource implements TimeSource
{
    private final AtomicLong timeInMillis = new AtomicLong(System.currentTimeMillis());

    @Override
    public long currentTimeMillis()
    {
        return timeInMillis.get();
    }

    @Override
    public long nanoTime()
    {
        return timeInMillis.get() * 1_000_000;
    }

    @Override
    public TimeSource sleep(long sleepFor, TimeUnit unit)
    {
        long current = timeInMillis.get();
        long sleepInMillis = TimeUnit.MILLISECONDS.convert(sleepFor, unit);
        boolean elapsed;
        do
        {
            long newTime = current + sleepInMillis;
            elapsed = timeInMillis.compareAndSet(current, newTime);
            if (!elapsed)
            {
                long updated = timeInMillis.get();
                if (updated - current >= sleepInMillis)
                {
                    elapsed = true;
                }
                else
                {
                    sleepInMillis -= updated - current;
                    current = updated;
                }
            }
        }
        while (!elapsed);
        return this;
    }

    @Override
    public TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit)
    {
        return sleep(sleepFor, unit);
    }
}
