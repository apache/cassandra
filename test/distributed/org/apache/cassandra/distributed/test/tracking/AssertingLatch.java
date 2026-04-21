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

package org.apache.cassandra.distributed.test.tracking;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A CountDownLatch wrapper that throws on timeout. Interruption during shutdown is benign.
 */
public class AssertingLatch
{
    private static final long DEFAULT_TIMEOUT = 30;
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.SECONDS;

    private final CountDownLatch latch;
    private final String description;
    private final long timeout;
    private final TimeUnit unit;

    public AssertingLatch(String description)
    {
        this(1, DEFAULT_TIMEOUT, DEFAULT_UNIT, description);
    }

    public AssertingLatch(int count, String description)
    {
        this(count, DEFAULT_TIMEOUT, DEFAULT_UNIT, description);
    }

    public AssertingLatch(int count, long timeout, TimeUnit unit, String description)
    {
        this.latch = new CountDownLatch(count);
        this.description = description;
        this.timeout = timeout;
        this.unit = unit;
    }

    public void await()
    {
        try
        {
            if (!latch.await(timeout, unit))
                throw new AssertionError("Timed out after " + timeout + " " + unit + " waiting for: " + description);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    public void countDown()
    {
        latch.countDown();
    }

    public void release()
    {
        while (latch.getCount() > 0)
            latch.countDown();
    }

    public long getCount()
    {
        return latch.getCount();
    }
}