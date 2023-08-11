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

package org.apache.cassandra.distributed.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.UncheckedTimeoutException;

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

@Shared
public class TwoWay implements AutoCloseable
{
    private volatile CountDownLatch first = new CountDownLatch(1);
    private final CountDownLatch second = new CountDownLatch(1);

    public void awaitAndEnter()
    {
        await();
        enter();
    }

    public void await()
    {
        await(first);
        first = null;
    }

    public void enter()
    {
        if (first != null)
        {
            first.countDown();
            await(second);
        }
        else
        {
            second.countDown();
        }
    }

    private void await(CountDownLatch latch)
    {
        try
        {
            if (!latch.await(1, TimeUnit.MINUTES))
                throw new UncheckedTimeoutException("Timeout waiting on " + (latch == second ? "second" : "first") + " latch");
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    @Override
    public void close()
    {
        if (first != null)
            first.countDown();
        first = null;
        second.countDown();
    }
}
