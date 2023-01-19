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
package org.apache.cassandra.inject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CyclicBarrier
{
    private static final int LATCH_TIMEOUT_SECONDS = 20;

    public final String name;
    public final int parties;
    public final boolean cyclic;

    private CountDownLatch latch;

    public CyclicBarrier(String name, int parties, boolean cyclic)
    {
        this.name = name;
        this.parties = parties;
        this.cyclic = cyclic;
        this.latch = new CountDownLatch(parties);
    }

    public void await() throws InterruptedException
    {
        await(true, true);
    }

    public void await(boolean doCountDown, boolean doAwait) throws InterruptedException
    {
        if (doCountDown)
        {
            latch.countDown();
        }
        if (doAwait)
        {
            latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (cyclic)
            {
                reset();
            }
        }
    }

    public void reset()
    {
        latch = new CountDownLatch(parties);
    }

    public long getCount()
    {
        return latch.getCount();
    }
}
