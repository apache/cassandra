package org.apache.cassandra.concurrent;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.cassandra.Util;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.junit.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;
import static org.junit.Assert.*;

public class WaitQueueTest
{

    @Test
    public void testSerial() throws InterruptedException
    {
        testSerial(newWaitQueue());
    }
    public void testSerial(final WaitQueue queue) throws InterruptedException
    {
        final AtomicInteger ready = new AtomicInteger();
        Thread[] ts = new Thread[4];
        for (int i = 0 ; i < ts.length ; i++)
            ts[i] = NamedThreadFactory.createAnonymousThread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitQueue.Signal wait = queue.register();
                ready.incrementAndGet();
                try
                {
                    wait.await();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });
        for (Thread t : ts)
            t.start();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        while (ready.get() < ts.length)
            random.nextLong();
        for (Thread t : ts)
            queue.signal();
        for (Thread t : ts)
        {
            Util.joinThread(t);
            assertFalse(queue.getClass().getName(), t.isAlive());
        }
    }

    @Test
    public void testCondition() throws InterruptedException
    {
        testCondition(newWaitQueue());
    }
    public void testCondition(final WaitQueue queue) throws InterruptedException
    {
        final AtomicBoolean ready = new AtomicBoolean(false);
        final AtomicBoolean condition = new AtomicBoolean(false);
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t = NamedThreadFactory.createAnonymousThread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitQueue.Signal wait = queue.register();
                if (condition.get())
                {
                    System.err.println("");
                    fail.set(true);
                    ready.set(true);
                    return;
                }

                ready.set(true);
                wait.awaitUninterruptibly();
                if (!condition.get())
                {
                    System.err.println("Woke up when condition not met");
                    fail.set(true);
                }
            }
        });
        t.start();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        while (!ready.get())
            random.nextLong();
        condition.set(true);
        queue.signal();
        Util.joinThread(t);
        assertFalse(queue.getClass().getName(), t.isAlive());
        assertFalse(fail.get());
    }

}
