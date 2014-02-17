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


import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class WaitQueueTest
{

    @Test
    public void testSerial() throws InterruptedException
    {
        testSerial(new WaitQueue());
    }
    public void testSerial(final WaitQueue queue) throws InterruptedException
    {
        Thread[] ts = new Thread[4];
        for (int i = 0 ; i < ts.length ; i++)
            ts[i] = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitQueue.Signal wait = queue.register();
                try
                {
                    wait.await();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });
        for (int i = 0 ; i < ts.length ; i++)
            ts[i].start();
        Thread.sleep(100);
        queue.signal();
        queue.signal();
        queue.signal();
        queue.signal();
        for (int i = 0 ; i < ts.length ; i++)
        {
            ts[i].join(100);
            assertFalse(queue.getClass().getName(), ts[i].isAlive());
        }
    }


    @Test
    public void testCondition1() throws InterruptedException
    {
        testCondition1(new WaitQueue());
    }

    public void testCondition1(final WaitQueue queue) throws InterruptedException
    {
        final AtomicBoolean cond1 = new AtomicBoolean(false);
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t1 = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep(200);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                WaitQueue.Signal wait = queue.register();
                if (!cond1.get())
                {
                    System.err.println("Condition should have already been met");
                    fail.set(true);
                }
            }
        });
        t1.start();
        Thread.sleep(50);
        cond1.set(true);
        Thread.sleep(300);
        queue.signal();
        t1.join(300);
        assertFalse(queue.getClass().getName(), t1.isAlive());
        assertFalse(fail.get());
    }

    @Test
    public void testCondition2() throws InterruptedException
    {
        testCondition2(new WaitQueue());
    }
    public void testCondition2(final WaitQueue queue) throws InterruptedException
    {
        final AtomicBoolean condition = new AtomicBoolean(false);
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitQueue.Signal wait = queue.register();
                if (condition.get())
                {
                    System.err.println("");
                    fail.set(true);
                }

                try
                {
                    Thread.sleep(200);
                    wait.await();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                if (!condition.get())
                {
                    System.err.println("Woke up when condition not met");
                    fail.set(true);
                }
            }
        });
        t.start();
        Thread.sleep(50);
        condition.set(true);
        queue.signal();
        t.join(300);
        assertFalse(queue.getClass().getName(), t.isAlive());
        assertFalse(fail.get());
    }

}
