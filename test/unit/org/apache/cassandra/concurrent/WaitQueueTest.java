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
import java.util.concurrent.TimeUnit;
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

    @Test
    public void testInterruptOfSignalAwaitingThread() throws InterruptedException
    {
        final WaitQueue waitQueue = newWaitQueue();
        Thread writerAwaitThread = createThread(() -> {
            Thread.currentThread().interrupt();
            WaitQueue.Signal signal = waitQueue.register();
            signal.awaitUninterruptibly();

        }, "writer.await");

        writerAwaitThread.start();

        Thread.sleep(1_000); // wait to enter signal.awaitUninterruptibly()
        waitQueue.signalAll();

        writerAwaitThread.join(4_000);
        if (writerAwaitThread.isAlive())
        {
            printThreadStackTrace(writerAwaitThread);
            fail("signal.awaitUninterruptibly() is stuck");
        }
    }

    @Test
    public void testInterruptOfSignalAwaitingWithTimeoutThread() throws InterruptedException
    {
        final WaitQueue waitQueue = newWaitQueue();
        Thread writerAwaitThread = createThread(() -> {
            Thread.currentThread().interrupt();
            WaitQueue.Signal signal = waitQueue.register();
            signal.awaitUninterruptibly(100_000, TimeUnit.MILLISECONDS);
        }, "writer.await");

        writerAwaitThread.start();

        Thread.sleep(1_000); // wait to enter signal.awaitUninterruptibly()
        waitQueue.signalAll();

        writerAwaitThread.join(4_000);
        if (writerAwaitThread.isAlive())
        {
            printThreadStackTrace(writerAwaitThread);
            fail("signal.awaitUninterruptibly() is stuck");
        }
    }

    private static Thread createThread(Runnable job, String name)
    {
        Thread thread = new Thread(job, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void printThreadStackTrace(Thread thread)
    {
        System.out.println("Stack trace for thread: " + thread.getName());
        StackTraceElement[] stackTrace = thread.getStackTrace();
        if (stackTrace.length == 0)
        {
            System.out.println("The thread is not currently running or has no stack trace.");
        } else
        {
            for (StackTraceElement element : stackTrace)
            {
                System.out.println("\tat " + element);
            }
        }
    }
}
