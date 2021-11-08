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


import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

// TODO: we don't currently test SAFE functionality at all!
// TODO: should also test markBlocking and SyncOrdered
public class LongOpOrderTest
{

    private static final Logger logger = LoggerFactory.getLogger(LongOpOrderTest.class);

    static final int CONSUMERS = 4;
    static final int PRODUCERS = 32;

    static final long RUNTIME = TimeUnit.MINUTES.toMillis(5);
    static final long REPORT_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    static final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            System.err.println(t.getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    };

    final OpOrder order = new OpOrder();
    final AtomicInteger errors = new AtomicInteger();

    class TestOrdering implements Runnable
    {

        final int[] waitNanos = new int[1 << 16];
        volatile State state = new State();
        final ScheduledExecutorService sched;

        TestOrdering(ExecutorService exec, ScheduledExecutorService sched)
        {
            this.sched = sched;
            final ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0 ; i < waitNanos.length ; i++)
                waitNanos[i] = rnd.nextInt(5000);
            for (int i = 0 ; i < PRODUCERS / CONSUMERS ; i++)
                exec.execute(new Producer());
            exec.execute(this);
        }

        @Override
        public void run()
        {
            final long until = currentTimeMillis() + RUNTIME;
            long lastReport = currentTimeMillis();
            long count = 0;
            long opCount = 0;
            while (true)
            {
                long now = currentTimeMillis();
                if (now > until)
                    break;
                if (now > lastReport + REPORT_INTERVAL)
                {
                    lastReport = now;
                    logger.info(String.format("%s: Executed %d barriers with %d operations. %.0f%% complete.",
                            Thread.currentThread().getName(), count, opCount, 100 * (1 - ((until - now) / (double) RUNTIME))));
                }
                try
                {
                    Thread.sleep(0, waitNanos[((int) (count & (waitNanos.length - 1)))]);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                final State s = state;
                s.barrier = order.newBarrier();
                s.replacement = new State();
                s.barrier.issue();
                s.barrier.await();
                s.check();
                opCount += s.totalCount();
                state = s.replacement;
                sched.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        s.check();
                    }
                }, 1, TimeUnit.SECONDS);
                count++;
            }
        }

        class State
        {

            volatile OpOrder.Barrier barrier;
            volatile State replacement;
            final NonBlockingHashMap<OpOrder.Group, AtomicInteger> count = new NonBlockingHashMap<>();
            int checkCount = -1;

            boolean accept(OpOrder.Group opGroup)
            {
                if (barrier != null && !barrier.isAfter(opGroup))
                    return false;
                AtomicInteger c;
                if (null == (c = count.get(opGroup)))
                {
                    count.putIfAbsent(opGroup, new AtomicInteger());
                    c = count.get(opGroup);
                }
                c.incrementAndGet();
                return true;
            }

            int totalCount()
            {
                int c = 0;
                for (AtomicInteger v : count.values())
                    c += v.intValue();
                return c;
            }

            void check()
            {
                boolean delete;
                if (checkCount >= 0)
                {
                    if (checkCount != totalCount())
                    {
                        errors.incrementAndGet();
                        logger.error("Received size changed after barrier finished: {} vs {}", checkCount, totalCount());
                    }
                    delete = true;
                }
                else
                {
                    checkCount = totalCount();
                    delete = false;
                }
                for (Map.Entry<OpOrder.Group, AtomicInteger> e : count.entrySet())
                {
                    if (e.getKey().compareTo(barrier.getSyncPoint()) > 0)
                    {
                        errors.incrementAndGet();
                        logger.error("Received an operation that was created after the barrier was issued.");
                    }
                    if (TestOrdering.this.count.get(e.getKey()).intValue() != e.getValue().intValue())
                    {
                        errors.incrementAndGet();
                        logger.error("Missing registered operations. {} vs {}", TestOrdering.this.count.get(e.getKey()).intValue(), e.getValue().intValue());
                    }
                    if (delete)
                        TestOrdering.this.count.remove(e.getKey());
                }
            }

        }

        final NonBlockingHashMap<OpOrder.Group, AtomicInteger> count = new NonBlockingHashMap<>();

        class Producer implements Runnable
        {
            public void run()
            {
                while (true)
                {
                    AtomicInteger c;
                    try (OpOrder.Group opGroup = order.start())
                    {
                        if (null == (c = count.get(opGroup)))
                        {
                            count.putIfAbsent(opGroup, new AtomicInteger());
                            c = count.get(opGroup);
                        }
                        c.incrementAndGet();
                        State s = state;
                        while (!s.accept(opGroup))
                            s = s.replacement;
                    }
                }
            }
        }

    }

    @Test
    public void testOrdering() throws InterruptedException
    {
        errors.set(0);
        Thread.setDefaultUncaughtExceptionHandler(handler);
        final ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("checker"));
        final ScheduledExecutorService checker = Executors.newScheduledThreadPool(1, new NamedThreadFactory("checker"));
        for (int i = 0 ; i < CONSUMERS ; i++)
            new TestOrdering(exec, checker);
        exec.shutdown();
        exec.awaitTermination((long) (RUNTIME * 1.1), TimeUnit.MILLISECONDS);
        assertTrue(exec.isShutdown());
        assertTrue(errors.get() == 0);
    }


}
