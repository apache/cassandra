package org.apache.cassandra.scheduler;

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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.RequestSchedulerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RoundRobinSchedulerTest
{
    ExecutorService executor;
    IRequestScheduler scheduler;
    AtomicInteger counter = new AtomicInteger(0);
    static final String KS1 = "TestKeyspace";
    static final String KS2 = "DevKeyspace";
    static final String KS3 = "ProdKeyspace";
    
    Map<Integer, Integer> testValues = new HashMap<Integer, Integer>();

    @Before
    public void setUp()
    {
        RequestSchedulerOptions options = new RequestSchedulerOptions();
        options.throttle_limit = 5;
        scheduler = new RoundRobinScheduler(options);
        SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();

        executor = new ThreadPoolExecutor(20,
                                          Integer.MAX_VALUE,
                                          60*1000,
                                          TimeUnit.MILLISECONDS,
                                          queue);
        // When there are large no. of threads, the results become
        // more unpredictable because of the JVM thread scheduling
        // and that will be very hard to provide a consistent test
        runKs1(1, 10);
        runKs2(11, 13);
        runKs3(14, 15);

        try
        {
            Thread.sleep(3000);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testScheduling()
    {
        for (Integer initialValue : testValues.keySet())
        {
            // Makes sure, requests to each keyspace get an equal chance
            // Requests from one keyspace will not block requests from
            // another keyspacce
            if (initialValue > 10)
            {
                assertTrue(initialValue >= testValues.get(initialValue));
            }
        }
    }

    @After
    public void shutDown()
    {
        executor.shutdown();
    }

    private void runKs1(int start, int end)
    {
        for (int i=start; i<=end; i++)
        {
            executor.execute(new Worker(KS1, i));
        }
    }

    private void runKs2(int start, int end)
    {
        for (int i=start; i<=end; i++)
        {
            executor.execute(new Worker(KS2, i)); 
        }
    }

    private void runKs3(int start, int end)
    {
        for (int i=start; i<=end; i++)
        {
            executor.execute(new Worker(KS3, i)); 
        }
    }

    class Worker implements Runnable
    {
        String id;
        int initialCount;
        int runCount;

        public Worker(String id, int count)
        {
            this.id = id;
            initialCount = count;
        }

        public void run()
        {
            scheduler.queue(Thread.currentThread(), id);

            runCount = counter.incrementAndGet();

            synchronized(scheduler)
            {
                testValues.put(initialCount, runCount);
            }
            scheduler.release();
        }
    }
}
