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

package org.apache.cassandra.concurrent;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;

public class DebuggableScheduledThreadPoolExecutorTest
{

    static EmbeddedCassandraService service;

    @BeforeClass
    public static void startup() throws IOException
    {
        //The DSTPE checks for if we are in the service shutdown hook so
        //to test it we need to start C* internally.
        service = new EmbeddedCassandraService();
        service.start();
    }

    @Test
    public void testShutdown() throws ExecutionException, InterruptedException, IOException
    {
        DebuggableScheduledThreadPoolExecutor testPool = new DebuggableScheduledThreadPoolExecutor("testpool");

        final AtomicInteger value = new AtomicInteger(0);

        //Normal scheduled task
        ScheduledFuture future = testPool.schedule(new Runnable()
        {
            public void run()
            {
                value.incrementAndGet();
            }
        }, 1, TimeUnit.SECONDS);

        future.get();
        assert value.get() == 1;


        //Shut down before schedule
        future = testPool.schedule(new Runnable()
        {
            public void run()
            {
                value.incrementAndGet();
            }
        }, 10, TimeUnit.SECONDS);


        StorageService.instance.drain();
        testPool.shutdown();

        future.get();
        assert value.get() == 2;


        //Now shut down verify task isn't just swallowed
        future = testPool.schedule(new Runnable()
        {
            public void run()
            {
                value.incrementAndGet();
            }
        }, 1, TimeUnit.SECONDS);


        try
        {
            future.get(2, TimeUnit.SECONDS);
            Assert.fail("Task should be cancelled");
        }
        catch (CancellationException e)
        {

        }
        catch (TimeoutException e)
        {
            Assert.fail("Task should be cancelled");
        }

        assert future.isCancelled();
        assert value.get() == 2;
    }
}
