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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.DAEMON;

public class InfiniteLoopExecutorTest
{
    @Test
    public void testShutdownNow() throws InterruptedException, ExecutionException, TimeoutException
    {
        Semaphore semaphore = new Semaphore(0);
        InfiniteLoopExecutor e1 = new InfiniteLoopExecutor("test", ignore -> semaphore.acquire(1), DAEMON);
        ExecutorService exec = Executors.newCachedThreadPool();
        Future<?> f = exec.submit(() -> e1.awaitTermination(1L, TimeUnit.MINUTES));
        e1.shutdownNow();
        f.get(1L, TimeUnit.SECONDS);
    }

    @Test
    public void testShutdown() throws InterruptedException, ExecutionException, TimeoutException
    {
        AtomicBoolean active = new AtomicBoolean(false);
        Semaphore semaphore = new Semaphore(0);
        InfiniteLoopExecutor e1 = new InfiniteLoopExecutor("test", ignore -> {
            active.set(true);
            semaphore.acquire(1);
            active.set(false);
            semaphore.release();
        }, DAEMON);
        ExecutorService exec = Executors.newCachedThreadPool();
        Future<?> f = exec.submit(() -> e1.awaitTermination(1L, TimeUnit.MINUTES));
        // do ten normal loops
        for (int i = 0 ; i < 10 ; ++i)
        {
            semaphore.release();
            semaphore.acquire();
        }
        // confirm we've re-entered the runnable
        while (!active.get()) Thread.yield();
        // then shutdown, and expect precisely one more
        e1.shutdown();
        try
        {
            f.get(10L, TimeUnit.MILLISECONDS);
            Assert.fail();
        }
        catch (TimeoutException ignore)
        {
        }
        semaphore.release();
        f.get(1L, TimeUnit.SECONDS);
    }
}
