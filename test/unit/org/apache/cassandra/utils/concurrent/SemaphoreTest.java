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

package org.apache.cassandra.utils.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class SemaphoreTest
{

    @Test
    public void testUnfair() throws InterruptedException
    {
        Semaphore s = Semaphore.newSemaphore(2);
        List<Future<Boolean>> fs = start(s);
        s.release(1);
        while (s.permits() == 1) Thread.yield();
        Assert.assertEquals(1, fs.stream().filter(Future::isDone).count());
        s.release(1);
        while (s.permits() == 1) Thread.yield();
        Assert.assertEquals(2, fs.stream().filter(Future::isDone).count());
        s.release(1);
        while (s.permits() == 1) Thread.yield();
        Assert.assertEquals(3, fs.stream().filter(Future::isDone).count());
        s.release(1);
        Assert.assertEquals(1, s.permits());
    }

    @Test
    public void testFair() throws InterruptedException, ExecutionException, TimeoutException
    {
        Semaphore s = Semaphore.newFairSemaphore(2);
        List<Future<Boolean>> fs = start(s);
        s.release(1);
        fs.get(0).get(1L, MINUTES);
        s.release(1);
        fs.get(1).get(1L, MINUTES);
        s.release(1);
        fs.get(2).get(1L, MINUTES);
        s.release(1);
        Assert.assertEquals(1, s.permits());
    }

    private List<java.util.concurrent.Future<Boolean>> start(Semaphore s) throws InterruptedException
    {
        ExecutorService exec = Executors.newCachedThreadPool();
        try
        {
            Assert.assertTrue(s.tryAcquire(1));
            s.drain();
            Assert.assertFalse(s.tryAcquire(1));
            Assert.assertFalse(s.tryAcquire(1, 1L, MILLISECONDS));
            Thread.currentThread().interrupt();
            try { s.acquireThrowUncheckedOnInterrupt(1); Assert.fail(); } catch (UncheckedInterruptedException ignore) { }
            Thread.currentThread().interrupt();
            try { s.tryAcquire(1, 1L, MILLISECONDS); Assert.fail(); } catch (InterruptedException ignore) { }
            Thread.currentThread().interrupt();
            try { s.tryAcquireUntil(1, nanoTime() + MILLISECONDS.toNanos(1L)); Assert.fail(); } catch (InterruptedException ignore) { }
            List<Future<Boolean>> fs = new ArrayList<>();
            fs.add(exec.submit(() -> s.tryAcquire(1, 1L, MINUTES)));
            while (s instanceof Semaphore.Standard && ((Semaphore.Standard) s).waiting() == 0) Thread.yield();
            fs.add(exec.submit(() -> s.tryAcquireUntil(1, System.nanoTime() + MINUTES.toNanos(1L))));
            while (s instanceof Semaphore.Standard && ((Semaphore.Standard) s).waiting() == 1) Thread.yield();
            fs.add(exec.submit(() -> { s.acquire(1); return true; } ));
            return fs;
        }
        finally
        {
            exec.shutdown();
        }
    }

}
