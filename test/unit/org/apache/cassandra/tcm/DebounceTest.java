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

package org.apache.cassandra.tcm;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public class DebounceTest
{
    @Test
    public void testDebounce() throws Throwable
    {
        int threads = 20;
        WaitQueue waitQueue = WaitQueue.newWaitQueue();
        ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("debounce-test", threads);

        for (int i = 0; i < 100; i++)
        {
            CountDownLatch latch = CountDownLatch.newCountDownLatch(threads);

            AtomicInteger integer = new AtomicInteger();
            RemoteProcessor.Debounce<Integer> debounce = new RemoteProcessor.Debounce<>(() -> {
                integer.incrementAndGet();
                return integer.get();
            });

            List<Future<Integer>> futures = new CopyOnWriteArrayList<>();
            for (int j = 0; j < threads; j++)
            {
                executor.submit(() -> {
                    latch.decrement();
                    waitQueue.register().awaitUninterruptibly();
                    futures.add(debounce.getAsync());
                });
            }

            latch.awaitUninterruptibly();
            waitQueue.signalAll();

            while (futures.size() < threads)
                Thread.sleep(10);

            for (Future<Integer> future : futures)
                Assert.assertEquals(1, (int) future.get());
        }
    }
}
