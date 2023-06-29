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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

public class DebounceTest
{
    @Test
    public void testDebounce() throws Throwable
    {
        int threads = 20;
        WaitQueue threadsCanCompeteForFutures = WaitQueue.newWaitQueue();
        ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("debounce-test", threads);

        for (int i = 0; i < 1000; i++)
        {
            CountDownLatch allThreadsStarted = CountDownLatch.newCountDownLatch(threads);
            CountDownLatch allThreadsGrabbedFuture = CountDownLatch.newCountDownLatch(threads);

            AtomicInteger integer = new AtomicInteger();
            RemoteProcessor.Debounce<Integer> debounce = new RemoteProcessor.Debounce<>(() -> {
                integer.incrementAndGet();
                allThreadsGrabbedFuture.awaitUninterruptibly();
                return integer.get();
            });

            List<Future<Integer>> futures = new CopyOnWriteArrayList<>();
            for (int j = 0; j < threads; j++)
            {
                executor.submit(() -> {
                    allThreadsStarted.decrement();
                    threadsCanCompeteForFutures.register().awaitUninterruptibly();
                    futures.add(debounce.getAsync());
                    allThreadsGrabbedFuture.decrement();
                });
            }

            allThreadsStarted.awaitUninterruptibly();
            threadsCanCompeteForFutures.signalAll();

            while (futures.size() < threads)
                Thread.sleep(10);

            for (Future<Integer> future : futures)
                assertEquals(1, (int) future.get());
        }
    }

    @Test
    public void testEpochAwareDebounce() throws ExecutionException, InterruptedException
    {
        // todo: confusing test, run this through simulator
        PeerLogFetcher.EpochAwareDebounce<Integer> debounce = new PeerLogFetcher.EpochAwareDebounce<>();
        CountDownLatch returnFirst = CountDownLatch.newCountDownLatch(1);
        Epoch epoch = Epoch.FIRST;
        Future<Integer> first = debounce.getAsync(() -> {
            returnFirst.await();
            return 1;
        }, epoch);


        int threads = 20;
        List<Future<Integer>> futures = new ArrayList<>(threads);
        CountDownLatch submitDebounce = CountDownLatch.newCountDownLatch(1);
        CountDownLatch returnSecond = CountDownLatch.newCountDownLatch(1);
        CountDownLatch threadsStarted = CountDownLatch.newCountDownLatch(threads);
        AtomicInteger calls = new AtomicInteger();
        ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("debounce-test", threads);
        for (int i = 0; i < threads; i++)
        {
            executor.submit(() -> {
                threadsStarted.decrement();
                submitDebounce.awaitUninterruptibly();
                futures.add(debounce.getAsync(() -> {
                    calls.incrementAndGet();
                    returnSecond.await();
                    return 2;
                }, epoch.nextEpoch()));
            });
        }
        // now we have one thread waiting on epoch 1 `first`, and we submit 20 threads waiting for epoch 2;
        threadsStarted.awaitUninterruptibly();
        submitDebounce.decrement();;
        while (calls.get() < 1)
            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        returnSecond.decrement();

        for (Future<Integer> f : futures)
            assertEquals(2, (int)f.get());
        assertEquals(1, calls.get());
        returnFirst.decrement();
        assertEquals(1, (int)first.get());
    }
}
