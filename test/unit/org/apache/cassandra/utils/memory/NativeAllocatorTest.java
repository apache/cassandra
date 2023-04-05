/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.memory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class NativeAllocatorTest
{
    private ScheduledExecutorService exec;
    private OpOrder order;
    private OpOrder.Group group;
    private CountDownLatch canClean;
    private CountDownLatch isClean;
    private AtomicReference<NativeAllocator> allocatorRef;
    private AtomicReference<OpOrder.Barrier> barrier;
    private NativePool pool;
    private NativeAllocator allocator;
    private Runnable markBlocking;

    @Before
    public void setUp()
    {
        exec = Executors.newScheduledThreadPool(2);
        order = new OpOrder();
        group = order.start();
        canClean = new CountDownLatch(1);
        isClean = new CountDownLatch(1);
        allocatorRef = new AtomicReference<>();
        barrier = new AtomicReference<>();
        pool = new NativePool(1, 100, 0.75f, () -> {
            try
            {
                canClean.await();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
            if (isClean.getCount() > 0)
            {
                allocatorRef.get().offHeap().released(80);
                isClean.countDown();
            }
            return ImmediateFuture.success(true);
        });
        allocator = new NativeAllocator(pool);
        allocatorRef.set(allocator);
        markBlocking = () -> {
            barrier.set(order.newBarrier());
            barrier.get().issue();
            barrier.get().markBlocking();
        };
    }

    private void verifyUsedReclaiming(long used, long reclaiming)
    {
        Assert.assertEquals(used, allocator.offHeap().owns());
        Assert.assertEquals(used, pool.offHeap.used());
        Assert.assertEquals(reclaiming, allocator.offHeap().getReclaiming());
        Assert.assertEquals(reclaiming, pool.offHeap.getReclaiming());
    }

    @Test
    public void testBookKeeping() throws ExecutionException, InterruptedException
    {
        final Runnable test = () -> {
            // allocate normal, check accounted and not cleaned
            allocator.allocate(10, group);
            verifyUsedReclaiming(10, 0);

            // confirm adjustment works
            allocator.offHeap().adjust(-10, group);
            verifyUsedReclaiming(0, 0);

            allocator.offHeap().adjust(10, group);
            verifyUsedReclaiming(10, 0);

            // confirm we cannot allocate negative
            boolean success = false;
            try
            {
                allocator.offHeap().allocate(-10, group);
            }
            catch (AssertionError e)
            {
                success = true;
            }

            Assert.assertTrue(success);
            Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
            Assert.assertEquals(1, isClean.getCount());

            // allocate above watermark
            allocator.allocate(70, group);
            verifyUsedReclaiming(80, 0);

            // let the cleaner run, it will release 80 bytes
            canClean.countDown();
            try
            {
                Assert.assertTrue(isClean.await(10L, TimeUnit.SECONDS));
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
            Assert.assertEquals(0, isClean.getCount());
            verifyUsedReclaiming(0, 0);

            // allocate, then set discarding, then allocated some more
            allocator.allocate(30, group);
            verifyUsedReclaiming(30, 0);
            allocator.setDiscarding();
            Assert.assertFalse(allocator.isLive());
            verifyUsedReclaiming(30, 30);
            allocator.allocate(50, group);
            verifyUsedReclaiming(80, 80);

            // allocate above limit, check we block until "marked blocking"
            exec.schedule(markBlocking, 10L, TimeUnit.MILLISECONDS);
            allocator.allocate(30, group);
            Assert.assertNotNull(barrier.get());
            verifyUsedReclaiming(110, 110);

            // release everything
            allocator.setDiscarded();
            Assert.assertFalse(allocator.isLive());
            verifyUsedReclaiming(0, 0);
        };
        exec.submit(test).get();
    }
}


