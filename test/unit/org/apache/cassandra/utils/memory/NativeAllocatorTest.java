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
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class NativeAllocatorTest
{

    @Test
    public void testBookKeeping() throws ExecutionException, InterruptedException
    {
        {
            final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
            final OpOrder order = new OpOrder();
            final OpOrder.Group group = order.start();
            final CountDownLatch canClean = new CountDownLatch(1);
            final CountDownLatch isClean = new CountDownLatch(1);
            final AtomicReference<NativeAllocator> allocatorRef = new AtomicReference<>();
            final AtomicReference<OpOrder.Barrier> barrier = new AtomicReference<>();
            final NativeAllocator allocator = new NativeAllocator(new NativePool(1, 100, 0.75f, new Runnable()
            {
                public void run()
                {
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
                }
            }));
            allocatorRef.set(allocator);
            final Runnable markBlocking = new Runnable()
            {

                public void run()
                {
                    barrier.set(order.newBarrier());
                    barrier.get().issue();
                    barrier.get().markBlocking();
                }
            };
            final Runnable run = new Runnable()
            {
                public void run()
                {
                    // allocate normal, check accounted and not cleaned
                    allocator.allocate(10, group);
                    Assert.assertEquals(10, allocator.offHeap().owns());
                    // confirm adjustment works
                    allocator.offHeap().adjust(-10, group);
                    Assert.assertEquals(0, allocator.offHeap().owns());
                    allocator.offHeap().adjust(10, group);
                    Assert.assertEquals(10, allocator.offHeap().owns());
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

                    // allocate above watermark, check cleaned
                    allocator.allocate(70, group);
                    Assert.assertEquals(80, allocator.offHeap().owns());
                    canClean.countDown();
                    try
                    {
                        isClean.await(10L, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError();
                    }
                    Assert.assertEquals(0, isClean.getCount());
                    Assert.assertEquals(0, allocator.offHeap().owns());

                    // allocate above limit, check we block until "marked blocking"
                    exec.schedule(markBlocking, 10L, TimeUnit.MILLISECONDS);
                    allocator.allocate(110, group);
                    Assert.assertNotNull(barrier.get());
                    Assert.assertEquals(110, allocator.offHeap().owns());
                }
            };
            exec.submit(run).get();
        }
    }

}
