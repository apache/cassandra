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

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

public class LwtKeyPrioritizingTaskQueueTest
{
    private LwtKeyPrioritizingTaskQueue queue;

    @Before
    public void setup()
    {
        queue = new LwtKeyPrioritizingTaskQueue();
    }

    static class FakeTask implements Runnable, PrioritizableTask
    {
        final String name;
        final boolean isLWT;
        final ByteBuffer partitionKey;

        FakeTask(String name, boolean isLWT, String key)
        {
            this.name = name;
            this.isLWT = isLWT;
            this.partitionKey = key != null ? ByteBufferUtil.bytes(key) : null;
        }

        @Override public void run() {}
        @Override public boolean isLWT() { return isLWT; }
        @Override public ByteBuffer partitionKey() { return partitionKey; }
        @Override public long creationTimeNanos() { return 0; }
        @Override public long startTimeNanos() { return 0; }
        @Override public String description() { return name; }
        @Override public String toString() { return name; }
    }

    @Test
    public void testNonLwtTasksPreserveFifo()
    {
        FakeTask r1 = new FakeTask("read1", false, "k1");
        FakeTask w1 = new FakeTask("write1", false, "k1");
        FakeTask unk = new FakeTask("unprepared", false, null);

        queue.add(r1);
        queue.add(w1);
        queue.add(unk);

        Assert.assertSame(r1, queue.poll());
        Assert.assertSame(w1, queue.poll());
        Assert.assertSame(unk, queue.poll());
        Assert.assertNull(queue.poll());
    }

    @Test
    public void testSameKeyLwtContentionBypass()
    {
        ByteBuffer keyA = ByteBufferUtil.bytes("keyA");
        FakeTask lwtA1 = new FakeTask("lwtA1", true, "keyA");
        FakeTask lwtA2 = new FakeTask("lwtA2", true, "keyA");
        FakeTask readB = new FakeTask("readB", false, "keyB");

        queue.add(lwtA1);
        queue.add(lwtA2);
        queue.add(readB);

        Runnable polled1 = queue.poll();
        Assert.assertSame(lwtA1, polled1);
        Runnable polled2 = queue.poll();
        Assert.assertSame(readB, polled2);
        Assert.assertNull(queue.poll());
        queue.onTaskCompleted(lwtA1);
        Runnable polled3 = queue.poll();
        Assert.assertSame(lwtA2, polled3);

        queue.onTaskCompleted(lwtA2);
        Assert.assertNull(queue.poll());
    }

    @Test
    public void testIndependentLwtKeysRunConcurrently()
    {
        FakeTask lwtA = new FakeTask("lwtA", true, "keyA");
        FakeTask lwtB = new FakeTask("lwtB", true, "keyB");
        FakeTask lwtC = new FakeTask("lwtC", true, "keyC");

        queue.add(lwtA);
        queue.add(lwtB);
        queue.add(lwtC);

        Assert.assertSame(lwtA, queue.poll());
        Assert.assertSame(lwtB, queue.poll());
        Assert.assertSame(lwtC, queue.poll());
        Assert.assertNull(queue.poll());

        queue.onTaskCompleted(lwtA);
        queue.onTaskCompleted(lwtB);
        queue.onTaskCompleted(lwtC);
    }

    @Test
    public void testByteBufferPositionMutationSafety()
    {
        FakeTask lwt = new FakeTask("lwt", true, "keyA");
        queue.add(lwt);

        Runnable polled = queue.poll();
        Assert.assertSame(lwt, polled);
        lwt.partitionKey.position(lwt.partitionKey.limit());
        queue.onTaskCompleted(lwt);
        FakeTask lwt2 = new FakeTask("lwt2", true, "keyA");
        queue.add(lwt2);
        Assert.assertSame(lwt2, queue.poll());
        queue.onTaskCompleted(lwt2);
    }

    @Test
    public void testConcurrentExecutionUnderPressure() throws InterruptedException
    {
        int numKeys = 5;
        int tasksPerKey = 20;
        int totalTasks = numKeys * tasksPerKey;

        for (int i = 0; i < totalTasks; i++)
        {
            String key = "key_" + (i % numKeys);
            queue.add(new FakeTask("task_" + i, true, key));
        }

        int numWorkers = 8;
        ExecutorService workers = Executors.newFixedThreadPool(numWorkers);
        CountDownLatch latch = new CountDownLatch(totalTasks);
        AtomicInteger processed = new AtomicInteger(0);

        for (int w = 0; w < numWorkers; w++)
        {
            workers.submit(() -> {
                while (processed.get() < totalTasks)
                {
                    Runnable t = queue.poll();
                    if (t != null)
                    {
                        try
                        {
                            Thread.sleep(1);
                        }
                        catch (InterruptedException ignored) {}
                        finally
                        {
                            queue.onTaskCompleted(t);
                            processed.incrementAndGet();
                            latch.countDown();
                        }
                    }
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        workers.shutdownNow();

        Assert.assertTrue("All tasks should complete without deadlock", completed);
        Assert.assertEquals(totalTasks, processed.get());
        Assert.assertTrue(queue.isEmpty());
    }
}