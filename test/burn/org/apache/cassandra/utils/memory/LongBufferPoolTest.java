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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.DynamicList;

import static org.junit.Assert.*;

public class LongBufferPoolTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBufferPoolTest.class);

    @Test
    public void testAllocate() throws InterruptedException, ExecutionException
    {
        testAllocate(Runtime.getRuntime().availableProcessors() * 2, TimeUnit.MINUTES.toNanos(2L), 16 << 20);
    }

    private static final class BufferCheck
    {
        final ByteBuffer buffer;
        final long val;
        DynamicList.Node<BufferCheck> listnode;

        private BufferCheck(ByteBuffer buffer, long val)
        {
            this.buffer = buffer;
            this.val = val;
        }

        void validate()
        {
            ByteBuffer read = buffer.duplicate();
            while (read.remaining() > 8)
                assert read.getLong() == val;
        }

        void init()
        {
            ByteBuffer write = buffer.duplicate();
            while (write.remaining() > 8)
                write.putLong(val);
        }
    }

    public void testAllocate(int threadCount, long duration, int poolSize) throws InterruptedException, ExecutionException
    {
        final int avgBufferSize = 16 << 10;
        final int stdevBufferSize = 10 << 10; // picked to ensure exceeding buffer size is rare, but occurs
        final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        System.out.println(String.format("%s - testing %d threads for %dm",
                                         dateFormat.format(new Date()),
                                         threadCount,
                                         TimeUnit.NANOSECONDS.toMinutes(duration)));

        final long until = System.nanoTime() + duration;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final SPSCQueue<BufferCheck>[] sharedRecycle = new SPSCQueue[threadCount];
        final AtomicBoolean[] makingProgress = new AtomicBoolean[threadCount];
        for (int i = 0 ; i < sharedRecycle.length ; i++)
        {
            sharedRecycle[i] = new SPSCQueue<>();
            makingProgress[i] = new AtomicBoolean(true);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount + 2);
        List<Future<Boolean>> ret = new ArrayList<>(threadCount);
        long prevPoolSize = BufferPool.MEMORY_USAGE_THRESHOLD;
        BufferPool.MEMORY_USAGE_THRESHOLD = poolSize;
        BufferPool.DEBUG = true;
        // sum(1..n) = n/2 * (n + 1); we set zero to CHUNK_SIZE, so have n=threadCount-1
        int targetSizeQuanta = ((threadCount) * (threadCount - 1)) / 2;
        // fix targetSizeQuanta at 1/64th our poolSize, so that we only consciously exceed our pool size limit
        targetSizeQuanta = (targetSizeQuanta * poolSize) / 64;

        {
            // setup some high churn allocate/deallocate, without any checking
            final SPSCQueue<ByteBuffer> burn = new SPSCQueue<>();
            final CountDownLatch doneAdd = new CountDownLatch(1);
            executorService.submit(new TestUntil(until)
            {
                int count = 0;
                void testOne() throws Exception
                {
                    if (count * BufferPool.CHUNK_SIZE >= poolSize / 10)
                    {
                        if (burn.exhausted)
                            count = 0;
                        else
                            Thread.yield();
                        return;
                    }

                    ByteBuffer buffer = BufferPool.tryGet(BufferPool.CHUNK_SIZE);
                    if (buffer == null)
                    {
                        Thread.yield();
                        return;
                    }

                    BufferPool.put(buffer);
                    burn.add(buffer);
                    count++;
                }
                void cleanup()
                {
                    doneAdd.countDown();
                }
            });
            executorService.submit(new TestUntil(until)
            {
                void testOne() throws Exception
                {
                    ByteBuffer buffer = burn.poll();
                    if (buffer == null)
                    {
                        Thread.yield();
                        return;
                    }
                    BufferPool.put(buffer);
                }
                void cleanup()
                {
                    Uninterruptibles.awaitUninterruptibly(doneAdd);
                }
            });
        }

        for (int t = 0; t < threadCount; t++)
        {
            final int threadIdx = t;
            final int targetSize = t == 0 ? BufferPool.CHUNK_SIZE : targetSizeQuanta * t;

            ret.add(executorService.submit(new TestUntil(until)
            {
                final SPSCQueue<BufferCheck> shareFrom = sharedRecycle[threadIdx];
                final DynamicList<BufferCheck> checks = new DynamicList<>((int) Math.max(1, targetSize / (1 << 10)));
                final SPSCQueue<BufferCheck> shareTo = sharedRecycle[(threadIdx + 1) % threadCount];
                final ThreadLocalRandom rand = ThreadLocalRandom.current();
                int totalSize = 0;
                int freeingSize = 0;
                int size = 0;

                void checkpoint()
                {
                    if (!makingProgress[threadIdx].get())
                        makingProgress[threadIdx].set(true);
                }

                void testOne() throws Exception
                {

                    long currentTargetSize = rand.nextInt(poolSize / 1024) == 0 ? 0 : targetSize;
                    int spinCount = 0;
                    while (totalSize > currentTargetSize - freeingSize)
                    {
                        // free buffers until we're below our target size
                        if (checks.size() == 0)
                        {
                            // if we're out of buffers to free, we're waiting on our neighbour to free them;
                            // first check if the consuming neighbour has caught up, and if so mark that free
                            if (shareTo.exhausted)
                            {
                                totalSize -= freeingSize;
                                freeingSize = 0;
                            }
                            else if (!recycleFromNeighbour())
                            {
                                if (++spinCount > 1000 && System.nanoTime() > until)
                                    return;
                                // otherwise, free one of our other neighbour's buffers if can; and otherwise yield
                                Thread.yield();
                            }
                            continue;
                        }

                        // pick a random buffer, with preference going to earlier ones
                        BufferCheck check = sample();
                        checks.remove(check.listnode);
                        check.validate();

                        size = BufferPool.roundUpNormal(check.buffer.capacity());
                        if (size > BufferPool.CHUNK_SIZE)
                            size = 0;

                        // either share to free, or free immediately
                        if (rand.nextBoolean())
                        {
                            shareTo.add(check);
                            freeingSize += size;
                            // interleave this with potentially messing with the other neighbour's stuff
                            recycleFromNeighbour();
                        }
                        else
                        {
                            check.validate();
                            BufferPool.put(check.buffer);
                            totalSize -= size;
                        }
                    }

                    // allocate a new buffer
                    size = (int) Math.max(1, avgBufferSize + (stdevBufferSize * rand.nextGaussian()));
                    if (size <= BufferPool.CHUNK_SIZE)
                    {
                        totalSize += BufferPool.roundUpNormal(size);
                        allocate(size);
                    }
                    else if (rand.nextBoolean())
                    {
                        allocate(size);
                    }
                    else
                    {
                        // perform a burst allocation to exhaust all available memory
                        while (totalSize < poolSize)
                        {
                            size = (int) Math.max(1, avgBufferSize + (stdevBufferSize * rand.nextGaussian()));
                            if (size <= BufferPool.CHUNK_SIZE)
                            {
                                allocate(size);
                                totalSize += BufferPool.roundUpNormal(size);
                            }
                        }
                    }

                    // validate a random buffer we have stashed
                    checks.get(rand.nextInt(checks.size())).validate();

                    // free all of our neighbour's remaining shared buffers
                    while (recycleFromNeighbour());
                }

                void cleanup()
                {
                    while (checks.size() > 0)
                    {
                        BufferCheck check = checks.get(0);
                        BufferPool.put(check.buffer);
                        checks.remove(check.listnode);
                    }
                    latch.countDown();
                }

                boolean recycleFromNeighbour()
                {
                    BufferCheck check = shareFrom.poll();
                    if (check == null)
                        return false;
                    check.validate();
                    BufferPool.put(check.buffer);
                    return true;
                }

                BufferCheck allocate(int size)
                {
                    ByteBuffer buffer = BufferPool.get(size);
                    assertNotNull(buffer);
                    BufferCheck check = new BufferCheck(buffer, rand.nextLong());
                    assertEquals(size, buffer.capacity());
                    assertEquals(0, buffer.position());
                    check.init();
                    check.listnode = checks.append(check);
                    return check;
                }

                BufferCheck sample()
                {
                    // sample with preference to first elements:
                    // element at index n will be selected with likelihood (size - n) / sum1ToN(size)
                    int size = checks.size();

                    // pick a random number between 1 and sum1toN(size)
                    int sampleRange = sum1toN(size);
                    int sampleIndex = rand.nextInt(sampleRange);

                    // then binary search for the N, such that [sum1ToN(N), sum1ToN(N+1)) contains this random number
                    int moveBy = Math.max(size / 4, 1);
                    int index = size / 2;
                    while (true)
                    {
                        int baseSampleIndex = sum1toN(index);
                        int endOfSampleIndex = sum1toN(index + 1);
                        if (sampleIndex >= baseSampleIndex)
                        {
                            if (sampleIndex < endOfSampleIndex)
                                break;
                            index += moveBy;
                        }
                        else index -= moveBy;
                        moveBy = Math.max(moveBy / 2, 1);
                    }

                    // this gives us the inverse of our desired value, so just subtract it from the last index
                    index = size - (index + 1);

                    return checks.get(index);
                }

                private int sum1toN(int n)
                {
                    return (n * (n + 1)) / 2;
                }
            }));
        }

        boolean first = true;
        while (!latch.await(10L, TimeUnit.SECONDS))
        {
            if (!first)
                BufferPool.assertAllRecycled();
            first = false;
            for (AtomicBoolean progress : makingProgress)
            {
                assert progress.get();
                progress.set(false);
            }
        }

        for (SPSCQueue<BufferCheck> queue : sharedRecycle)
        {
            BufferCheck check;
            while ( null != (check = queue.poll()) )
            {
                check.validate();
                BufferPool.put(check.buffer);
            }
        }

        assertEquals(0, executorService.shutdownNow().size());

        BufferPool.MEMORY_USAGE_THRESHOLD = prevPoolSize;
        for (Future<Boolean> r : ret)
            assertTrue(r.get());

        System.out.println(String.format("%s - finished.",
                                         dateFormat.format(new Date())));
    }

    static abstract class TestUntil implements Callable<Boolean>
    {
        final long until;
        protected TestUntil(long until)
        {
            this.until = until;
        }

        abstract void testOne() throws Exception;
        void checkpoint() {}
        void cleanup() {}

        public Boolean call() throws Exception
        {
            try
            {
                while (System.nanoTime() < until)
                {
                    checkpoint();
                    for (int i = 0 ; i < 100 ; i++)
                        testOne();
                }
            }
            catch (Exception ex)
            {
                logger.error("Got exception {}, current chunk {}",
                             ex.getMessage(),
                             BufferPool.currentChunk());
                ex.printStackTrace();
                return false;
            }
            finally
            {
                cleanup();
            }
            return true;
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        new LongBufferPoolTest().testAllocate(Runtime.getRuntime().availableProcessors(), TimeUnit.HOURS.toNanos(2L), 16 << 20);
    }

    /**
     * A single producer, single consumer queue.
     */
    private static final class SPSCQueue<V>
    {
        static final class Node<V>
        {
            volatile Node<V> next;
            final V value;
            Node(V value)
            {
                this.value = value;
            }
        }

        private volatile boolean exhausted = true;
        Node<V> head = new Node<>(null);
        Node<V> tail = head;

        void add(V value)
        {
            exhausted = false;
            tail = tail.next = new Node<>(value);
        }

        V poll()
        {
            Node<V> next = head.next;
            if (next == null)
            {
                // this is racey, but good enough for our purposes
                exhausted = true;
                return null;
            }
            head = next;
            return next.value;
        }
    }

}