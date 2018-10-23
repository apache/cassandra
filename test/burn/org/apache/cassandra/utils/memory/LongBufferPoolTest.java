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

/**
 * Long BufferPool test - make sure that the BufferPool allocates and recycles
 * ByteBuffers under heavy concurrent usage.
 *
 * The test creates two groups of threads
 *
 * - the burn producer/consumer pair that allocates 1/10 poolSize and then returns
 *   all the memory to the pool. 50% is freed by the producer, 50% passed to the consumer thread.
 *
 * - a ring of worker threads that allocate buffers and either immediately free them,
 *   or pass to the next worker thread for it to be freed on it's behalf.  Periodically
 *   all memory is freed by the thread.
 *
 * While the burn/worker threads run, the original main thread checks that all of the threads are still
 * making progress every 10s (no locking issues, or exits from assertion failures),
 * and that every chunk has been freed at least once during the previous cycle (if that was possible).
 *
 * The test does not expect to survive out-of-memory errors, so needs sufficient heap memory
 * for non-direct buffers and the debug tracking objects that check the allocate buffers.
 * (The timing is very interesting when Xmx is lowered to increase garbage collection pauses, but do
 * not set it too low).
 */
public class LongBufferPoolTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBufferPoolTest.class);

    private static final int AVG_BUFFER_SIZE = 16 << 10;
    private static final int STDEV_BUFFER_SIZE = 10 << 10; // picked to ensure exceeding buffer size is rare, but occurs
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

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

    private static final class TestEnvironment
    {
        final int threadCount;
        final long duration;
        final int poolSize;
        final long until;
        final CountDownLatch latch;
        final SPSCQueue<BufferCheck>[] sharedRecycle;
        final AtomicBoolean[] makingProgress;
        final AtomicBoolean burnFreed;
        final AtomicBoolean[] freedAllMemory;
        final ExecutorService executorService;
        final List<Future<Boolean>> threadResultFuture;
        final int targetSizeQuanta;

        TestEnvironment(int threadCount, long duration, int poolSize)
        {
            this.threadCount = threadCount;
            this.duration = duration;
            this.poolSize = poolSize;
            until = System.nanoTime() + duration;
            latch = new CountDownLatch(threadCount);
            sharedRecycle = new SPSCQueue[threadCount];
            makingProgress = new AtomicBoolean[threadCount];
            burnFreed = new AtomicBoolean(false);
            freedAllMemory = new AtomicBoolean[threadCount];
            executorService = Executors.newFixedThreadPool(threadCount + 2);
            threadResultFuture = new ArrayList<>(threadCount);

            for (int i = 0; i < sharedRecycle.length; i++)
            {
                sharedRecycle[i] = new SPSCQueue<>();
                makingProgress[i] = new AtomicBoolean(false);
                freedAllMemory[i] = new AtomicBoolean(false);
            }

            // Divide the poolSize across our threads, deliberately over-subscribing it.  Threads
            // allocate a different amount of memory each - 1*quanta, 2*quanta, ... N*quanta.
            // Thread0 is always going to be a single CHUNK, then to allocate increasing amounts
            // using their own algorithm the targetSize should be poolSize / targetSizeQuanta.
            //
            // This should divide double the poolSize across the working threads,
            // plus CHUNK_SIZE for thread0 and 1/10 poolSize for the burn producer/consumer pair.
            targetSizeQuanta = 2 * poolSize / sum1toN(threadCount - 1);
        }

        void addCheckedFuture(Future<Boolean> future)
        {
            threadResultFuture.add(future);
        }

        int countStalledThreads()
        {
            int stalledThreads = 0;

            for (AtomicBoolean progress : makingProgress)
            {
                if (!progress.getAndSet(false))
                    stalledThreads++;
            }
            return stalledThreads;
        }

        int countDoneThreads()
        {
            int doneThreads = 0;
            for (Future<Boolean> r : threadResultFuture)
            {
                if (r.isDone())
                    doneThreads++;
            }
            return doneThreads;
        }

        void assertCheckedThreadsSucceeded()
        {
            try
            {
                for (Future<Boolean> r : threadResultFuture)
                    assertTrue(r.get());
            }
            catch (InterruptedException ex)
            {
                // If interrupted while checking, restart and check everything.
                assertCheckedThreadsSucceeded();
            }
            catch (ExecutionException ex)
            {
                fail("Checked thread threw exception: " + ex.toString());
            }
        }
    }

    public void testAllocate(int threadCount, long duration, int poolSize) throws InterruptedException, ExecutionException
    {
        System.out.println(String.format("%s - testing %d threads for %dm",
                                         DATE_FORMAT.format(new Date()),
                                         threadCount,
                                         TimeUnit.NANOSECONDS.toMinutes(duration)));
        long prevPoolSize = BufferPool.MEMORY_USAGE_THRESHOLD;
        logger.info("Overriding configured BufferPool.MEMORY_USAGE_THRESHOLD={} and enabling BufferPool.DEBUG", poolSize);
        BufferPool.MEMORY_USAGE_THRESHOLD = poolSize;
        BufferPool.DEBUG = true;

        TestEnvironment testEnv = new TestEnvironment(threadCount, duration, poolSize);

        startBurnerThreads(testEnv);

        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
            testEnv.addCheckedFuture(startWorkerThread(testEnv, threadIdx));

        while (!testEnv.latch.await(10L, TimeUnit.SECONDS))
        {
            int stalledThreads = testEnv.countStalledThreads();
            int doneThreads = testEnv.countDoneThreads();

            if (doneThreads == 0) // If any threads have completed, they will stop making progress/recycling buffers.
            {                     // Assertions failures on the threads will be caught below.
                assert stalledThreads == 0;
                boolean allFreed = testEnv.burnFreed.getAndSet(false);
                for (AtomicBoolean freedMemory : testEnv.freedAllMemory)
                    allFreed = allFreed && freedMemory.getAndSet(false);
                if (allFreed)
                    BufferPool.assertAllRecycled();
                else
                    logger.info("All threads did not free all memory in this time slot - skipping buffer recycle check");
            }
        }

        for (SPSCQueue<BufferCheck> queue : testEnv.sharedRecycle)
        {
            BufferCheck check;
            while ( null != (check = queue.poll()) )
            {
                check.validate();
                BufferPool.put(check.buffer);
            }
        }

        assertEquals(0, testEnv.executorService.shutdownNow().size());

        logger.info("Reverting BufferPool.MEMORY_USAGE_THRESHOLD={}", prevPoolSize);
        BufferPool.MEMORY_USAGE_THRESHOLD = prevPoolSize;
        BufferPool.DEBUG = false;

        testEnv.assertCheckedThreadsSucceeded();

        System.out.println(String.format("%s - finished.",
                                         DATE_FORMAT.format(new Date())));
    }

    private Future<Boolean> startWorkerThread(TestEnvironment testEnv, final int threadIdx)
    {
        return testEnv.executorService.submit(new TestUntil(testEnv.until)
        {
            final int targetSize = threadIdx == 0 ? BufferPool.CHUNK_SIZE : testEnv.targetSizeQuanta * threadIdx;
            final SPSCQueue<BufferCheck> shareFrom = testEnv.sharedRecycle[threadIdx];
            final DynamicList<BufferCheck> checks = new DynamicList<>((int) Math.max(1, targetSize / (1 << 10)));
            final SPSCQueue<BufferCheck> shareTo = testEnv.sharedRecycle[(threadIdx + 1) % testEnv.threadCount];
            final ThreadLocalRandom rand = ThreadLocalRandom.current();
            int totalSize = 0;
            int freeingSize = 0;
            int size = 0;

            void checkpoint()
            {
                if (!testEnv.makingProgress[threadIdx].get())
                    testEnv.makingProgress[threadIdx].set(true);
            }

            void testOne() throws Exception
            {

                long currentTargetSize = (rand.nextInt(testEnv.poolSize / 1024) == 0 || !testEnv.freedAllMemory[threadIdx].get()) ? 0 : targetSize;
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

                if (currentTargetSize == 0)
                    testEnv.freedAllMemory[threadIdx].compareAndSet(false, true);

                // allocate a new buffer
                size = (int) Math.max(1, AVG_BUFFER_SIZE + (STDEV_BUFFER_SIZE * rand.nextGaussian()));
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
                    while (totalSize < testEnv.poolSize)
                    {
                        size = (int) Math.max(1, AVG_BUFFER_SIZE + (STDEV_BUFFER_SIZE * rand.nextGaussian()));
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
                testEnv.latch.countDown();
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
        });
    }

    private void startBurnerThreads(TestEnvironment testEnv)
    {
        // setup some high churn allocate/deallocate, without any checking
        final SPSCQueue<ByteBuffer> burn = new SPSCQueue<>();
        final CountDownLatch doneAdd = new CountDownLatch(1);
        testEnv.addCheckedFuture(testEnv.executorService.submit(new TestUntil(testEnv.until)
        {
            int count = 0;
            final ThreadLocalRandom rand = ThreadLocalRandom.current();
            void testOne() throws Exception
            {
                if (count * BufferPool.CHUNK_SIZE >= testEnv.poolSize / 10)
                {
                    if (burn.exhausted)
                    {
                        count = 0;
                        testEnv.burnFreed.compareAndSet(false, true);
                    } else
                    {
                        Thread.yield();
                    }
                    return;
                }

                ByteBuffer buffer = BufferPool.tryGet(BufferPool.CHUNK_SIZE);
                if (buffer == null)
                {
                    Thread.yield();
                    return;
                }

                // 50/50 chance of returning the buffer from the producer thread, or
                // pass it on to the consumer.
                if (rand.nextBoolean())
                    BufferPool.put(buffer);
                else
                    burn.add(buffer);

                count++;
            }
            void cleanup()
            {
                doneAdd.countDown();
            }
        }));
        testEnv.threadResultFuture.add(testEnv.executorService.submit(new TestUntil(testEnv.until)
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
        }));
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
            catch (Throwable tr) // for java.lang.OutOfMemoryError
            {
                logger.error("Got throwable {}, current chunk {}",
                             tr.getMessage(),
                             BufferPool.currentChunk());
                tr.printStackTrace();
                return false;
            }
            finally
            {
                cleanup();
            }
            return true;
        }
    }

    public static void main(String[] args)
    {
        try
        {
            new LongBufferPoolTest().testAllocate(Runtime.getRuntime().availableProcessors(),
                                                  TimeUnit.HOURS.toNanos(2L), 16 << 20);
            System.exit(0);
        }
        catch (Throwable tr)
        {
            System.out.println(String.format("Test failed - %s", tr.getMessage()));
            System.exit(1); // Force exit so that non-daemon threads like REQUEST-SCHEDULER do not hang the process on failure
        }
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

    private static int sum1toN(int n)
    {
        return (n * (n + 1)) / 2;
    }
}
