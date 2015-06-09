/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.*;

public class BufferPoolTest
{
    @Before
    public void setUp()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = 8 * 1024L * 1024L;
        BufferPool.DISABLED = false;
    }

    @After
    public void cleanUp()
    {
        BufferPool.reset();
    }

    @Test
    public void testGetPut() throws InterruptedException
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;

        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertEquals(true, buffer.isDirect());

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());

        BufferPool.put(buffer);
        assertEquals(null, BufferPool.currentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());
    }


    @Test
    public void testPageAligned()
    {
        final int size = 1024;
        for (int i = size;
                 i <= BufferPool.CHUNK_SIZE;
                 i += size)
        {
            checkPageAligned(i);
        }
    }

    private void checkPageAligned(int size)
    {
        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertTrue(buffer.isDirect());

        long address = MemoryUtil.getAddress(buffer);
        assertTrue((address % MemoryUtil.pageSize()) == 0);

        BufferPool.put(buffer);
    }

    @Test
    public void testDifferentSizes() throws InterruptedException
    {
        final int size1 = 1024;
        final int size2 = 2048;

        ByteBuffer buffer1 = BufferPool.get(size1);
        assertNotNull(buffer1);
        assertEquals(size1, buffer1.capacity());

        ByteBuffer buffer2 = BufferPool.get(size2);
        assertNotNull(buffer2);
        assertEquals(size2, buffer2.capacity());

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());

        BufferPool.put(buffer1);
        BufferPool.put(buffer2);

        assertEquals(null, BufferPool.currentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());
    }

    @Test
    public void testMaxMemoryExceededDirect()
    {
        boolean cur = BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED;
        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = false;

        requestDoubleMaxMemory();

        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = cur;
    }

    @Test
    public void testMaxMemoryExceededHeap()
    {
        boolean cur = BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED;
        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = true;

        requestDoubleMaxMemory();

        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = cur;
    }

    @Test
    public void testMaxMemoryExceeded_SameAsChunkSize()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = BufferPool.GlobalPool.MACRO_CHUNK_SIZE;
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceeded_SmallerThanChunkSize()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / 2;
        requestDoubleMaxMemory();
    }

    @Test
    public void testRecycle()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, 3 * BufferPool.CHUNK_SIZE);
    }

    private void requestDoubleMaxMemory()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, (int)(2 * BufferPool.MEMORY_USAGE_THRESHOLD));
    }

    private void requestUpToSize(int bufferSize, int totalSize)
    {
        final int numBuffers = totalSize / bufferSize;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
        {
            ByteBuffer buffer = BufferPool.get(bufferSize);
            assertNotNull(buffer);
            assertEquals(bufferSize, buffer.capacity());

            if (BufferPool.sizeInBytes() > BufferPool.MEMORY_USAGE_THRESHOLD)
                assertEquals(BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED, !buffer.isDirect());

            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            BufferPool.put(buffer);

    }

    @Test
    public void testBigRequest()
    {
        final int size = BufferPool.CHUNK_SIZE + 1;

        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        BufferPool.put(buffer);
    }

    @Test
    public void testFillUpChunks()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        final int numBuffers = BufferPool.CHUNK_SIZE / size;

        List<ByteBuffer> buffers1 = new ArrayList<>(numBuffers);
        List<ByteBuffer> buffers2 = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
            buffers1.add(BufferPool.get(size));

        BufferPool.Chunk chunk1 = BufferPool.currentChunk();
        assertNotNull(chunk1);

        for (int i = 0; i < numBuffers; i++)
            buffers2.add(BufferPool.get(size));

        assertEquals(2, BufferPool.numChunks());

        for (ByteBuffer buffer : buffers1)
            BufferPool.put(buffer);

        assertEquals(1, BufferPool.numChunks());

        for (ByteBuffer buffer : buffers2)
            BufferPool.put(buffer);

        assertEquals(0, BufferPool.numChunks());

        buffers2.clear();
    }

    @Test
    public void testOutOfOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testInOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = maxFreeSlots - 1 - i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testRandomFrees()
    {
        doTestRandomFrees(12345567878L);

        BufferPool.reset();
        doTestRandomFrees(20452249587L);

        BufferPool.reset();
        doTestRandomFrees(82457252948L);

        BufferPool.reset();
        doTestRandomFrees(98759284579L);

        BufferPool.reset();
        doTestRandomFrees(19475257244L);
    }

    private void doTestRandomFrees(long seed)
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = maxFreeSlots - 1 - i;

        Random rnd = new Random();
        rnd.setSeed(seed);
        for (int i = idxs.length - 1; i > 0; i--)
        {
            int idx = rnd.nextInt(i+1);
            int v = idxs[idx];
            idxs[idx] = idxs[i];
            idxs[i] = v;
        }

        doTestFrees(size, maxFreeSlots, idxs);
    }

    private void doTestFrees(final int size, final int maxFreeSlots, final int[] toReleaseIdxs)
    {
        List<ByteBuffer> buffers = new ArrayList<>(maxFreeSlots);
        for (int i = 0; i < maxFreeSlots; i++)
        {
            buffers.add(BufferPool.get(size));
        }

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertFalse(chunk.isFree());

        int freeSize = BufferPool.CHUNK_SIZE - maxFreeSlots * size;
        assertEquals(freeSize, chunk.free());

        for (int i : toReleaseIdxs)
        {
            ByteBuffer buffer = buffers.get(i);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());

            BufferPool.put(buffer);

            freeSize += size;
            if (freeSize == chunk.capacity())
                assertEquals(0, chunk.free());
            else
                assertEquals(freeSize, chunk.free());
        }

        assertFalse(chunk.isFree());
    }

    @Test
    public void testDifferentSizeBuffersOnOneChunk()
    {
        int[] sizes = new int[] {
            5, 1024, 4096, 8, 16000, 78, 512, 256, 63, 55, 89, 90, 255, 32, 2048, 128
        };

        int sum = 0;
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
        {
            ByteBuffer buffer = BufferPool.get(sizes[i]);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += BufferPool.currentChunk().roundUp(buffer.capacity());
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);

        Random rnd = new Random();
        rnd.setSeed(298347529L);
        while (!buffers.isEmpty())
        {
            int index = rnd.nextInt(buffers.size());
            ByteBuffer buffer = buffers.remove(index);

            BufferPool.put(buffer);
        }

        assertEquals(null, BufferPool.currentChunk());
        assertEquals(0, chunk.free());
    }

    @Test
    public void testChunkExhausted()
    {
        final int size = BufferPool.CHUNK_SIZE / 64; // 1kbit
        int[] sizes = new int[128];
        Arrays.fill(sizes, size);

        int sum = 0;
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
        {
            ByteBuffer buffer = BufferPool.get(sizes[i]);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += buffer.capacity();
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);

        for (int i = 0; i < sizes.length; i++)
        {
            BufferPool.put(buffers.get(i));
        }

        assertEquals(null, BufferPool.currentChunk());
        assertEquals(0, chunk.free());
    }

    @Test
    public void testCompactIfOutOfCapacity()
    {
        final int size = 4096;
        final int numBuffersInChunk = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / size;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffersInChunk);
        Set<Long> addresses = new HashSet<>(numBuffersInChunk);

        for (int i = 0; i < numBuffersInChunk; i++)
        {
            ByteBuffer buffer = BufferPool.get(size);
            buffers.add(buffer);
            addresses.add(MemoryUtil.getAddress(buffer));
        }

        for (int i = numBuffersInChunk - 1; i >= 0; i--)
            BufferPool.put(buffers.get(i));

        buffers.clear();

        for (int i = 0; i < numBuffersInChunk; i++)
        {
            ByteBuffer buffer = BufferPool.get(size);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());
            addresses.remove(MemoryUtil.getAddress(buffer));

            buffers.add(buffer);
        }

        assertTrue(addresses.isEmpty()); // all 5 released buffers were used

        for (ByteBuffer buffer : buffers)
            BufferPool.put(buffer);
    }

    @Test
    public void testHeapBuffer()
    {
        ByteBuffer buffer = BufferPool.get(1024, BufferType.ON_HEAP);
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertFalse(buffer.isDirect());
        assertNotNull(buffer.array());
        BufferPool.put(buffer);
    }

    @Test
    public void testSingleBufferOneChunk()
    {
        checkBuffer(0);

        checkBuffer(1);
        checkBuffer(2);
        checkBuffer(4);
        checkBuffer(5);
        checkBuffer(8);
        checkBuffer(16);
        checkBuffer(32);
        checkBuffer(64);

        checkBuffer(65);
        checkBuffer(127);
        checkBuffer(128);

        checkBuffer(129);
        checkBuffer(255);
        checkBuffer(256);

        checkBuffer(512);
        checkBuffer(1024);
        checkBuffer(2048);
        checkBuffer(4096);
        checkBuffer(8192);
        checkBuffer(16384);

        checkBuffer(16385);
        checkBuffer(32767);
        checkBuffer(32768);

        checkBuffer(32769);
        checkBuffer(33172);
        checkBuffer(33553);
        checkBuffer(36000);
        checkBuffer(65535);
        checkBuffer(65536);

        checkBuffer(65537);
    }

    private void checkBuffer(int size)
    {
        ByteBuffer buffer = BufferPool.get(size);
        assertEquals(size, buffer.capacity());

        if (size > 0 && size < BufferPool.CHUNK_SIZE)
        {
            BufferPool.Chunk chunk = BufferPool.currentChunk();
            assertNotNull(chunk);
            assertEquals(chunk.capacity(), chunk.free() + chunk.roundUp(size));
        }

        BufferPool.put(buffer);
    }

    @Test
    public void testMultipleBuffersOneChunk()
    {
        checkBuffers(32768, 33553);
        checkBuffers(32768, 32768);
        checkBuffers(48450, 33172);
        checkBuffers(32768, 15682, 33172);
    }

    private void checkBuffers(int ... sizes)
    {
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);

        for (int size : sizes)
        {
            ByteBuffer buffer = BufferPool.get(size);
            assertEquals(size, buffer.capacity());

            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            BufferPool.put(buffer);
    }

    @Test
    public void testBuffersWithGivenSlots()
    {
        checkBufferWithGivenSlots(21241, (-1L << 27) ^ (1L << 40));
    }

    private void checkBufferWithGivenSlots(int size, long freeSlots)
    {
        //first allocate to make sure there is a chunk
        ByteBuffer buffer = BufferPool.get(size);

        // now get the current chunk and override the free slots mask
        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        long oldFreeSlots = chunk.setFreeSlots(freeSlots);

        // now check we can still get the buffer with the free slots mask changed
        ByteBuffer buffer2 = BufferPool.get(size);
        assertEquals(size, buffer.capacity());
        BufferPool.put(buffer2);

        // reset the free slots
        chunk.setFreeSlots(oldFreeSlots);
        BufferPool.put(buffer);
    }

    @Test
    public void testZeroSizeRequest()
    {
        ByteBuffer buffer = BufferPool.get(0);
        assertNotNull(buffer);
        assertEquals(0, buffer.capacity());
        BufferPool.put(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSizeRequest()
    {
        BufferPool.get(-1);
    }

    @Test
    public void testBufferPoolDisabled()
    {
        BufferPool.DISABLED = true;
        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = true;
        ByteBuffer buffer = BufferPool.get(1024);
        assertEquals(0, BufferPool.numChunks());
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertFalse(buffer.isDirect());
        assertNotNull(buffer.array());
        BufferPool.put(buffer);
        assertEquals(0, BufferPool.numChunks());

        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = false;
        buffer = BufferPool.get(1024);
        assertEquals(0, BufferPool.numChunks());
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertTrue(buffer.isDirect());
        BufferPool.put(buffer);
        assertEquals(0, BufferPool.numChunks());

        // clean-up
        BufferPool.DISABLED = false;
        BufferPool.ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = true;
    }

    @Test
    public void testMT_SameSizeImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, true, RandomAccessReader.DEFAULT_BUFFER_SIZE);
    }

    @Test
    public void testMT_SameSizePostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, false, RandomAccessReader.DEFAULT_BUFFER_SIZE);
    }

    @Test
    public void testMT_TwoSizesOneBufferImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, true, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesOneBufferPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, false, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesTwoBuffersImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 2, true, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesTwoBuffersPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 2, false, 1024, 2048);
    }

    @Test
    public void testMT_MultipleSizesOneBufferImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             1,
                             true,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesOneBufferPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             1,
                             false,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesMultipleBuffersImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             4,
                             true,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesMultipleBuffersPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             3,
                             false,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    private void checkMultipleThreads(int threadCount, int numBuffersPerThread, final boolean returnImmediately, final int ... sizes) throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            final int[] threadSizes = new int[numBuffersPerThread];
            for (int j = 0; j < threadSizes.length; j++)
                threadSizes[j] = sizes[(i * numBuffersPerThread + j) % sizes.length];

            final Random rand = new Random();
            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(rand.nextInt(3));

                        List<ByteBuffer> toBeReturned = new ArrayList<ByteBuffer>(threadSizes.length);

                        for (int j = 0; j < threadSizes.length; j++)
                        {
                            ByteBuffer buffer = BufferPool.get(threadSizes[j]);
                            assertNotNull(buffer);
                            assertEquals(threadSizes[j], buffer.capacity());

                            for (int i = 0; i < 10; i++)
                                buffer.putInt(i);

                            buffer.rewind();

                            Thread.sleep(rand.nextInt(3));

                            for (int i = 0; i < 10; i++)
                                assertEquals(i, buffer.getInt());

                            if (returnImmediately)
                                BufferPool.put(buffer);
                            else
                                toBeReturned.add(buffer);

                            assertTrue(BufferPool.sizeInBytes() > 0);
                        }

                        Thread.sleep(rand.nextInt(3));

                        for (ByteBuffer buffer : toBeReturned)
                            BufferPool.put(buffer);
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        fail(ex.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        // Make sure thread local storage gets GC-ed
        for (int i = 0; i < 5; i++)
        {
            System.gc();
            Thread.sleep(100);
        }
    }

    @Ignore
    public void testMultipleThreadsReleaseSameBuffer() throws InterruptedException
    {
        doMultipleThreadsReleaseBuffers(45, 4096);
    }

    @Ignore
    public void testMultipleThreadsReleaseDifferentBuffer() throws InterruptedException
    {
        doMultipleThreadsReleaseBuffers(45, 4096, 8192);
    }

    private void doMultipleThreadsReleaseBuffers(final int threadCount, final int ... sizes) throws InterruptedException
    {
        final ByteBuffer[] buffers = new ByteBuffer[sizes.length];
        int sum = 0;
        for (int i = 0; i < sizes.length; i++)
        {
            buffers[i] = BufferPool.get(sizes[i]);
            assertNotNull(buffers[i]);
            assertEquals(sizes[i], buffers[i].capacity());
            sum += BufferPool.currentChunk().roundUp(buffers[i].capacity());
        }

        final BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertFalse(chunk.isFree());

        // if we use multiple chunks the test will fail, adjust sizes accordingly
        assertTrue(sum < BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            final int idx = i % sizes.length;
            final ByteBuffer buffer = buffers[idx];

            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        assertNotSame(chunk, BufferPool.currentChunk());
                        BufferPool.put(buffer);
                    }
                    catch (AssertionError ex)
                    { //this is expected if we release a buffer more than once
                        ex.printStackTrace();
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        fail(t.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        executorService = null;

        // Make sure thread local storage gets GC-ed
        System.gc();
        System.gc();
        System.gc();

        assertTrue(BufferPool.currentChunk().isFree());

        //make sure the main thread can still allocate buffers
        ByteBuffer buffer = BufferPool.get(sizes[0]);
        assertNotNull(buffer);
        assertEquals(sizes[0], buffer.capacity());
        BufferPool.put(buffer);
    }
}
