/*
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

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.*;

public class BufferPoolTest
{
    private BufferPool bufferPool;

    @Before
    public void setUp()
    {
        bufferPool = new BufferPool("test_pool", 8 * 1024 * 1024, true);
    }

    @Test
    public void testGetPut()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;

        ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertEquals(true, buffer.isDirect());
        assertEquals(size, bufferPool.usedSizeInBytes());

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());

        bufferPool.put(buffer);
        assertEquals(null, bufferPool.unsafeCurrentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());
        assertEquals(0, bufferPool.usedSizeInBytes());
    }

    @Test
    public void testTryGet()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;

        ByteBuffer buffer = bufferPool.tryGet(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertEquals(true, buffer.isDirect());
        assertEquals(size, bufferPool.usedSizeInBytes());

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());

        bufferPool.put(buffer);
        assertEquals(null, bufferPool.unsafeCurrentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());
        assertEquals(0, bufferPool.usedSizeInBytes());
    }

    @Test
    public void testPageAligned()
    {
        final int size = 1024;
        for (int i = size;
                 i <= BufferPool.NORMAL_CHUNK_SIZE;
                 i += size)
        {
            checkPageAligned(i);
        }
    }

    private void checkPageAligned(int size)
    {
        ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertTrue(buffer.isDirect());

        long address = MemoryUtil.getAddress(buffer);
        assertTrue((address % MemoryUtil.pageSize()) == 0);

        bufferPool.put(buffer);
    }

    @Test
    public void testDifferentSizes() throws InterruptedException
    {
        final int size1 = 1024;
        final int size2 = 2048;

        ByteBuffer buffer1 = bufferPool.get(size1, BufferType.OFF_HEAP);
        assertNotNull(buffer1);
        assertEquals(size1, buffer1.capacity());
        assertEquals(size1, bufferPool.usedSizeInBytes());

        ByteBuffer buffer2 = bufferPool.get(size2, BufferType.OFF_HEAP);
        assertNotNull(buffer2);
        assertEquals(size2, buffer2.capacity());
        assertEquals(size1 + size2, bufferPool.usedSizeInBytes());

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());

        bufferPool.put(buffer1);
        bufferPool.put(buffer2);

        assertEquals(null, bufferPool.unsafeCurrentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, bufferPool.sizeInBytes());
        assertEquals(0, bufferPool.usedSizeInBytes());
    }

    @Test
    public void testMaxMemoryExceededDirect()
    {
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceededHeap()
    {
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceeded_SameAsChunkSize()
    {
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceeded_SmallerThanChunkSize()
    {
        bufferPool = new BufferPool("test_pool", BufferPool.GlobalPool.MACRO_CHUNK_SIZE / 2, false);
        requestDoubleMaxMemory();
    }

    @Test
    public void testRecycle()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, 3 * BufferPool.NORMAL_CHUNK_SIZE);
    }

    private void requestDoubleMaxMemory()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, (int)(2 * bufferPool.memoryUsageThreshold()));
    }

    private void requestUpToSize(int bufferSize, int totalSize)
    {
        final int numBuffers = totalSize / bufferSize;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
        {
            ByteBuffer buffer = bufferPool.get(bufferSize, BufferType.OFF_HEAP);
            assertNotNull(buffer);
            assertEquals(bufferSize, buffer.capacity());
            assertTrue(buffer.isDirect());
            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            bufferPool.put(buffer);
    }

    @Test
    public void testBigRequest()
    {
        final int size = BufferPool.NORMAL_CHUNK_SIZE + 1;

        ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        bufferPool.put(buffer);
    }

    @Test
    public void testFillUpChunks()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        final int numBuffers = BufferPool.NORMAL_CHUNK_SIZE / size;

        List<ByteBuffer> buffers1 = new ArrayList<>(numBuffers);
        List<ByteBuffer> buffers2 = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
            buffers1.add(bufferPool.get(size, BufferType.OFF_HEAP));

        BufferPool.Chunk chunk1 = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk1);

        for (int i = 0; i < numBuffers; i++)
            buffers2.add(bufferPool.get(size, BufferType.OFF_HEAP));

        assertEquals(2, bufferPool.unsafeNumChunks());

        for (ByteBuffer buffer : buffers1)
            bufferPool.put(buffer);

        assertEquals(1, bufferPool.unsafeNumChunks());

        for (ByteBuffer buffer : buffers2)
            bufferPool.put(buffer);

        assertEquals(0, bufferPool.unsafeNumChunks());

        buffers2.clear();
    }

    @Test
    public void testOutOfOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.NORMAL_CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testInOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.NORMAL_CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = maxFreeSlots - 1 - i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testRandomFrees()
    {
        doTestRandomFrees(12345567878L);

        bufferPool.unsafeReset();
        doTestRandomFrees(20452249587L);

        bufferPool.unsafeReset();
        doTestRandomFrees(82457252948L);

        bufferPool.unsafeReset();
        doTestRandomFrees(98759284579L);

        bufferPool.unsafeReset();
        doTestRandomFrees(19475257244L);
    }

    private void doTestRandomFrees(long seed)
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.NORMAL_CHUNK_SIZE / size;

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
            buffers.add(bufferPool.get(size, BufferType.OFF_HEAP));
        }

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertFalse(chunk.isFree());

        int freeSize = BufferPool.NORMAL_CHUNK_SIZE - maxFreeSlots * size;
        assertEquals(freeSize, chunk.free());

        for (int i : toReleaseIdxs)
        {
            ByteBuffer buffer = buffers.get(i);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());

            bufferPool.put(buffer);

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
            ByteBuffer buffer = bufferPool.get(sizes[i], BufferType.OFF_HEAP);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += bufferPool.unsafeCurrentChunk().roundUp(buffer.capacity());
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);

        Random rnd = new Random();
        rnd.setSeed(298347529L);
        while (buffers.size() > 1)
        {
            int index = rnd.nextInt(buffers.size());
            ByteBuffer buffer = buffers.remove(index);

            bufferPool.put(buffer);
        }
        bufferPool.put(buffers.remove(0));

        assertEquals(null, bufferPool.unsafeCurrentChunk());
        assertEquals(0, chunk.free());
    }

    @Test
    public void testChunkExhausted()
    {
        final int size = BufferPool.NORMAL_CHUNK_SIZE / 64; // 1kibibit
        int[] sizes = new int[128];
        Arrays.fill(sizes, size);

        int sum = 0;
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
        {
            ByteBuffer buffer = bufferPool.get(sizes[i], BufferType.OFF_HEAP);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += buffer.capacity();
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);

        for (int i = 0; i < sizes.length; i++)
        {
            bufferPool.put(buffers.get(i));
        }

        assertEquals(null, bufferPool.unsafeCurrentChunk());
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
            ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
            buffers.add(buffer);
            addresses.add(MemoryUtil.getAddress(buffer));
        }

        for (int i = numBuffersInChunk - 1; i >= 0; i--)
            bufferPool.put(buffers.get(i));

        buffers.clear();

        for (int i = 0; i < numBuffersInChunk; i++)
        {
            ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());
            assert addresses.remove(MemoryUtil.getAddress(buffer));

            buffers.add(buffer);
        }

        assertTrue(addresses.isEmpty()); // all 5 released buffers were used

        for (ByteBuffer buffer : buffers)
            bufferPool.put(buffer);
    }

    @Test
    public void testHeapBuffer()
    {
        ByteBuffer buffer = bufferPool.get(1024, BufferType.ON_HEAP);
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertFalse(buffer.isDirect());
        assertNotNull(buffer.array());
        bufferPool.put(buffer);
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
        ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
        assertEquals(size, buffer.capacity());

        if (size > 0 && size < BufferPool.NORMAL_CHUNK_SIZE)
        {
            BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
            assertNotNull(chunk);
            assertEquals(chunk.capacity(), chunk.free() + chunk.roundUp(size));
        }

        bufferPool.put(buffer);
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
            ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);
            assertEquals(size, buffer.capacity());

            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            bufferPool.put(buffer);
    }

    @Test
    public void testBuffersWithGivenSlots()
    {
        checkBufferWithGivenSlots(21241, (-1L << 27) ^ (1L << 40));
    }

    private void checkBufferWithGivenSlots(int size, long freeSlots)
    {
        //first allocate to make sure there is a chunk
        ByteBuffer buffer = bufferPool.get(size, BufferType.OFF_HEAP);

        // now get the current chunk and override the free slots mask
        BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
        assertNotNull(chunk);
        long oldFreeSlots = chunk.setFreeSlots(freeSlots);

        // now check we can still get the buffer with the free slots mask changed
        ByteBuffer buffer2 = bufferPool.get(size, BufferType.OFF_HEAP);
        assertEquals(size, buffer.capacity());
        bufferPool.put(buffer2);

        // unsafeReset the free slots
        chunk.setFreeSlots(oldFreeSlots);
        bufferPool.put(buffer);
    }

    @Test
    public void testZeroSizeRequest()
    {
        ByteBuffer buffer = bufferPool.get(0, BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(0, buffer.capacity());
        bufferPool.put(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSizeRequest()
    {
        bufferPool.get(-1, BufferType.OFF_HEAP);
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
                            ByteBuffer buffer = bufferPool.get(threadSizes[j], BufferType.OFF_HEAP);
                            assertNotNull(buffer);
                            assertEquals(threadSizes[j], buffer.capacity());

                            for (int i = 0; i < 10; i++)
                                buffer.putInt(i);

                            buffer.rewind();

                            Thread.sleep(rand.nextInt(3));

                            for (int i = 0; i < 10; i++)
                                assertEquals(i, buffer.getInt());

                            if (returnImmediately)
                                bufferPool.put(buffer);
                            else
                                toBeReturned.add(buffer);

                            assertTrue(bufferPool.sizeInBytes() > 0);
                        }

                        Thread.sleep(rand.nextInt(3));

                        for (ByteBuffer buffer : toBeReturned)
                            bufferPool.put(buffer);
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
            buffers[i] = bufferPool.get(sizes[i], BufferType.OFF_HEAP);
            assertNotNull(buffers[i]);
            assertEquals(sizes[i], buffers[i].capacity());
            sum += bufferPool.unsafeCurrentChunk().roundUp(buffers[i].capacity());
        }

        final BufferPool.Chunk chunk = bufferPool.unsafeCurrentChunk();
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
                        assertNotSame(chunk, bufferPool.unsafeCurrentChunk());
                        bufferPool.put(buffer);
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

        assertTrue(bufferPool.unsafeCurrentChunk().isFree());

        //make sure the main thread can still allocate buffers
        ByteBuffer buffer = bufferPool.get(sizes[0], BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(sizes[0], buffer.capacity());
        bufferPool.put(buffer);
    }

    @Test
    public void testOverflowAllocation()
    {
        int macroChunkSize = BufferPool.GlobalPool.MACRO_CHUNK_SIZE;
        int allocationSize = BufferPool.NORMAL_CHUNK_SIZE;
        int allocations = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / allocationSize;

        // occupy entire buffer pool
        List<ByteBuffer> buffers = new ArrayList<>();
        allocate(allocations, allocationSize, buffers);

        assertEquals(macroChunkSize, bufferPool.sizeInBytes());
        assertEquals(macroChunkSize, bufferPool.usedSizeInBytes());
        assertEquals(0, bufferPool.overflowMemoryInBytes());

        // allocate overflow due to pool exhaust
        ByteBuffer overflowBuffer = bufferPool.get(BufferPool.NORMAL_ALLOCATION_UNIT, BufferType.OFF_HEAP);

        assertEquals(macroChunkSize + overflowBuffer.capacity(), bufferPool.sizeInBytes());
        assertEquals(macroChunkSize + overflowBuffer.capacity(), bufferPool.usedSizeInBytes());
        assertEquals(overflowBuffer.capacity(), bufferPool.overflowMemoryInBytes());

        // free all buffer
        bufferPool.put(overflowBuffer);
        release(buffers);

        assertEquals(macroChunkSize, bufferPool.sizeInBytes());
        assertEquals(0, bufferPool.usedSizeInBytes());
        assertEquals(0, bufferPool.overflowMemoryInBytes());

        // allocate overflow due to on-heap
        overflowBuffer = bufferPool.get(BufferPool.NORMAL_ALLOCATION_UNIT, BufferType.ON_HEAP);
        assertEquals(macroChunkSize + overflowBuffer.capacity(), bufferPool.sizeInBytes());
        assertEquals(overflowBuffer.capacity(), bufferPool.usedSizeInBytes());
        assertEquals(overflowBuffer.capacity(), bufferPool.overflowMemoryInBytes());
        bufferPool.put(overflowBuffer);

        // allocate overflow due to over allocation size
        overflowBuffer = bufferPool.get(2 * BufferPool.NORMAL_CHUNK_SIZE, BufferType.ON_HEAP);
        assertEquals(macroChunkSize + overflowBuffer.capacity(), bufferPool.sizeInBytes());
        assertEquals(overflowBuffer.capacity(), bufferPool.usedSizeInBytes());
        assertEquals(overflowBuffer.capacity(), bufferPool.overflowMemoryInBytes());
    }

    @Test
    public void testRecyclePartialFreeChunk()
    {
        // normal chunk size is 128KiB
        int halfNormalChunk = BufferPool.NORMAL_CHUNK_SIZE / 2; // 64KiB, half of normal chunk
        List<ByteBuffer> toRelease = new ArrayList<>();

        // allocate three buffers on different chunks
        ByteBuffer buffer0 = bufferPool.get(halfNormalChunk, BufferType.OFF_HEAP);
        BufferPool.Chunk chunk0 = BufferPool.Chunk.getParentChunk(buffer0);
        assertFalse(chunk0.isFree());
        allocate(1, halfNormalChunk, toRelease); // allocate remaining buffers in the chunk

        ByteBuffer buffer1 = bufferPool.get(halfNormalChunk, BufferType.OFF_HEAP);
        BufferPool.Chunk chunk1 = BufferPool.Chunk.getParentChunk(buffer1);
        assertFalse(chunk1.isFree());
        assertNotEquals(chunk0, chunk1);
        allocate(1, halfNormalChunk, toRelease); // allocate remaining buffers in the chunk

        ByteBuffer buffer2 = bufferPool.get(halfNormalChunk, BufferType.OFF_HEAP);
        BufferPool.Chunk chunk2 = BufferPool.Chunk.getParentChunk(buffer2);
        assertFalse(chunk2.isFree());
        assertNotEquals(chunk0, chunk2);
        assertNotEquals(chunk1, chunk2);
        allocate(1, halfNormalChunk, toRelease); // allocate remaining buffers in the chunk

        // now all 3 chunks in local pool is full, allocate one more buffer to evict chunk2
        ByteBuffer buffer3 = bufferPool.get(halfNormalChunk, BufferType.OFF_HEAP);
        BufferPool.Chunk chunk3 = BufferPool.Chunk.getParentChunk(buffer3);
        assertNotEquals(chunk0, chunk3);
        assertNotEquals(chunk1, chunk3);
        assertNotEquals(chunk2, chunk3);

        // verify chunk2 got evicted, it doesn't have a owner
        assertNotNull(chunk0.owner());
        assertEquals(BufferPool.Chunk.Status.IN_USE, chunk0.status());
        assertNotNull(chunk1.owner());
        assertEquals(BufferPool.Chunk.Status.IN_USE, chunk1.status());
        assertNull(chunk2.owner());
        assertEquals(BufferPool.Chunk.Status.EVICTED, chunk2.status());

        // release half buffers for chunk0/1/2
        release(toRelease);
        BufferPool.Chunk partiallyFreed = chunk2;

        // try to recirculate chunk2 and verify freed space
        assertFalse(bufferPool.globalPool().isFullyFreed(partiallyFreed));
        assertTrue(bufferPool.globalPool().isPartiallyFreed(partiallyFreed));
        assertEquals(BufferPool.Chunk.Status.IN_USE, partiallyFreed.status());
        assertEquals(halfNormalChunk, partiallyFreed.free());
        ByteBuffer buffer = partiallyFreed.get(halfNormalChunk, false, null);
        assertEquals(halfNormalChunk, buffer.capacity());

        // cleanup allocated buffers
        for (ByteBuffer buf : Arrays.asList(buffer0, buffer1, buffer2, buffer3, buffer))
            bufferPool.put(buf);

        // verify that fully freed chunk are prioritized over partially freed chunks
        List<BufferPool.Chunk> remainingChunks = new ArrayList<>();
        BufferPool.Chunk chunkForAllocation;
        while ((chunkForAllocation = bufferPool.globalPool().get()) != null)
            remainingChunks.add(chunkForAllocation);

        int totalNormalChunks = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / BufferPool.NORMAL_CHUNK_SIZE; // 64;
        assertEquals(totalNormalChunks, remainingChunks.size());
        assertSame(partiallyFreed, remainingChunks.get(remainingChunks.size() - 1)); // last one is partially freed

        // cleanup polled chunks
        remainingChunks.forEach(BufferPool.Chunk::release);
    }

    @Test
    public void testTinyPool()
    {
        int total = 0;
        final int size = BufferPool.TINY_ALLOCATION_UNIT;
        final int allocationPerChunk = 64;

        // occupy 3 tiny chunks
        List<ByteBuffer> buffers0 = new ArrayList<>();
        BufferPool.Chunk chunk0 = allocate(allocationPerChunk, size, buffers0);
        assertTrue(chunk0.owner().isTinyPool());
        List<ByteBuffer> buffers1 = new ArrayList<>();
        BufferPool.Chunk chunk1 = allocate(allocationPerChunk, size, buffers1);
        assertTrue(chunk1.owner().isTinyPool());
        List<ByteBuffer> buffers2 = new ArrayList<>();
        BufferPool.Chunk chunk2 = allocate(allocationPerChunk, size, buffers2);
        assertTrue(chunk2.owner().isTinyPool());
        total += 3 * BufferPool.TINY_CHUNK_SIZE;
        assertEquals(total, bufferPool.usedSizeInBytes());

        // allocate another tiny chunk.. chunk2 should be evicted
        List<ByteBuffer> buffers3 = new ArrayList<>();
        BufferPool.Chunk chunk3 = allocate(allocationPerChunk, size, buffers3);
        assertTrue(chunk3.owner().isTinyPool());
        total += BufferPool.TINY_CHUNK_SIZE;
        assertEquals(total, bufferPool.usedSizeInBytes());

        // verify chunk2 is full and evicted
        assertEquals(0, chunk2.free());
        assertNull(chunk2.owner());

        // release chunk2's buffer
        for (int i = 0; i < buffers2.size(); i++)
        {
            bufferPool.put(buffers2.get(i));
            total -= buffers2.get(i).capacity();
            assertEquals(total, bufferPool.usedSizeInBytes());
        }

        // cleanup allocated buffers
        for (ByteBuffer buffer : Iterables.concat(buffers0, buffers1, buffers3))
            bufferPool.put(buffer);
    }

    @Test
    public void testReleaseLocal()
    {
        final int size = BufferPool.TINY_ALLOCATION_UNIT;
        final int allocationPerChunk = 64;

        // occupy 3 tiny chunks
        List<ByteBuffer> buffers0 = new ArrayList<>();
        BufferPool.Chunk chunk0 = allocate(allocationPerChunk, size, buffers0);
        List<ByteBuffer> buffers1 = new ArrayList<>();
        BufferPool.Chunk chunk1 = allocate(allocationPerChunk, size, buffers1);
        List<ByteBuffer> buffers2 = new ArrayList<>();
        BufferPool.Chunk chunk2 = allocate(allocationPerChunk, size, buffers2);

        // release them from the pool
        bufferPool.releaseLocal();

        assertNull(chunk0.owner());
        assertNull(chunk1.owner());
        assertNull(chunk2.owner());
        assertEquals(BufferPool.Chunk.Status.EVICTED, chunk0.status());
        assertEquals(BufferPool.Chunk.Status.EVICTED, chunk1.status());
        assertEquals(BufferPool.Chunk.Status.EVICTED, chunk2.status());

        // cleanup allocated buffers, that should still work fine even though we released them from the localPool
        for (ByteBuffer buffer : Iterables.concat(buffers0, buffers1, buffers2))
            bufferPool.put(buffer);

        assertEquals(0, bufferPool.usedSizeInBytes());
    }

    @Test
    public void testPuttingUnusedPortion()
    {
        final int expectedCapacity = BufferPool.TINY_ALLOCATION_UNIT * 4;
        final int quarterUnit = BufferPool.TINY_ALLOCATION_UNIT / 4;
        final int requestedCapacity = expectedCapacity - 3 * quarterUnit;

        ByteBuffer buffer = bufferPool.getAtLeast(requestedCapacity, BufferType.OFF_HEAP);
        assertNotNull(buffer);
        assertEquals(expectedCapacity, buffer.capacity());
        assertEquals(expectedCapacity, bufferPool.usedSizeInBytes());

        buffer.limit(requestedCapacity); // 3.25 x unit
        bufferPool.putUnusedPortion(buffer);

        // the unused portion was too small to be returned, the buffer remains unchanged
        assertEquals(expectedCapacity, buffer.capacity());
        // used size is didn't change either
        assertEquals(expectedCapacity, bufferPool.usedSizeInBytes());

        buffer.limit(expectedCapacity - BufferPool.TINY_ALLOCATION_UNIT); // 3.0 x unit
        bufferPool.putUnusedPortion(buffer);

        // now we should notice a change
        assertEquals(BufferPool.TINY_ALLOCATION_UNIT * 3, buffer.capacity());
        assertEquals(BufferPool.TINY_ALLOCATION_UNIT * 3, bufferPool.usedSizeInBytes());

        bufferPool.put(buffer);

        assertEquals(0, bufferPool.usedSizeInBytes());
    }

    private BufferPool.Chunk allocate(int num, int bufferSize, List<ByteBuffer> buffers)
    {
        for (int i = 0; i < num; i++)
            buffers.add(bufferPool.get(bufferSize, BufferType.OFF_HEAP));

        return BufferPool.Chunk.getParentChunk(buffers.get(buffers.size() - 1));
    }

    private void release(List<ByteBuffer> toRelease)
    {
        for (ByteBuffer buffer : toRelease)
            bufferPool.put(buffer);
    }
}
