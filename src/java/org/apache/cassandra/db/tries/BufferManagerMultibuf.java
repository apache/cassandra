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

package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.concurrent.UnsafeBuffer;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.db.tries.InMemoryReadTrie.CELL_SIZE;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.getBufferIdx;

/// Multi-buffer implementation of a buffer manager, where multiple buffers of growing size are maintained and adding a
/// new cell when the space is exhausted is accomplished by adding a new buffer with twice the size of the last one.
/// This has some extra complexity compared to using a single buffer, but avoids having to copy buffers of up to 1GiB
/// of data to grow.
///
/// EXPANDABLE DATA STORAGE
///
/// The tries will need more and more space in buffers and content lists as they grow. Instead of using ArrayList-like
/// reallocation with copying, which may be prohibitively expensive for large buffers, we use a sequence of
/// buffers/content arrays that double in size on every expansion.
///
/// For a given address `x` the index of the buffer can be found with the following calculation:
///    ```index_of_most_significant_set_bit(x / min_size + 1)```
/// (relying on `sum (2^i) for i in [0, n-1] == 2^n - 1`) which can be performed quickly on modern hardware.
///
/// Finding the offset within the buffer is then
///    ```x + min - (min << buffer_index)```
///
/// The allocated space starts at 256 bytes for the buffer and 16 entries for the content list.
///
/// Note that a buffer is not allowed to split 32-byte cells (code assumes same buffer can be used for all bytes
/// inside the cell).
///
/// This class can optionally recycle cells that are no longer in use.
public class BufferManagerMultibuf implements BufferManager
{
    static final int BUF_START_SHIFT = 8;
    static final int BUF_START_SIZE = 1 << BUF_START_SHIFT;

    static
    {
        assert BUF_START_SIZE % CELL_SIZE == 0 : "Initial buffer size must fit a full cell.";
    }

    /// Trie size limit. This is not enforced, but users must check from time to time that it is not exceeded (using
    /// [#reachedAllocatedSizeThreshold()]) and start switching to a new trie if it is.
    /// This must be done to avoid tries growing beyond their hard 2GB size limit (due to the 32-bit pointers).
    @VisibleForTesting
    static final int ALLOCATED_SIZE_THRESHOLD;

    static
    {
        // Default threshold + 10% == 2 GB. This should give the owner enough time to react to the
        // {@link #reachedAllocatedSizeThreshold()} signal and switch this trie out before it fills up.
        int limitInMB = CassandraRelevantProperties.MEMTABLE_TRIE_SIZE_LIMIT.getInt(2048 * 10 / 11);
        if (limitInMB < 1 || limitInMB > 2047)
            throw new AssertionError(CassandraRelevantProperties.MEMTABLE_TRIE_SIZE_LIMIT.getKey() +
                                     " must be within 1 and 2047");
        ALLOCATED_SIZE_THRESHOLD = 1024 * 1024 * limitInMB;
    }

    private int allocatedPos = 0;

    final BufferType bufferType;    // on or off heap
    final MemoryAllocationStrategy cellAllocator;

    final UnsafeBuffer[] buffers;

    /// Creates a new buffer manager with the given buffer type (on- or off-heap) and expected lifetime.
    /// Short-lived managers will not recycle cells as it is simpler to throw the whole thing away at the end of its
    /// lifecycle, while long-lived will track freed cells and will reuse them after the given opOrder indicates that
    /// all operations that may be using them have finished.
    public BufferManagerMultibuf(BufferType bufferType,
                                 InMemoryBaseTrie.ExpectedLifetime lifetime,
                                 OpOrder opOrder)
    {
        this.buffers = new UnsafeBuffer[31 - BUF_START_SHIFT]; // last one is 1G for a total of ~2G bytes
        this.bufferType = bufferType;

        switch (lifetime)
        {
            case SHORT:
                cellAllocator = new MemoryAllocationStrategy.NoReuseStrategy(this::allocateNewCell);
                break;
            case LONG:
                cellAllocator = new MemoryAllocationStrategy.OpOrderReuseStrategy(this::allocateNewCell, opOrder);
                break;
            default:
                throw new AssertionError();
        }

    }

    @Override
    public UnsafeBuffer getBuffer(int pos)
    {
        int leadBit = getBufferIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return buffers[leadBit];
    }

    @Override
    public int inBufferOffset(int pos)
    {
        int leadBit = getBufferIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return InMemoryReadTrie.inBufferOffset(pos, leadBit, BUF_START_SIZE);
    }


    /// Allocate a new cell in the data buffers. This is called by the memory allocation strategy when it runs out of
    /// free cells to reuse.
    private int allocateNewCell() throws TrieSpaceExhaustedException
    {
        // Note: If this method is modified, please run InMemoryTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        int v = allocatedPos;
        if (inBufferOffset(v) == 0)
        {
            int leadBit = getBufferIdx(v, BUF_START_SHIFT, BUF_START_SIZE);
            if (leadBit + BUF_START_SHIFT == 31)
                throw new TrieSpaceExhaustedException();

            ByteBuffer newBuffer = bufferType.allocate(BUF_START_SIZE << leadBit);
            buffers[leadBit] = new UnsafeBuffer(newBuffer);
            // Note: Since we are not moving existing data to a new buffer, we are okay with no happens-before enforcing
            // writes. Any reader that sees a pointer in the new buffer may only do so after reading the volatile write
            // that attached the new path.
        }

        allocatedPos += CELL_SIZE;
        return v;
    }

    @Override
    public int allocateCell() throws TrieSpaceExhaustedException
    {
        int cell = cellAllocator.allocate();
        getBuffer(cell).setMemory(inBufferOffset(cell), CELL_SIZE, (byte) 0);
        return cell;
    }

    @Override
    public void recycleCell(int cell)
    {
        cellAllocator.recycle(cell & -CELL_SIZE);
    }

    @Override
    public int copyCell(int cell) throws TrieSpaceExhaustedException
    {
        int copy = cellAllocator.allocate();
        getBuffer(copy).putBytes(inBufferOffset(copy), getBuffer(cell), inBufferOffset(cell & -CELL_SIZE), CELL_SIZE);
        recycleCell(cell);
        return copy | (cell & (CELL_SIZE - 1));
    }

    @Override
    public void completeMutation()
    {
        cellAllocator.completeMutation();
    }

    @Override
    public void abortMutation()
    {
        cellAllocator.abortMutation();
    }

    @Override
    public boolean reachedAllocatedSizeThreshold()
    {
        return allocatedPos >= ALLOCATED_SIZE_THRESHOLD;
    }

    /// For tests only! Advance the allocation pointer (and allocate space) to the given position to test behaviour
    /// close to full. If the parameter is -1, consume all the space until the next request would throw an exception.
    @VisibleForTesting
    int advanceAllocatedPos(int wantedPos) throws TrieSpaceExhaustedException
    {
        if (wantedPos == -1)
        {
            if (cellAllocator instanceof MemoryAllocationStrategy.OpOrderReuseStrategy)
                wantedPos = (int) (0x80000000L - BUF_START_SIZE - MemoryAllocationStrategy.REUSE_BLOCK_SIZE * 32);
            else
                wantedPos = (int) (0x80000000L - BUF_START_SIZE - 32);
        }

        while (allocatedPos < wantedPos)
            allocateCell();

        if (cellAllocator instanceof MemoryAllocationStrategy.OpOrderReuseStrategy)
        {
            // grab all the cells that were just prepared
            for (int i = 1; i < MemoryAllocationStrategy.REUSE_BLOCK_SIZE; ++i)
                allocateCell();
        }

        return allocatedPos;
    }

    /// For tests only! Returns the current allocation position.
    @VisibleForTesting
    int getAllocatedPos()
    {
        return allocatedPos;
    }

    @Override
    public long usedSizeOffHeap()
    {
        return (bufferType == BufferType.ON_HEAP ? 0 : usedBufferSpace());
    }

    @Override
    public long usedSizeOnHeap()
    {
        return (bufferType == BufferType.ON_HEAP ? usedBufferSpace() : 0) +
               InMemoryBaseTrie.REFERENCE_ARRAY_ON_HEAP_SIZE * getBufferIdx(allocatedPos, BUF_START_SHIFT, BUF_START_SIZE);
    }

    @Override
    @VisibleForTesting
    public long usedBufferSpace()
    {
        return allocatedPos - cellAllocator.indexCountInPipeline() * CELL_SIZE;
    }

    @Override
    public long unusedReservedOnHeapMemory()
    {
        long bufferOverhead = 0;
        if (bufferType == BufferType.ON_HEAP)
        {
            int pos = this.allocatedPos;
            UnsafeBuffer buffer = getBuffer(pos);
            if (buffer != null)
                bufferOverhead = buffer.capacity() - inBufferOffset(pos);
            bufferOverhead += cellAllocator.indexCountInPipeline() * CELL_SIZE;
        }
        return bufferOverhead;
    }

    @Override
    public void discardBuffers()
    {
        if (bufferType == BufferType.ON_HEAP)
            return; // no cleaning needed

        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                MemoryUtil.clean(b.byteBuffer());
        }
    }

    @Override
    public void overwriteAllBuffers()
    {
        // also overwrite on-heap buffers to simulate overwrite by reuse
        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                ByteBufferUtil.overwriteWithRandomBytes(b.byteBuffer());
        }
    }

    @Override
    public BufferType bufferType()
    {
        return bufferType;
    }
}
