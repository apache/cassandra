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
package org.apache.cassandra.io.util;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.MemoryUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * An off-heap region of memory that must be manually free'd when no longer needed.
 */
public class Memory implements AutoCloseable
{
    private static final Unsafe unsafe;
    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static final long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    private static final boolean bigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    private static final boolean unaligned;

    public static final ByteBuffer[] NO_BYTE_BUFFERS = new ByteBuffer[0];

    static
    {
        String arch = System.getProperty("os.arch");
        unaligned = arch.equals("i386") || arch.equals("x86")
                    || arch.equals("amd64") || arch.equals("x86_64") || arch.equals("s390x");
    }

    protected long peer;
    // size of the memory region
    protected final long size;

    protected Memory(long bytes)
    {
        if (bytes <= 0)
            throw new AssertionError();
        size = bytes;
        peer = MemoryUtil.allocate(size);
        // we permit a 0 peer iff size is zero, since such an allocation makes no sense, and an allocator would be
        // justified in returning a null pointer (and permitted to do so: http://www.cplusplus.com/reference/cstdlib/malloc)
        if (peer == 0)
            throw new OutOfMemoryError();
    }

    // create a memory object that references the exacy same memory location as the one provided.
    // this should ONLY be used by SafeMemory
    protected Memory(Memory copyOf)
    {
        size = copyOf.size;
        peer = copyOf.peer;
    }

    public static Memory allocate(long bytes)
    {
        if (bytes < 0)
            throw new IllegalArgumentException();

        if (Ref.DEBUG_ENABLED)
            return new SafeMemory(bytes);

        return new Memory(bytes);
    }

    public void setByte(long offset, byte b)
    {
        checkBounds(offset, offset + 1);
        unsafe.putByte(peer + offset, b);
    }

    public void setMemory(long offset, long bytes, byte b)
    {
        checkBounds(offset, offset + bytes);
        // check if the last element will fit into the memory
        unsafe.setMemory(peer + offset, bytes, b);
    }

    public void setLong(long offset, long l)
    {
        checkBounds(offset, offset + 8);
        if (unaligned)
        {
            unsafe.putLong(peer + offset, l);
        }
        else
        {
            putLongByByte(peer + offset, l);
        }
    }

    private void putLongByByte(long address, long value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 56));
            unsafe.putByte(address + 1, (byte) (value >> 48));
            unsafe.putByte(address + 2, (byte) (value >> 40));
            unsafe.putByte(address + 3, (byte) (value >> 32));
            unsafe.putByte(address + 4, (byte) (value >> 24));
            unsafe.putByte(address + 5, (byte) (value >> 16));
            unsafe.putByte(address + 6, (byte) (value >> 8));
            unsafe.putByte(address + 7, (byte) (value));
        }
        else
        {
            unsafe.putByte(address + 7, (byte) (value >> 56));
            unsafe.putByte(address + 6, (byte) (value >> 48));
            unsafe.putByte(address + 5, (byte) (value >> 40));
            unsafe.putByte(address + 4, (byte) (value >> 32));
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    public void setInt(long offset, int l)
    {
        checkBounds(offset, offset + 4);
        if (unaligned)
        {
            unsafe.putInt(peer + offset, l);
        }
        else
        {
            putIntByByte(peer + offset, l);
        }
    }

    private void putIntByByte(long address, int value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 24));
            unsafe.putByte(address + 1, (byte) (value >> 16));
            unsafe.putByte(address + 2, (byte) (value >> 8));
            unsafe.putByte(address + 3, (byte) (value));
        }
        else
        {
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    public void setShort(long offset, short l)
    {
        checkBounds(offset, offset + 2);
        if (unaligned)
        {
            unsafe.putShort(peer + offset, l);
        }
        else
        {
            putShortByByte(peer + offset, l);
        }
    }

    private void putShortByByte(long address, short value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 8));
            unsafe.putByte(address + 1, (byte) (value));
        }
        else
        {
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (buffer.remaining() == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + buffer.remaining());
        if (buffer.hasArray())
        {
            setBytes(memoryOffset, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        else if (buffer instanceof DirectBuffer)
        {
            unsafe.copyMemory(((DirectBuffer) buffer).address() + buffer.position(), peer + memoryOffset, buffer.remaining());
        }
        else
            throw new IllegalStateException();
    }
    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void setBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0
                 || count < 0
                 || bufferOffset + count > buffer.length)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + count);
        unsafe.copyMemory(buffer, BYTE_ARRAY_BASE_OFFSET + bufferOffset, null, peer + memoryOffset, count);
    }

    public byte getByte(long offset)
    {
        checkBounds(offset, offset + 1);
        return unsafe.getByte(peer + offset);
    }

    public long getLong(long offset)
    {
        checkBounds(offset, offset + 8);
        if (unaligned) {
            return unsafe.getLong(peer + offset);
        } else {
            return getLongByByte(peer + offset);
        }
    }

    private long getLongByByte(long address) {
        if (bigEndian) {
            return  (((long) unsafe.getByte(address    )       ) << 56) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 6) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address + 7) & 0xff)      );
        } else {
            return  (((long) unsafe.getByte(address + 7)       ) << 56) |
                    (((long) unsafe.getByte(address + 6) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    public int getInt(long offset)
    {
        checkBounds(offset, offset + 4);
        if (unaligned) {
            return unsafe.getInt(peer + offset);
        } else {
            return getIntByByte(peer + offset);
        }
    }

    private int getIntByByte(long address) {
        if (bigEndian) {
            return  (((int) unsafe.getByte(address    )       ) << 24) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 8 ) |
                    (((int) unsafe.getByte(address + 3) & 0xff)      );
        } else {
            return  (((int) unsafe.getByte(address + 3)       ) << 24) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    /**
     * Transfers count bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void getBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + count);
        FastByteOperations.UnsafeOperations.copy(null, peer + memoryOffset, buffer, bufferOffset, count);
    }

    @Inline
    protected void checkBounds(long start, long end)
    {
        assert peer != 0 : "Memory was freed";
        assert start >= 0 && end <= size && start <= end : "Illegal bounds [" + start + ".." + end + "); size: " + size;
    }

    public void put(long trgOffset, Memory memory, long srcOffset, long size)
    {
        checkBounds(trgOffset, trgOffset + size);
        memory.checkBounds(srcOffset, srcOffset + size);
        unsafe.copyMemory(memory.peer + srcOffset, peer + trgOffset, size);
    }

    public Memory copy(long newSize)
    {
        Memory copy = Memory.allocate(newSize);
        copy.put(0, this, 0, Math.min(size(), newSize));
        return copy;
    }

    public void free()
    {
        if (peer != 0) MemoryUtil.free(peer);
        else assert size == 0;
        peer = 0;
    }

    public void close()
    {
        free();
    }

    public long size()
    {
        assert peer != 0;
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Memory))
            return false;
        Memory b = (Memory) o;
        if (peer == b.peer && size == b.size)
            return true;
        return false;
    }

    public ByteBuffer[] asByteBuffers(long offset, long length)
    {
        checkBounds(offset, offset + length);
        if (size() == 0)
            return NO_BYTE_BUFFERS;

        ByteBuffer[] result = new ByteBuffer[(int) (length / Integer.MAX_VALUE) + 1];
        int size = (int) (size() / result.length);
        for (int i = 0 ; i < result.length - 1 ; i++)
        {
            result[i] = MemoryUtil.getByteBuffer(peer + offset, size);
            offset += size;
            length -= size;
        }
        result[result.length - 1] = MemoryUtil.getByteBuffer(peer + offset, (int) length);
        return result;
    }

    public ByteBuffer asByteBuffer(long offset, int length)
    {
        checkBounds(offset, offset + length);
        return MemoryUtil.getByteBuffer(peer + offset, length);
    }

    // MUST provide a buffer created via MemoryUtil.getHollowDirectByteBuffer()
    public void setByteBuffer(ByteBuffer buffer, long offset, int length)
    {
        checkBounds(offset, offset + length);
        MemoryUtil.setByteBuffer(buffer, peer + offset, length);
    }

    public String toString()
    {
        return toString(peer, size);
    }

    protected static String toString(long peer, long size)
    {
        return String.format("Memory@[%x..%x)", peer, peer + size);
    }
}
