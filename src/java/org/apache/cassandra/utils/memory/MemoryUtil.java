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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sun.jna.Native;

import org.apache.cassandra.utils.Architecture;

import sun.misc.Unsafe;

public abstract class MemoryUtil
{
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    private static final Unsafe unsafe;
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS, RO_DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
    private static final Class<?> BYTE_BUFFER_CLASS;
    private static final long BYTE_BUFFER_OFFSET_OFFSET;
    private static final long BYTE_BUFFER_HB_OFFSET;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_POSITION_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("position"));
            DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET = unsafe.objectFieldOffset(clazz.getDeclaredField("att"));
            DIRECT_BYTE_BUFFER_CLASS = clazz;
            RO_DIRECT_BYTE_BUFFER_CLASS = ByteBuffer.allocateDirect(0).asReadOnlyBuffer().getClass();

            clazz = ByteBuffer.allocate(0).getClass();
            BYTE_BUFFER_OFFSET_OFFSET = unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
            BYTE_BUFFER_HB_OFFSET = unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
            BYTE_BUFFER_CLASS = clazz;

            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static int pageSize()
    {
        return unsafe.pageSize();
    }

    public static long getAddress(ByteBuffer buffer)
    {
        assert buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return unsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
    }

    public static long allocate(long size)
    {
        return Native.malloc(size);
    }

    public static void free(long peer)
    {
        Native.free(peer);
    }

    public static void setByte(long address, byte b)
    {
        unsafe.putByte(address, b);
    }

    public static void setByte(long address, int count, byte b)
    {
        unsafe.setMemory(address, count, b);
    }

    public static void setShort(long address, short s)
    {
        unsafe.putShort(address, Architecture.BIG_ENDIAN ? Short.reverseBytes(s) : s);
    }

    public static void setInt(long address, int l)
    {
        if (Architecture.IS_UNALIGNED)
            unsafe.putInt(address, Architecture.BIG_ENDIAN ? Integer.reverseBytes(l) : l);
        else
            putIntByByte(address, l);
    }

    public static void setLong(long address, long l)
    {
        if (Architecture.IS_UNALIGNED)
            unsafe.putLong(address, Architecture.BIG_ENDIAN ? Long.reverseBytes(l) : l);
        else
            putLongByByte(address, l);
    }

    public static byte getByte(long address)
    {
        return unsafe.getByte(address);
    }

    public static int getShort(long address)
    {
        if (Architecture.IS_UNALIGNED)
            return (Architecture.BIG_ENDIAN ? Short.reverseBytes(unsafe.getShort(address)) : unsafe.getShort(address)) & 0xffff;
        else
            return getShortByByte(address) & 0xffff;
	}

    public static int getInt(long address)
    {
        if (Architecture.IS_UNALIGNED)
            return Architecture.BIG_ENDIAN ? Integer.reverseBytes(unsafe.getInt(address)) : unsafe.getInt(address);
        else
            return getIntByByte(address);
	}

    public static long getLong(long address)
    {
        if (Architecture.IS_UNALIGNED)
            return Architecture.BIG_ENDIAN ? Long.reverseBytes(unsafe.getLong(address)) : unsafe.getLong(address);
        else
            return getLongByByte(address);
	}

    public static ByteBuffer getByteBuffer(long address, int length)
    {
        return getByteBuffer(address, length, ByteOrder.nativeOrder());
    }

    public static ByteBuffer getByteBuffer(long address, int length, ByteOrder order)
    {
        ByteBuffer instance = getHollowDirectByteBuffer(order);
        setDirectByteBuffer(instance, address, length);
        return instance;
    }

    public static ByteBuffer getHollowDirectByteBuffer()
    {
        return getHollowDirectByteBuffer(ByteOrder.nativeOrder());
    }

    public static ByteBuffer getHollowDirectByteBuffer(ByteOrder order)
    {
        ByteBuffer instance;
        try
        {
            instance = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
        }
        catch (InstantiationException e)
        {
            throw new AssertionError(e);
        }
        instance.order(order);
        return instance;
    }

    public static ByteBuffer getHollowByteBuffer()
    {
        ByteBuffer instance;
        try
        {
            instance = (ByteBuffer) unsafe.allocateInstance(BYTE_BUFFER_CLASS);
        }
        catch (InstantiationException e)
        {
            throw new AssertionError(e);
        }
        instance.order(ByteOrder.nativeOrder());
        return instance;
    }

    public static boolean isExactlyDirect(ByteBuffer buffer)
    {
        return buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    }

    public static Object getAttachment(ByteBuffer instance)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return unsafe.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
    }

    // Note: If encryption is used, the Object attached must implement sun.nio.ch.DirectBuffer
    // @see CASSANDRA-18081
    public static void setAttachment(ByteBuffer instance, Object next)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        unsafe.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
    }

    public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer)
    {
        assert source.getClass() == DIRECT_BYTE_BUFFER_CLASS || source.getClass() == RO_DIRECT_BYTE_BUFFER_CLASS;
        unsafe.putLong(hollowBuffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, unsafe.getLong(source, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_POSITION_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET));
        return hollowBuffer;
    }

    public static ByteBuffer sliceDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer, int offset, int length)
    {
        assert source.getClass() == DIRECT_BYTE_BUFFER_CLASS || source.getClass() == RO_DIRECT_BYTE_BUFFER_CLASS;
        setDirectByteBuffer(hollowBuffer, offset + unsafe.getLong(source, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET), length);
        return hollowBuffer;
    }

    public static void setDirectByteBuffer(ByteBuffer instance, long address, int length)
    {
        unsafe.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_POSITION_OFFSET, 0);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
    }

    public static void setByteBufferCapacity(ByteBuffer instance, int capacity)
    {
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, capacity);
    }

    public static long getLongByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((long) unsafe.getByte(address    )       ) << 56) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 6) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address + 7) & 0xff)      );
        }
        else
        {
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

    public static int getIntByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((int) unsafe.getByte(address    )       ) << 24) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 8 ) |
                    (((int) unsafe.getByte(address + 3) & 0xff)      );
        }
        else
        {
            return  (((int) unsafe.getByte(address + 3)       ) << 24) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }


    public static int getShortByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((int) unsafe.getByte(address    )       ) << 8) |
                    (((int) unsafe.getByte(address + 1) & 0xff)     );
        }
        else
        {
            return  (((int) unsafe.getByte(address + 1)       ) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    public static void putLongByByte(long address, long value)
    {
        if (Architecture.BIG_ENDIAN)
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

    public static void putIntByByte(long address, int value)
    {
        if (Architecture.BIG_ENDIAN)
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

    public static void setBytes(long address, ByteBuffer buffer)
    {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;

        if (buffer.isDirect())
            setBytes(getAddress(buffer) + start, address, count);
        else
            setBytes(address, buffer.array(), buffer.arrayOffset() + start, count);
    }

    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param address start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public static void setBytes(long address, byte[] buffer, int bufferOffset, int count)
    {
        assert buffer != null;
        assert !(bufferOffset < 0 || count < 0 || bufferOffset + count > buffer.length);
        setBytes(buffer, bufferOffset, address, count);
    }

    public static void setBytes(long src, long trg, long count)
    {
        while (count > 0)
        {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, trg, size);
            count -= size;
            src += size;
            trg+= size;
        }
    }

    public static void setBytes(byte[] src, int offset, long trg, long count)
    {
        while (count > 0)
        {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + offset, null, trg, size);
            count -= size;
            offset += size;
            trg += size;
        }
    }

    /**
     * Transfers count bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param address start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public static void getBytes(long address, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        unsafe.copyMemory(null, address, buffer, BYTE_ARRAY_BASE_OFFSET + bufferOffset, count);
    }
}
