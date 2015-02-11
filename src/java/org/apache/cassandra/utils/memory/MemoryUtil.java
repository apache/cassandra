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

import com.google.common.primitives.*;
import sun.misc.Unsafe;

public abstract class MemoryUtil
{
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    private static final Unsafe unsafe;
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static final boolean UNALIGNED;
    public static final boolean INVERTED_ORDER;

    static
    {
        String arch = System.getProperty("os.arch");
        UNALIGNED = arch.equals("i386") || arch.equals("x86")
                || arch.equals("amd64") || arch.equals("x86_64");
        INVERTED_ORDER = UNALIGNED && !BIG_ENDIAN;
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_CLASS = clazz;
            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static void setByte(long address, byte b)
    {
        unsafe.putByte(address, b);
    }

    public static void setShort(long address, short s)
    {
        unsafe.putShort(address, s);
    }

    public static void setInt(long address, int l)
    {
        if (UNALIGNED)
            unsafe.putInt(address, l);
        else
            putIntByByte(address, l);
    }

    public static void setLong(long address, long l)
    {
        if (UNALIGNED)
            unsafe.putLong(address, l);
        else
            putLongByByte(address, l);
    }

    public static byte getByte(long address)
    {
        return unsafe.getByte(address);
    }

    public static int getShort(long address)
    {
        return UNALIGNED ? unsafe.getShort(address) : getShortByByte(address);
    }

    public static int getInt(long address)
    {
        return UNALIGNED ? unsafe.getInt(address) : getIntByByte(address);
    }

    public static long getLong(long address)
    {
        return UNALIGNED ? unsafe.getLong(address) : getLongByByte(address);
    }

    public static ByteBuffer getByteBuffer(long address, int length)
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

        unsafe.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
        instance.order(ByteOrder.nativeOrder());
        return instance;
    }

    public static long getLongByByte(long address)
    {
        if (BIG_ENDIAN)
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
        if (BIG_ENDIAN)
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
        if (BIG_ENDIAN)
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
        if (BIG_ENDIAN)
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
        if (BIG_ENDIAN)
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
            setBytes(unsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET) + start, address, count);
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
