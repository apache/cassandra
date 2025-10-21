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

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public abstract class MemoryUtil
{
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    protected static final Unsafe unsafe;
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS, RO_DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
    protected static final Class<?> BYTE_BUFFER_CLASS;
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

    public static byte getByte(long address)
    {
        return unsafe.getByte(address);
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

    /**
     * Transfers count bytes to Memory starting at memoryOffset from ByteBuffer starting at bufferOffset
     *
     * @param targetAddress target start offset in the memory
     * @param sourceBuffer the source data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public static void setBytes(long targetAddress, ByteBuffer sourceBuffer, int bufferOffset, int count)
    {
        if (count == 0)
            return;
        int start = sourceBuffer.position() + bufferOffset;

        if (sourceBuffer.isDirect())
            setBytes(getAddress(sourceBuffer) + start, targetAddress, count);
        else
            setBytes(targetAddress, sourceBuffer.array(), sourceBuffer.arrayOffset() + start, count);
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

    /**
     * Transfers count bytes from Memory starting at address to ByteBuffer starting at bufferOffset
     *
     * @param sourceAddress start offset in the memory
     * @param targetBuffer the target data buffer
     * @param bufferOffset start offset of the buffer
     * @param length number of bytes to transfer
     */
    public static void getBytes(long sourceAddress, ByteBuffer targetBuffer, int bufferOffset, int length)
    {
        if (targetBuffer == null)
            throw new NullPointerException();
        else if (length < 0 || length > targetBuffer.remaining())
            throw new IndexOutOfBoundsException();
        else if (length == 0)
            return;

        Object obj;
        long offset;
        if (targetBuffer.hasArray())
        {
            obj = targetBuffer.array();
            offset = BYTE_ARRAY_BASE_OFFSET + targetBuffer.arrayOffset();
        }
        else
        {
            obj = null;
            offset = unsafe.getLong(targetBuffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
        }
        offset += targetBuffer.position();
        offset += bufferOffset;

        unsafe.copyMemory(null, sourceAddress, obj, offset, length);
    }

    /**
     * Transfers count bytes from Memory starting at address to ByteBuffer
     *
     * @param sourceAddress start offset in the memory
     * @param targetBuffer the target data buffer
     * @param length number of bytes to transfer
     */
    public static void getBytes(long sourceAddress, ByteBuffer targetBuffer, int length)
    {
        getBytes(sourceAddress, targetBuffer, 0, length);
    }

    public static void clean(ByteBuffer buffer)
    {
        if (buffer == null || !buffer.isDirect())
            return;

        DirectBuffer db = (DirectBuffer) buffer;
        if (db.attachment() != null)
            return; // duplicate or slice

        unsafe.invokeCleaner(buffer);
    }
}
