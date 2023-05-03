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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.util.concurrent.FastThreadLocal;

import static org.apache.cassandra.config.CassandraRelevantProperties.DATA_OUTPUT_BUFFER_ALLOCATE_TYPE;
import static org.apache.cassandra.config.CassandraRelevantProperties.DOB_DOUBLING_THRESHOLD_MB;
import static org.apache.cassandra.config.CassandraRelevantProperties.DOB_MAX_RECYCLE_BYTES;

/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided.
 *
 * This class is completely thread unsafe.
 */
public class DataOutputBuffer extends BufferedDataOutputStreamPlus
{
    /*
     * Threshold at which resizing transitions from doubling to increasing by 50%
     */
    static final long DOUBLING_THRESHOLD = DOB_DOUBLING_THRESHOLD_MB.getLong();

    /*
     * Only recycle OutputBuffers up to 1Mb. Larger buffers will be trimmed back to this size.
     */
    private static final int MAX_RECYCLE_BUFFER_SIZE = DOB_MAX_RECYCLE_BYTES.getInt();
    private enum AllocationType { DIRECT, ONHEAP }
    private static final AllocationType ALLOCATION_TYPE = DATA_OUTPUT_BUFFER_ALLOCATE_TYPE.getEnum(AllocationType.DIRECT);

    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 128;

    /**
     * Scratch buffers used mostly for serializing in memory. It's important to call #close() when finished
     * to keep the memory overhead from being too large in the system.
     */
    public static final FastThreadLocal<DataOutputBuffer> scratchBuffer = new FastThreadLocal<DataOutputBuffer>()
    {
        @Override
        protected DataOutputBuffer initialValue()
        {
            return new DataOutputBuffer()
            {
                @Override
                public void close()
                {
                    if (buffer != null && buffer.capacity() <= MAX_RECYCLE_BUFFER_SIZE)
                    {
                        buffer.clear();
                    }
                    else
                    {
                        setBuffer(allocate(DEFAULT_INITIAL_BUFFER_SIZE));
                    }
                }

                @Override
                protected ByteBuffer allocate(int size)
                {
                    return ALLOCATION_TYPE == AllocationType.DIRECT ?
                           ByteBuffer.allocateDirect(size) :
                           ByteBuffer.allocate(size);
                }
            };
        }
    };

    public DataOutputBuffer()
    {
        super(DEFAULT_INITIAL_BUFFER_SIZE);
    }

    public DataOutputBuffer(int size)
    {
        super(size);
    }

    public DataOutputBuffer(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public void flush()
    {

    }

    //The actual value observed in Hotspot is only -2
    //ByteArrayOutputStream uses -8
    @VisibleForTesting
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    @VisibleForTesting
    static int saturatedArraySizeCast(long size)
    {
        Preconditions.checkArgument(size >= 0);
        return (int)Math.min(MAX_ARRAY_SIZE, size);
    }

    @VisibleForTesting
    static int checkedArraySizeCast(long size)
    {
        Preconditions.checkArgument(size >= 0);
        Preconditions.checkArgument(size <= MAX_ARRAY_SIZE);
        return (int)size;
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        expandToFit(count);
    }

    //Hack for test, make it possible to override checking the buffer capacity
    @VisibleForTesting
    long capacity()
    {
        return buffer.capacity();
    }

    @VisibleForTesting
    long validateReallocation(long newSize)
    {
        int saturatedSize = saturatedArraySizeCast(newSize);
        if (saturatedSize <= capacity())
            throw new BufferOverflowException();
        return saturatedSize;
    }

    @VisibleForTesting
    long calculateNewSize(long count)
    {
        long capacity = capacity();
        //Both sides of this max expression need to use long arithmetic to avoid integer overflow
        //count and capacity are longs so that ensures it right now.
        long newSize = capacity + count;

        //For large buffers don't double, increase by 50%
        if (capacity > 1024L * 1024L * DOUBLING_THRESHOLD)
            newSize = Math.max((capacity * 3L) / 2L, newSize);
        else
            newSize = Math.max(capacity * 2L, newSize);

        return validateReallocation(newSize);
    }

    protected void expandToFit(long count)
    {
        if (count <= 0)
            return;
        ByteBuffer newBuffer = allocate(checkedArraySizeCast(calculateNewSize(count)));
        buffer.flip();
        newBuffer.put(buffer);
        setBuffer(newBuffer);
    }

    protected void setBuffer(ByteBuffer newBuffer)
    {
        FileUtils.clean(buffer); // free if direct
        buffer = newBuffer;
    }

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        return new GrowingChannel();
    }

    public void clear()
    {
        buffer.clear();
    }

    @VisibleForTesting
    final class GrowingChannel implements WritableByteChannel
    {
        public int write(ByteBuffer src)
        {
            int count = src.remaining();
            expandToFit(count);
            buffer.put(src);
            return count;
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close()
        {
        }
    }

    @Override
    public void close()
    {
    }

    public ByteBuffer buffer()
    {
        return buffer(true);
    }

    public ByteBuffer buffer(boolean duplicate)
    {
        if (!duplicate)
        {
            ByteBuffer buf = buffer;
            buf.flip();
            buffer = null;
            return buf;
        }

        ByteBuffer result = buffer.duplicate();
        result.flip();
        return result;
    }

    /**
     * Gets the underlying ByteBuffer and calls {@link ByteBuffer#flip()}.  This method is "unsafe" in the sense that
     * it returns the underlying buffer, which may be modified by other methods after calling this method (or cleared on
     * {@link #close()}). If the calling logic knows that no new calls to this object will happen after calling this
     * method, then this method can avoid the copying done in {@link #asNewBuffer()}, and {@link #buffer()}.
     */
    public ByteBuffer unsafeGetBufferAndFlip()
    {
        buffer.flip();
        return buffer;
    }

    public byte[] getData()
    {
        assert buffer.arrayOffset() == 0;
        return buffer.array();
    }

    public int getLength()
    {
        return buffer.position();
    }

    public boolean hasPosition()
    {
        return true;
    }

    public long position()
    {
        return getLength();
    }

    public ByteBuffer asNewBuffer()
    {
        return ByteBuffer.wrap(toByteArray());
    }

    public byte[] toByteArray()
    {
        ByteBuffer buffer = buffer();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
