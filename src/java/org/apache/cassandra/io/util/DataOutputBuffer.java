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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.config.Config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
    private static final long DOUBLING_THRESHOLD = Long.getLong(Config.PROPERTY_PREFIX + "DOB_DOUBLING_THRESHOLD_MB", 64);

    public DataOutputBuffer()
    {
        this(128);
    }

    public DataOutputBuffer(int size)
    {
        super(ByteBuffer.allocate(size));
    }

    protected DataOutputBuffer(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public void flush() throws IOException
    {
        throw new UnsupportedOperationException();
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
        reallocate(count);
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
            throw new RuntimeException();
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

    protected void reallocate(long count)
    {
        if (count <= 0)
            return;
        ByteBuffer newBuffer = ByteBuffer.allocate(checkedArraySizeCast(calculateNewSize(count)));
        buffer.flip();
        newBuffer.put(buffer);
        buffer = newBuffer;
    }

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        return new GrowingChannel();
    }

    @VisibleForTesting
    final class GrowingChannel implements WritableByteChannel
    {
        public int write(ByteBuffer src) throws IOException
        {
            int count = src.remaining();
            reallocate(count);
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
        ByteBuffer result = buffer.duplicate();
        result.flip();
        return result;
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

    public byte[] toByteArray()
    {
        ByteBuffer buffer = buffer();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
