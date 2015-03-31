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


/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided.
 *
 * This class is completely thread unsafe.
 */
public class DataOutputBuffer extends BufferedDataOutputStreamPlus
{
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

    @Override
    protected void doFlush() throws IOException
    {
        reallocate(buffer.capacity() * 2);
    }

    protected void reallocate(long newSize)
    {
        assert newSize <= Integer.MAX_VALUE;
        ByteBuffer newBuffer = ByteBuffer.allocate((int) newSize);
        buffer.flip();
        newBuffer.put(buffer);
        buffer = newBuffer;
    }

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        return new GrowingChannel();
    }

    private final class GrowingChannel implements WritableByteChannel
    {
        public int write(ByteBuffer src) throws IOException
        {
            int count = src.remaining();
            reallocate(Math.max((buffer.capacity() * 3) / 2, buffer.capacity() + count));
            buffer.put(src);
            return count;
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close() throws IOException
        {
        }
    }

    @Override
    public void close() throws IOException
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
        return buffer.array();
    }

    public int getLength()
    {
        return buffer.position();
    }

    public byte[] toByteArray()
    {
        ByteBuffer buffer = buffer();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
