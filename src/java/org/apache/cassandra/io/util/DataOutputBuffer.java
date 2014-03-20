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
import java.util.Arrays;


/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided.
 *
 * This class is completely thread unsafe.
 */
public final class DataOutputBuffer extends DataOutputStreamPlus
{
    public DataOutputBuffer()
    {
        this(128);
    }

    public DataOutputBuffer(int size)
    {
        super(new FastByteArrayOutputStream(size));
    }

    @Override
    public void write(int b)
    {
        try
        {
            super.write(b);
        }
        catch (IOException e)
        {
            throw new AssertionError(e); // FBOS does not throw IOE
        }
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        try
        {
            super.write(b, off, len);
        }
        catch (IOException e)
        {
            throw new AssertionError(e); // FBOS does not throw IOE
        }
    }

    public void write(ByteBuffer buffer) throws IOException
    {
        ((FastByteArrayOutputStream) out).write(buffer);
    }

    /**
     * Returns the current contents of the buffer. Data is only valid to
     * {@link #getLength()}.
     */
    public byte[] getData()
    {
        return ((FastByteArrayOutputStream) out).buf;
    }

    public byte[] toByteArray()
    {
        FastByteArrayOutputStream out = (FastByteArrayOutputStream) this.out;
        return Arrays.copyOfRange(out.buf, 0, out.count);

    }

    public ByteBuffer asByteBuffer()
    {
        FastByteArrayOutputStream out = (FastByteArrayOutputStream) this.out;
        return ByteBuffer.wrap(out.buf, 0, out.count);
    }

    /** Returns the length of the valid data currently in the buffer. */
    public int getLength()
    {
        return ((FastByteArrayOutputStream) out).count;
    }
}
