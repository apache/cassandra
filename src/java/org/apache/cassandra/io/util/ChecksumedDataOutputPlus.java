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
import java.util.function.Supplier;
import java.util.zip.Checksum;

public class ChecksumedDataOutputPlus implements DataOutputPlus, Checksumed
{
    private final DataOutputPlus delegate;
    private final Checksum checksum;

    public ChecksumedDataOutputPlus(DataOutputPlus delegate, Checksum checksum)
    {
        this.delegate = delegate;
        this.checksum = checksum;
    }

    public ChecksumedDataOutputPlus(DataOutputPlus delegate, Supplier<Checksum> fn)
    {
        this(delegate, fn.get());
    }

    public DataOutputPlus delegate()
    {
        return delegate;
    }

    @Override
    public Checksum checksum()
    {
        return checksum;
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        checksum.update(buffer.duplicate());
        delegate().write(buffer);
    }

    @Override
    public void write(int b) throws IOException
    {
        checksum.update(b);
        delegate().write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        checksum.update(b);
        delegate().write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        checksum.update(b, off, len);
        delegate().write(b, off, len);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        checksum.update(v);
        delegate().writeByte(v);
    }

    private byte writeBuffer[] = new byte[8];

    @Override
    public void writeLong(long v) throws IOException
    {
        writeBuffer[0] = (byte)(v >>> 56);
        writeBuffer[1] = (byte)(v >>> 48);
        writeBuffer[2] = (byte)(v >>> 40);
        writeBuffer[3] = (byte)(v >>> 32);
        writeBuffer[4] = (byte)(v >>> 24);
        writeBuffer[5] = (byte)(v >>> 16);
        writeBuffer[6] = (byte)(v >>>  8);
        writeBuffer[7] = (byte)(v >>>  0);
        checksum.update(writeBuffer);
        delegate().writeLong(v);
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        throw new UnsupportedOperationException("TODO");
    }
}
