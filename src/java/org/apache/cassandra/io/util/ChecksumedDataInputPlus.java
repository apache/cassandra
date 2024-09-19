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
import java.util.function.Supplier;
import java.util.zip.Checksum;

public class ChecksumedDataInputPlus implements DataInputPlus, Checksumed
{
    private final DataInputPlus delegate;
    private final Checksum checksum;

    public ChecksumedDataInputPlus(DataInputPlus delegate, Checksum checksum)
    {
        this.delegate = delegate;
        this.checksum = checksum;
    }

    public ChecksumedDataInputPlus(DataInputPlus delegate, Supplier<Checksum> fn)
    {
        this(delegate, fn.get());
    }

    public DataInputPlus delegate()
    {
        return delegate;
    }

    @Override
    public Checksum checksum()
    {
        return checksum;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        delegate().readFully(b);
        checksum.update(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        delegate().readFully(b, off, len);
        checksum.update(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        int skipped = delegate().skipBytes(n);
        checksum.reset();
        return skipped;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        int value = delegate().readUnsignedByte();
        checksum.update(value);
        return value;
    }

    private byte writeBuffer[] = new byte[8];

    @Override
    public long readLong() throws IOException
    {
        long v = delegate().readLong();
        writeBuffer[0] = (byte)(v >>> 56);
        writeBuffer[1] = (byte)(v >>> 48);
        writeBuffer[2] = (byte)(v >>> 40);
        writeBuffer[3] = (byte)(v >>> 32);
        writeBuffer[4] = (byte)(v >>> 24);
        writeBuffer[5] = (byte)(v >>> 16);
        writeBuffer[6] = (byte)(v >>>  8);
        writeBuffer[7] = (byte)(v >>>  0);
        checksum.update(writeBuffer);
        return v;
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
