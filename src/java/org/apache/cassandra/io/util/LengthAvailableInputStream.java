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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * a FilterInputStream that returns the remaining bytes to read from available()
 * regardless of whether the device is ready to provide them.
 */
public class LengthAvailableInputStream extends FilterInputStream
{
    private long remainingBytes;

    public LengthAvailableInputStream(InputStream in, long totalLength)
    {
        super(in);
        remainingBytes = totalLength;
    }

    @Override
    public int read() throws IOException
    {
        int b = in.read();
        --remainingBytes;
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        int length = in.read(b);
        remainingBytes -= length;
        return length;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int length = in.read(b, off, len);
        remainingBytes -= length;
        return length;
    }

    @Override
    public long skip(long n) throws IOException
    {
        long length = in.skip(n);
        remainingBytes -= length;
        return length;
    }

    @Override
    public int available() throws IOException
    {
        return (remainingBytes <= 0) ? 0 : ((remainingBytes > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)remainingBytes);
    }

    @Override
    public void close() throws IOException
    {
        in.close();
    }

    @Override
    public synchronized void mark(int readlimit)
    {
    }

    @Override
    public synchronized void reset() throws IOException
    {
        throw new IOException("Mark/Reset not supported");
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }
}
