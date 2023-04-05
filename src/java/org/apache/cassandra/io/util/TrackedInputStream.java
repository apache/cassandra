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
 * This class is to track bytes read from given DataInputStream
 */
public class TrackedInputStream extends FilterInputStream implements BytesReadTracker
{
    private long bytesRead;

    public TrackedInputStream(InputStream source)
    {
        super(source);
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    /**
     * reset counter to @param count
     */
    public void reset(long count)
    {
        bytesRead = count;
    }

    public int read() throws IOException
    {
        int read = super.read();
        bytesRead += 1;
        return read;
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int read = super.read(b, off, len);
        bytesRead += read;
        return read;
    }

    public int read(byte[] b) throws IOException
    {
        int read = super.read(b);
        bytesRead += read;
        return read;
    }

    public long skip(long n) throws IOException
    {
        long skip = super.skip(n);
        bytesRead += skip;
        return skip;
    }
}
