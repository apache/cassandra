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
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This class is to track bytes read from given DataInput
 */
public class BytesReadTracker implements DataInput
{

    private long bytesRead;
    private final DataInput source;

    public BytesReadTracker(DataInput source)
    {
        this.source = source;
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

    public boolean readBoolean() throws IOException
    {
        boolean bool = source.readBoolean();
        bytesRead += 1;
        return bool;
    }

    public byte readByte() throws IOException
    {
        byte b = source.readByte();
        bytesRead += 1;
        return b;
    }

    public char readChar() throws IOException
    {
        char c = source.readChar();
        bytesRead += 2;
        return c;
    }

    public double readDouble() throws IOException
    {
        double d = source.readDouble();
        bytesRead += 8;
        return d;
    }

    public float readFloat() throws IOException
    {
        float f = source.readFloat();
        bytesRead += 4;
        return f;
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        source.readFully(b, off, len);
        bytesRead += len;
    }

    public void readFully(byte[] b) throws IOException
    {
        source.readFully(b);
        bytesRead += b.length;
    }

    public int readInt() throws IOException
    {
        int i = source.readInt();
        bytesRead += 4;
        return i;
    }

    public String readLine() throws IOException
    {
        // since this method is deprecated and cannot track bytes read
        // just throw exception
        throw new UnsupportedOperationException();
    }

    public long readLong() throws IOException
    {
        long l = source.readLong();
        bytesRead += 8;
        return l;
    }

    public short readShort() throws IOException
    {
        short s = source.readShort();
        bytesRead += 2;
        return s;
    }

    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    public int readUnsignedByte() throws IOException
    {
        int i = source.readUnsignedByte();
        bytesRead += 1;
        return i;
    }

    public int readUnsignedShort() throws IOException
    {
        int i = source.readUnsignedShort();
        bytesRead += 2;
        return i;
    }

    public int skipBytes(int n) throws IOException
    {
        int skipped = source.skipBytes(n);
        bytesRead += skipped;
        return skipped;
    }
}
