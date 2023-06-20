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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.TypeSizes;

/**
 * This class is to track bytes read from given DataInput
 */
public class TrackedDataInputPlus implements DataInputPlus, BytesReadTracker
{
    private long bytesRead;
    private final long limit;
    final DataInput source;

    /**
     * Create a TrackedDataInputPlus from given DataInput with no limit of bytes to read
     */
    public TrackedDataInputPlus(DataInput source)
    {
        this(source, -1);
    }

    /**
     * Create a TrackedDataInputPlus from given DataInput with limit of bytes to read. If limit is reached
     * {@link IOException} will be thrown when trying to read more bytes.
     */
    public TrackedDataInputPlus(DataInput source, long limit)
    {
        this.source = source;
        this.limit = limit;
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
        checkCanRead(TypeSizes.BOOL_SIZE);
        boolean bool = source.readBoolean();
        bytesRead += TypeSizes.BOOL_SIZE;
        return bool;
    }

    public byte readByte() throws IOException
    {
        checkCanRead(TypeSizes.BYTE_SIZE);
        byte b = source.readByte();
        bytesRead += TypeSizes.BYTE_SIZE;
        return b;
    }

    public char readChar() throws IOException
    {
        checkCanRead(TypeSizes.CHAR_SIZE);
        char c = source.readChar();
        bytesRead += TypeSizes.CHAR_SIZE;
        return c;
    }

    public double readDouble() throws IOException
    {
        checkCanRead(TypeSizes.DOUBLE_SIZE);
        double d = source.readDouble();
        bytesRead += TypeSizes.DOUBLE_SIZE;
        return d;
    }

    public float readFloat() throws IOException
    {
        checkCanRead(TypeSizes.FLOAT_SIZE);
        float f = source.readFloat();
        bytesRead += TypeSizes.FLOAT_SIZE;
        return f;
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        checkCanRead(len);
        source.readFully(b, off, len);
        bytesRead += len;
    }

    public void readFully(byte[] b) throws IOException
    {
        checkCanRead(b.length);
        source.readFully(b);
        bytesRead += b.length;
    }

    public int readInt() throws IOException
    {
        checkCanRead(TypeSizes.INT_SIZE);
        int i = source.readInt();
        bytesRead += TypeSizes.INT_SIZE;
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
        checkCanRead(TypeSizes.LONG_SIZE);
        long l = source.readLong();
        bytesRead += TypeSizes.LONG_SIZE;
        return l;
    }

    public short readShort() throws IOException
    {
        checkCanRead(TypeSizes.SHORT_SIZE);
        short s = source.readShort();
        bytesRead += TypeSizes.SHORT_SIZE;
        return s;
    }

    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    public int readUnsignedByte() throws IOException
    {
        checkCanRead(TypeSizes.BYTE_SIZE);
        int i = source.readUnsignedByte();
        bytesRead += TypeSizes.BYTE_SIZE;
        return i;
    }

    public int readUnsignedShort() throws IOException
    {
        checkCanRead(TypeSizes.SHORT_SIZE);
        int i = source.readUnsignedShort();
        bytesRead += TypeSizes.SHORT_SIZE;
        return i;
    }

    public int skipBytes(int n) throws IOException
    {
        int skipped = source.skipBytes(limit < 0 ? n : (int) Math.min(limit - bytesRead, n));
        bytesRead += skipped;
        return skipped;
    }

    @Inline
    private void checkCanRead(int size) throws IOException
    {
        if (limit >= 0 && bytesRead + size > limit)
        {
            skipBytes((int) (limit - bytesRead));
            throw new EOFException("EOF after " + (limit - bytesRead) + " bytes out of " + size);
        }
    }
}
