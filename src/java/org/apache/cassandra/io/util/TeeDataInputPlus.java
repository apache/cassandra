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

import java.io.EOFException;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.Throwables;

/**
 * DataInput that also stores the raw inputs into an output buffer
 * This is useful for storing serialized buffers as they are deserialized.
 *
 * Note: If a non-zero limit is included it is important to for callers to check {@link #isLimitReached()}
 * before using the tee buffer as it could be cropped.
 */
public class TeeDataInputPlus implements DataInputPlus
{
    private final DataInputPlus source;
    private final DataOutputPlus teeBuffer;

    private final long limit;
    private boolean limitReached;

    public TeeDataInputPlus(DataInputPlus source, DataOutputPlus teeBuffer)
    {
        this(source, teeBuffer, 0);
    }

    public TeeDataInputPlus(DataInputPlus source, DataOutputPlus teeBuffer, long limit)
    {
        assert source != null && teeBuffer != null;
        this.source = source;
        this.teeBuffer = teeBuffer;
        this.limit = limit;
        this.limitReached = false;
    }

    private void maybeWrite(int length, Throwables.DiscreteAction<IOException> writeAction) throws IOException
    {
        if (limit <= 0 || (!limitReached && (teeBuffer.position() + length) < limit))
            writeAction.perform();
        else
            limitReached = true;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException
    {
        source.readFully(bytes);
        maybeWrite(bytes.length, () -> teeBuffer.write(bytes));
    }

    @Override
    public void readFully(byte[] bytes, int offset, int length) throws IOException
    {
        source.readFully(bytes, offset, length);
        maybeWrite(length, () -> teeBuffer.write(bytes, offset, length));
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        for (int i = 0; i < n; i++)
        {
            try
            {
                byte v = source.readByte();
                maybeWrite(TypeSizes.BYTE_SIZE, () -> teeBuffer.writeByte(v));
            }
            catch (EOFException eof)
            {
                return i;
            }
        }
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        boolean v = source.readBoolean();
        maybeWrite(TypeSizes.BOOL_SIZE, () -> teeBuffer.writeBoolean(v));
        return v;
    }

    @Override
    public byte readByte() throws IOException
    {
        byte v = source.readByte();
        maybeWrite(TypeSizes.BYTE_SIZE, () -> teeBuffer.writeByte(v));
        return v;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        int v = source.readUnsignedByte();
        maybeWrite(TypeSizes.BYTE_SIZE, () -> teeBuffer.writeByte(v));
        return v;
    }

    @Override
    public short readShort() throws IOException
    {
        short v = source.readShort();
        maybeWrite(TypeSizes.SHORT_SIZE, () -> teeBuffer.writeShort(v));
        return v;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int v = source.readUnsignedShort();
        maybeWrite(TypeSizes.SHORT_SIZE, () -> teeBuffer.writeShort(v));
        return v;
    }

    @Override
    public char readChar() throws IOException
    {
        char v = source.readChar();
        maybeWrite(TypeSizes.BYTE_SIZE, () -> teeBuffer.writeChar(v));
        return v;
    }

    @Override
    public int readInt() throws IOException
    {
        int v = source.readInt();
        maybeWrite(TypeSizes.INT_SIZE, () -> teeBuffer.writeInt(v));
        return v;
    }

    @Override
    public long readLong() throws IOException
    {
        long v = source.readLong();
        maybeWrite(TypeSizes.LONG_SIZE, () -> teeBuffer.writeLong(v));
        return v;
    }

    @Override
    public float readFloat() throws IOException
    {
        float v = source.readFloat();
        maybeWrite(TypeSizes.FLOAT_SIZE, () -> teeBuffer.writeFloat(v));
        return v;
    }

    @Override
    public double readDouble() throws IOException
    {
        double v = source.readDouble();
        maybeWrite(TypeSizes.DOUBLE_SIZE, () -> teeBuffer.writeDouble(v));
        return v;
    }

    @Override
    public String readLine() throws IOException
    {
        //This one isn't safe since we know the actual line termination type
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        String v = source.readUTF();
        maybeWrite(TypeSizes.sizeof(v), () -> teeBuffer.writeUTF(v));
        return v;
    }

    @Override
    public long readVInt() throws IOException
    {
        long v = source.readVInt();
        maybeWrite(TypeSizes.sizeofVInt(v), () -> teeBuffer.writeVInt(v));
        return v;
    }

    @Override
    public long readUnsignedVInt() throws IOException
    {
        long v = source.readUnsignedVInt();
        maybeWrite(TypeSizes.sizeofUnsignedVInt(v), () -> teeBuffer.writeUnsignedVInt(v));
        return v;
    }

    @Override
    public void skipBytesFully(int n) throws IOException
    {
        source.skipBytesFully(n);
        maybeWrite(n, () -> {
            for (int i = 0; i < n; i++)
                teeBuffer.writeByte(0);
        });
    }

    /**
     * Used to detect if the teeBuffer hit the supplied limit.
     * If true this means the teeBuffer does not contain the full input.
     */
    public boolean isLimitReached()
    {
        return limitReached;
    }
}
