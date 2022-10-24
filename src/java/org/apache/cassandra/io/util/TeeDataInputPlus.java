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

public class TeeDataInputPlus implements DataInputPlus
{
    private final DataInputPlus source;
    private final DataOutputPlus teeBuffer;

    public TeeDataInputPlus(DataInputPlus source, DataOutputPlus teeBuffer)
    {
        this.source = source;
        this.teeBuffer = teeBuffer;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException
    {
        source.readFully(bytes);
        teeBuffer.write(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int i, int i1) throws IOException
    {
        source.readFully(bytes, i, i1);
        teeBuffer.write(bytes, i, i1);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        for (int i = 0; i < n; i++)
        {
            try
            {
                byte v = source.readByte();
                teeBuffer.writeByte(v);
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
        teeBuffer.writeBoolean(v);
        return v;
    }

    @Override
    public byte readByte() throws IOException
    {
        byte v = source.readByte();
        teeBuffer.writeByte(v);
        return v;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        int v = source.readUnsignedByte();
        teeBuffer.write(v);
        return v;
    }

    @Override
    public short readShort() throws IOException
    {
        short v = source.readShort();
        teeBuffer.writeShort(v);
        return v;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int v = source.readUnsignedShort();
        teeBuffer.writeShort(v);
        return v;
    }

    @Override
    public char readChar() throws IOException
    {
        char v = source.readChar();
        teeBuffer.writeChar(v);
        return v;
    }

    @Override
    public int readInt() throws IOException
    {
        int v = source.readInt();
        teeBuffer.writeInt(v);
        return v;
    }

    @Override
    public long readLong() throws IOException
    {
        long v = source.readLong();
        teeBuffer.writeLong(v);
        return v;
    }

    @Override
    public float readFloat() throws IOException
    {
        float v = source.readFloat();
        teeBuffer.writeFloat(v);
        return v;
    }

    @Override
    public double readDouble() throws IOException
    {
        double v = source.readDouble();
        teeBuffer.writeDouble(v);
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
        teeBuffer.writeUTF(v);
        return v;
    }

    @Override
    public long readVInt() throws IOException
    {
        long r = source.readVInt();
        teeBuffer.writeVInt(r);
        return r;
    }

    @Override
    public long readUnsignedVInt() throws IOException
    {
        long r = source.readUnsignedVInt();
        teeBuffer.writeUnsignedVInt(r);
        return r;
    }

    @Override
    public void skipBytesFully(int n) throws IOException
    {
        source.skipBytesFully(n);
        for (int i = 0; i < n; i++)
            teeBuffer.writeByte(0);
    }
}
