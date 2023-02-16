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

import org.apache.cassandra.utils.vint.VIntCoding;

public class TrackedDataOutputPlus implements DataOutputPlus
{
    private final DataOutputPlus out;
    private int position = 0;

    private TrackedDataOutputPlus(DataOutputPlus out)
    {
        this.out = out;
    }

    public static TrackedDataOutputPlus wrap(DataOutputPlus out)
    {
        return new TrackedDataOutputPlus(out);
    }

    @Override
    public void write(int b) throws IOException
    {
        out.write(b);
        position += 1;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        out.write(b);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        out.write(b, off, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        out.writeBoolean(v);
        position += 1;
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        out.writeByte(v);
        position += 1;
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        out.writeShort(v);
        position += 2;
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        out.writeChar(v);
        position += 2;
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        out.writeInt(v);
        position += 4;
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        out.writeLong(v);
        position += 8;
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        out.writeFloat(v);
        position += 4;
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        out.writeDouble(v);
        position += 8;
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        out.writeBytes(s);
        position += s.length();
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        out.writeChars(s);
        position += s.length() * 2;
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        out.write(buffer);
        position += buffer.remaining();
    }

    @Override
    public void write(ReadableMemory memory, long offset, long length) throws IOException
    {
        out.write(memory, offset, length);
        position += length;
    }

    @Override
    public void writeVInt(long i) throws IOException
    {
        VIntCoding.writeVInt(i, this);
    }

    @Override
    public void writeUnsignedVInt(long i) throws IOException
    {
        VIntCoding.writeUnsignedVInt(i, this);
    }

    @Override
    public void writeMostSignificantBytes(long register, int bytes) throws IOException
    {
        out.writeMostSignificantBytes(register, bytes);
        position += bytes;
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public boolean hasPosition()
    {
        return true;
    }
}
