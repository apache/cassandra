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

package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;

public class ByteArrayAccessor implements ValueAccessor<byte[]>
{
    public static final ValueAccessor<byte[]> instance = new ByteArrayAccessor();
    private static final byte[] EMPTY = new byte[0];
    private static final byte[] TRUE = new byte[]{1};
    private static final byte[] FALSE = new byte[]{0};

    private ByteArrayAccessor() {}

    public int size(byte[] value)
    {
        return value.length;
    }

    public BackingKind getBackingKind()
    {
        return BackingKind.ARRAY;
    }

    public byte[][] createArray(int length)
    {
        return new byte[length][];
    }

    public void write(byte[] value, DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    public void write(byte[] value, ByteBuffer out)
    {
        out.put(value);
    }

    public byte[] read(DataInputPlus in, int length) throws IOException
    {
        byte[] b = new byte[length];
        in.readFully(b);
        return b;
    }

    public byte[] slice(byte[] input, int offset, int length)
    {
        return Arrays.copyOfRange(input, offset, offset + length);
    }

    public int compareUnsigned(byte[] left, byte[] right)
    {
        return ByteArrayUtil.compareUnsigned(left, right);
    }

    public ByteBuffer toBuffer(byte[] value)
    {
        return ByteBuffer.wrap(value);
    }

    public ByteBuffer toSafeBuffer(byte[] value)
    {
        return ByteBuffer.wrap(value);
    }

    public byte[] toArray(byte[] value)
    {
        return value;
    }

    public byte[] toArray(byte[] value, int offset, int length)
    {
        if (offset == 0 && length == value.length)
            return value;
        return slice(value, offset, length);
    }

    public String toString(byte[] value, Charset charset) throws CharacterCodingException
    {
        return new String(value, charset);
    }

    public String toHex(byte[] value)
    {
        return Hex.bytesToHex(value);
    }

    public byte toByte(byte[] value)
    {
        return value[0];
    }

    public byte getByte(byte[] value, int offset)
    {
        return value[offset];
    }

    public short toShort(byte[] value)
    {
        return getShort(value, 0);
    }

    public short getShort(byte[] value, int offset)
    {
        return ByteArrayUtil.getShort(value, offset);
    }

    public int toInt(byte[] value)
    {
        return getInt(value, 0);
    }

    public int getInt(byte[] value, int offset)
    {
        return ByteArrayUtil.getInt(value, offset);
    }

    public long toLong(byte[] value)
    {
        return getLong(value, 0);
    }

    public long getLong(byte[] value, int offset)
    {
        return ByteArrayUtil.getLong(value, offset);
    }

    public float toFloat(byte[] value)
    {
        return ByteArrayUtil.getFloat(value, 0);
    }

    public double toDouble(byte[] value)
    {
        return ByteArrayUtil.getDouble(value, 0);
    }

    public UUID toUUID(byte[] value)
    {
        return new UUID(getLong(value, 0), getLong(value, 8));
    }

    public byte[] empty()
    {
        return EMPTY;
    }

    public byte[] valueOf(byte[] bytes)
    {
        return bytes;
    }

    public byte[] valueOf(ByteBuffer bytes)
    {
        return ByteBufferUtil.getArray(bytes);
    }

    public byte[] valueOf(String s, Charset charset)
    {
        return ByteArrayUtil.bytes(s, charset);
    }

    public byte[] valueOf(UUID v)
    {
        return UUIDGen.decompose(v);
    }

    public byte[] valueOf(boolean v)
    {
        return v ? TRUE : FALSE;
    }

    public byte[] valueOf(byte v)
    {
        return ByteArrayUtil.bytes(v);
    }

    public byte[] valueOf(short v)
    {
        return ByteArrayUtil.bytes(v);
    }

    public byte[] valueOf(int v)
    {
        return ByteArrayUtil.bytes(v);
    }

    public byte[] valueOf(long v)
    {
        return ByteArrayUtil.bytes(v);
    }

    public byte[] valueOf(float v)
    {
        return ByteArrayUtil.bytes(v);
    }

    public byte[] valueOf(double v)
    {
        return ByteArrayUtil.bytes(v);
    }
}
