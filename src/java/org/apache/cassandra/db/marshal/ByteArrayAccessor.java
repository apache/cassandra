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

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;

public class ByteArrayAccessor implements ValueAccessor<byte[]>
{
    public static final ValueAccessor<byte[]> instance = new ByteArrayAccessor();
    public static final ObjectFactory<byte[]> factory = ByteArrayObjectFactory.instance;
    private static final byte[] EMPTY = new byte[0];

    private ByteArrayAccessor() {}

    @Override
    public int size(byte[] value)
    {
        return value.length;
    }

    @Override
    public byte[][] createArray(int length)
    {
        return new byte[length][];
    }

    @Override
    public void write(byte[] value, DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    @Override
    public void write(byte[] value, ByteBuffer out)
    {
        out.put(value);
    }

    @Override
    public <V2> int copyTo(byte[] src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size)
    {
        dstAccessor.copyByteArrayTo(src, srcOffset, dst, dstOffset, size);
        return size;
    }

    @Override
    public int copyByteArrayTo(byte[] src, int srcOffset, byte[] dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, srcOffset, dst, dstOffset, size);
        return size;
    }

    @Override
    public int copyByteBufferTo(ByteBuffer src, int srcOffset, byte[] dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, src.position() + srcOffset, dst, dstOffset, size);
        return size;
    }

    @Override
    public void digest(byte[] value, int offset, int size, Digest digest)
    {
        digest.update(value, offset, size);
    }

    @Override
    public byte[] read(DataInputPlus in, int length) throws IOException
    {
        byte[] b = new byte[length];
        in.readFully(b);
        return b;
    }

    @Override
    public byte[] slice(byte[] input, int offset, int length)
    {
        return Arrays.copyOfRange(input, offset, offset + length);
    }

    @Override
    public <V2> int compare(byte[] left, V2 right, ValueAccessor<V2> accessorR)
    {
        return accessorR.compareByteArrayTo(left, right);
    }

    @Override
    public int compareByteArrayTo(byte[] left, byte[] right)
    {
        return ByteArrayUtil.compareUnsigned(left, right);
    }

    @Override
    public int compareByteBufferTo(ByteBuffer left, byte[] right)
    {
        return ByteBufferUtil.compare(left, right);
    }

    @Override
    public ByteBuffer toBuffer(byte[] value)
    {
        if (value == null)
            return null;
        return ByteBuffer.wrap(value);
    }

    @Override
    public byte[] toArray(byte[] value)
    {
        return value;
    }

    @Override
    public byte[] toArray(byte[] value, int offset, int length)
    {
        if (value == null)
            return null;
        if (offset == 0 && length == value.length)
            return value;
        return slice(value, offset, length);
    }

    @Override
    public String toString(byte[] value, Charset charset) throws CharacterCodingException
    {
        return new String(value, charset);
    }

    @Override
    public String toHex(byte[] value)
    {
        return Hex.bytesToHex(value);
    }

    @Override
    public byte toByte(byte[] value)
    {
        return value[0];
    }

    @Override
    public byte getByte(byte[] value, int offset)
    {
        return value[offset];
    }

    @Override
    public short toShort(byte[] value)
    {
        return getShort(value, 0);
    }

    @Override
    public short getShort(byte[] value, int offset)
    {
        return ByteArrayUtil.getShort(value, offset);
    }

    @Override
    public int getUnsignedShort(byte[] value, int offset)
    {
        return ByteArrayUtil.getUnsignedShort(value, offset);
    }

    @Override
    public int toInt(byte[] value)
    {
        return getInt(value, 0);
    }

    @Override
    public int getInt(byte[] value, int offset)
    {
        return ByteArrayUtil.getInt(value, offset);
    }

    @Override
    public float getFloat(byte[] value, int offset)
    {
        return ByteArrayUtil.getFloat(value, offset);
    }

    @Override
    public double getDouble(byte[] value, int offset)
    {
        return ByteArrayUtil.getDouble(value, offset);
    }

    @Override
    public long toLong(byte[] value)
    {
        return getLong(value, 0);
    }

    @Override
    public long getLong(byte[] value, int offset)
    {
        return ByteArrayUtil.getLong(value, offset);
    }

    @Override
    public float toFloat(byte[] value)
    {
        return ByteArrayUtil.getFloat(value, 0);
    }

    @Override
    public float[] toFloatArray(byte[] value, int dimension)
    {
        float[] array = new float[dimension];
        int offset = 0;
        for (int i = 0; i < dimension; i++)
        {
            array[i] = getFloat(value, offset);
            offset += Float.BYTES;
        }
        return array;
    }

    @Override
    public double toDouble(byte[] value)
    {
        return ByteArrayUtil.getDouble(value, 0);
    }

    @Override
    public UUID toUUID(byte[] value)
    {
        return new UUID(getLong(value, 0), getLong(value, 8));
    }

    @Override
    public TimeUUID toTimeUUID(byte[] value)
    {
        return TimeUUID.fromBytes(getLong(value, 0), getLong(value, 8));
    }

    @Override
    public Ballot toBallot(byte[] value)
    {
        return Ballot.deserialize(value);
    }

    @Override
    public int putByte(byte[] dst, int offset, byte value)
    {
        dst[offset] = value;
        return TypeSizes.BYTE_SIZE;
    }

    @Override
    public int putShort(byte[] dst, int offset, short value)
    {
        ByteArrayUtil.putShort(dst, offset, value);
        return TypeSizes.SHORT_SIZE;
    }

    @Override
    public int putInt(byte[] dst, int offset, int value)
    {
        ByteArrayUtil.putInt(dst, offset, value);
        return TypeSizes.INT_SIZE;
    }

    @Override
    public int putLong(byte[] dst, int offset, long value)
    {
        ByteArrayUtil.putLong(dst, offset, value);
        return TypeSizes.LONG_SIZE;
    }

    @Override
    public int putFloat(byte[] dst, int offset, float value)
    {
        ByteArrayUtil.putFloat(dst, offset, value);
        return TypeSizes.FLOAT_SIZE;
    }

    @Override
    public byte[] empty()
    {
        return EMPTY;
    }

    @Override
    public byte[] valueOf(byte[] bytes)
    {
        return bytes;
    }

    @Override
    public byte[] valueOf(ByteBuffer bytes)
    {
        return ByteBufferUtil.getArray(bytes);
    }

    @Override
    public byte[] valueOf(String s, Charset charset)
    {
        return ByteArrayUtil.bytes(s, charset);
    }

    @Override
    public byte[] valueOf(UUID v)
    {
        return UUIDGen.decompose(v);
    }

    @Override
    public byte[] valueOf(boolean v)
    {
        return v ? new byte[] {1} : new byte[] {0};
    }

    @Override
    public byte[] valueOf(byte v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public byte[] valueOf(short v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public byte[] valueOf(int v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public byte[] valueOf(long v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public byte[] valueOf(float v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public byte[] valueOf(double v)
    {
        return ByteArrayUtil.bytes(v);
    }

    @Override
    public <V2> byte[] convert(V2 src, ValueAccessor<V2> accessor)
    {
        return accessor.toArray(src);
    }

    @Override
    public byte[] allocate(int size)
    {
        return new byte[size];
    }

    @Override
    public ObjectFactory<byte[]> factory()
    {
        return factory;
    }
}
