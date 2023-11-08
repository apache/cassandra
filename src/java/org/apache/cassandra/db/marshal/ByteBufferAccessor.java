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
import java.nio.FloatBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Accessor for ByteBuffer values. ByteBufferAccessor treates {@link ByteBuffer#position()} as index 0,
 * and {@link ByteBuffer#remaining()} as the length.
 */
public class ByteBufferAccessor implements ValueAccessor<ByteBuffer>
{
    public static final ValueAccessor<ByteBuffer> instance = new ByteBufferAccessor();

    private ByteBufferAccessor() {}

    @Override
    public int size(ByteBuffer value)
    {
        return value.remaining();
    }

    @Override
    public ByteBuffer[] createArray(int length)
    {
        return new ByteBuffer[length];
    }

    @Override
    public void write(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    @Override
    public void write(ByteBuffer value, ByteBuffer out)
    {
        out.put(value.duplicate());
    }

    @Override
    public <V2> int copyTo(ByteBuffer src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size)
    {
        dstAccessor.copyByteBufferTo(src, srcOffset, dst, dstOffset, size);
        return size;
    }

    @Override
    public int copyByteArrayTo(byte[] src, int srcOffset, ByteBuffer dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    @Override
    public int copyByteBufferTo(ByteBuffer src, int srcOffset, ByteBuffer dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, src.position() + srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    @Override
    public void digest(ByteBuffer value, int offset, int size, Digest digest)
    {
        digest.update(value, value.position() + offset, size);
    }

    @Override
    public ByteBuffer read(DataInputPlus in, int length) throws IOException
    {
        return ByteBufferUtil.read(in, length);
    }

    @Override
    public ByteBuffer slice(ByteBuffer input, int offset, int length)
    {
        int size = sizeFromOffset(input, offset);
        if (size < length)
            throw new IndexOutOfBoundsException(String.format("Attempted to read %d, but the size is %d", length, size));
        ByteBuffer copy = input.duplicate();
        copy.position(copy.position() + offset);
        copy.limit(copy.position() + length);
        return copy;
    }

    @Override
    public <V2> int compare(ByteBuffer left, V2 right, ValueAccessor<V2> accessorR)
    {
        return accessorR.compareByteBufferTo(left, right);
    }

    @Override
    public int compareByteArrayTo(byte[] left, ByteBuffer right)
    {
        return ByteBufferUtil.compare(left, right);
    }

    @Override
    public int compareByteBufferTo(ByteBuffer left, ByteBuffer right)
    {
        return ByteBufferUtil.compareUnsigned(left, right);
    }

    @Override
    public ByteBuffer toBuffer(ByteBuffer value)
    {
        return value;
    }

    @Override
    public byte[] toArray(ByteBuffer value)
    {
        if (value == null)
            return null;
        return ByteBufferUtil.getArray(value);
    }

    @Override
    public byte[] toArray(ByteBuffer value, int offset, int length)
    {
        if (value == null)
            return null;
        return ByteBufferUtil.getArray(value, value.position() + offset, length);
    }

    @Override
    public String toString(ByteBuffer value, Charset charset) throws CharacterCodingException
    {
        return ByteBufferUtil.string(value, charset);
    }

    @Override
    public ByteBuffer valueOf(UUID v)
    {
        return UUIDGen.toByteBuffer(v);
    }

    @Override
    public String toHex(ByteBuffer value)
    {
        return ByteBufferUtil.bytesToHex(value);
    }

    @Override
    public byte toByte(ByteBuffer value)
    {
        return ByteBufferUtil.toByte(value);
    }

    @Override
    public byte getByte(ByteBuffer value, int offset)
    {
        return value.get(value.position() + offset);
    }

    @Override
    public short toShort(ByteBuffer value)
    {
        return ByteBufferUtil.toShort(value);
    }

    @Override
    public short getShort(ByteBuffer value, int offset)
    {
        return value.getShort(value.position() + offset);
    }

    @Override
    public int getUnsignedShort(ByteBuffer value, int offset)
    {
        return ByteBufferUtil.getUnsignedShort(value, value.position() + offset);
    }

    @Override
    public int toInt(ByteBuffer value)
    {
        return ByteBufferUtil.toInt(value);
    }

    @Override
    public int getInt(ByteBuffer value, int offset)
    {
        return value.getInt(value.position() + offset);
    }

    @Override
    public float getFloat(ByteBuffer value, int offset)
    {
        return value.getFloat(offset);
    }

    @Override
    public double getDouble(ByteBuffer value, int offset)
    {
        return value.getDouble(offset);
    }

    @Override
    public long toLong(ByteBuffer value)
    {
        return ByteBufferUtil.toLong(value);
    }

    @Override
    public long getLong(ByteBuffer value, int offset)
    {
        return value.getLong(value.position() + offset);
    }

    @Override
    public float toFloat(ByteBuffer value)
    {
        return ByteBufferUtil.toFloat(value);
    }

    @Override
    public float[] toFloatArray(ByteBuffer value, int dimension)
    {
        FloatBuffer floatBuffer = value.asFloatBuffer();
        if (floatBuffer.remaining() != dimension)
            throw new IllegalArgumentException(String.format("Could not convert to a float[] with different dimension. " +
                                                             "Was expecting %d but got %d", dimension, floatBuffer.remaining()));
        float[] floatArray = new float[floatBuffer.remaining()];
        floatBuffer.get(floatArray);
        return floatArray;
    }

    @Override
    public double toDouble(ByteBuffer value)
    {
        return ByteBufferUtil.toDouble(value);
    }

    @Override
    public UUID toUUID(ByteBuffer value)
    {
        return UUIDGen.getUUID(value);
    }

    @Override
    public TimeUUID toTimeUUID(ByteBuffer value)
    {
        return TimeUUID.fromBytes(value.getLong(value.position()), value.getLong(value.position() + 8));
    }

    @Override
    public Ballot toBallot(ByteBuffer value)
    {
        return Ballot.deserialize(value);
    }

    @Override
    public int putByte(ByteBuffer dst, int offset, byte value)
    {
        dst.put(dst.position() + offset, value);
        return TypeSizes.BYTE_SIZE;
    }

    @Override
    public int putShort(ByteBuffer dst, int offset, short value)
    {
        dst.putShort(dst.position() + offset, value);
        return TypeSizes.SHORT_SIZE;
    }

    @Override
    public int putInt(ByteBuffer dst, int offset, int value)
    {
        dst.putInt(dst.position() + offset, value);
        return TypeSizes.INT_SIZE;
    }

    @Override
    public int putLong(ByteBuffer dst, int offset, long value)
    {
        dst.putLong(dst.position() + offset, value);
        return TypeSizes.LONG_SIZE;
    }

    @Override
    public int putFloat(ByteBuffer dst, int offset, float value)
    {
        dst.putFloat(dst.position() + offset, value);
        return TypeSizes.FLOAT_SIZE;
    }

    @Override
    public ByteBuffer empty()
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public ByteBuffer valueOf(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public ByteBuffer valueOf(ByteBuffer bytes)
    {
        return bytes;
    }

    @Override
    public ByteBuffer valueOf(String v, Charset charset)
    {
        return ByteBufferUtil.bytes(v, charset);
    }

    @Override
    public ByteBuffer valueOf(boolean v)
    {
        return v ? ByteBuffer.wrap(new byte[] {1}) : ByteBuffer.wrap(new byte[] {0});
    }

    @Override
    public ByteBuffer valueOf(byte v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public ByteBuffer valueOf(short v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public ByteBuffer valueOf(int v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public ByteBuffer valueOf(long v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public ByteBuffer valueOf(float v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public ByteBuffer valueOf(double v)
    {
        return ByteBufferUtil.bytes(v);
    }

    @Override
    public <V2> ByteBuffer convert(V2 src, ValueAccessor<V2> accessor)
    {
        return accessor.toBuffer(src);
    }

    @Override
    public ByteBuffer allocate(int size)
    {
        return ByteBuffer.allocate(size);
    }

    @Override
    public ObjectFactory<ByteBuffer> factory()
    {
        return ByteBufferObjectFactory.instance;
    }
}
