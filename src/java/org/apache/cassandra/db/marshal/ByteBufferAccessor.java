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
import java.util.UUID;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
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

    public int size(ByteBuffer value)
    {
        return value.remaining();
    }

    public ByteBuffer[] createArray(int length)
    {
        return new ByteBuffer[length];
    }

    public void write(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    public void write(ByteBuffer value, ByteBuffer out)
    {
        out.put(value.duplicate());
    }

    public <V2> int copyTo(ByteBuffer src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size)
    {
        dstAccessor.copyByteBufferTo(src, srcOffset, dst, dstOffset, size);
        return size;
    }

    public int copyByteArrayTo(byte[] src, int srcOffset, ByteBuffer dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    public int copyByteBufferTo(ByteBuffer src, int srcOffset, ByteBuffer dst, int dstOffset, int size)
    {
        FastByteOperations.copy(src, src.position() + srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    public void digest(ByteBuffer value, int offset, int size, Digest digest)
    {
        digest.update(value, value.position() + offset, size);
    }

    public ByteBuffer read(DataInputPlus in, int length) throws IOException
    {
        return ByteBufferUtil.read(in, length);
    }

    public ByteBuffer slice(ByteBuffer input, int offset, int length)
    {
        ByteBuffer copy = input.duplicate();
        copy.position(copy.position() + offset);
        copy.limit(copy.position() + length);
        return copy;
    }

    public <V2> int compare(ByteBuffer left, V2 right, ValueAccessor<V2> accessorR)
    {
        return accessorR.compareByteBufferTo(left, right);
    }

    public int compareByteArrayTo(byte[] left, ByteBuffer right)
    {
        return ByteBufferUtil.compare(left, right);
    }

    public int compareByteBufferTo(ByteBuffer left, ByteBuffer right)
    {
        return ByteBufferUtil.compareUnsigned(left, right);
    }

    public ByteBuffer toBuffer(ByteBuffer value)
    {
        return value;
    }

    public byte[] toArray(ByteBuffer value)
    {
        if (value == null)
            return null;
        return ByteBufferUtil.getArray(value);
    }

    public byte[] toArray(ByteBuffer value, int offset, int length)
    {
        if (value == null)
            return null;
        return ByteBufferUtil.getArray(value, value.position() + offset, length);
    }

    public String toString(ByteBuffer value, Charset charset) throws CharacterCodingException
    {
        return ByteBufferUtil.string(value, charset);
    }

    public ByteBuffer valueOf(UUID v)
    {
        return UUIDGen.toByteBuffer(v);
    }

    public String toHex(ByteBuffer value)
    {
        return ByteBufferUtil.bytesToHex(value);
    }

    public byte toByte(ByteBuffer value)
    {
        return ByteBufferUtil.toByte(value);
    }

    public byte getByte(ByteBuffer value, int offset)
    {
        return value.get(value.position() + offset);
    }

    public short toShort(ByteBuffer value)
    {
        return ByteBufferUtil.toShort(value);
    }

    public short getShort(ByteBuffer value, int offset)
    {
        return value.getShort(value.position() + offset);
    }

    public int toInt(ByteBuffer value)
    {
        return ByteBufferUtil.toInt(value);
    }

    public int getInt(ByteBuffer value, int offset)
    {
        return value.getInt(value.position() + offset);
    }

    public long toLong(ByteBuffer value)
    {
        return ByteBufferUtil.toLong(value);
    }

    public long getLong(ByteBuffer value, int offset)
    {
        return value.getLong(value.position() + offset);
    }

    public float toFloat(ByteBuffer value)
    {
        return ByteBufferUtil.toFloat(value);
    }

    public double toDouble(ByteBuffer value)
    {
        return ByteBufferUtil.toDouble(value);
    }

    public UUID toUUID(ByteBuffer value)
    {
        return UUIDGen.getUUID(value);
    }

    public int putShort(ByteBuffer dst, int offset, short value)
    {
        dst.putShort(dst.position() + offset, value);
        return TypeSizes.SHORT_SIZE;
    }

    public int putInt(ByteBuffer dst, int offset, int value)
    {
        dst.putInt(dst.position() + offset, value);
        return TypeSizes.INT_SIZE;
    }

    public int putLong(ByteBuffer dst, int offset, long value)
    {
        dst.putLong(dst.position() + offset, value);
        return TypeSizes.LONG_SIZE;
    }

    public ByteBuffer empty()
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    public ByteBuffer valueOf(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes);
    }

    public ByteBuffer valueOf(ByteBuffer bytes)
    {
        return bytes;
    }

    public ByteBuffer valueOf(String v, Charset charset)
    {
        return ByteBufferUtil.bytes(v, charset);
    }

    public ByteBuffer valueOf(boolean v)
    {
        return v ? ByteBuffer.wrap(new byte[] {1}) : ByteBuffer.wrap(new byte[] {0});
    }

    public ByteBuffer valueOf(byte v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public ByteBuffer valueOf(short v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public ByteBuffer valueOf(int v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public ByteBuffer valueOf(long v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public ByteBuffer valueOf(float v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public ByteBuffer valueOf(double v)
    {
        return ByteBufferUtil.bytes(v);
    }

    public <V2> ByteBuffer convert(V2 src, ValueAccessor<V2> accessor)
    {
        return accessor.toBuffer(src);
    }

    public ByteBuffer allocate(int size)
    {
        return ByteBuffer.allocate(size);
    }

    public ObjectFactory<ByteBuffer> factory()
    {
        return ByteBufferObjectFactory.instance;
    }
}
