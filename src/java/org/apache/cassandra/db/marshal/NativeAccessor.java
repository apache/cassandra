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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ValueAccessor has a lot of different methods are grouped together in a single interface.
 * Technically the methods can be classfied to 4 categories:
 * 1) basic methods to deal with the existing data as an abstract read-only container of bytes
 * 2) deserialization methods to decode the data into different data types
 * 3) serialization methods to encode and write different data types into the value entity
 * 4) Value object creation methods
 *
 *  NativeAccessor provides a support for real NativeData objects (on top of off-heap memory) for 1-3 categories
 *  with a focus on 1) category and only emulates 4th category using ByteBufferSliceNativeData on top of heap ByteBuffers.
 *  We expect NativeData is used only to store data in Memtables with an explicit allocator and memory regions lifecycle
 *  and not used to create short-living Mutation requests and transfer them between nodes.
 */
public class NativeAccessor implements ValueAccessor<NativeData>
{
    public static final ValueAccessor<NativeData> instance = new NativeAccessor();

    // -----------------------------------------------------------------------------
    // basic methods to deal with data as a read-only container of bytes

    @Override
    public int size(NativeData value)
    {
        return value.nativeDataSize();
    }

    @Override
    public void write(NativeData sourceValue, DataOutputPlus out) throws IOException
    {
        sourceValue.writeTo(out);
    }

    @Override
    public ByteBuffer toBuffer(NativeData value)
    {
        if (value == null)
            return null;
        return value.asByteBuffer();
    }

    @Override
    public void write(NativeData value, ByteBuffer out)
    {
        out.put(value.asByteBuffer().duplicate()); // ByteBufferSliceNativeDataasByteBuffer() returns a re-usable byte buffer
    }

    @Override
    public <V2> int copyTo(NativeData src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size)
    {
        dstAccessor.copyByteBufferTo(src.asByteBuffer(), srcOffset, dst, dstOffset, size);
        return size;
    }

    @Override
    public int copyByteArrayTo(byte[] src, int srcOffset, NativeData dstNative, int dstOffset, int size)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        FastByteOperations.copy(src, srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    @Override
    public int copyByteBufferTo(ByteBuffer src, int srcOffset, NativeData dstNative, int dstOffset, int size)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        FastByteOperations.copy(src, src.position() + srcOffset, dst, dst.position() + dstOffset, size);
        return size;
    }

    @Override
    public void digest(NativeData value, int offset, int size, Digest digest)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        digest.update(byteBuffer, byteBuffer.position() + offset, size);
    }

    @Override
    public NativeData slice(NativeData input, int offset, int length)
    {
        return input.slice(offset, length);
    }

    @Override
    public <VR> int compare(NativeData left, VR right, ValueAccessor<VR> accessorR)
    {
        if (right instanceof NativeData)
        {
            return left.compareTo((NativeData) right);
        }
        return left.compareTo(accessorR.toBuffer(right));
    }

    @Override
    public int compareByteArrayTo(byte[] left, NativeData right)
    {
        return ByteBufferUtil.compare(left, right.asByteBuffer());
    }

    @Override
    public int compareByteBufferTo(ByteBuffer left, NativeData right)
    {
        return -right.compareTo(left); // we want to avoid ByteBuffer retrieval from NativeData
    }

     // -----------------------------------------------------------------------------
     // Data deserialization methods

    @Override
    public byte[] toArray(NativeData value)
    {
        if (value == null)
            return null;
        return ByteBufferUtil.getArray(value.asByteBuffer());
    }

    @Override
    public byte[] toArray(NativeData value, int offset, int length)
    {
        if (value == null)
            return null;
        ByteBuffer byteBuffer = value.asByteBuffer();
        return ByteBufferUtil.getArray(byteBuffer, byteBuffer.position() + offset, length);
    }

    @Override
    public String toString(NativeData value, Charset charset) throws CharacterCodingException
    {
        return ByteBufferUtil.string(value.asByteBuffer(), charset);
    }

    @Override
    public String toHex(NativeData value)
    {
        return ByteBufferUtil.bytesToHex(value.asByteBuffer());
    }

    @Override
    public byte toByte(NativeData value)
    {
        return ByteBufferUtil.toByte(value.asByteBuffer());
    }

    @Override
    public byte getByte(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.get(byteBuffer.position() + offset);
    }

    @Override
    public short toShort(NativeData value)
    {
        return ByteBufferUtil.toShort(value.asByteBuffer());
    }

    @Override
    public short getShort(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.getShort(byteBuffer.position() + offset);
    }

    @Override
    public int getUnsignedShort(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return ByteBufferUtil.getUnsignedShort(byteBuffer, byteBuffer.position() + offset);
    }

    @Override
    public int toInt(NativeData value)
    {
        return ByteBufferUtil.toInt(value.asByteBuffer());
    }

    @Override
    public int getInt(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.getInt(byteBuffer.position() + offset);
    }

    @Override
    public long toLong(NativeData value)
    {
        return ByteBufferUtil.toLong(value.asByteBuffer());
    }

    @Override
    public long getLong(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.getLong(byteBuffer.position() + offset);
    }

    @Override
    public float getFloat(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.getFloat(byteBuffer.position() + offset);
    }

    @Override
    public double getDouble(NativeData value, int offset)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return byteBuffer.getDouble(byteBuffer.position() + offset);
    }

    @Override
    public float toFloat(NativeData value)
    {
        return ByteBufferUtil.toFloat(value.asByteBuffer());
    }

    @Override
    public double toDouble(NativeData value)
    {
        return ByteBufferUtil.toDouble(value.asByteBuffer());
    }

    @Override
    public UUID toUUID(NativeData value)
    {
        return UUIDGen.getUUID(value.asByteBuffer());
    }

    @Override
    public TimeUUID toTimeUUID(NativeData value)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        return TimeUUID.fromBytes(byteBuffer.getLong(byteBuffer.position()), byteBuffer.getLong(byteBuffer.position() + 8));
    }

    @Override
    public Ballot toBallot(NativeData value)
    {
        return Ballot.deserialize(value.asByteBuffer());
    }

    @Override
    public float[] toFloatArray(NativeData value, int dimension)
    {
        ByteBuffer byteBuffer = value.asByteBuffer();
        FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
        if (floatBuffer.remaining() != dimension)
            throw new IllegalArgumentException(String.format("Could not convert to a float[] with different dimension. " +
                                                             "Was expecting %d but got %d", dimension, floatBuffer.remaining()));
        float[] floatArray = new float[floatBuffer.remaining()];
        floatBuffer.get(floatArray);
        return floatArray;
    }


    // -----------------------------------------------------------------------------
    // Data serialization methods
    @Override
    public int putByte(NativeData dstNative, int offset, byte value)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        dst.put(dst.position() + offset, value);
        return TypeSizes.BYTE_SIZE;
    }

    @Override
    public int putShort(NativeData dstNative, int offset, short value)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        dst.putShort(dst.position() + offset, value);
        return TypeSizes.SHORT_SIZE;
    }

    @Override
    public int putInt(NativeData dstNative, int offset, int value)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        dst.putInt(dst.position() + offset, value);
        return TypeSizes.INT_SIZE;
    }

    @Override
    public int putLong(NativeData dstNative, int offset, long value)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        dst.putLong(dst.position() + offset, value);
        return TypeSizes.LONG_SIZE;
    }

    @Override
    public int putFloat(NativeData dstNative, int offset, float value)
    {
        ByteBuffer dst = dstNative.asByteBuffer();
        dst.putFloat(dst.position() + offset, value);
        return TypeSizes.FLOAT_SIZE;
    }

    @Override
    public NativeData[] createArray(int length)
    {
        return new NativeData[length];
    }

    // -----------------------------------------------------------------------------
    // Value object creation methods
    // We do not expect the methods are used in real logic for NativeData,
    // but they are needed to reuse existing unit tests written for other implementation of ValueAccessor.
    // The objects created by the methods don't actually represent a real native memory
    // but just heap ByteBuffer wrappers which provide NativeData

    @Override
    public NativeData read(DataInputPlus in, int length) throws IOException
    {
        ByteBuffer data = ByteBufferUtil.read(in, length);
        return new ByteBufferSliceNativeData(data);
    }

    @Override
    public NativeData empty()
    {
        return ByteBufferSliceNativeData.EMPTY;
    }

    @Override
    public NativeData valueOf(byte[] bytes)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(bytes));
    }

    @Override
    public NativeData valueOf(ByteBuffer bytes)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(bytes));
    }

    @Override
    public NativeData valueOf(String s, Charset charset)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(s, charset));
    }

    @Override
    public NativeData valueOf(UUID v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(boolean v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(byte v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(short v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(int v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(long v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(float v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(double v)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public <V2> NativeData convert(V2 src, ValueAccessor<V2> accessor)
    {
        return new ByteBufferSliceNativeData(accessor.toBuffer(src));
    }

    @Override
    public NativeData allocate(int size)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.allocate(size));
    }

    @Override
    public ObjectFactory<NativeData> factory()
    {
        // The method is used to de-serialize and create different parts of a Mutation object
        // to transfer it between Cassandra nodes.
        // The current implementation of NativeData does not support creating of such objects in-flight
        // because it requires to have a native memory pool/allocator and manage its lifecycle.
        throw new UnsupportedOperationException();
    }
}
