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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.memory.BigEndianMemoryUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

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
        out.writeMemory(sourceValue.getAddress(), sourceValue.nativeDataSize());
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
        int size = value.nativeDataSize();
        MemoryUtil.getBytes(value.getAddress(), out, size);
        out.position(out.position() + size);

    }

    @Override
    public <V2> int copyTo(NativeData src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size)
    {
        if (dstAccessor == ByteArrayAccessor.instance)
            MemoryUtil.getBytes(src.getAddress() + srcOffset, dstAccessor.toArray(dst), dstOffset, size);
        else if (dstAccessor == ByteBufferAccessor.instance)
        {
            ByteBuffer dstBuffer = dstAccessor.toBuffer(dst);
            MemoryUtil.getBytes(src.getAddress() + srcOffset, dstBuffer, dstOffset, size);
            // note: position of dstBuffer expected to stay the same
        }
        else if (dstAccessor == NativeAccessor.instance)
            MemoryUtil.setBytes(src.getAddress() + srcOffset, ((NativeData) dst).getAddress() + dstOffset, size);
        else // just in case of new implementations of ValueAccessor appear
            dstAccessor.copyByteBufferTo(src.asByteBuffer(), srcOffset, dst, dstOffset, size);

        return size;
    }

    @Override
    public int copyByteArrayTo(byte[] src, int srcOffset, NativeData dstNative, int dstOffset, int size)
    {
        MemoryUtil.setBytes(src, srcOffset, dstNative.getAddress() + dstOffset, size);
        return size;
    }

    @Override
    public int copyByteBufferTo(ByteBuffer src, int srcOffset, NativeData dstNative, int dstOffset, int size)
    {
        MemoryUtil.setBytes(dstNative.getAddress() + dstOffset, src, srcOffset, size);
        return size;
    }

    @Override
    public void digest(NativeData value, int offset, int size, Digest digest)
    {
        // not used for NativeData (we copy data to heap during a select)
        // so, there is no much reason to optimize to avoid a ByteBuffer object allocation
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

        if (accessorR == ByteArrayAccessor.instance)
            return -compareByteArrayTo(accessorR.toArray(right), left);
        else if (accessorR == ByteBufferAccessor.instance)
            return -compareByteBufferTo(accessorR.toBuffer(right), left);
        if (accessorR == NativeAccessor.instance)
        {
            NativeData rightNative = (NativeData) right;
            int leftSize = left.nativeDataSize();
            int rightSize = rightNative.nativeDataSize();
            return FastByteOperations.compareMemoryUnsigned(left.getAddress(), leftSize, rightNative.getAddress(), rightSize);
        } else // just in case of new implementations of ValueAccessor appear
            return ByteBufferUtil.compareUnsigned(left.asByteBuffer(), accessorR.toBuffer(right));
    }

    @Override
    public int compareByteArrayTo(byte[] left, NativeData right)
    {
        return FastByteOperations.compareWithMemoryUnsigned(left, 0, left.length, right.getAddress(), right.nativeDataSize());
    }

    @Override
    public int compareByteBufferTo(ByteBuffer left, NativeData right)
    {
        return FastByteOperations.compareWithMemoryUnsigned(left, right.getAddress(), right.nativeDataSize());
    }

     // -----------------------------------------------------------------------------
     // Data deserialization methods

    @Override
    public byte[] toArray(NativeData value)
    {
        if (value == null)
            return null;
        int size = value.nativeDataSize();
        byte[] result = new byte[size];
        MemoryUtil.getBytes(value.getAddress(), result, 0, size);
        return result;
    }

    @Override
    public byte[] toArray(NativeData value, int offset, int length)
    {
        if (value == null)
            return null;
        int size = value.nativeDataSize();
        if (length > size)
            throw new IllegalArgumentException("length (" + length + ") cannot be more than the value size (" + size + ")");

        byte[] result = new byte[length];
        MemoryUtil.getBytes(value.getAddress() + offset, result, 0, length);
        return result;
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
        return getByte(value, 0);
    }

    @Override
    public byte getByte(NativeData value, int offset)
    {
        return MemoryUtil.getByte(value.getAddress() + offset);
    }

    @Override
    public short toShort(NativeData value)
    {
        return getShort(value, 0);
    }

    @Override
    public short getShort(NativeData value, int offset)
    {
        return (short) BigEndianMemoryUtil.getUnsignedShort(value.getAddress() + offset);
    }

    @Override
    public int getUnsignedShort(NativeData value, int offset)
    {
        return BigEndianMemoryUtil.getUnsignedShort(value.getAddress() + offset);
    }

    @Override
    public int toInt(NativeData value)
    {
        return getInt(value, 0);
    }

    @Override
    public int getInt(NativeData value, int offset)
    {
        return BigEndianMemoryUtil.getInt(value.getAddress() + offset);
    }

    @Override
    public long toLong(NativeData value)
    {
        return getLong(value, 0);
    }

    @Override
    public long getLong(NativeData value, int offset)
    {
        return BigEndianMemoryUtil.getLong(value.getAddress() + offset);
    }

    @Override
    public float getFloat(NativeData value, int offset)
    {
        return Float.intBitsToFloat(BigEndianMemoryUtil.getInt(value.getAddress() + offset));
    }

    @Override
    public double getDouble(NativeData value, int offset)
    {
        return Double.longBitsToDouble(BigEndianMemoryUtil.getLong(value.getAddress() + offset));
    }

    @Override
    public float toFloat(NativeData value)
    {
        return getFloat(value, 0);
    }

    @Override
    public double toDouble(NativeData value)
    {
        return getDouble(value, 0);
    }

    @Override
    public UUID toUUID(NativeData value)
    {
        long mostSigBits = getLong(value, 0);
        long leastSigBits = getLong(value, 8);

        return UUIDGen.getUUID(mostSigBits, leastSigBits);
    }

    @Override
    public TimeUUID toTimeUUID(NativeData value)
    {
        long mostSigBits = getLong(value, 0);
        long leastSigBits = getLong(value, 8);
        return TimeUUID.fromBytes(mostSigBits, leastSigBits);
    }

    @Override
    public Ballot toBallot(NativeData value)
    {
        long mostSigBits = getLong(value, 0);
        long leastSigBits = getLong(value, 8);
        return Ballot.fromBytes(mostSigBits, leastSigBits);
    }

    @Override
    public float[] toFloatArray(NativeData value, int dimension)
    {
        int arraySize = value.nativeDataSize() / Float.BYTES;
        if (arraySize != dimension)
            throw new IllegalArgumentException(String.format("Could not convert to a float[] with different dimension. " +
                                                             "Was expecting %d but got %d", dimension, arraySize));
        float[] floatArray = new float[arraySize];
        for (int i = 0; i < arraySize; i++)
        {
            floatArray[i] = Float.intBitsToFloat(getInt(value, i * Float.BYTES));
        }
        return floatArray;
    }


    // -----------------------------------------------------------------------------
    // Data serialization methods
    @Override
    public int putByte(NativeData dstNative, int offset, byte value)
    {
        BigEndianMemoryUtil.setByte(dstNative.getAddress() + offset, value);
        return TypeSizes.BYTE_SIZE;
    }

    @Override
    public int putShort(NativeData dstNative, int offset, short value)
    {
        BigEndianMemoryUtil.setShort(dstNative.getAddress() + offset, value);
        return TypeSizes.SHORT_SIZE;
    }

    @Override
    public int putInt(NativeData dstNative, int offset, int value)
    {
        BigEndianMemoryUtil.setInt(dstNative.getAddress() + offset, value);
        return TypeSizes.INT_SIZE;
    }

    @Override
    public int putLong(NativeData dstNative, int offset, long value)
    {
        BigEndianMemoryUtil.setLong(dstNative.getAddress() + offset, value);
        return TypeSizes.LONG_SIZE;
    }

    @Override
    public int putFloat(NativeData dstNative, int offset, float value)
    {
        putInt(dstNative, offset, Float.floatToIntBits(value));
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
    
    private static NativeDataAllocator allocator = NativeDataAllocator.UNSUPPORTED;

    @VisibleForTesting
    public static void setNativeMemoryAllocator(NativeDataAllocator allocatorToSet)
    {
        allocator = allocatorToSet;
    }

    @Override
    public NativeData read(DataInputPlus in, int length) throws IOException
    {
        ByteBuffer data = ByteBufferUtil.read(in, length);
        return allocator.allocateBasedOnBuffer(data);
    }

    @Override
    public NativeData empty()
    {
        return AddressBasedNativeData.EMPTY;
    }

    @Override
    public NativeData valueOf(byte[] bytes)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(bytes));
    }

    @Override
    public NativeData valueOf(ByteBuffer bytes)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(bytes));
    }

    @Override
    public NativeData valueOf(String s, Charset charset)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(s, charset));
    }

    @Override
    public NativeData valueOf(UUID v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(boolean v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(byte v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(short v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(int v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(long v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(float v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public NativeData valueOf(double v)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.valueOf(v));
    }

    @Override
    public <V2> NativeData convert(V2 src, ValueAccessor<V2> accessor)
    {
        if (accessor == NativeAccessor.instance)
            return (NativeData) src;
        return allocator.allocateBasedOnBuffer(accessor.toBuffer(src));
    }

    @Override
    public NativeData allocate(int size)
    {
        return allocator.allocateBasedOnBuffer(ByteBufferAccessor.instance.allocate(size));
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
