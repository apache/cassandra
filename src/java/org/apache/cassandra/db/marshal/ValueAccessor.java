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
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.*;

/**
 * ValueAccessor allows serializers and other code dealing with raw bytes to operate on different backing types
 * (ie: byte arrays, byte buffers, etc) without requiring that the supported backing types share a common type
 * ancestor and without incuring the allocation cost of a wrapper object.
 *
 * A note on byte buffers for implementors: the "value" of a byte buffer is always interpreted as beginning at
 * it's {@link ByteBuffer#position()} and having a length of {@link ByteBuffer#remaining()}. ValueAccessor
 * implementations need to maintain this internally. ValueAccessors should also never modify the state of the
 * byte buffers view (ie: offset, limit). This would also apply to value accessors for simlilar types
 * (ie: netty's ByteBuf}.
 *
 * @param <V> the backing type
 */
public interface ValueAccessor<V>
{

    /**
     * Creates db objects using the given accessors value type. ObjectFactory instances are meant to be returned
     * by the factory() method of a value accessor.
     * @param <V> the backing type
     */
    public interface ObjectFactory<V>
    {
        Cell<V> cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, V value, CellPath path);
        Clustering<V> clustering(V... values);
        Clustering<V> clustering();
        Clustering<V> staticClustering();
        ClusteringBound<V> bound(ClusteringPrefix.Kind kind, V... values);
        ClusteringBound<V> bound(ClusteringPrefix.Kind kind);
        ClusteringBoundary<V> boundary(ClusteringPrefix.Kind kind, V... values);
        default ClusteringBoundOrBoundary<V> boundOrBoundary(ClusteringPrefix.Kind kind, V... values)
        {
            return kind.isBoundary() ? boundary(kind, values) : bound(kind, values);
        }

        default ClusteringBound<V> inclusiveOpen(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? INCL_END_BOUND : INCL_START_BOUND, boundValues);
        }

        default ClusteringBound<V> exclusiveOpen(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? EXCL_END_BOUND : EXCL_START_BOUND, boundValues);
        }

        default ClusteringBound<V> inclusiveClose(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? INCL_START_BOUND : INCL_END_BOUND, boundValues);
        }

        default ClusteringBound<V> exclusiveClose(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? EXCL_START_BOUND : EXCL_END_BOUND, boundValues);
        }

        default ClusteringBoundary<V> inclusiveCloseExclusiveOpen(boolean reversed, V[] boundValues)
        {
            return boundary(reversed ? EXCL_END_INCL_START_BOUNDARY : INCL_END_EXCL_START_BOUNDARY, boundValues);
        }

        default ClusteringBoundary<V> exclusiveCloseInclusiveOpen(boolean reversed, V[] boundValues)
        {
            return boundary(reversed ? INCL_END_EXCL_START_BOUNDARY : EXCL_END_INCL_START_BOUNDARY, boundValues);
        }
    }
    /**
     * @return the size of the given value
     */
    int size(V value);

    /** serializes size including a vint length prefix */
    default int sizeWithVIntLength(V value)
    {
        int size = size(value);
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }

    /** serialized size including a short length prefix */
    default int sizeWithShortLength(V value)
    {
        return 2 + size(value);
    }

    default int remaining(V value, int offset)
    {
        int size = size(value);
        int rem = size - offset;
        return rem > 0 ? rem : 0;
    }

    /**
     * @return true if the size of the given value is zero, false otherwise
     */
    default boolean isEmpty(V value)
    {
        return size(value) == 0;
    }

    /**
     * @return the number of bytes remaining in the value from the given offset
     */
    default int sizeFromOffset(V value, int offset)
    {
        return size(value) - offset;
    }

    /**
     * @return true if there are no bytes present after the given offset, false otherwise
     */
    default boolean isEmptyFromOffset(V value, int offset)
    {
        return sizeFromOffset(value, offset) == 0;
    }

    /**
     * allocate an instance of the accessors backing type
     * @param length size of backing typ to allocate
     */
    V[] createArray(int length);

    /**
     * Write the contents of the given value into the a DataOutputPlus
     */
    void write(V value, DataOutputPlus out) throws IOException;

    default void writeWithVIntLength(V value, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(size(value));
        write(value, out);
    }

    /**
     * Write the contents of the given value into the ByteBuffer
     */
    void write(V value, ByteBuffer out);

    /**
     * copy the {@param size} bytes from the {@param src} value, starting at the offset {@param srcOffset} into
     * the {@param dst} value, starting at the offset {@param dstOffset}, using the accessor {@param dstAccessor}
     * @param <V2> the destination value type
     * @return the number of bytes copied ({@param size})
     */
    <V2> int copyTo(V src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size);

    /**
     * copies a byte array into this accessors value.
     */
    int copyByteArrayTo(byte[] src, int srcOffset, V dst, int dstOffset, int size);

    /**
     * copies a byte buffer into this accessors value.
     */
    int copyByteBufferTo(ByteBuffer src, int srcOffset, V dst, int dstOffset, int size);

    /**
     * updates {@param digest} with {@param size} bytes from the contents of {@param value} starting
     * at offset {@param offset}
     */
    void digest(V value, int offset, int size, Digest digest);

    /**
     * updates {@param digest} with te contents of {@param value}
     */
    default void digest(V value, Digest digest)
    {
        digest(value, 0, size(value), digest);
    }

    /**
     * Reads a value of {@param length} bytes from {@param in}
     */
    V read(DataInputPlus in, int length) throws IOException;

    /**
     * Returns a value with the contents of {@param input} from {@param offset} to {@param length}.
     *
     * Depending on the accessor implementation, this method may:
     *  * allocate a new {@param <V>} object of {@param length}, and copy data into it
     *  * return a view of {@param input} where changes to one will be reflected in the other
     */
    V slice(V input, int offset, int length);

    /**
     * same as {@link ValueAccessor#slice(Object, int, int)}, except the length is taken from the first
     * 2 bytes from the given offset (and not included in the return value)
     */
    default V sliceWithShortLength(V input, int offset)
    {
        int size = getUnsignedShort(input, offset);
        return slice(input, offset + 2, size);
    }

    /**
     * lexicographically compare {@param left} to {@param right}
     * @param <VR> backing type of
     */
    <VR> int compare(V left, VR right, ValueAccessor<VR> accessorR);

    /**
     * compare a byte array on the left with a {@param <V>} on the right}
     */
    int compareByteArrayTo(byte[] left, V right);

    /**
     * compare a byte buffer on the left with a {@param <V>} on the right}
     */
    int compareByteBufferTo(ByteBuffer left, V right);

    default int hashCode(V value)
    {
        if (value == null)
            return 0;

        int result = 1;
        for (int i=0, isize=size(value); i<isize; i++)
            result = 31 * result + (int) getByte(value, i);

        return result;
    }

    /**
     * returns a ByteBuffer with the contents of {@param value}
     *
     * Depending on the accessor implementation, this method may:
     *  * allocate a new ByteBuffer and copy data into it
     *  * return the value, if the backing type is a bytebuffer
     */
    ByteBuffer toBuffer(V value);

    /**
     * returns a byte[] with the contents of {@param value}
     *
     * Depending on the accessor implementation, this method may:
     *  * allocate a new byte[] object and copy data into it
     *  * return the value, if the backing type is byte[]
     */
    byte[] toArray(V value);

    /**
     * returns a byte[] with {@param length} bytes copied from the contents of {@param value}
     * starting at offset {@param offset}.
     *
     * Depending on the accessor implementation, this method may:
     *  * allocate a new byte[] object and copy data into it
     *  * return the value, if the backing type is byte[], offset is 0 and {@param length} == size(value)
     */
    byte[] toArray(V value, int offset, int length);
    String toString(V value, Charset charset) throws CharacterCodingException;

    default String toString(V value) throws CharacterCodingException
    {
        return toString(value, StandardCharsets.UTF_8);
    }

    String toHex(V value);

    /** returns a boolean from offset {@param offset} */
    default boolean getBoolean(V value, int offset)
    {
        return getByte(value, offset) != 0;
    }

    /** returns a byte from offset 0 */
    byte toByte(V value);
    /** returns a byte from offset {@param offset} */
    byte getByte(V value, int offset);
    /** returns a short from offset 0 */
    short toShort(V value);
    /** returns a short from offset {@param offset} */
    short getShort(V value, int offset);
    /** returns an unsigned short from offset {@param offset} */
    int getUnsignedShort(V value, int offset);
    /** returns an int from offset 0 */
    int toInt(V value);
    /** returns an int from offset {@param offset} */
    int getInt(V value, int offset);

    default long getUnsignedVInt(V value, int offset)
    {
        return VIntCoding.getUnsignedVInt(value, this, offset);
    }

    default int getUnsignedVInt32(V value, int offset)
    {
        return VIntCoding.getUnsignedVInt32(value, this, offset);
    }

    default long getVInt(V value, int offset)
    {
        return VIntCoding.getVInt(value, this, offset);
    }

    default int getVInt32(V value, int offset)
    {
        return VIntCoding.getVInt32(value, this, offset);
    }

    float getFloat(V value, int offset);
    double getDouble(V value, int offset);
    /** returns a long from offset 0 */
    long toLong(V value);
    /** returns a long from offset {@param offset} */
    long getLong(V value, int offset);
    /** returns a float from offset 0 */
    float toFloat(V value);

    /** returns a double from offset 0 */
    double toDouble(V value);

    /** returns a UUID from offset 0 */
    UUID toUUID(V value);

    /** returns a TimeUUID from offset 0 */
    TimeUUID toTimeUUID(V value);

    /** returns a TimeUUID from offset 0 */
    Ballot toBallot(V value);

    /** returns a float[] from offset 0 */
    float[] toFloatArray(V value, int dimension);

    /**
     * writes the byte value {@param value} to {@param dst} at offset {@param offset}
     * @return the number of bytes written to {@param value}
     */
    int putByte(V dst, int offset, byte value);

    /**
     * writes the short value {@param value} to {@param dst} at offset {@param offset}
     * @return the number of bytes written to {@param value}
     */
    int putShort(V dst, int offset, short value);

    /**
     * writes the int value {@param value} to {@param dst} at offset {@param offset}
     * @return the number of bytes written to {@param value}
     */
    int putInt(V dst, int offset, int value);

    /**
     * writes the long value {@param value} to {@param dst} at offset {@param offset}
     * @return the number of bytes written to {@param value}
     */
    int putLong(V dst, int offset, long value);

    /**
     * writes the float value {@param value} to {@param dst} at offset {@param offset}
     * @return the number of bytes written to {@param value}
     */
    int putFloat(V dst, int offset, float value);

    default int putBytes(V dst, int offset, byte[] src, int srcOffset, int length)
    {
        return ByteArrayAccessor.instance.copyTo(src, srcOffset, dst, this, offset, length);
    }

    default int putBytes(V dst, int offset, byte[] src)
    {
        return putBytes(dst, offset, src, 0, src.length);
    }

    default int putUnsignedVInt(V dst, int offset, long value)
    {
        return VIntCoding.writeUnsignedVInt(value, dst, offset, this);
    }

    default int putUnsignedVInt32(V dst, int offset, int value)
    {
        return VIntCoding.writeUnsignedVInt32(value, dst, offset, this);
    }

    default int putVInt(V dst, int offset, long value)
    {
        return VIntCoding.writeVInt(value, dst, offset, this);
    }

    default int putVInt32(V dst, int offset, int value)
    {
        return VIntCoding.writeVInt32(value, dst, offset, this);
    }

    /** return a value with a length of 0 */
    V empty();

    /**
     * return a value containing the {@param bytes}
     *
     * Caller should assume that modifying the returned value
     * will also modify the contents of {@param bytes}
     */
    V valueOf(byte[] bytes);

    /**
     * return a value containing the {@param bytes}
     *
     * {@param src} and the returned value may share a common byte array instance, so caller should
     * assume that modifying the returned value will also modify the contents of {@param src}
     */
    V valueOf(ByteBuffer bytes);

    /** return a value containing the bytes for the given string and charset */
    V valueOf(String s, Charset charset);

    /** return a value with the bytes from {@param v}*/
    V valueOf(UUID v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(boolean v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(byte v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(short v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(int v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(long v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(float v);
    /** return a value with the bytes from {@param v}*/
    V valueOf(double v);

    /**
     * Convert the data in {@param src} to {@param <V>}
     *
     * {@param src} and the returned value may share a common byte array instance, so caller should
     * assume that modifying the returned value will also modify the contents of {@param src}
     */
    <V2> V convert(V2 src, ValueAccessor<V2> accessor);

    /**
     * Allocate and return a {@param <V>} instance of {@param size} bytes on the heap.
     */
    V allocate(int size);

    /**
     * returns the {@link ValueAccessor.ObjectFactory} for the backing type {@param <V>}
     */
    ObjectFactory<V> factory();

    /**
     * lexicographically compare {@param left} to {@param right}
     */
    public static <L, R> int compare(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        return leftAccessor.compare(left, right, rightAccessor);
    }

    public static <L, R> boolean equals(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        return compare(left, leftAccessor, right, rightAccessor) == 0;
    }
}
