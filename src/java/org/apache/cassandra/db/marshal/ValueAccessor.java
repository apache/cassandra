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
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface ValueAccessor<V>
{
    public enum BackingKind { BUFFER, ARRAY, NATIVE }

    int size(V value);

    BackingKind getBackingKind();

    default int sizeFromOffset(V value, int offset)
    {
        return size(value) - offset;
    }

    V[] createArray(int length);

    void write(V value, DataOutputPlus out) throws IOException;
    void write(V value, ByteBuffer out);
    V read(DataInputPlus in, int length) throws IOException;
    V slice(V input, int offset, int length);
    default V sliceWithShortLength(V input, int offset)
    {
        int size = getShort(input, offset);
        return slice(input, offset + 2, size);
    }

    default int sizeWithShortLength(V value)
    {
        return 2 + size(value);
    }

    default int getShortLength(V v, int position)
    {
        int length = (getByte(v, position) & 0xFF) << 8;
        return length | (getByte(v, position + 1) & 0xFF);
    }

    int compareUnsigned(V left, V right);

    ByteBuffer toBuffer(V value);

    /**
     * returns a modifiable buffer
     */
    ByteBuffer toSafeBuffer(V value);

    byte[] toArray(V value);
    byte[] toArray(V value, int offset, int length);
    String toString(V value, Charset charset) throws CharacterCodingException;
    default String toString(V value) throws CharacterCodingException
    {
        return toString(value, StandardCharsets.UTF_8);
    }
    String toHex(V value);

    byte toByte(V value);
    byte getByte(V value, int offset);
    short toShort(V value);
    short getShort(V value, int offset);
    int toInt(V value);
    int getInt(V value, int offset);
    long toLong(V value);
    long getLong(V value, int offset);
    float toFloat(V value);
    double toDouble(V value);
    UUID toUUID(V value);

    default void writeWithVIntLength(V value, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(size(value));
        write(value, out);
    }

    default boolean isEmpty(V value)
    {
        return size(value) == 0;
    }

    default int sizeWithVIntLength(V value)
    {
        int size = size(value);
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }

    V empty();
    V valueOf(byte[] bytes);
    V valueOf(ByteBuffer bytes);
    V valueOf(String s, Charset charset);
    V valueOf(UUID v);
    V valueOf(boolean v);
    V valueOf(byte v);
    V valueOf(short v);
    V valueOf(int v);
    V valueOf(long v);
    V valueOf(float v);
    V valueOf(double v);

    public static <L, R> int compare(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        return leftAccessor.toBuffer(left).compareTo(rightAccessor.toBuffer(right)); /// FIXME:
    }

    public static <L, R> int compareUnsigned(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        if (leftAccessor.getBackingKind() == BackingKind.ARRAY && rightAccessor.getBackingKind() == BackingKind.ARRAY)
            return ByteArrayUtil.compareUnsigned(leftAccessor.toArray(left), rightAccessor.toArray(right));
        return ByteBufferUtil.compareUnsigned(leftAccessor.toBuffer(left), rightAccessor.toBuffer(right));  // FIXME
    }

    public static <L, R> int compare(ValueAware<L> left, ValueAware<R> right)
    {
        return compare(left.value(), left.accessor(), right.value(), right.accessor());
    }

    public static <L, R> boolean equals(L left, ValueAccessor<L> accessorL, R right, ValueAccessor<R> accessorR)
    {
        return Objects.equals(accessorL.toBuffer(left), accessorR.toBuffer(right));  // FIXME
    }

    public static <T> int hashCode(T value, ValueAccessor<T> accessor)
    {
        return Objects.hashCode(accessor.toBuffer(value));  // FIXME
    }
}
