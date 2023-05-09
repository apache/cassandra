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

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionSerializer<T> extends TypeSerializer<T>
{
    protected abstract List<ByteBuffer> serializeValues(T value);
    protected abstract int getElementCount(T value);

    public abstract <V> T deserializeForNativeProtocol(V value, ValueAccessor<V> accessor, ProtocolVersion version);

    public T deserializeForNativeProtocol(ByteBuffer value, ProtocolVersion version)
    {
        return deserializeForNativeProtocol(value, ByteBufferAccessor.instance, version);
    }

    public abstract <V> void validateForNativeProtocol(V value, ValueAccessor<V> accessor, ProtocolVersion version);

    public ByteBuffer serialize(T input)
    {
        List<ByteBuffer> values = serializeValues(input);
        // See deserialize() for why using the protocol v3 variant is the right thing to do.
        return pack(values, ByteBufferAccessor.instance, getElementCount(input), ProtocolVersion.V3);
    }

    public <V> T deserialize(V value, ValueAccessor<V> accessor)
    {
        // The only cases we serialize/deserialize collections internally (i.e. not for the protocol sake),
        // is:
        //  1) when collections are frozen
        //  2) for internal calls.
        // In both case, using the protocol 3 version variant is the right thing to do.
        return deserializeForNativeProtocol(value, accessor, ProtocolVersion.V3);
    }

    public <T1> void validate(T1 value, ValueAccessor<T1> accessor) throws MarshalException
    {
        // Same thing as above
        validateForNativeProtocol(value, accessor, ProtocolVersion.V3);
    }

    public static ByteBuffer pack(Collection<ByteBuffer> values, int elements, ProtocolVersion version)
    {
        return pack(values, ByteBufferAccessor.instance, elements, version);
    }

    public static <V> V pack(Collection<V> values, ValueAccessor<V> accessor, int elements, ProtocolVersion version)
    {
        int size = 0;
        for (V value : values)
            size += sizeOfValue(value, accessor, version);

        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize(elements, version) + size);
        writeCollectionSize(result, elements, version);
        for (V value : values)
        {
            writeValue(result, value, accessor, version);
        }
        return accessor.valueOf((ByteBuffer) result.flip());
    }

    protected static void writeCollectionSize(ByteBuffer output, int elements, ProtocolVersion version)
    {
        output.putInt(elements);
    }

    public static int readCollectionSize(ByteBuffer input, ProtocolVersion version)
    {
        return readCollectionSize(input, ByteBufferAccessor.instance, version);
    }

    public static <V> int readCollectionSize(V value, ValueAccessor<V> accessor, ProtocolVersion version)
    {
        return accessor.toInt(value);
    }

    public static int sizeOfCollectionSize(int elements, ProtocolVersion version)
    {
        return TypeSizes.INT_SIZE;
    }

    public static <V> void writeValue(ByteBuffer output, V value, ValueAccessor<V> accessor, ProtocolVersion version)
    {
        if (value == null)
        {
            output.putInt(-1);
            return;
        }

        output.putInt(accessor.size(value));
        accessor.write(value, output);
    }

    public static <V> V readValue(V input, ValueAccessor<V> accessor, int offset, ProtocolVersion version)
    {
        int size = accessor.getInt(input, offset);
        if (size < 0)
            return null;

        return accessor.slice(input, offset + TypeSizes.INT_SIZE, size);
    }

    public static <V> V readNonNullValue(V input, ValueAccessor<V> accessor, int offset, ProtocolVersion version)
    {
        V value = readValue(input, accessor, offset, version);
        if (value == null)
            throw new MarshalException("Null value read when not allowed");
        return value;
    }

    protected static void skipValue(ByteBuffer input, ProtocolVersion version)
    {
        int size = input.getInt();
        input.position(input.position() + size);
    }

    public static <V> int skipValue(V input, ValueAccessor<V> accessor, int offset, ProtocolVersion version)
    {
        int size = accessor.getInt(input, offset);
        return TypeSizes.sizeof(size) + size;
    }

    public static <V> int sizeOfValue(V value, ValueAccessor<V> accessor, ProtocolVersion version)
    {
        return value == null ? 4 : 4 + accessor.size(value);
    }

    /**
     * Extract an element from a serialized collection.
     * <p>
     * Note that this is only supported to sets and maps. For sets, this mostly ends up being
     * a check for the presence of the provide key: it will return the key if it's present and
     * {@code null} otherwise.
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param key the key to extract (This cannot be {@code null} nor {@code ByteBufferUtil.UNSET_BYTE_BUFFER}).
     * @param comparator the type to use to compare the {@code key} value to those
     * in the collection.
     * @return the value associated with {@code key} if one exists, {@code null} otherwise
     */
    public abstract ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator);

    /**
     * Returns the slice of a collection directly from its serialized value.
     * <p>If the slice contains no elements an empty collection will be returned for frozen collections, and a 
     * {@code null} one for non-frozen collections.</p>
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param from the left bound of the slice to extract. This cannot be {@code null} but if this is
     * {@code ByteBufferUtil.UNSET_BYTE_BUFFER}, then the returned slice starts at the beginning
     * of {@code collection}.
     * @param comparator the type to use to compare the {@code from} and {@code to} values to those
     * in the collection.
     * @param frozen {@code true} if the collection is a frozen one, {@code false} otherwise
     * @return a serialized collection corresponding to slice {@code [from, to]} of {@code collection}.
     */
    public abstract ByteBuffer getSliceFromSerialized(ByteBuffer collection,
                                                      ByteBuffer from,
                                                      ByteBuffer to,
                                                      AbstractType<?> comparator,
                                                      boolean frozen);

    /**
     * Creates a new serialized map composed from the data from {@code input} between {@code startPos}
     * (inclusive) and {@code endPos} (exclusive), assuming that data holds {@code count} elements.
     */
    protected ByteBuffer copyAsNewCollection(ByteBuffer input, int count, int startPos, int endPos, ProtocolVersion version)
    {
        int sizeLen = sizeOfCollectionSize(count, version);
        if (count == 0)
            return ByteBuffer.allocate(sizeLen);

        int bodyLen = endPos - startPos;
        ByteBuffer output = ByteBuffer.allocate(sizeLen + bodyLen);
        writeCollectionSize(output, count, version);
        output.position(0);
        ByteBufferUtil.copyBytes(input, startPos, output, sizeLen, bodyLen);
        return output;
    }
}
