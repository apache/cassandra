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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.Range;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionSerializer<T> extends TypeSerializer<T>
{
    protected abstract List<ByteBuffer> serializeValues(T value);
    protected abstract int getElementCount(T value);

    @Override
    public ByteBuffer serialize(T input)
    {
        List<ByteBuffer> values = serializeValues(input);
        return pack(values, ByteBufferAccessor.instance, getElementCount(input));
    }

    public static ByteBuffer pack(Collection<ByteBuffer> values, int elements)
    {
        return pack(values, ByteBufferAccessor.instance, elements);
    }

    public static <V> V pack(Collection<V> values, ValueAccessor<V> accessor, int elements)
    {
        int size = 0;
        for (V value : values)
            size += sizeOfValue(value, accessor);

        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize() + size);
        writeCollectionSize(result, elements);
        for (V value : values)
        {
            writeValue(result, value, accessor);
        }
        return accessor.valueOf((ByteBuffer) result.flip());
    }

    protected static void writeCollectionSize(ByteBuffer output, int elements)
    {
        output.putInt(elements);
    }

    public static <V> int readCollectionSize(V value, ValueAccessor<V> accessor)
    {
        return accessor.toInt(value);
    }

    public static int sizeOfCollectionSize()
    {
        return TypeSizes.INT_SIZE;
    }

    public static <V> void writeValue(ByteBuffer output, V value, ValueAccessor<V> accessor)
    {
        if (value == null)
        {
            output.putInt(-1);
            return;
        }

        output.putInt(accessor.size(value));
        accessor.write(value, output);
    }

    public static <V> V readValue(V input, ValueAccessor<V> accessor, int offset)
    {
        int size = accessor.getInt(input, offset);
        if (size < 0)
            return null;

        return accessor.slice(input, offset + TypeSizes.INT_SIZE, size);
    }

    public static <V> V readNonNullValue(V input, ValueAccessor<V> accessor, int offset)
    {
        V value = readValue(input, accessor, offset);
        if (value == null)
            throw new MarshalException("Null value read when not allowed");
        return value;
    }

    protected static void skipValue(ByteBuffer input)
    {
        int size = input.getInt();
        input.position(input.position() + size);
    }

    public static <V> int skipValue(V input, ValueAccessor<V> accessor, int offset)
    {
        int size = accessor.getInt(input, offset);
        return TypeSizes.sizeof(size) + size;
    }

    public static <V> int sizeOfValue(V value, ValueAccessor<V> accessor)
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
     * Returns the index of an element in a serialized collection.
     * <p>
     * Note that this is only supported by sets and maps, but not by lists.
     *
     * @param collection The serialized collection. This cannot be {@code null}.
     * @param key The key for which the index must be found. This cannot be {@code null} nor
     * {@link ByteBufferUtil#UNSET_BYTE_BUFFER}).
     * @param comparator The type to use to compare the {@code key} value to those in the collection.
     * @return The index of the element associated with {@code key} if one exists, {@code -1} otherwise.
     */
    public abstract int getIndexFromSerialized(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator);

    /**
     * Returns the range of indexes corresponding to the specified range of elements in the serialized collection.
     * <p>
     * Note that this is only supported by sets and maps, but not by lists.
     *
     * @param collection The serialized collection. This cannot be {@code null}.
     * @param from  The left bound of the slice to extract. This cannot be {@code null} but if this is
     * {@link ByteBufferUtil#UNSET_BYTE_BUFFER}, then the returned slice starts at the beginning of the collection.
     * @param to The left bound of the slice to extract. This cannot be {@code null} but if this is
     * {@link ByteBufferUtil#UNSET_BYTE_BUFFER}, then the returned slice ends at the end of the collection.
     * @param comparator The type to use to compare the {@code from} and {@code to} values to those in the collection.
     * @return The range of indexes corresponding to specified range of elements.
     */
    public abstract Range<Integer> getIndexesRangeFromSerialized(ByteBuffer collection,
                                                                 ByteBuffer from,
                                                                 ByteBuffer to,
                                                                 AbstractType<?> comparator);

    /**
     * Creates a new serialized map composed from the data from {@code input} between {@code startPos}
     * (inclusive) and {@code endPos} (exclusive), assuming that data holds {@code count} elements.
     */
    protected ByteBuffer copyAsNewCollection(ByteBuffer input, int count, int startPos, int endPos)
    {
        int sizeLen = sizeOfCollectionSize();
        if (count == 0)
            return ByteBuffer.allocate(sizeLen);

        int bodyLen = endPos - startPos;
        ByteBuffer output = ByteBuffer.allocate(sizeLen + bodyLen);
        writeCollectionSize(output, count);
        output.position(0);
        ByteBufferUtil.copyBytes(input, startPos, output, sizeLen, bodyLen);
        return output;
    }

    public void forEach(ByteBuffer input, Consumer<ByteBuffer> action)
    {
        try
        {
            int collectionSize = readCollectionSize(input, ByteBufferAccessor.instance);
            int offset = sizeOfCollectionSize();

            for (int i = 0; i < collectionSize; i++)
            {
                ByteBuffer value = readValue(input, ByteBufferAccessor.instance, offset);
                offset += sizeOfValue(value, ByteBufferAccessor.instance);

                action.accept(value);
            }
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }
}
