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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import com.google.common.collect.Range;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;

public class ListSerializer<T> extends CollectionSerializer<List<T>>
{
    // interning instances
    @SuppressWarnings("rawtypes")
    private static final ConcurrentMap<TypeSerializer<?>, ListSerializer> instances = new ConcurrentHashMap<>();

    public final TypeSerializer<T> elements;

    @SuppressWarnings("unchecked")
    public static <T> ListSerializer<T> getInstance(TypeSerializer<T> elements)
    {
        ListSerializer<T> t = instances.get(elements);
        if (t == null)
            t = instances.computeIfAbsent(elements, ListSerializer::new);
        return t;
    }

    private ListSerializer(TypeSerializer<T> elements)
    {
        this.elements = elements;
    }

    @Override
    protected List<ByteBuffer> serializeValues(List<T> values)
    {
        List<ByteBuffer> output = new ArrayList<>(values.size());
        for (T value: values)
            output.add(elements.serialize(value));
        return output;
    }

    @Override
    public int getElementCount(List<T> value)
    {
        return value.size();
    }

    @Override
    public <V> void validate(V input, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(input))
            throw new MarshalException("Not enough bytes to read a list");
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();
            for (int i = 0; i < n; i++)
            {
                V value = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(value, accessor);
                elements.validate(value, accessor);
            }

            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after list value");
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    @Override
    public <V> List<T> deserialize(V input, ValueAccessor<V> accessor)
    {
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a list");

            // If the received bytes are not corresponding to a list, n might be a huge number.
            // In such a case we do not want to initialize the list with that size as it can result
            // in an OOM (see CASSANDRA-12618). On the other hand we do not want to have to resize the list
            // if we can avoid it, so we put a reasonable limit on the initialCapacity.
            List<T> l = new ArrayList<>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                // CASSANDRA-6839: "We can have nulls in lists that are used for IN values"
                // CASSANDRA-8613 checks IN clauses and throws an exception if null is in the list.
                // Leaving for this as-is for now in case there is some unknown use
                // for it, but should likely be changed to readNonNull. Validate has been
                // changed to throw on null elements as otherwise it would NPE, and it's unclear
                // if callers could handle null elements.
                V databb = readValue(input, accessor, offset);
                offset += sizeOfValue(databb, accessor);
                if (databb != null)
                {
                    elements.validate(databb, accessor);
                    l.add(elements.deserialize(databb, accessor));
                }
                else
                {
                    l.add(null);
                }
            }

            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after list value");

            return l;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    public boolean anyMatch(ByteBuffer serializedList, Predicate<ByteBuffer> predicate)
    {
        return anyMatch(serializedList, ByteBufferAccessor.instance, predicate);
    }

    public <V> boolean anyMatch(V input, ValueAccessor<V> accessor, Predicate<V> predicate)
    {
        try
        {
            int s = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();

            for (int i = 0; i < s; i++)
            {
                int size = accessor.getInt(input, offset);
                if (size < 0)
                    continue;

                offset += TypeSizes.INT_SIZE;

                V value = accessor.slice(input, offset, size);

                if (predicate.test(value))
                    return true;

                offset += size;
            }
            return false;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    /**
     * Returns the element at the given index in a list.
     * @param input a serialized list
     * @param index the index to get
     * @return the serialized element at the given index, or null if the index exceeds the list size
     */
    public <V> V getElement(V input, ValueAccessor<V> accessor, int index)
    {
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();
            if (n <= index)
                return null;

            for (int i = 0; i < index; i++)
            {
                int length = accessor.getInt(input, offset);
                offset += TypeSizes.INT_SIZE + length;
            }
            return readValue(input, accessor, offset);
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    public ByteBuffer getElement(ByteBuffer input, int index)
    {
        return getElement(input, ByteBufferAccessor.instance, index);
    }

    @Override
    public String toString(List<T> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        sb.append('[');
        for (T element : value)
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(", ");
            sb.append(elements.toString(element));
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<List<T>> getType()
    {
        return (Class) List.class;
    }

    @Override
    public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        // We don't allow selecting an element of a list, so we don't need this.
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getSliceFromSerialized(ByteBuffer collection,
                                             ByteBuffer from,
                                             ByteBuffer to,
                                             AbstractType<?> comparator,
                                             boolean frozen)
    {
        // We don't allow slicing of lists, so we don't need this.
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIndexFromSerialized(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range<Integer> getIndexesRangeFromSerialized(ByteBuffer collection,
                                                        ByteBuffer from,
                                                        ByteBuffer to,
                                                        AbstractType<?> comparator)
    {
        throw new UnsupportedOperationException();
    }
}
