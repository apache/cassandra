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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.marshal.ValueComparators;

public class SetSerializer<T> extends AbstractMapSerializer<Set<T>>
{
    // interning instances
    @SuppressWarnings("rawtypes")
    private static final ConcurrentMap<TypeSerializer<?>, SetSerializer> instances = new ConcurrentHashMap<>();

    public final TypeSerializer<T> elements;
    private final ValueComparators comparators;

    @SuppressWarnings("unchecked")
    public static <T> SetSerializer<T> getInstance(TypeSerializer<T> elements, ValueComparators comparators)
    {
        SetSerializer<T> t = instances.get(elements);
        if (t == null)
            t = instances.computeIfAbsent(elements, k -> new SetSerializer<>(k, comparators) );
        return t;
    }

    public SetSerializer(TypeSerializer<T> elements, ValueComparators comparators)
    {
        super(false);
        this.elements = elements;
        this.comparators = comparators;
    }

    @Override
    public List<ByteBuffer> serializeValues(Set<T> values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (T value : values)
            buffers.add(elements.serialize(value));
        buffers.sort(comparators.buffer);
        return buffers;
    }

    @Override
    public int getElementCount(Set<T> value)
    {
        return value.size();
    }

    @Override
    public <V> void validate(V input, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(input))
            throw new MarshalException("Not enough bytes to read a set");
        try
        {
            // Empty values are still valid.
            if (accessor.isEmpty(input)) return;
            
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();
            for (int i = 0; i < n; i++)
            {
                V value = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(value, accessor);
                elements.validate(value, accessor);
            }
            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after set value");
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

    @Override
    public <V> Set<T> deserialize(V input, ValueAccessor<V> accessor)
    {
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a set");

            // If the received bytes are not corresponding to a set, n might be a huge number.
            // In such a case we do not want to initialize the set with that initialCapacity as it can result
            // in an OOM when add is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the set if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Set<T> l = new LinkedHashSet<>(Math.min(n, 256));

            for (int i = 0; i < n; i++)
            {
                V value = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(value, accessor);
                elements.validate(value, accessor);
                l.add(elements.deserialize(value, accessor));
            }
            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after set value");
            return l;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

    @Override
    public String toString(Set<T> value)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean isFirst = true;
        for (T element : value)
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sb.append(", ");
            }
            sb.append(elements.toString(element));
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<Set<T>> getType()
    {
        return (Class) Set.class;
    }

    @Override
    public ByteBuffer getSerializedValue(ByteBuffer input, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            int n = readCollectionSize(input, ByteBufferAccessor.instance);
            int offset = sizeOfCollectionSize();

            for (int i = 0; i < n; i++)
            {
                ByteBuffer value = readValue(input, ByteBufferAccessor.instance, offset);
                offset += sizeOfValue(value, ByteBufferAccessor.instance);
                int comparison = comparator.compareForCQL(value, key);
                if (comparison == 0)
                    return value;
                else if (comparison > 0)
                    // since the set is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
                // else, we're before the element so continue
            }
            return null;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }
}
