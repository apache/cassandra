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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.marshal.ValueComparators;
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends AbstractMapSerializer<Map<K, V>>
{
    // interning instances
    @SuppressWarnings("rawtypes")
    private static final ConcurrentMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new ConcurrentHashMap<>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;
    private final ValueComparators comparators;

    @SuppressWarnings("unchecked")
    public static <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, ValueComparators comparators)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
            t = instances.computeIfAbsent(p, k -> new MapSerializer<>(k.left, k.right, comparators));
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, ValueComparators comparators)
    {
        super(true);
        this.keys = keys;
        this.values = values;
        this.comparators = comparators;
    }

    @Override
    public List<ByteBuffer> serializeValues(Map<K, V> map)
    {
        List<Pair<ByteBuffer, ByteBuffer>> pairs = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet())
            pairs.add(Pair.create(keys.serialize(entry.getKey()), values.serialize(entry.getValue())));
        pairs.sort((l, r) -> comparators.buffer.compare(l.left, r.left));
        List<ByteBuffer> buffers = new ArrayList<>(pairs.size() * 2);
        for (Pair<ByteBuffer, ByteBuffer> p : pairs)
        {
            buffers.add(p.left);
            buffers.add(p.right);
        }
        return buffers;
    }

    @Override
    public int getElementCount(Map<K, V> value)
    {
        return value.size();
    }

    @Override
    public <T> void validate(T input, ValueAccessor<T> accessor)
    {
        if (accessor.isEmpty(input))
            throw new MarshalException("Not enough bytes to read a map");
        try
        {
            // Empty values are still valid.
            if (accessor.isEmpty(input)) return;

            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();
            for (int i = 0; i < n; i++)
            {
                T key = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(key, accessor);
                keys.validate(key, accessor);

                T value = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(value, accessor);
                values.validate(value, accessor);
            }
            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after map value");
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public <I> Map<K, V> deserialize(I input, ValueAccessor<I> accessor)
    {
        try
        {
            int n = readCollectionSize(input, accessor);
            int offset = sizeOfCollectionSize();

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a map");

            // If the received bytes are not corresponding to a map, n might be a huge number.
            // In such a case we do not want to initialize the map with that initialCapacity as it can result
            // in an OOM when put is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the map if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Map<K, V> m = new LinkedHashMap<>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                I key = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(key, accessor);
                keys.validate(key, accessor);

                I value = readNonNullValue(input, accessor, offset);
                offset += sizeOfValue(value, accessor);
                values.validate(value, accessor);

                m.put(keys.deserialize(key, accessor), values.deserialize(value, accessor));
            }
            if (!accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Unexpected extraneous bytes after map value");
            return m;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ByteBufferAccessor.instance);
            int offset = sizeOfCollectionSize();
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, ByteBufferAccessor.instance, offset);
                offset += sizeOfValue(kbb, ByteBufferAccessor.instance);
                int comparison = comparator.compareForCQL(kbb, key);
                if (comparison == 0)
                    return readValue(input, ByteBufferAccessor.instance, offset);
                else if (comparison > 0)
                    // since the map is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
                else // comparison < 0
                    offset += skipValue(input, ByteBufferAccessor.instance, offset);
            }
            return null;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public String toString(Map<K, V> value)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean isFirst = true;
        for (Map.Entry<K, V> element : value.entrySet())
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(", ");
            sb.append(keys.toString(element.getKey()));
            sb.append(": ");
            sb.append(values.toString(element.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<Map<K, V>> getType()
    {
        return (Class) Map.class;
    }

    @Override
    public void forEach(ByteBuffer input, Consumer<ByteBuffer> action)
    {
        throw new UnsupportedOperationException();
    }
}
