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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueComparators;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final ConcurrentMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new ConcurrentHashMap<>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;
    private final ValueComparators comparators;

    public static <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, ValueComparators comparators)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
            t = instances.computeIfAbsent(p, k -> new MapSerializer<>(k.left, k.right, comparators) );
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, ValueComparators comparators)
    {
        this.keys = keys;
        this.values = values;
        this.comparators = comparators;
    }

    public List<ByteBuffer> serializeValues(Map<K, V> map)
    {
        List<Pair<ByteBuffer, ByteBuffer>> pairs = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet())
            pairs.add(Pair.create(keys.serialize(entry.getKey()), values.serialize(entry.getValue())));
        Collections.sort(pairs, (l, r) -> comparators.buffer.compare(l.left, r.left));
        List<ByteBuffer> buffers = new ArrayList<>(pairs.size() * 2);
        for (Pair<ByteBuffer, ByteBuffer> p : pairs)
        {
            buffers.add(p.left);
            buffers.add(p.right);
        }
        return buffers;
    }

    public int getElementCount(Map<K, V> value)
    {
        return value.size();
    }

    public <T> void validateForNativeProtocol(T input, ValueAccessor<T> accessor, ProtocolVersion version)
    {
        try
        {
            // Empty values are still valid.
            if (accessor.isEmpty(input)) return;
            
            int n = readCollectionSize(input, accessor, version);
            int offset = sizeOfCollectionSize(n, version);
            for (int i = 0; i < n; i++)
            {
                T key = readNonNullValue(input, accessor, offset, version);
                offset += sizeOfValue(key, accessor, version);
                keys.validate(key, accessor);

                T value = readNonNullValue(input, accessor, offset, version);
                offset += sizeOfValue(value, accessor, version);
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

    public <I> Map<K, V> deserializeForNativeProtocol(I input, ValueAccessor<I> accessor, ProtocolVersion version)
    {
        try
        {
            int n = readCollectionSize(input, accessor, version);
            int offset = sizeOfCollectionSize(n, version);

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a map");

            // If the received bytes are not corresponding to a map, n might be a huge number.
            // In such a case we do not want to initialize the map with that initialCapacity as it can result
            // in an OOM when put is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the map if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Map<K, V> m = new LinkedHashMap<K, V>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                I key = readNonNullValue(input, accessor, offset, version);
                offset += sizeOfValue(key, accessor, version);
                keys.validate(key, accessor);

                I value = readNonNullValue(input, accessor, offset, version);
                offset += sizeOfValue(value, accessor, version);
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
            int n = readCollectionSize(input, ProtocolVersion.V3);
            int offset = sizeOfCollectionSize(n, ProtocolVersion.V3);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, ByteBufferAccessor.instance, offset, ProtocolVersion.V3);
                offset += sizeOfValue(kbb, ByteBufferAccessor.instance, ProtocolVersion.V3);
                int comparison = comparator.compareForCQL(kbb, key);
                if (comparison == 0)
                    return readValue(input, ByteBufferAccessor.instance, offset, ProtocolVersion.V3);
                else if (comparison > 0)
                    // since the map is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
                else // comparison < 0
                    offset += skipValue(input, ByteBufferAccessor.instance, offset, ProtocolVersion.V3);
            }
            return null;
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public ByteBuffer getSliceFromSerialized(ByteBuffer collection,
                                             ByteBuffer from,
                                             ByteBuffer to,
                                             AbstractType<?> comparator,
                                             boolean frozen)
    {
        if (from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return collection;

        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            input.position(input.position() + sizeOfCollectionSize(n, ProtocolVersion.V3));
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for (int i = 0; i < n; i++)
            {
                int pos = input.position();
                ByteBuffer kbb = readValue(input, ByteBufferAccessor.instance, 0, ProtocolVersion.V3); // key
                input.position(input.position() + sizeOfValue(kbb, ByteBufferAccessor.instance, ProtocolVersion.V3));

                // If we haven't passed the start already, check if we have now
                if (!inSlice)
                {
                    int comparison = comparator.compareForCQL(from, kbb);
                    if (comparison <= 0)
                    {
                        // We're now within the slice
                        inSlice = true;
                        startPos = pos;
                    }
                    else
                    {
                        // We're before the slice so we know we don't care about this element
                        skipValue(input, ProtocolVersion.V3); // value
                        continue;
                    }
                }

                // Now check if we're done
                int comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? -1 : comparator.compareForCQL(kbb, to);
                if (comparison > 0)
                {
                    // We're done and shouldn't include the key we just read
                    input.position(pos);
                    break;
                }

                // Otherwise, we'll include that element
                skipValue(input, ProtocolVersion.V3); // value
                ++count;

                // But if we know if was the last of the slice, we break early
                if (comparison == 0)
                    break;
            }

            if (count == 0 && !frozen)
                return null;

            return copyAsNewCollection(collection, count, startPos, input.position(), ProtocolVersion.V3);
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

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

    public Class<Map<K, V>> getType()
    {
        return (Class)Map.class;
    }
}
