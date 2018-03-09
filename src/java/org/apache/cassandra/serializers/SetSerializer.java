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

import org.apache.cassandra.transport.ProtocolVersion;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SetSerializer<T> extends CollectionSerializer<Set<T>>
{
    // interning instances
    private static final ConcurrentMap<TypeSerializer<?>, SetSerializer> instances = new ConcurrentHashMap<TypeSerializer<?>, SetSerializer>();

    public final TypeSerializer<T> elements;
    private final Comparator<ByteBuffer> comparator;

    public static <T> SetSerializer<T> getInstance(TypeSerializer<T> elements, Comparator<ByteBuffer> elementComparator)
    {
        SetSerializer<T> t = instances.get(elements);
        if (t == null)
            t = instances.computeIfAbsent(elements, k -> new SetSerializer<>(k, elementComparator) );
        return t;
    }

    private SetSerializer(TypeSerializer<T> elements, Comparator<ByteBuffer> comparator)
    {
        this.elements = elements;
        this.comparator = comparator;
    }

    public List<ByteBuffer> serializeValues(Set<T> values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (T value : values)
            buffers.add(elements.serialize(value));
        Collections.sort(buffers, comparator);
        return buffers;
    }

    public int getElementCount(Set<T> value)
    {
        return value.size();
    }

    public void validateForNativeProtocol(ByteBuffer bytes, ProtocolVersion version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            for (int i = 0; i < n; i++)
                elements.validate(readValue(input, version));
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after set value");
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

    public Set<T> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a set");

            // If the received bytes are not corresponding to a set, n might be a huge number.
            // In such a case we do not want to initialize the set with that initialCapacity as it can result
            // in an OOM when add is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the set if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Set<T> l = new LinkedHashSet<T>(Math.min(n, 256));

            for (int i = 0; i < n; i++)
            {
                ByteBuffer databb = readValue(input, version);
                elements.validate(databb);
                l.add(elements.deserialize(databb));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after set value");
            return l;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

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

    public Class<Set<T>> getType()
    {
        return (Class) Set.class;
    }

    @Override
    public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer value = readValue(input, ProtocolVersion.V3);
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
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
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
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for (int i = 0; i < n; i++)
            {
                int pos = input.position();
                ByteBuffer value = readValue(input, ProtocolVersion.V3);

                // If we haven't passed the start already, check if we have now
                if (!inSlice)
                {
                    int comparison = comparator.compareForCQL(from, value);
                    if (comparison <= 0)
                    {
                        // We're now within the slice
                        inSlice = true;
                        startPos = pos;
                    }
                    else
                    {
                        // We're before the slice so we know we don't care about this value
                        continue;
                    }
                }

                // Now check if we're done
                int comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? -1 : comparator.compareForCQL(value, to);
                if (comparison > 0)
                {
                    // We're done and shouldn't include the value we just read
                    input.position(pos);
                    break;
                }

                // Otherwise, we'll include that value
                ++count;

                // But if we know if was the last of the slice, we break early
                if (comparison == 0)
                    break;
            }

            if (count == 0 && !frozen)
                return null;

            return copyAsNewCollection(collection, count, startPos, input.position(), ProtocolVersion.V3);
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }
}
