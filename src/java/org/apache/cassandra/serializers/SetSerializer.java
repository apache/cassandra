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

import org.apache.cassandra.transport.ProtocolVersion;

public class SetSerializer<T> extends CollectionSerializer<Set<T>>
{
    // interning instances
    private static final Map<TypeSerializer<?>, SetSerializer> instances = new HashMap<TypeSerializer<?>, SetSerializer>();

    public final TypeSerializer<T> elements;
    private final Comparator<ByteBuffer> comparator;

    public static synchronized <T> SetSerializer<T> getInstance(TypeSerializer<T> elements, Comparator<ByteBuffer> elementComparator)
    {
        SetSerializer<T> t = instances.get(elements);
        if (t == null)
        {
            t = new SetSerializer<T>(elements, elementComparator);
            instances.put(elements, t);
        }
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
}
