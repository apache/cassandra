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

import org.apache.cassandra.transport.ProtocolVersion;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;

public class ListSerializer<T> extends CollectionSerializer<List<T>>
{
    // interning instances
    private static final Map<TypeSerializer<?>, ListSerializer> instances = new HashMap<TypeSerializer<?>, ListSerializer>();

    public final TypeSerializer<T> elements;

    public static synchronized <T> ListSerializer<T> getInstance(TypeSerializer<T> elements)
    {
        ListSerializer<T> t = instances.get(elements);
        if (t == null)
        {
            t = new ListSerializer<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListSerializer(TypeSerializer<T> elements)
    {
        this.elements = elements;
    }

    public List<ByteBuffer> serializeValues(List<T> values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (T value : values)
            buffers.add(elements.serialize(value));
        return buffers;
    }

    public int getElementCount(List<T> value)
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
                throw new MarshalException("Unexpected extraneous bytes after list value");
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    public List<T> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a list");

            // If the received bytes are not corresponding to a list, n might be a huge number.
            // In such a case we do not want to initialize the list with that size as it can result
            // in an OOM (see CASSANDRA-12618). On the other hand we do not want to have to resize the list
            // if we can avoid it, so we put a reasonable limit on the initialCapacity.
            List<T> l = new ArrayList<T>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                // We can have nulls in lists that are used for IN values
                ByteBuffer databb = readValue(input, version);
                if (databb != null)
                {
                    elements.validate(databb);
                    l.add(elements.deserialize(databb));
                }
                else
                {
                    l.add(null);
                }
            }

            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after list value");

            return l;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    /**
     * Returns the element at the given index in a list.
     * @param serializedList a serialized list
     * @param index the index to get
     * @return the serialized element at the given index, or null if the index exceeds the list size
     */
    public ByteBuffer getElement(ByteBuffer serializedList, int index)
    {
        try
        {
            ByteBuffer input = serializedList.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            if (n <= index)
                return null;

            for (int i = 0; i < index; i++)
            {
                int length = input.getInt();
                input.position(input.position() + length);
            }
            return readValue(input, ProtocolVersion.V3);
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

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

    public Class<List<T>> getType()
    {
        return (Class) List.class;
    }
}
