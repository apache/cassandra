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

import org.apache.cassandra.utils.ByteBufferUtil;

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

    public List<T> deserialize(ByteBuffer bytes)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = ByteBufferUtil.readShortLength(input);
            List<T> l = new ArrayList<T>(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer databb = ByteBufferUtil.readBytesWithShortLength(input);
                elements.validate(databb);
                l.add(elements.deserialize(databb));
            }
            return l;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    /**
     * Layout is: {@code <n><s_1><b_1>...<s_n><b_n> }
     * where:
     *   n is the number of elements
     *   s_i is the number of bytes composing the ith element
     *   b_i is the s_i bytes composing the ith element
     */
    public ByteBuffer serialize(List<T> value)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
        int size = 0;
        for (T elt : value)
        {
            ByteBuffer bb = elements.serialize(elt);
            bbs.add(bb);
            size += 2 + bb.remaining();
        }
        return pack(bbs, value.size(), size);
    }

    public String toString(List<T> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (T element : value)
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sb.append("; ");
            }
            sb.append(elements.toString(element));
        }
        return sb.toString();
    }

    public Class<List<T>> getType()
    {
        return (Class) List.class;
    }
}
