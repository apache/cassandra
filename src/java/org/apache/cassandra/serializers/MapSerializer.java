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
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new HashMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;

    public static synchronized <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.<TypeSerializer<?>, TypeSerializer<?>>create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapSerializer<K, V>(keys, values);
            instances.put(p, t);
        }
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values)
    {
        this.keys = keys;
        this.values = values;
    }

    public Map<K, V> deserialize(ByteBuffer bytes)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = ByteBufferUtil.readShortLength(input);
            Map<K, V> m = new LinkedHashMap<K, V>(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = ByteBufferUtil.readBytesWithShortLength(input);
                keys.validate(kbb);

                ByteBuffer vbb = ByteBufferUtil.readBytesWithShortLength(input);
                values.validate(vbb);

                m.put(keys.deserialize(kbb), values.deserialize(vbb));
            }
            return m;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    /**
     * Layout is: {@code <n><sk_1><k_1><sv_1><v_1>...<sk_n><k_n><sv_n><v_n> }
     * where:
     *   n is the number of elements
     *   sk_i is the number of bytes composing the ith key k_i
     *   k_i is the sk_i bytes composing the ith key
     *   sv_i is the number of bytes composing the ith value v_i
     *   v_i is the sv_i bytes composing the ith value
     */
    public ByteBuffer serialize(Map<K, V> value)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
        int size = 0;
        for (Map.Entry<K, V> entry : value.entrySet())
        {
            ByteBuffer bbk = keys.serialize(entry.getKey());
            ByteBuffer bbv = values.serialize(entry.getValue());
            bbs.add(bbk);
            bbs.add(bbv);
            size += 4 + bbk.remaining() + bbv.remaining();
        }
        return pack(bbs, value.size(), size);
    }

    public String toString(Map<K, V> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Map.Entry<K, V> element : value.entrySet())
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sb.append("; ");
            }
            sb.append('(');
            sb.append(keys.toString(element.getKey()));
            sb.append(", ");
            sb.append(values.toString(element.getValue()));
            sb.append(')');
        }
        return sb.toString();
    }

    public Class<Map<K, V>> getType()
    {
        return (Class)Map.class;
    }
}
