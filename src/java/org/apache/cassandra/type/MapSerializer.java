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

package org.apache.cassandra.type;

import org.apache.cassandra.utils.Pair;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<AbstractSerializer<?>, AbstractSerializer<?>>, MapSerializer> instances = new HashMap<Pair<AbstractSerializer<?>, AbstractSerializer<?>>, MapSerializer>();

    public final AbstractSerializer<K> keys;
    public final AbstractSerializer<V> values;

    public static synchronized <K, V> MapSerializer<K, V> getInstance(AbstractSerializer<K> keys, AbstractSerializer<V> values)
    {
        Pair<AbstractSerializer<?>, AbstractSerializer<?>> p = Pair.<AbstractSerializer<?>, AbstractSerializer<?>>create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapSerializer<K, V>(keys, values);
            instances.put(p, t);
        }
        return t;
    }

    private MapSerializer(AbstractSerializer<K> keys, AbstractSerializer<V> values)
    {
        this.keys = keys;
        this.values = values;
    }

    @Override
    public Map<K, V> serialize(ByteBuffer bytes)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = getUnsignedShort(input);
            Map<K, V> m = new LinkedHashMap<K, V>(n);
            for (int i = 0; i < n; i++)
            {
                int sk = getUnsignedShort(input);
                byte[] datak = new byte[sk];
                input.get(datak);
                ByteBuffer kbb = ByteBuffer.wrap(datak);
                keys.validate(kbb);

                int sv = getUnsignedShort(input);
                byte[] datav = new byte[sv];
                input.get(datav);
                ByteBuffer vbb = ByteBuffer.wrap(datav);
                values.validate(vbb);

                m.put(keys.serialize(kbb), values.serialize(vbb));
            }
            return m;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public ByteBuffer deserialize(Map<K, V> value)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
        int size = 0;
        for (Map.Entry<K, V> entry : value.entrySet())
        {
            ByteBuffer bbk = keys.deserialize(entry.getKey());
            ByteBuffer bbv = values.deserialize(entry.getValue());
            bbs.add(bbk);
            bbs.add(bbv);
            size += 4 + bbk.remaining() + bbv.remaining();
        }
        return pack(bbs, value.size(), size);
    }

    @Override
    public String toString(Map<K, V> value)
    {
        StringBuffer sb = new StringBuffer();
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

    @Override
    public Class<Map<K, V>> getType()
    {
        return (Class)Map.class;
    }
}
