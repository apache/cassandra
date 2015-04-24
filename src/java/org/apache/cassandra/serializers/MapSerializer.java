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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.Server;
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

    public List<ByteBuffer> serializeValues(Map<K, V> map)
    {
        List<ByteBuffer> buffers = new ArrayList<>(map.size() * 2);
        for (Map.Entry<K, V> entry : map.entrySet())
        {
            buffers.add(keys.serialize(entry.getKey()));
            buffers.add(values.serialize(entry.getValue()));
        }
        return buffers;
    }

    public int getElementCount(Map<K, V> value)
    {
        return value.size();
    }

    public void validateForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            for (int i = 0; i < n; i++)
            {
                keys.validate(readValue(input, version));
                values.validate(readValue(input, version));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after map value");
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

    public Map<K, V> deserializeForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            Map<K, V> m = new LinkedHashMap<K, V>(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, version);
                keys.validate(kbb);

                ByteBuffer vbb = readValue(input, version);
                values.validate(vbb);

                m.put(keys.deserialize(kbb), values.deserialize(vbb));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after map value");
            return m;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    /**
     * Given a serialized map, gets the value associated with a given key.
     * @param serializedMap a serialized map
     * @param serializedKey a serialized key
     * @param keyType the key type for the map
     * @return the value associated with the key if one exists, null otherwise
     */
    public ByteBuffer getSerializedValue(ByteBuffer serializedMap, ByteBuffer serializedKey, AbstractType keyType)
    {
        try
        {
            ByteBuffer input = serializedMap.duplicate();
            int n = readCollectionSize(input, Server.VERSION_3);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, Server.VERSION_3);
                ByteBuffer vbb = readValue(input, Server.VERSION_3);
                int comparison = keyType.compare(kbb, serializedKey);
                if (comparison == 0)
                    return vbb;
                else if (comparison > 0)
                    // since the map is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
            }
            return null;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public String toString(Map<K, V> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Map.Entry<K, V> element : value.entrySet())
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append("; ");
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
