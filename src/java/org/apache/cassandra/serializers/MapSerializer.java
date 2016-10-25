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
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new HashMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;
    private final Comparator<Pair<ByteBuffer, ByteBuffer>> comparator;

    public static synchronized <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.<TypeSerializer<?>, TypeSerializer<?>>create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapSerializer<K, V>(keys, values, comparator);
            instances.put(p, t);
        }
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator)
    {
        this.keys = keys;
        this.values = values;
        this.comparator = (p1, p2) -> comparator.compare(p1.left, p2.left);
    }

    public List<ByteBuffer> serializeValues(Map<K, V> map)
    {
        List<Pair<ByteBuffer, ByteBuffer>> pairs = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet())
            pairs.add(Pair.create(keys.serialize(entry.getKey()), values.serialize(entry.getValue())));
        Collections.sort(pairs, comparator);
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

    public void validateForNativeProtocol(ByteBuffer bytes, ProtocolVersion version)
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
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public Map<K, V> deserializeForNativeProtocol(ByteBuffer bytes, ProtocolVersion version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a map");

            // If the received bytes are not corresponding to a map, n might be a huge number.
            // In such a case we do not want to initialize the map with that initialCapacity as it can result
            // in an OOM when put is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the map if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Map<K, V> m = new LinkedHashMap<K, V>(Math.min(n, 256));
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
            int n = readCollectionSize(input, ProtocolVersion.V3);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, ProtocolVersion.V3);
                ByteBuffer vbb = readValue(input, ProtocolVersion.V3);
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
