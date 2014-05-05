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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new HashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType>();

    public final AbstractType<K> keys;
    public final AbstractType<V> values;
    private final MapSerializer<K, V> serializer;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1));
    }

    public static synchronized <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values)
    {
        Pair<AbstractType<?>, AbstractType<?>> p = Pair.<AbstractType<?>, AbstractType<?>>create(keys, values);
        MapType<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapType<K, V>(keys, values);
            instances.put(p, t);
        }
        return t;
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values)
    {
        super(Kind.MAP);
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(), values.getSerializer());
    }

    public AbstractType<K> nameComparator()
    {
        return keys;
    }

    public AbstractType<V> valueComparator()
    {
        return values;
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        // Note that this is only used if the collection is inside an UDT
        if (o1 == null || !o1.hasRemaining())
            return o2 == null || !o2.hasRemaining() ? 0 : -1;
        if (o2 == null || !o2.hasRemaining())
            return 1;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int size1 = ByteBufferUtil.readShortLength(bb1);
        int size2 = ByteBufferUtil.readShortLength(bb2);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            ByteBuffer k1 = ByteBufferUtil.readBytesWithShortLength(bb1);
            ByteBuffer k2 = ByteBufferUtil.readBytesWithShortLength(bb2);
            int cmp = keys.compare(k1, k2);
            if (cmp != 0)
                return cmp;

            ByteBuffer v1 = ByteBufferUtil.readBytesWithShortLength(bb1);
            ByteBuffer v2 = ByteBufferUtil.readBytesWithShortLength(bb2);
            cmp = values.compare(v1, v2);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    @Override
    public TypeSerializer<Map<K, V>> getSerializer()
    {
        return serializer;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(keys, values)));
    }

    /**
     * Creates the same output than serialize, but from the internal representation.
     */
    public ByteBuffer serialize(List<Pair<ByteBuffer, Column>> columns)
    {
        columns = enforceLimit(columns);

        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * columns.size());
        int size = 0;
        for (Pair<ByteBuffer, Column> p : columns)
        {
            bbs.add(p.left);
            bbs.add(p.right.value());
            size += 4 + p.left.remaining() + p.right.value().remaining();
        }
        return pack(bbs, columns.size(), size);
    }
}
