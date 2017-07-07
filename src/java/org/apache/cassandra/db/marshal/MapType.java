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

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new HashMap<>();
    private static final Map<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new HashMap<>();

    private final AbstractType<K> keys;
    private final AbstractType<V> values;
    private final MapSerializer<K, V> serializer;
    private final boolean isMultiCell;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1), true);
    }

    public static synchronized <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        Map<Pair<AbstractType<?>, AbstractType<?>>, MapType> internMap = isMultiCell ? instances : frozenInstances;
        Pair<AbstractType<?>, AbstractType<?>> p = Pair.<AbstractType<?>, AbstractType<?>>create(keys, values);
        MapType<K, V> t = internMap.get(p);
        if (t == null)
        {
            t = new MapType<>(keys, values, isMultiCell);
            internMap.put(p, t);
        }
        return t;
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.MAP);
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(), values.getSerializer(), keys);
        this.isMultiCell = isMultiCell;
    }

    @Override
    public boolean referencesUserType(String userTypeName)
    {
        return getKeysType().referencesUserType(userTypeName) ||
               getValuesType().referencesUserType(userTypeName);
    }

    @Override
    public boolean referencesDuration()
    {
        // Maps cannot be created with duration as keys
        return getValuesType().referencesDuration();
    }

    public AbstractType<K> getKeysType()
    {
        return keys;
    }

    public AbstractType<V> getValuesType()
    {
        return values;
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
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public AbstractType<?> freeze()
    {
        if (isMultiCell)
            return getInstance(this.keys, this.values, false);
        else
            return this;
    }

    @Override
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        if (!isMultiCell())
            return this;

        AbstractType<?> keyType = (keys.isFreezable() && keys.isMultiCell())
                                ? keys.freeze()
                                : keys.freezeNestedMulticellTypes();

        AbstractType<?> valueType = (values.isFreezable() && values.isMultiCell())
                                  ? values.freeze()
                                  : values.freezeNestedMulticellTypes();

        return getInstance(keyType, valueType, isMultiCell);
    }

    @Override
    public boolean isCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType tprev = (MapType) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isCompatibleWith(tprev.values);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType tprev = (MapType) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isValueCompatibleWith(tprev.values);
    }

    @Override
    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        return compareMaps(keys, values, o1, o2);
    }

    public static int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, ByteBuffer o1, ByteBuffer o2)
    {
         if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        ProtocolVersion protocolVersion = ProtocolVersion.V3;
        int size1 = CollectionSerializer.readCollectionSize(bb1, protocolVersion);
        int size2 = CollectionSerializer.readCollectionSize(bb2, protocolVersion);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            ByteBuffer k1 = CollectionSerializer.readValue(bb1, protocolVersion);
            ByteBuffer k2 = CollectionSerializer.readValue(bb2, protocolVersion);
            int cmp = keysComparator.compare(k1, k2);
            if (cmp != 0)
                return cmp;

            ByteBuffer v1 = CollectionSerializer.readValue(bb1, protocolVersion);
            ByteBuffer v2 = CollectionSerializer.readValue(bb2, protocolVersion);
            cmp = valuesComparator.compare(v1, v2);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    @Override
    public MapSerializer<K, V> getSerializer()
    {
        return serializer;
    }

    @Override
    protected int collectionSize(List<ByteBuffer> values)
    {
        return values.size() / 2;
    }

    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(keys, values), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
        {
            Cell c = cells.next();
            bbs.add(c.path().get(0));
            bbs.add(c.value());
        }
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<Object, Object> map = (Map<Object, Object>) parsed;
        Map<Term, Term> terms = new HashMap<>(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet())
        {
            if (entry.getKey() == null)
                throw new MarshalException("Invalid null key in map");

            if (entry.getValue() == null)
                throw new MarshalException("Invalid null value in map");

            terms.put(keys.fromJSONObject(entry.getKey()), values.fromJSONObject(entry.getValue()));
        }
        return new Maps.DelayedValue(keys, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("{");
        int size = CollectionSerializer.readCollectionSize(value, protocolVersion);
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            // map keys must be JSON strings, so convert non-string keys to strings
            String key = keys.toJSONString(CollectionSerializer.readValue(value, protocolVersion), protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(Json.quoteAsJsonString(key)).append('"');

            sb.append(": ");
            sb.append(values.toJSONString(CollectionSerializer.readValue(value, protocolVersion), protocolVersion));
        }
        return sb.append("}").toString();
    }
}
