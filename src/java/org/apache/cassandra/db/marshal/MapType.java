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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.Pair;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new ConcurrentHashMap<>();

    private final MapSerializer<K, V> serializer;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1), true);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        return getInstance(isMultiCell ? instances : frozenInstances,
                           Pair.create(keys, values),
                           () -> new MapType<>(keys, values, isMultiCell));
    }

    @Override
    public MapType<K,V> overrideKeyspace(Function<String, String> overrideKeyspace)
    {
        AbstractType<K> oldKeyType = getKeysType();
        AbstractType<V> oldValueType = getValuesType();

        AbstractType<K> newKeyType = oldKeyType.overrideKeyspace(overrideKeyspace);
        AbstractType<V> newValueType = oldValueType.overrideKeyspace(overrideKeyspace);

        if (newKeyType == oldKeyType && newValueType == oldValueType)
            return this;

        return getInstance(newKeyType, newValueType, isMultiCell());
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        super(Kind.MAP, ImmutableList.of(keys, values), isMultiCell);
        this.serializer = MapSerializer.getInstance(keys.getSerializer(),
                                                    values.getSerializer(),
                                                    keys.comparatorSet);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MapType<K, V> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        Preconditions.checkArgument(subTypes.size() == 2,
                                    "Invalid number of subTypes for MapType (got %s)", subTypes.size());
        return getInstance((AbstractType<K>)subTypes.get(0), (AbstractType<V>)subTypes.get(1), isMultiCell);
    }

    @Override
    public MapType<?,?> withUpdatedUserType(UserType udt)
    {
        // If our subtypes do contain the UDT and this is requested, there is a fair chance we won't be re-using an
        // instance with our own exact subtypes, so clean up the interned instance.
        if (referencesUserType(udt.name))
            (isMultiCell() ? instances : frozenInstances).remove(Pair.create(getKeysType(), getValuesType()));

        return (MapType<?, ?>) super.withUpdatedUserType(udt);
    }

    @Override
    public boolean referencesDuration()
    {
        // Maps cannot be created with duration as keys
        return getValuesType().referencesDuration();
    }

    @SuppressWarnings("unchecked")
    public AbstractType<K> getKeysType()
    {
        return (AbstractType<K>) subTypes.get(0);
    }

    @SuppressWarnings("unchecked")
    public AbstractType<V> getValuesType()
    {
        return (AbstractType<V>) subTypes.get(1);
    }

    @Override
    public AbstractType<K> nameComparator()
    {
        return getKeysType();
    }

    @Override
    public AbstractType<V> valueComparator()
    {
        return getValuesType();
    }

    @Override
    public <RL, TR> int compareCustom(RL left, ValueAccessor<RL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        return compareMaps(getKeysType(), getValuesType(), left, accessorL, right, accessorR);
    }

    public static <TL, TR> int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, TL left, ValueAccessor<TL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));


        ProtocolVersion protocolVersion = ProtocolVersion.V3;
        int sizeL = CollectionSerializer.readCollectionSize(left, accessorL, protocolVersion);
        int sizeR = CollectionSerializer.readCollectionSize(right, accessorR, protocolVersion);

        int offsetL = CollectionSerializer.sizeOfCollectionSize(sizeL, protocolVersion);
        int offsetR = CollectionSerializer.sizeOfCollectionSize(sizeR, protocolVersion);

        for (int i = 0; i < Math.min(sizeL, sizeR); i++)
        {
            TL k1 = CollectionSerializer.readValue(left, accessorL, offsetL, protocolVersion);
            offsetL += CollectionSerializer.sizeOfValue(k1, accessorL, protocolVersion);
            TR k2 = CollectionSerializer.readValue(right, accessorR, offsetR, protocolVersion);
            offsetR += CollectionSerializer.sizeOfValue(k2, accessorR, protocolVersion);
            int cmp = keysComparator.compare(k1, accessorL, k2, accessorR);
            if (cmp != 0)
                return cmp;

            TL v1 = CollectionSerializer.readValue(left, accessorL, offsetL, protocolVersion);
            offsetL += CollectionSerializer.sizeOfValue(v1, accessorL, protocolVersion);
            TR v2 = CollectionSerializer.readValue(right, accessorR, offsetR, protocolVersion);
            offsetR += CollectionSerializer.sizeOfValue(v2, accessorR, protocolVersion);
            cmp = valuesComparator.compare(v1, accessorL, v2, accessorR);
            if (cmp != 0)
                return cmp;
        }

        return sizeL == sizeR ? 0 : (sizeL < sizeR ? -1 : 1);
    }

    @Override
    public <T> ByteSource asComparableBytes(ValueAccessor<T> accessor, T data, Version version)
    {
        return asComparableBytesMap(getKeysType(), getValuesType(), accessor, data, version);
    }

    @Override
    public <T> T fromComparableBytes(ValueAccessor<T> accessor, ByteSource.Peekable comparableBytes, Version version)
    {
        return fromComparableBytesMap(accessor, comparableBytes, version, getKeysType(), getValuesType());
    }

    static <V> ByteSource asComparableBytesMap(AbstractType<?> keysComparator,
                                               AbstractType<?> valuesComparator,
                                               ValueAccessor<V> accessor,
                                               V data,
                                               Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        ProtocolVersion protocolVersion = ProtocolVersion.V3;
        int offset = 0;
        int size = CollectionSerializer.readCollectionSize(data, accessor, protocolVersion);
        offset += CollectionSerializer.sizeOfCollectionSize(size, protocolVersion);
        ByteSource[] srcs = new ByteSource[size * 2];
        for (int i = 0; i < size; ++i)
        {
            V k = CollectionSerializer.readValue(data, accessor, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(k, accessor, protocolVersion);
            srcs[i * 2 + 0] = keysComparator.asComparableBytes(accessor, k, version);
            V v = CollectionSerializer.readValue(data, accessor, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(v, accessor, protocolVersion);
            srcs[i * 2 + 1] = valuesComparator.asComparableBytes(accessor, v, version);
        }
        return ByteSource.withTerminator(version == Version.LEGACY ? 0x00 : ByteSource.TERMINATOR, srcs);
    }

    static <V> V fromComparableBytesMap(ValueAccessor<V> accessor,
                                        ByteSource.Peekable comparableBytes,
                                        Version version,
                                        AbstractType<?> keysComparator,
                                        AbstractType<?> valuesComparator)
    {
        if (comparableBytes == null)
            return accessor.empty();

        List<V> buffers = new ArrayList<>();
        int terminator = version == Version.LEGACY
                         ? 0x00
                         : ByteSource.TERMINATOR;
        int separator = comparableBytes.next();
        while (separator != terminator)
        {
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : keysComparator.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : valuesComparator.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
        }
        return CollectionSerializer.pack(buffers, accessor,buffers.size() / 2, ProtocolVersion.V3);
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

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append('(');
        sb.append(getClass().getName())
          .append(TypeParser.stringifyTypeParameters(subTypes, ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell();
        List<ByteBuffer> bbs = new ArrayList<>();
        while (cells.hasNext())
        {
            Cell<?> c = cells.next();
            bbs.add(c.path().get(0));
            bbs.add(c.buffer());
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

        Map<?, ?> map = (Map<?, ?>) parsed;
        Map<Term, Term> terms = new HashMap<>(map.size());
        AbstractType<K> keys = getKeysType();
        AbstractType<V> values = getValuesType();
        for (Map.Entry<?, ?> entry : map.entrySet())
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
        int size = CollectionSerializer.readCollectionSize(value, ByteBufferAccessor.instance, protocolVersion);
        int offset = CollectionSerializer.sizeOfCollectionSize(size, protocolVersion);
        AbstractType<K> keys = getKeysType();
        AbstractType<V> values = getValuesType();
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            // map keys must be JSON strings, so convert non-string keys to strings
            ByteBuffer kv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(kv, ByteBufferAccessor.instance, protocolVersion);
            String key = keys.toJSONString(kv, protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(Json.quoteAsJsonString(key)).append('"');

            sb.append(": ");
            ByteBuffer vv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(vv, ByteBufferAccessor.instance, protocolVersion);
            sb.append(values.toJSONString(vv, protocolVersion));
        }
        return sb.append('}').toString();
    }
}
