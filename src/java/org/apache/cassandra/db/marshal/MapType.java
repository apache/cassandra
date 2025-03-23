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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.terms.MultiElements;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.Pair;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new ConcurrentHashMap<>();

    private final AbstractType<K> keys;
    private final AbstractType<V> values;
    private final MapSerializer<K, V> serializer;
    private final boolean isMultiCell;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0).freeze(), l.get(1).freeze(), true);
    }

    public static <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> internMap = isMultiCell ? instances : frozenInstances;
        Pair<AbstractType<?>, AbstractType<?>> p = Pair.create(keys, values);
        MapType<K, V> t = internMap.get(p);
        return null == t
             ? internMap.computeIfAbsent(p, k -> new MapType<>(k.left, k.right, isMultiCell))
             : t;
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.MAP);
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(),
                                                    values.getSerializer(),
                                                    keys.comparatorSet);
        this.isMultiCell = isMultiCell;
    }

    @Override
    public <T> boolean referencesUserType(T name, ValueAccessor<T> accessor)
    {
        return keys.referencesUserType(name, accessor) || values.referencesUserType(name, accessor);
    }

    @Override
    public MapType<?,?> withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        (isMultiCell ? instances : frozenInstances).remove(Pair.create(keys, values));

        return getInstance(keys.withUpdatedUserType(udt), values.withUpdatedUserType(udt), isMultiCell);
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return getInstance(keys.expandUserTypes(), values.expandUserTypes(), isMultiCell);
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
    public List<AbstractType<?>> subTypes()
    {
        return Arrays.asList(keys, values);
    }

    @Override
    public AbstractType<?> freeze()
    {
        // freeze key/value to match org.apache.cassandra.cql3.CQL3Type.Raw.RawCollection.freeze
        return isMultiCell ? getInstance(this.keys.freeze(), this.values.freeze(), false) : this;
    }

    @Override
    public AbstractType<?> unfreeze()
    {
        return isMultiCell ? this : getInstance(this.keys, this.values, true);
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
        MapType<?, ?> tprev = (MapType<?, ?>) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isCompatibleWith(tprev.values);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType<?, ?> tprev = (MapType<?, ?>) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isValueCompatibleWith(tprev.values);
    }

    public <RL, TR> int compareCustom(RL left, ValueAccessor<RL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        return compareMaps(keys, values, left, accessorL, right, accessorR);
    }

    public static <TL, TR> int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, TL left, ValueAccessor<TL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));


        int sizeL = CollectionSerializer.readCollectionSize(left, accessorL);
        int sizeR = CollectionSerializer.readCollectionSize(right, accessorR);

        int offsetL = CollectionSerializer.sizeOfCollectionSize();
        int offsetR = CollectionSerializer.sizeOfCollectionSize();

        for (int i = 0; i < Math.min(sizeL, sizeR); i++)
        {
            TL k1 = CollectionSerializer.readValue(left, accessorL, offsetL);
            offsetL += CollectionSerializer.sizeOfValue(k1, accessorL);
            TR k2 = CollectionSerializer.readValue(right, accessorR, offsetR);
            offsetR += CollectionSerializer.sizeOfValue(k2, accessorR);
            int cmp = keysComparator.compare(k1, accessorL, k2, accessorR);
            if (cmp != 0)
                return cmp;

            TL v1 = CollectionSerializer.readValue(left, accessorL, offsetL);
            offsetL += CollectionSerializer.sizeOfValue(v1, accessorL);
            TR v2 = CollectionSerializer.readValue(right, accessorR, offsetR);
            offsetR += CollectionSerializer.sizeOfValue(v2, accessorR);
            cmp = valuesComparator.compare(v1, accessorL, v2, accessorR);
            if (cmp != 0)
                return cmp;
        }

        return Integer.compare(sizeL, sizeR);
    }

    @Override
    public <T> ByteSource asComparableBytes(ValueAccessor<T> accessor, T data, Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        int offset = 0;
        int size = CollectionSerializer.readCollectionSize(data, accessor);
        offset += CollectionSerializer.sizeOfCollectionSize();
        ByteSource[] srcs = new ByteSource[size * 2];
        for (int i = 0; i < size; ++i)
        {
            T k = CollectionSerializer.readValue(data, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(k, accessor);
            srcs[i * 2 + 0] = keys.asComparableBytes(accessor, k, version);
            T v = CollectionSerializer.readValue(data, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(v, accessor);
            srcs[i * 2 + 1] = values.asComparableBytes(accessor, v, version);
        }
        return ByteSource.withTerminatorMaybeLegacy(version, 0x00, srcs);
    }

    @Override
    public <T> T fromComparableBytes(ValueAccessor<T> accessor, ByteSource.Peekable comparableBytes, Version version)
    {
        if (comparableBytes == null)
            return accessor.empty();
        assert version != Version.LEGACY; // legacy translation is not reversible

        List<T> buffers = new ArrayList<>();
        int separator = comparableBytes.next();
        while (separator != ByteSource.TERMINATOR)
        {
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : keys.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : values.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
        }
        return getSerializer().pack(buffers, accessor);
    }

    @Override
    public MapSerializer<K, V> getSerializer()
    {
        return serializer;
    }

    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append('(');
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(keys, values), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
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
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<?, ?> map = (Map<?, ?>) parsed;
        List<Term> terms = new ArrayList<>(map.size() << 1);
        for (Map.Entry<?, ?> entry : map.entrySet())
        {
            if (entry.getKey() == null)
                throw new MarshalException("Invalid null key in map");

            if (entry.getValue() == null)
                throw new MarshalException("Invalid null value in map");

            terms.add(keys.fromJSONObject(entry.getKey()));
            terms.add(values.fromJSONObject(entry.getValue()));
        }
        return new MultiElements.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("{");
        int size = CollectionSerializer.readCollectionSize(value, ByteBufferAccessor.instance);
        int offset = CollectionSerializer.sizeOfCollectionSize();
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            // map keys must be JSON strings, so convert non-string keys to strings
            ByteBuffer kv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(kv, ByteBufferAccessor.instance);
            String key = keys.toJSONString(kv, protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(JsonUtils.quoteAsJsonString(key)).append('"');

            sb.append(": ");
            ByteBuffer vv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(vv, ByteBufferAccessor.instance);
            sb.append(values.toJSONString(vv, protocolVersion));
        }
        return sb.append("}").toString();
    }

    @Override
    public void forEach(ByteBuffer input, Consumer<ByteBuffer> action)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return decompose(Collections.emptyMap());
    }

    @Override
    public List<ByteBuffer> filterSortAndValidateElements(List<ByteBuffer> buffers)
    {
        // We depend on Maps to be properly sorted by their keys, so use a sorted map implementation here.
        SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>(getKeysType());
        Iterator<ByteBuffer> iter = buffers.iterator();
        while (iter.hasNext())
        {
            ByteBuffer keyBytes = iter.next();
            ByteBuffer valueBytes = iter.next();

            if (keyBytes == null || valueBytes == null)
                throw new MarshalException("null is not supported inside collections");

            getKeysType().validate(keyBytes);
            getValuesType().validate(valueBytes);

            map.put(keyBytes, valueBytes);
        }

        List<ByteBuffer> sortedBuffers = new ArrayList<>(map.size() << 1);
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
        {
            sortedBuffers.add(entry.getKey());
            sortedBuffers.add(entry.getValue());
        }
        return sortedBuffers;
    }

    protected int compareNextCell(Iterator<Cell<?>> cellIterator, Iterator<ByteBuffer> elementIter)
    {
        Cell<?> c = cellIterator.next();

        // compare the keys
        int comparison = getKeysType().compare(c.path().get(0), elementIter.next());
        if (comparison != 0)
            return comparison;

        // compare the values
        return getValuesType().compare(c.buffer(), elementIter.next());
    }

    @Override
    public boolean contains(ComplexColumnData columnData, ByteBuffer value)
    {
        Iterator<Cell<?>> iter = columnData.iterator();
        while(iter.hasNext())
        {
            ByteBuffer cellValue = iter.next().buffer();
            if(valueComparator().compare(cellValue, value) == 0)
                return true;
        }
        return false;
    }

    @Override
    public AbstractType<?> elementType(ByteBuffer keyOrIndex)
    {
        return getValuesType();
    }

    @Override
    public ByteBuffer getElement(@Nullable ColumnData columnData, ByteBuffer keyOrIndex)
    {
        if (columnData == null)
            return null;

        if (isMultiCell())
        {
            Cell<?> cell = ((ComplexColumnData) columnData).getCell(CellPath.create(keyOrIndex));
            return cell == null ? null : cell.buffer();
        }

        return getSerializer().getSerializedValue(((Cell<?>) columnData).buffer(), keyOrIndex, getValuesType());
    }
}
