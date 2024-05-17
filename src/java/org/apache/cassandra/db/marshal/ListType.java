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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class ListType<T> extends CollectionType<List<T>>
{
    // interning instances
    private static final ConcurrentHashMap<AbstractType<?>, ListType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<AbstractType<?>, ListType> frozenInstances = new ConcurrentHashMap<>();

    private final AbstractType<T> elements;
    public final ListSerializer<T> serializer;

    public static ListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0).freeze(), true);
    }

    @SuppressWarnings("unchecked")
    public static <T> ListType<T> getInstance(AbstractType<T> elements, boolean isMultiCell)
    {
        return getInstance(isMultiCell ? instances : frozenInstances,
                           elements,
                           () -> new ListType<>(elements, isMultiCell));
    }

    private ListType(AbstractType<T> elements, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.LIST, isMultiCell, ImmutableList.of(elements));
        this.elements = elements;
        this.serializer = ListSerializer.getInstance(elements.getSerializer());
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListType<T> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        Preconditions.checkArgument(subTypes.size() == 1,
                                    "Invalid number of subTypes for ListType (got %s)", subTypes.size());
        return getInstance((AbstractType<T>) subTypes.get(0), isMultiCell);
    }

    public AbstractType<T> getElementsType()
    {
        return elements;
    }

    @Override
    public AbstractType<TimeUUID> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<T> valueComparator()
    {
        return elements;
    }

    public ListSerializer<T> getSerializer()
    {
        return serializer;
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return compareListOrSet(elements, left, accessorL, right, accessorR);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, Version version)
    {
        return asComparableBytesListOrSet(getElementsType(), accessor, data, version);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, Version version)
    {
        return fromComparableBytesListOrSet(accessor, comparableBytes, version, getElementsType());
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(subTypes, ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
            bbs.add(cells.next().buffer());
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
                    "Expected a list, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;
        List<Term> terms = new ArrayList<>(list.size());
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in list");
            terms.add(elements.fromJSONObject(element));
        }

        return new Lists.DelayedValue(terms);
    }

    public ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to)
    {
        // We don't support slicing on lists so we don't need that function
        throw new UnsupportedOperationException();
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return setOrListToJsonString(buffer, elements, protocolVersion);
    }

    @Override
    public void forEach(ByteBuffer input, Consumer<ByteBuffer> action)
    {
        serializer.forEach(input, action);
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return decompose(Collections.emptyList());
    }
}
