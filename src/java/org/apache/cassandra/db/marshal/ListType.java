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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class ListType<T> extends CollectionType<List<T>>
{
    // interning instances
    private static final ConcurrentHashMap<AbstractType<?>, ListType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<AbstractType<?>, ListType> frozenInstances = new ConcurrentHashMap<>();

    public final ListSerializer<T> serializer;

    public static ListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0), true);
    }

    @SuppressWarnings("unchecked")
    public static <T> ListType<T> getInstance(AbstractType<T> elements, boolean isMultiCell)
    {
        return getInstance(isMultiCell ? instances : frozenInstances,
                           elements,
                           () -> new ListType<>(elements, isMultiCell));
    }

    @Override
    public ListType<T> overrideKeyspace(Function<String, String> overrideKeyspace)
    {
        AbstractType<T> oldType = getElementsType();
        AbstractType<T> newType = oldType.overrideKeyspace(overrideKeyspace);

        if (newType == oldType)
            return this;

        return getInstance(newType, isMultiCell());
    }

    private ListType(AbstractType<T> elements, boolean isMultiCell)
    {
        super(Kind.LIST, ImmutableList.of(elements), isMultiCell);
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

    @Override
    public ListType<?> withUpdatedUserType(UserType udt)
    {
        // If our subtypes do contain the UDT and this is requested, there is a fair chance we won't be re-using an
        // instance with our own exact subtypes, so clean up the interned instance.
        if (referencesUserType(udt.name))
            (isMultiCell() ? instances : frozenInstances).remove(getElementsType());

        return (ListType<?>) super.withUpdatedUserType(udt);
    }

    @Override
    public boolean referencesDuration()
    {
        return getElementsType().referencesDuration();
    }

    @SuppressWarnings("unchecked")
    public AbstractType<T> getElementsType()
    {
        return (AbstractType<T>) subTypes.get(0);
    }

    public AbstractType<UUID> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<T> valueComparator()
    {
        return getElementsType();
    }

    public ListSerializer<T> getSerializer()
    {
        return serializer;
    }

    @Override
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return compareListOrSet(getElementsType(), left, accessorL, right, accessorR);
    }

    static <VL, VR> int compareListOrSet(AbstractType<?> elementsComparator, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        // Note that this is only used if the collection is frozen
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int sizeL = CollectionSerializer.readCollectionSize(left, accessorL, ProtocolVersion.V3);
        int offsetL = CollectionSerializer.sizeOfCollectionSize(sizeL, ProtocolVersion.V3);
        int sizeR = CollectionSerializer.readCollectionSize(right, accessorR, ProtocolVersion.V3);
        int offsetR = TypeSizes.INT_SIZE;

        for (int i = 0; i < Math.min(sizeL, sizeR); i++)
        {
            VL v1 = CollectionSerializer.readValue(left, accessorL, offsetL, ProtocolVersion.V3);
            offsetL += CollectionSerializer.sizeOfValue(v1, accessorL, ProtocolVersion.V3);
            VR v2 = CollectionSerializer.readValue(right, accessorR, offsetR, ProtocolVersion.V3);
            offsetR += CollectionSerializer.sizeOfValue(v2, accessorR, ProtocolVersion.V3);
            int cmp = elementsComparator.compare(v1, accessorL, v2, accessorR);
            if (cmp != 0)
                return cmp;
        }

        return sizeL == sizeR ? 0 : (sizeL < sizeR ? -1 : 1);
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

    static <V> ByteSource asComparableBytesListOrSet(AbstractType<?> elementsComparator,
                                                     ValueAccessor<V> accessor,
                                                     V data,
                                                     Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        int offset = 0;
        int size = CollectionSerializer.readCollectionSize(data, accessor, ProtocolVersion.V3);
        offset += CollectionSerializer.sizeOfCollectionSize(size, ProtocolVersion.V3);
        ByteSource[] srcs = new ByteSource[size];
        for (int i = 0; i < size; ++i)
        {
            V v = CollectionSerializer.readValue(data, accessor, offset, ProtocolVersion.V3);
            offset += CollectionSerializer.sizeOfValue(v, accessor, ProtocolVersion.V3);
            srcs[i] = elementsComparator.asComparableBytes(accessor, v, version);
        }
        return ByteSource.withTerminator(version == Version.LEGACY ? 0x00 : ByteSource.TERMINATOR, srcs);
    }

    static <V> V fromComparableBytesListOrSet(ValueAccessor<V> accessor,
                                              ByteSource.Peekable comparableBytes,
                                              Version version,
                                              AbstractType<?> elementType)
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
            if (!ByteSourceInverse.nextComponentNull(separator))
                buffers.add(elementType.fromComparableBytes(accessor, comparableBytes, version));
            else
                buffers.add(null);
            separator = comparableBytes.next();
        }
        return CollectionSerializer.pack(buffers, accessor, buffers.size(), ProtocolVersion.V3);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append('(');
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(subTypes, ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell();
        List<ByteBuffer> bbs = new ArrayList<>();
        while (cells.hasNext())
            bbs.add(cells.next().buffer());
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
                    "Expected a list, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;
        List<Term> terms = new ArrayList<>(list.size());
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in list");
            terms.add(getElementsType().fromJSONObject(element));
        }

        return new Lists.DelayedValue(terms);
    }

    public static String setOrListToJsonString(ByteBuffer buffer, AbstractType<?> elementsType, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("[");
        int size = CollectionSerializer.readCollectionSize(value, ByteBufferAccessor.instance, protocolVersion);
        int offset = CollectionSerializer.sizeOfCollectionSize(size, protocolVersion);
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");
            ByteBuffer element = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance, protocolVersion);
            sb.append(elementsType.toJSONString(element, protocolVersion));
        }
        return sb.append(']').toString();
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return setOrListToJsonString(buffer, getElementsType(), protocolVersion);
    }
}
