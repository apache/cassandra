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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * The abstract validator that is the base for maps, sets and lists (both frozen and non-frozen).
 *
 * Please note that this comparator shouldn't be used "manually" (as a custom
 * type for instance).
 */
public abstract class CollectionType<T> extends AbstractType<T>
{
    public static CellPath.Serializer cellPathSerializer = new CollectionPathSerializer();

    public enum Kind
    {
        MAP
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            }
        },
        SET
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Sets.valueSpecOf(collection);
            }
        },
        LIST
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Lists.valueSpecOf(collection);
            }
        };

        public abstract ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey);
    }

    public final Kind kind;

    protected CollectionType(ComparisonType comparisonType, Kind kind)
    {
        super(comparisonType);
        this.kind = kind;
    }

    public abstract AbstractType<?> nameComparator();
    public abstract AbstractType<?> valueComparator();

    protected abstract List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells);

    @Override
    public abstract CollectionSerializer<T> getSerializer();

    public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
    {
        return kind.makeCollectionReceiver(collection, isKey);
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return BytesType.instance.getString(value, accessor);
    }

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    public boolean isCollection()
    {
        return true;
    }

    @Override
    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.isEmpty(value))
            throw new MarshalException("Not enough bytes to read a " + kind.name().toLowerCase());
        super.validate(value, accessor);
    }

    @Override
    public <V> void validateCellValue(V cellValue, ValueAccessor<V> accessor) throws MarshalException
    {
        if (isMultiCell())
            valueComparator().validateCellValue(cellValue, accessor);
        else
            super.validateCellValue(cellValue, accessor);
    }

    /**
     * Checks if this collection is Map.
     * @return <code>true</code> if this collection is a Map, <code>false</code> otherwise.
     */
    public boolean isMap()
    {
        return kind == Kind.MAP;
    }

    @Override
    public boolean isFreezable()
    {
        return true;
    }

    // Overrided by maps
    protected int collectionSize(List<ByteBuffer> values)
    {
        return values.size();
    }

    public ByteBuffer serializeForNativeProtocol(Iterator<Cell<?>> cells)
    {
        assert isMultiCell();
        List<ByteBuffer> values = serializedValues(cells);
        int size = collectionSize(values);
        return CollectionSerializer.pack(values, ByteBufferAccessor.instance, size);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        CollectionType<?> tprev = (CollectionType<?>) previous;
        if (this.isMultiCell() != tprev.isMultiCell())
            return false;

        // subclasses should handle compatibility checks for frozen collections
        if (!this.isMultiCell())
            return isCompatibleWithFrozen(tprev);

        if (!this.nameComparator().isCompatibleWith(tprev.nameComparator()))
            return false;

        // the value comparator is only used for Cell values, so sorting doesn't matter
        return this.valueComparator().isSerializationCompatibleWith(tprev.valueComparator());
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        // for multi-cell collections, compatibility and value-compatibility are the same
        if (this.isMultiCell())
            return isCompatibleWith(previous);

        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        CollectionType<?> tprev = (CollectionType<?>) previous;
        if (this.isMultiCell() != tprev.isMultiCell())
            return false;

        // subclasses should handle compatibility checks for frozen collections
        return isValueCompatibleWithFrozen(tprev);
    }

    @Override
    public boolean isSerializationCompatibleWith(AbstractType<?> previous)
    {
        if (!isValueCompatibleWith(previous))
            return false;

        return valueComparator().isSerializationCompatibleWith(((CollectionType<?>)previous).valueComparator());
    }

    /** A version of isCompatibleWith() to deal with non-multicell (frozen) collections */
    protected abstract boolean isCompatibleWithFrozen(CollectionType<?> previous);

    /** A version of isValueCompatibleWith() to deal with non-multicell (frozen) collections */
    protected abstract boolean isValueCompatibleWithFrozen(CollectionType<?> previous);

    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Collection(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CollectionType))
            return false;

        CollectionType<?> other = (CollectionType<?>) o;

        if (kind != other.kind)
            return false;

        if (isMultiCell() != other.isMultiCell())
            return false;

        return nameComparator().equals(other.nameComparator()) && valueComparator().equals(other.valueComparator());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, isMultiCell(), nameComparator(), valueComparator());
    }

    @Override
    public String toString()
    {
        return this.toString(false);
    }

    static <VL, VR> int compareListOrSet(AbstractType<?> elementsComparator, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        // Note that this is only used if the collection is frozen
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int sizeL = CollectionSerializer.readCollectionSize(left, accessorL);
        int offsetL = CollectionSerializer.sizeOfCollectionSize();
        int sizeR = CollectionSerializer.readCollectionSize(right, accessorR);
        int offsetR = TypeSizes.INT_SIZE;

        for (int i = 0; i < Math.min(sizeL, sizeR); i++)
        {
            VL v1 = CollectionSerializer.readValue(left, accessorL, offsetL);
            offsetL += CollectionSerializer.sizeOfValue(v1, accessorL);
            VR v2 = CollectionSerializer.readValue(right, accessorR, offsetR);
            offsetR += CollectionSerializer.sizeOfValue(v2, accessorR);
            int cmp = elementsComparator.compare(v1, accessorL, v2, accessorR);
            if (cmp != 0)
                return cmp;
        }

        return Integer.compare(sizeL, sizeR);
    }

    static <V> ByteSource asComparableBytesListOrSet(AbstractType<?> elementsComparator,
                                                     ValueAccessor<V> accessor,
                                                     V data,
                                                     ByteComparable.Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        int offset = 0;
        int size = CollectionSerializer.readCollectionSize(data, accessor);
        offset += CollectionSerializer.sizeOfCollectionSize();
        ByteSource[] srcs = new ByteSource[size];
        for (int i = 0; i < size; ++i)
        {
            V v = CollectionSerializer.readValue(data, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(v, accessor);
            srcs[i] = elementsComparator.asComparableBytes(accessor, v, version);
        }
        return ByteSource.withTerminatorMaybeLegacy(version, 0x00, srcs);
    }

    static <V> V fromComparableBytesListOrSet(ValueAccessor<V> accessor,
                                              ByteSource.Peekable comparableBytes,
                                              ByteComparable.Version version,
                                              AbstractType<?> elementType)
    {
        if (comparableBytes == null)
            return accessor.empty();
        assert version != ByteComparable.Version.LEGACY; // legacy translation is not reversible

        List<V> buffers = new ArrayList<>();
        int separator = comparableBytes.next();
        while (separator != ByteSource.TERMINATOR)
        {
            if (!ByteSourceInverse.nextComponentNull(separator))
                buffers.add(elementType.fromComparableBytes(accessor, comparableBytes, version));
            else
                buffers.add(null);
            separator = comparableBytes.next();
        }
        return CollectionSerializer.pack(buffers, accessor, buffers.size());
    }

    public static String setOrListToJsonString(ByteBuffer buffer, AbstractType<?> elementsType, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("[");
        int size = CollectionSerializer.readCollectionSize(value, ByteBufferAccessor.instance);
        int offset = CollectionSerializer.sizeOfCollectionSize();
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");
            ByteBuffer element = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(element, ByteBufferAccessor.instance);
            sb.append(elementsType.toJSONString(element, protocolVersion));
        }
        return sb.append("]").toString();
    }

    private static class CollectionPathSerializer implements CellPath.Serializer
    {
        public void serialize(CellPath path, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(path.get(0), out);
        }

        public CellPath deserialize(DataInputPlus in) throws IOException
        {
            return CellPath.create(ByteBufferUtil.readWithVIntLength(in));
        }

        public long serializedSize(CellPath path)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(path.get(0));
        }

        public void skip(DataInputPlus in) throws IOException
        {
            ByteBufferUtil.skipWithVIntLength(in);
        }
    }

    public int size(ByteBuffer buffer)
    {
        return CollectionSerializer.readCollectionSize(buffer.duplicate(), ByteBufferAccessor.instance);
    }

    public abstract void forEach(ByteBuffer input, Consumer<ByteBuffer> action);
}
