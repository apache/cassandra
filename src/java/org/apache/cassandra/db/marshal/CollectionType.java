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
import java.util.List;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The abstract validator that is the base for maps, sets and lists (both frozen and non-frozen).
 * <p>
 * Please note that this comparator shouldn't be used "manually" (as a custom type for instance).
 */
public abstract class CollectionType<T> extends MultiCellCapableType<T>
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

    protected CollectionType(Kind kind, ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        super(subTypes, isMultiCell);
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

    // Overrided by maps
    protected int collectionSize(List<ByteBuffer> values)
    {
        return values.size();
    }

    public ByteBuffer serializeForNativeProtocol(Iterator<Cell<?>> cells, ProtocolVersion version)
    {
        assert isMultiCell();
        List<ByteBuffer> values = serializedValues(cells);
        int size = collectionSize(values);
        return CollectionSerializer.pack(values, ByteBufferAccessor.instance, size, version);
    }

    @Override
    protected boolean isCompatibleWhenFrozenWith(AbstractType<?> previous)
    {
        // When frozen, the full collection is a blob, so everything must be sorted-compatible for the whole blob to
        // be sorted-compatible. Note that for lists and sets, the first condition will always be true (as their
        // nameComparator() is hard-coded), but this method is not so performance sensitive that it's worth bothering.
        CollectionType<?> prev = (CollectionType<?>)previous;
        return nameComparator().isCompatibleWith(prev.nameComparator())
               && valueComparator().isCompatibleWith(prev.valueComparator());
    }

    @Override
    protected boolean isCompatibleWhenNonFrozenWith(AbstractType<?> previous)
    {
        // When multi-cell, the name comparator is the one used to compare cell-path so must be sorted-compatible
        // (same remarks than in isCompatibleWhenFrozenWith for lists and sets), but the value comparator is never used
        // for sorting so value-compatibility is enough.
        CollectionType<?> prev = (CollectionType<?>)previous;
        return nameComparator().isCompatibleWith(prev.nameComparator())
               && valueComparator().isValueCompatibleWith(prev.valueComparator());
    }

    @Override
    protected boolean isValueCompatibleWhenFrozenWith(AbstractType<?> previous)
    {
        // When frozen, the full collection is a blob, so value-compatibility is all we care for everything.
        CollectionType<?> prev = (CollectionType<?>)previous;
        return nameComparator().isValueCompatibleWith(prev.nameComparator())
               && valueComparator().isValueCompatibleWith(prev.valueComparator());
    }

    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Collection(this);
    }

    @Override
    protected boolean equalsNoFrozenNoSubtypes(AbstractType<?> that)
    {
        return kind == ((CollectionType<?>)that).kind;
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
}
