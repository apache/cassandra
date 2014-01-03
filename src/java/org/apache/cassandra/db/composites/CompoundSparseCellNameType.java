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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompoundSparseCellNameType extends AbstractCompoundCellNameType
{
    private static final ColumnIdentifier rowMarkerId = new ColumnIdentifier(ByteBufferUtil.EMPTY_BYTE_BUFFER, UTF8Type.instance);
    private static final CellName rowMarkerNoPrefix = new CompoundSparseCellName(rowMarkerId);

    // For CQL3 columns, this is always UTF8Type. However, for compatibility with super columns, we need to allow it to be non-UTF8.
    private final AbstractType<?> columnNameType;
    protected final Map<ByteBuffer, ColumnIdentifier> internedIds;

    public CompoundSparseCellNameType(List<AbstractType<?>> types)
    {
        this(types, UTF8Type.instance);
    }

    public CompoundSparseCellNameType(List<AbstractType<?>> types, AbstractType<?> columnNameType)
    {
        this(new CompoundCType(types), columnNameType);
    }

    private CompoundSparseCellNameType(CompoundCType clusteringType, AbstractType<?> columnNameType)
    {
        this(clusteringType, columnNameType, makeCType(clusteringType, columnNameType, null), new HashMap<ByteBuffer, ColumnIdentifier>());
    }

    private CompoundSparseCellNameType(CompoundCType clusteringType, AbstractType<?> columnNameType, CompoundCType fullType, Map<ByteBuffer, ColumnIdentifier> internedIds)
    {
        super(clusteringType, fullType);
        this.columnNameType = columnNameType;
        this.internedIds = internedIds;
    }

    protected static CompoundCType makeCType(CompoundCType clusteringType, AbstractType<?> columnNameType, ColumnToCollectionType collectionType)
    {
        List<AbstractType<?>> allSubtypes = new ArrayList<AbstractType<?>>(clusteringType.size() + (collectionType == null ? 1 : 2));
        for (int i = 0; i < clusteringType.size(); i++)
            allSubtypes.add(clusteringType.subtype(i));
        allSubtypes.add(columnNameType);
        if (collectionType != null)
            allSubtypes.add(collectionType);
        return new CompoundCType(allSubtypes);
    }

    public CellNameType setSubtype(int position, AbstractType<?> newType)
    {
        if (position < clusteringSize)
            return new CompoundSparseCellNameType(clusteringType.setSubtype(position, newType), columnNameType, fullType.setSubtype(position, newType), internedIds);

        if (position == clusteringSize)
            throw new IllegalArgumentException();

        throw new IndexOutOfBoundsException();
    }

    @Override
    public CellNameType addCollection(ColumnIdentifier columnName, CollectionType newCollection)
    {
        return new WithCollection(clusteringType, ColumnToCollectionType.getInstance(Collections.singletonMap(columnName.bytes, newCollection)), internedIds);
    }

    public boolean isDense()
    {
        return false;
    }

    public boolean supportCollections()
    {
        return true;
    }

    public CellName create(Composite prefix, ColumnIdentifier columnName)
    {
        assert prefix.size() == clusteringSize;

        if (prefix.isEmpty())
            return new CompoundSparseCellName(columnName);

        assert prefix instanceof CompoundComposite;
        CompoundComposite lc = (CompoundComposite)prefix;
        return new CompoundSparseCellName(lc.elements, clusteringSize, columnName);
    }

    public CellName rowMarker(Composite prefix)
    {
        if (prefix.isEmpty())
            return rowMarkerNoPrefix;

        return create(prefix, rowMarkerId);
    }

    protected ColumnIdentifier idFor(ByteBuffer bb)
    {
        ColumnIdentifier id = internedIds.get(bb);
        return id == null ? new ColumnIdentifier(bb, columnNameType) : id;
    }

    protected Composite makeWith(ByteBuffer[] components, int size, Composite.EOC eoc)
    {
        if (size < clusteringSize + 1 || eoc != Composite.EOC.NONE)
            return new CompoundComposite(components, size).withEOC(eoc);

        return new CompoundSparseCellName(components, clusteringSize, idFor(components[clusteringSize]));
    }

    protected Composite copyAndMakeWith(ByteBuffer[] components, int size, Composite.EOC eoc)
    {
        if (size < clusteringSize + 1 || eoc != Composite.EOC.NONE)
            return new CompoundComposite(Arrays.copyOfRange(components, 0, size), size).withEOC(eoc);

        ByteBuffer[] clusteringColumns = Arrays.copyOfRange(components, 0, clusteringSize);
        return new CompoundSparseCellName(clusteringColumns, idFor(components[clusteringSize]));
    }

    public void addCQL3Column(ColumnIdentifier id)
    {
        internedIds.put(id.bytes, id);
    }

    public void removeCQL3Column(ColumnIdentifier id)
    {
        internedIds.remove(id.bytes);
    }

    public CQL3Row.Builder CQL3RowBuilder(long now)
    {
        return makeSparseCQL3RowBuilder(now);
    }

    public static class WithCollection extends CompoundSparseCellNameType
    {
        private final ColumnToCollectionType collectionType;

        public WithCollection(List<AbstractType<?>> types, ColumnToCollectionType collectionType)
        {
            this(new CompoundCType(types), collectionType);
        }

        WithCollection(CompoundCType clusteringType, ColumnToCollectionType collectionType)
        {
            this(clusteringType, collectionType, new HashMap<ByteBuffer, ColumnIdentifier>());
        }

        private WithCollection(CompoundCType clusteringType, ColumnToCollectionType collectionType, Map<ByteBuffer, ColumnIdentifier> internedIds)
        {
            this(clusteringType, makeCType(clusteringType, UTF8Type.instance, collectionType), collectionType, internedIds);
        }

        private WithCollection(CompoundCType clusteringType, CompoundCType fullCType, ColumnToCollectionType collectionType, Map<ByteBuffer, ColumnIdentifier> internedIds)
        {
            super(clusteringType, UTF8Type.instance, fullCType, internedIds);
            this.collectionType = collectionType;
        }

        @Override
        public CellNameType setSubtype(int position, AbstractType<?> newType)
        {
            if (position < clusteringSize)
                return new WithCollection(clusteringType.setSubtype(position, newType), collectionType, internedIds);

            throw position >= fullType.size() ? new IndexOutOfBoundsException() : new IllegalArgumentException();
        }

        @Override
        public CellNameType addCollection(ColumnIdentifier columnName, CollectionType newCollection)
        {
            Map<ByteBuffer, CollectionType> newMap = new HashMap<>(collectionType.defined);
            newMap.put(columnName.bytes, newCollection);
            return new WithCollection(clusteringType, ColumnToCollectionType.getInstance(newMap), internedIds);
        }

        @Override
        public CellName create(Composite prefix, ColumnIdentifier columnName, ByteBuffer collectionElement)
        {
            // We ignore the columnName because it's just the COMPACT_VALUE name which is not store in the cell name
            assert prefix.size() == clusteringSize;

            if (prefix.isEmpty())
                return new CompoundSparseCellName.WithCollection(columnName, collectionElement);

            assert prefix instanceof CompoundComposite;
            CompoundComposite lc = (CompoundComposite)prefix;
            return new CompoundSparseCellName.WithCollection(lc.elements, clusteringSize, columnName, collectionElement);
        }

        @Override
        public boolean hasCollections()
        {
            return true;
        }

        @Override
        public ColumnToCollectionType collectionType()
        {
            return collectionType;
        }

        @Override
        protected Composite makeWith(ByteBuffer[] components, int size, Composite.EOC eoc)
        {
            if (size < fullSize)
                return super.makeWith(components, size, eoc);

            return new CompoundSparseCellName.WithCollection(components, clusteringSize, idFor(components[clusteringSize]), components[fullSize - 1]);
        }

        protected Composite copyAndMakeWith(ByteBuffer[] components, int size, Composite.EOC eoc)
        {
            if (size < fullSize)
                return super.copyAndMakeWith(components, size, eoc);

            ByteBuffer[] clusteringColumns = Arrays.copyOfRange(components, 0, clusteringSize);
            return new CompoundSparseCellName.WithCollection(clusteringColumns, idFor(components[clusteringSize]), components[clusteringSize + 1]);
        }
    }
}

