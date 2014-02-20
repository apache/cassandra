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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.PoolAllocator;

public class CompoundSparseCellName extends CompoundComposite implements CellName
{
    private static final ByteBuffer[] EMPTY_PREFIX = new ByteBuffer[0];

    private static final long HEAP_SIZE = ObjectSizes.measure(new CompoundSparseCellName(null, false));

    protected final ColumnIdentifier columnName;

    // Not meant to be used directly, you should use the CellNameType method instead
    CompoundSparseCellName(ColumnIdentifier columnName, boolean isStatic)
    {
        this(EMPTY_PREFIX, columnName, isStatic);
    }

    CompoundSparseCellName(ByteBuffer[] elements, ColumnIdentifier columnName, boolean isStatic)
    {
        this(elements, elements.length, columnName, isStatic);
    }

    CompoundSparseCellName(ByteBuffer[] elements, int size, ColumnIdentifier columnName, boolean isStatic)
    {
        super(elements, size, isStatic);
        this.columnName = columnName;
    }

    public int size()
    {
        return size + 1;
    }

    public ByteBuffer get(int i)
    {
        return i == size ? columnName.bytes : elements[i];
    }

    public int clusteringSize()
    {
        return size;
    }

    public ColumnIdentifier cql3ColumnName()
    {
        return columnName;
    }

    public ByteBuffer collectionElement()
    {
        return null;
    }

    public boolean isCollectionCell()
    {
        return false;
    }

    public boolean isSameCQL3RowAs(CellName other)
    {
        if (clusteringSize() != other.clusteringSize() || other.isStatic() != isStatic())
            return false;

        for (int i = 0; i < clusteringSize(); i++)
        {
            if (!elements[i].equals(other.get(i)))
                return false;
        }
        return true;
    }

    public CellName copy(AbstractAllocator allocator)
    {
        if (elements.length == 0)
            return this;

        // We don't copy columnName because it's interned in SparseCellNameType
        return new CompoundSparseCellName(elementsCopy(allocator), columnName, isStatic());
    }

    public static class WithCollection extends CompoundSparseCellName
    {
        private static final long HEAP_SIZE = ObjectSizes.measure(new WithCollection(null, ByteBufferUtil.EMPTY_BYTE_BUFFER, false));

        private final ByteBuffer collectionElement;

        WithCollection(ColumnIdentifier columnName, ByteBuffer collectionElement, boolean isStatic)
        {
            this(EMPTY_PREFIX, columnName, collectionElement, isStatic);
        }

        WithCollection(ByteBuffer[] elements, ColumnIdentifier columnName, ByteBuffer collectionElement, boolean isStatic)
        {
            this(elements, elements.length, columnName, collectionElement, isStatic);
        }

        WithCollection(ByteBuffer[] elements, int size, ColumnIdentifier columnName, ByteBuffer collectionElement, boolean isStatic)
        {
            super(elements, size, columnName, isStatic);
            this.collectionElement = collectionElement;
        }

        public int size()
        {
            return size + 2;
        }

        public ByteBuffer get(int i)
        {
            return i == size + 1 ? collectionElement : super.get(i);
        }

        @Override
        public ByteBuffer collectionElement()
        {
            return collectionElement;
        }

        @Override
        public boolean isCollectionCell()
        {
            return true;
        }

        @Override
        public CellName copy(AbstractAllocator allocator)
        {
            // We don't copy columnName because it's interned in SparseCellNameType
            return new CompoundSparseCellName.WithCollection(elements.length == 0 ? elements : elementsCopy(allocator), size, columnName, allocator.clone(collectionElement), isStatic());
        }

        @Override
        public long unsharedHeapSize()
        {
            return super.unsharedHeapSize() + ObjectSizes.sizeOnHeapOf(collectionElement);
        }

        @Override
        public long excessHeapSizeExcludingData()
        {
            return super.excessHeapSizeExcludingData() + ObjectSizes.sizeOnHeapExcludingData(collectionElement);
        }

        @Override
        public void free(PoolAllocator<?> allocator)
        {
            super.free(allocator);
            allocator.free(collectionElement);
        }
    }
}
