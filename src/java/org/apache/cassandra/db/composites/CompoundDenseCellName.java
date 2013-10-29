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
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ObjectSizes;

public class CompoundDenseCellName extends CompoundComposite implements CellName
{
    // Not meant to be used directly, you should use the CellNameType method instead
    CompoundDenseCellName(ByteBuffer[] elements)
    {
        super(elements, elements.length);
    }

    public int clusteringSize()
    {
        return size;
    }

    public ColumnIdentifier cql3ColumnName()
    {
        return null;
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
        // Dense cell imply one cell by CQL row so no other cell will be the same row.
        return equals(other);
    }

    @Override
    public long memorySize()
    {
        return ObjectSizes.getSuperClassFieldSize(super.memorySize());
    }

    public CellName copy(Allocator allocator)
    {
        return new CompoundDenseCellName(elementsCopy(allocator));
    }
}
