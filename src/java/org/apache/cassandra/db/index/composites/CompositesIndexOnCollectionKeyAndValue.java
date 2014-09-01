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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on the element and value of cells participating in a collection.
 *
 * The row keys for this index are a composite of the collection element
 * and value of indexed columns.
 */
public class CompositesIndexOnCollectionKeyAndValue extends CompositesIndexIncludingCollectionKey
{
    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        CollectionType colType = (CollectionType)columnDef.type;
        return CompositeType.getInstance(colType.nameComparator(), colType.valueComparator());
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path)
    {
        return CompositeType.build(path.get(0), cellValue);
    }

    public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec)
    {
        ByteBuffer[] components = ((CompositeType)getIndexKeyComparator()).split(indexValue);
        ByteBuffer mapKey = components[0];
        ByteBuffer mapValue = components[1];

        Cell cell = data.getCell(columnDef, CellPath.create(mapKey));
        if (cell == null || !cell.isLive(nowInSec))
            return true;

        AbstractType<?> valueComparator = ((CollectionType)columnDef.type).valueComparator();
        return valueComparator.compare(mapValue, cell.value()) != 0;
    }
}
